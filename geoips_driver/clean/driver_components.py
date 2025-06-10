"""Generic objects used to drive GeoIPS in a observer-spawner -based fashion.

These objects provide the utility to listen for a single file or a set of files in one
or more locations. Once the required files have arrived, we create job scripts from
jinja2 templates that spawn GeoIPS processing based on the provided inputs.
"""

from datetime import datetime, timedelta, timezone
from glob import glob
import logging
from multiprocessing import Pool
import os
from pathlib import Path
import subprocess
import time
from types import SimpleNamespace

import bz2
from jinja2 import Environment, FileSystemLoader
from jinja2.environment import Template
from pyhdf.SD import SD, SDC
import xarray

LOG = logging.getLogger(__name__)


class DriverUtilities:
    """Generic class containing utility functions that can help drive GeoIPS.

    Methods attached to this class have been put here as they are called commonly when
    driving GeoIPS, but don't fit within any of the other classes.
    """

    def dict_to_namespace(self, iter) -> SimpleNamespace:
        """Recursively transform an iterable into a namespace.

        If a dictionary, run this again on each if its values.
        If a list, run this function on each of its items.
        Otherwise return the value provided.

        Parameters
        ----------
        iter: iterable or value
            - If an iterable, run this function on each of its items. Otherwise return
              this value
        """
        if isinstance(iter, dict):
            return SimpleNamespace(
                **{key: self.dict_to_namespace(val) for key, val in iter.items()}
            )
        elif isinstance(iter, list):
            return [self.dict_to_namespace(item) for item in iter]
        else:
            return iter


class DateUtilities:
    """Object containing datetime methods used for converting and calculating time.

    Useful for determining what files you should be looking for in NRT processing, as
    well as converting from julian date to calendar date, and more.
    """

    def nearest_half_hour_utc(self) -> str:
        """Return the nearest half hour increment in UTC time.

        Formatted: (str) hhnn. I.e. '2030' or '0100', ...
        """
        now = datetime.now(timezone.utc)
        minute = now.minute
        if minute < 25:
            # Round down to the previous hour
            result = now.replace(minute=0, second=0, microsecond=0)
        elif minute < 55:
            # Round to the nearest half-hour
            result = now.replace(minute=30, second=0, microsecond=0)
        else:
            # Round up to the next hour
            result = (now + timedelta(hours=1)).replace(
                minute=0, second=0, microsecond=0
            )
        return f"{str(result.hour).zfill(2)}{str(result.minute).zfill(2)}"

    def curr_calendar_date(self) -> str:
        """Return the current calendar date in string format {yyyy}{mm}{dd}."""
        curr_dt = datetime.now(timezone.utc)
        year, month, day = curr_dt.year, curr_dt.month, curr_dt.day
        return f"{year}{str(month).zfill(2)}{str(day).zfill(2)}"

    def calendar_to_julian(self, cal_dt=None) -> str:
        """Convert the provided calendar date into a julian date (len=3).

        Parameters
        ----------
        cal_dt: datetime object, default=None
            - The calendar date to be converted to a julian date. If None, uses the
              current calendar date.

        Returns
        -------
        full_jdate: str
            - A string julian date converted from cal_dt.
            - Formatted: 'yyyyjjj'
        """
        if cal_dt is None:
            cal_dt = datetime.now(timezone.utc)
        year, month, day = cal_dt.year, cal_dt.month, cal_dt.day
        date_obj = datetime(year, month, day)
        epoch = datetime(year, 1, 1)
        jdate = (date_obj - epoch).days + 1
        full_jdate = f"{year}{str(jdate).zfill(3)}"  # insert 0's if len != 3
        return full_jdate

    def julian_to_calendar(self, julian_date, fmt="%Y_%m_%d") -> str:
        """Convert the julian date formatted yyyyjjj to a calendar date using fmt.

        Where fmt is a string that can be interpreted from datetime.strftime.

        Parameters
        ----------
        julian_date: str
            - The julian date to convert. Formatted: 'yyyyjjj'
        fmt: str, default="%Y_%m_%d"
            - The format to convert to.

        Returns
        -------
        calendar_date: str
            - By default, the format of the calendar date returned goes as such:
            - 'yyyy_mm_dd_jjj'
        """
        # Extract the year and the day of the year (jjj)
        year = int(str(julian_date)[:4])
        day_of_year = int(str(julian_date)[4:])
        # Create a datetime object for the first day of the year
        date = datetime(year, 1, 1) + timedelta(days=day_of_year - 1)
        # Return in a standard format like YYYY_MM_DD_JJJ  # NOQA
        calendar_date = f"{date.strftime(fmt)}_{day_of_year}"
        return calendar_date


class FileLocator:
    """Object which is able to locate sets of files stemming from multiple directories.

    See 'example_finfo' for an example of how to set up your search space. Information
    needed is the 'searchdir' (search directory) for where your data comes from,
    'fpatterns' (file patterns to match), and 'num_expected_files' (number of expected
    files to find).

    Once this data is provided, you can use this class to search through inputted
    directories for your files.
    """

    example_finfo = {
        "GOES16": {
            "searchdir": "/mnt/grb/goes16/2024/2024_11_21_326/abi/L1b/RadF",
            "fpatterns": ["*M6C13*s20243261600*"],
            "num_expected_files": 1,
        },
        "GOES18": {
            "searchdir": "/mnt/grb/goes18/2024/2024_11_21_326/abi/L1b/RadF",
            "fpatterns": ["*M6C13*s20243261600*"],
            "num_expected_files": 1,
        },
        "M09": {
            "searchdir": "/mnt/meteosat-09/20241121/MSG2",
            "fpatterns": [
                "H-000-MSG2__-MSG2_IODC___-_________-EPI______-202411211600-__",
                "H-000-MSG2__-MSG2_IODC___-_________-PRO______-202411211600-__",
                "H-000-MSG2__-MSG2_IODC___-IR_108___-00000[1-8]___-202411211600-C_",
            ],
            "num_expected_files": 10,
        },
        "M10": {
            "searchdir": "/mnt/meteosat-10/20241121/MSG3",
            "fpatterns": [
                "H-000-MSG3__-MSG3_IODC___-_________-EPI______-202411211600-__",
                "H-000-MSG3__-MSG3_IODC___-_________-PRO______-2024112116000-__",
                "H-000-MSG3__-MSG3_IODC___-IR_108___-00000[1-8]___-202411211600-C_",
            ],
            "num_expected_files": 10,
        },
        "GK2a": {
            "searchdir": "/mnt/GK2A/AMI/L1B/FD/202411/21/16",
            "fpatterns": ["*ir105*202411211600*"],
            "num_expected_files": 1,
        },
        "M10": {
            "searchdir": "/mnt/ahi/himawari9/20241121",
            "fpatterns": ["*20241121_1600_B13_FLDK_*_S[01][0-9]10*"],
            "num_expected_files": 10,
        },
    }

    def __init__(self, finfo) -> None:
        """Instantiate a FileLocator object.

        Given 'finfo', a dictionary of dictionaries representing the information needed
        to locate your file(s), determine whether or not all of those files exist.

        Parameters
        ----------
        finfo: dict
            - A dictionary of file patterns to match. This could be a list of a single
              string to match, or a set of strings that can match a variety of files.
            - Assumes all keys provided are used to locate all the files you need. A
              key's value can either be a string or a list of strings.
        """
        self.files_found = False
        self.required_filepaths = []
        self.searchdirs = {}
        self.fpatterns = {}
        self.num_expected_files = {}
        for key, val in finfo.items():
            if not isinstance(val, dict) or list(val.keys()) != [
                "searchdir",
                "fpatterns",
                "num_expected_files",
            ]:
                raise RuntimeError(
                    "Error: 'fpatterns' must be a dictionary that matches this format: "
                    r"key1: {'searchdir': str, 'fpatterns': list(str), 'num_expected_files': int}, ..."  # NOQA
                    r"keyX: {'searchdir': str, 'fpatterns': list(str), 'num_expected_files': int}"  # NOQA
                )
            self.searchdirs[key] = val["searchdir"]
            self.fpatterns[key] = val["fpatterns"]
            self.num_expected_files[key] = val["num_expected_files"]

    def generate_required_filepaths(self) -> None:
        """Generate one or more filepaths pointing towards expected existing files.

        These file(s) don't necessarily exist, but are needed based on the inputs to
        this class.
        """
        self.required_filepaths = []
        for key, val in self.fpatterns.items():
            if isinstance(val, list):
                # List of values provided, loop over all of those values
                ffound = []
                for fpattern in val:
                    if not isinstance(fpattern, str):
                        raise RuntimeError(
                            "Error: cannot match search for a file pattern that is not "
                            "a string."
                        )
                    print(f"PATTERN = {self.searchdirs[key]}/{fpattern}")
                    ffound_by_pattern = glob(f"{self.searchdirs[key]}/{fpattern}")
                    ffound += ffound_by_pattern
                ffound = list(set(ffound))
                print(ffound)
                # print(f"{self.searchdirs[key]}/{fpattern}")
                if len(ffound) != self.num_expected_files[key]:
                    raise FileNotFoundError(
                        "Expected files have not yet been created. Either switch "
                        "inputs or wait until those files have been created."
                    )
                self.required_filepaths += ffound
            else:
                raise RuntimeError(
                    "Error: cannot match search for a file pattern that is not "
                    "list of strings (min length = 1)."
                )
        self.required_filepaths = sorted(self.required_filepaths)

    def all_files_found(self) -> bool:
        """Determine if all the required files in 'searchdir' have been found.

        If all files have been found, then set self.files_found to True. This way
        a user will have to update the file patterns provided to locate a new file or
        set of files.

        Returns
        -------
        files_found: bool
            - Whether or not all required files were found.
        """
        if self.files_found:
            # All files have been found. If you want to run this function again, you'll
            # need to reset
            raise RuntimeWarning(
                "Warning: All files have been found with the initial arguments "
                "provided to this class. If you want to view a new search space, "
                "please call 'FileLocator.reset_search(finfo)' ."
            )
        try:
            self.generate_required_filepaths()
            self.files_found = True
        except FileNotFoundError as e:
            self.files_found = False
        return self.files_found

    def reset_search(self, finfo) -> None:
        """Reset your search parameters using 'finfo'.

        Assuming all files have been located, call this function with a new 'finfo'
        dictionary so we can search along different parameters.

        Parameters
        ----------
        finfo: dict
            - A dictionary of file patterns to match. This could be a list of a single
              string to match, or a set of strings that can match a variety of files.
            - Assumes all keys provided are used to locate all the files you need. A
              key's value can either be a string or a list of strings.
        """
        self.__init__(finfo)


class Templater:
    """Object which reads templates, renders to a string, and writes to a real file.

    Templates must take on the format specified by jinja2 using a '.j2' extension.
    """

    # NOTE: Need to implement custom contexts for bash script or slurm job files. Users
    # might want to fill in different contexts compared to those hardcoded in these
    # classes.

    alg_info = None

    def get_template(self, template_name, template_dir="./templates") -> Template:
        """Retrieve the appropriate jinja2 template from the specified arguments.

        Parameters
        ----------
        template_name: str
            - The name of the template to load
        template_dir: str
            - The filepath (string) to the directory in which the template exists

        Returns
        -------
        template: jinja2 Template
            - A jinja2 template requested form template_name
        """
        # Grab the appropriate template
        env = Environment(loader=FileSystemLoader(template_dir))
        template = env.get_template(f"{template_name}.j2")

        return template

    def create_bash_script(
        self,
        product_name,
        output_type,
        fpath=None,
        sector=None,
        template_name="geoips_clavrx_template",
    ) -> str:
        """Generate a bash script for creating products via GeoIPS.

        Where a bash script expects a filepath, product_name, output_name, and
        optionally a valid GeoIPS sector.

        Parameters
        ----------
        product_name: str
            - The name of the product plugin we'll use in GeoIPS
        output_type: str
            - The name of the output_formatter plugin we'll use in GeoIPS
        fpath: str, optional(default=None)
            - The path to the file(s) being used in this script
        sector: str, optional(default=None)
            - The name of the sector plugin we'll be using in the GeoIPS process
        template_name: str, optional(default="geoips_clavrx_template")
            - The name of the template we'll use to render the specified context

        Returns
        -------
        script_path: str
            - The path to the bash script that was created from the specified template
        """
        # Load the specified Jinja2 bash script template
        template = self.get_template(template_name)

        match output_type:
            case "imagery_annotated":
                outdir_type = "annotated_imagery"
            case "imagery_clean":
                outdir_type = "clean_imagery"
            case "unprojected_image":
                outdir_type = "unprojected_imagery"
            case _:
                outdir_type = output_type
        # Create a modified product name for slider devs: needed for accurate placement
        # of rsync'd data
        ovcst_product_name = (
            product_name.lower().replace("unprojected-", "").replace("-", "_")
        )
        # Define the context for the template
        context = {
            "file_path": fpath,
            "product_name": product_name,
            "ovcst_product_name": ovcst_product_name,
            "output_type": output_type,
            "outdir_type": outdir_type,
        }

        if sector is not None and isinstance(sector, str):
            context["sector"] = sector
        else:
            context["sector"] = "self_registered"

        if any(
            pname in product_name.lower()
            for pname in ["cloud-type", "cloud-water-content"]
        ):
            context["is_3d"] = "True"
        else:
            context["is_3d"] = "False"

        # Render the template with the context
        bash_script = template.render(context)
        script_path = (
            f"{self.outdir}/temp_scripts/{self.alg_info.name}/"
            f"{product_name}_{output_type}.sh"
        )

        # If the parent directories of 'script_path' don't exist, create them.
        if not os.path.exists(os.path.dirname(script_path)):
            os.makedirs(os.path.dirname(script_path))

        # Write the rendered template to a file
        with open(script_path, "w") as f:
            f.write(bash_script)

        subprocess.run(["chmod", "+x", script_path], check=True)

        print(f"Bash Script '{product_name}_{output_type}_{sector}.sh' was created.")
        return script_path

    def create_slurm_jobfile(
        self,
        job_name,
        executable,
        template_name="slurm_job_template",
        **kwargs,
    ) -> str:
        """Generate a slurm job file from the arguments provided using jinja.

        Where a job file expects a job_name, output_file, error_file, and an executable.
        Generated from a jinja slurm job template.

        Parameters
        ----------
        job_name: str
            - The name of the job we will submit to slurm.
        executable: str
            - The path to the file which we will be executing in slurm.
        template_name: str
            - The name of the template we'll use to render the specified context
        kwargs: dict of opt arguments
            - ntasks: int
                - Number of tasks (typically corresponds to CPU cores)
            - time: int
                - Maximum runtime for the job
            - partition: str
                - The partition (queue) to submit the job to
            - mem_per_cpu: int
                - Memory allocated per CPU core
            - executable_args: dict of str
                - A dictionary of arguments to send to the executable file

        Returns
        -------
        job_path: str
            - The path to the slurm bash script that was created from the specified
              template
        """
        # Load the specified Jinja2 Slurm template
        template = self.get_template(template_name)

        # Define the context for the template
        context = {
            "job_name": f"process_{job_name}",
            "output_file": f"{self.outdir}/output/{job_name}.log",
            "error_file": f"{self.outdir}/error/{job_name}.log",
            "executable": executable,
        }
        for kwarg in kwargs:
            if kwargs[kwarg]:
                # If the kwarg is not None, then add it to the context
                context[kwarg] = kwargs[kwarg]

        # NOTE: Need to speak with Jeremy to determine the appropriate amount of
        # resources to allocate to each job.

        # Render the template with the context
        job_script = template.render(context)
        job_path = f"{self.outdir}/jobfiles/{job_name}.sh"

        # Write the rendered template to a file
        with open(job_path, "w") as f:
            f.write(job_script)

        print(f"SLURM job file '{job_name}.sh' has been created.")
        return job_path


class FileOperator:
    """Object which performs file operations (cp, mv, tar, zip..) for NRT processing.

    Pythonic file operations used commonly by NRT processing suites.
    """

    # Set this attributes in the '__init__' function of your child class which inherits
    # this class.
    watch_directory = None

    def bzip_files_temporarily(self, orig_fpaths) -> None:
        """Decompress .bz2 files to a new location temporarily for GeoColor processing.

        Check that all files for Bands 1, 2, 3, 4, 7, & 13 are found in 'datadir' at
        time <cdate>_<hhmm>. This will return either True, all files found, or false,
        some files missing.

        Parameters
        ----------
        orig_fpaths: list(str)
            - List of filepaths where the bzipped data currently lies.
        """
        in_out_fpaths = [
            (fpath, f"{self.watch_directory}{os.path.basename(fpath)[:-4]}")
            for fpath in orig_fpaths
        ]
        with Pool(processes=os.cpu_count() // 8) as pool:
            pool.starmap(
                self._decompress_bzip_file,
                in_out_fpaths,
            )

    def _decompress_bzip_file(self, input_fpath, output_fpath) -> None:
        """Decompress a .bz2 file found at input_fpath to output_fpath.

        Parameters
        ----------
        input_fpath: str
            - The full path to the .bz2 file.
        output_fpath: str
            - The full path to the decompressed file, whose data comes from
              'intput_fpath'.
        """
        with bz2.BZ2File(input_fpath, "rb") as file:
            with open(output_fpath, "wb") as output:
                output.write(file.read())

    def file_fully_written(self, fpath) -> bool:
        """Determine if a file has been fully written to disk.

        Parameters
        ----------
        fpath: str
            - The path to the file we're checking to see if it's fully written.

        Returns
        -------
        fully_written: bool
            - The truth value as to whether or not the file has been fully written to
              disc
        """
        initial_size = os.path.getsize(fpath)
        time.sleep(60)
        next_size = os.path.getsize(fpath)

        if (
            Path(fpath).suffix == ".nc"
            and initial_size == next_size
            and len(xarray.open_dataset(fpath).attrs)
        ):
            return True
        elif (
            Path(fpath).suffix == ".hdf"
            and initial_size == next_size
            and len(SD(fpath, SDC.READ).attributes())
        ):
            return True
        elif Path(fpath).suffix not in [".nc", ".hdf"]:
            raise RuntimeError(
                f"ERROR: Retrieved file '{os.path.basename(fpath)}' has an extension "
                "that we don't know how to handle. Cannot determine if this file has "
                "been fully written."
            )
        return False


class ProcessSpawner(Templater, FileOperator):
    """Object which spawns processe(s) for near real time (NRT) product creation.

    This can either be serial processes, multiprocesses, or Slurm based job submission.
    """

    # Set these attributes in the '__init__' function of your child class which inherits
    # this class.
    outdir = None
    alg_info = None
    max_cpu_count = os.cpu_count() // 4
    use_slurm = False

    def spawn_processes(
        self,
        fpath=None,
        template_name="geoips_clavrx_template",
        sync=False,
    ) -> None:
        """Execute a series of processes using data found at file_path.

        If self.use_slurm is True, Submit a series jobfiles to slurm via 'sbatch' for
        the incoming file(s).
        Otherwise, use multiprocessing to execute your processes in parallel.

        Where the set of processes / jobfiles is from the number of output types and
        product types.

            - In sequence:
                - output_type_1 (in parallel if using multiprocessing):
                    - product_1 (jobfile if using slurm)
                    - product_2 (otherwise it is one of the parallel processes)
                    - ...
                - output_type_2:
                    - product_1
                    - product_2
                - ...

        Parameters
        ----------
        fpath: str, optional(default=None)
            - The path to the data file which just arrived.
        template_name: str, optional(default="geoips_clavrx_template")
            - The name of the bash script template you want to use
        sync: book, optional(default=False)
            - Whether or not we want to sync the created outputs with another machine.
        """
        for output_type in self.alg_info.output_types:
            # Produce all specified output types
            script_paths = []
            for sector in list(self.alg_info.sector_mapping.values()):
                # For the sectors specified in your algorithm
                for product_name in self.alg_info.product_names:
                    # Do this for a set of GeoIPS products that we want to produce
                    script_path = self.create_bash_script(
                        product_name,
                        output_type,
                        fpath=fpath,
                        sector=sector,
                        template_name=template_name,
                    )
                    if self.use_slurm:
                        # Create, submit, and the execute job using sbatch (SLURM)
                        fname = f"{product_name}_{output_type}"
                        self.run_jobfile(fname, script_path)
                    else:
                        # Otherwise just add the recently created bash script to the
                        # list of scripts we'll execute
                        script_paths.append(script_path)
            if not self.use_slurm:
                self.parallel_process(script_paths)
        if sync:
            self.sync_with_slider()

    def parallel_process(self, script_paths) -> None:
        """Execute a set of processes in parallel for each script in 'script_paths'.

        Parameters
        ----------
        script_paths: list[str]
            - A list of file paths that are associated with GeoIPS bash scripts used
              for processing
        """
        print(f"Beginning parallel processing of {self.alg_info.name} products.")
        with Pool(
            processes=min(len(self.alg_info.product_names), self.max_cpu_count)
        ) as pool:
            # Execute your GeoIPS bash scripts in parallel
            results = pool.map(
                self.run_bash_script,
                script_paths,
            )
            # Save outputs to individual files
            for i, result in enumerate(results):
                # Create a unique output file name
                output_file = (
                    f"{self.outdir}/output/output_"
                    f"{os.path.basename(script_paths[i])[:-3]}.log"
                )
                with open(output_file, "w") as f:
                    f.write(result)
                    print(f"Output for script {script_paths[i]} saved to {output_file}")

    def run_bash_script(self, fpath) -> str:
        """Execute the bash script at 'fpath' while adhering to bandit protocols.

        Parameters
        ----------
        fpath: str
            - The path to the data file which just arrived.

        Returns
        -------
        output: str
            - The output of the processes that were ran. Can be errors as well.
        """
        try:
            # Run the bash script using subprocess.run
            result = subprocess.run(
                ["/bin/bash", fpath], check=True, capture_output=True, text=True
            )
            return (
                result.stdout.strip()
            )  # Return output, stripped of leading/trailing whitespace
        except subprocess.CalledProcessError as e:
            return f"Error running script: {e}"
        except Exception as e:
            return f"Error: {e}"

    def run_jobfile(self, fname, script_path) -> None:
        """Execute a slurm jobfile using the file found at 'script_path'.

        Parameters
        ----------
        fname: str
            - The name of the file. (basename(fpath)). Used as the jobfile name for
              tracking w/in slurm.
        fpath: str
            - The path to the data file which just arrived.
        script_path: str
            - The path to the GeoIPS bash script used for processing.
        """
        job_path = self.create_slurm_jobfile(fname, script_path)
        # Submit the job using sbatch (SLURM)
        try:
            subprocess.run(["sbatch", job_path], check=True)
            print(f"Submitted job for file: {fname}")
        except subprocess.CalledProcessError as e:
            LOG.error(f"Failed to submit job for file: {fname}, error: {e}")

    def sync_with_slider(self) -> None:
        """Sync the produced output with SLIDER, on machine overcast4.

        Template has hardcoded values for where to grab data from (source) and where to
        place data (destination). Since this is sending data to a different machine,
        you'll need to set up an ssh-key on that machine so that this can be ran as a
        daemon.
        """
        # Grab the appropriate template for rsync
        template = self.get_template("rsync_data_template")
        for product_name in self.alg_info.product_names:
            if any(
                pname in product_name.lower()
                for pname in ["cloud-type", "cloud-water-content"]
            ):
                continue
            # NOTE: Remove the if statement above once Zayd is ready for 3D data
            ovcst_pname = (
                product_name.lower().replace("unprojected-", "").replace("-", "_")
            )
            context = {"product_name": ovcst_pname}
            # Render the template with the context
            rsync_bash_script = template.render(context)
            script_path = f"{self.outdir}/temp_scripts/rsync/{ovcst_pname}.sh"

            if not os.path.exists(os.path.dirname(script_path)):
                os.makedirs(os.path.dirname(script_path))

            # Write the rendered template to a file
            with open(script_path, "w") as f:
                f.write(rsync_bash_script)

            # Modify the file permissions to be executable
            subprocess.run(["chmod", "+x", script_path], check=True)
            self.run_bash_script(script_path)
