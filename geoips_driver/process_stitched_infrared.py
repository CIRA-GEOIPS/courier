"""Daemon which produces GeoIPS Stitched-Infrared unprojected imagery.

Uses data from satellite sensor pairs GOES16/18-ABI, M09/10-SEVIRI, GK2A-AMI, and
H09-AHI on bands at or near 10.4 micron (Channel 13).
"""

import bz2
import logging
import os
import shutil
import subprocess
import time
from multiprocessing import Pool
from pathlib import Path

from jinja2 import Environment, FileSystemLoader
from watchdog.events import FileSystemEventHandler

from geoips_driver.algorithm_info import (
    NewJulianDateException,
    algorithms,
    calendar_to_julian,
    curr_calendar_date,
    nearest_half_hour_utc,
)
from geoips_driver.driver_components import FileLocator

LOG = logging.getLogger(__name__)


class NASWatcher(FileSystemEventHandler):
    """Daemon which watches the given directory and submits job files upon arrival.

    Submits jobfiles via slurm or executes multiple processes in parallel using python
    multiprocess for GeoIPS-based processing. This will likely need to be adjusted based
    on the hardware in which you are running on.
    """

    last_processed_hhnn = None

    def __init__(self, algorithm, sat, sensor, sector, use_slurm=True):
        """Initialize the daemon to listen for arriving data at 'watch_directory.

        Parameters
        ----------
        algorithm: str
            - The algorithm which we want observe for produced data
        sat: str
            - The name of the satellite which uses 'algorithm'
        sensor: str
            - The sensor on 'sat' which makes use of 'algorithm'
        sector: str
            - The geospatial sector containing data produced from 'algorithm.sat.sensor'
        use_slurm: bool (default = True)
            - Whether or not we want to use slurm-based automated processing. If False,
              we'll use multiprocessing to automate.
        """
        if algorithm not in algorithms:
            raise NotImplementedError(
                f"Algorithm '{algorithm}' hasn't been implemented in "
                "geoips_driver.algorithm_info:algorithms. Please create an info "
                f"container for '{algorithm}' before instantiating a watcher for it.",
            )
        # Initialize variables for the AHI GeoColor Watcher
        self.max_cpu_count = os.cpu_count() // 4
        self.alg_info = algorithms[algorithm]
        self.use_slurm = use_slurm
        # NOTE: most likely need to add a check here to ensure that
        # sat, sensor, and sector are valid keys to this information dictionary
        self.watch_directory = f"{self.alg_info.paths[sat][sensor][sector]}/data/"
        if not os.path.exists(self.watch_directory):
            os.makedirs(self.watch_directory)

        self.sector = self.alg_info.sector_mapping[sector]

        if self.use_slurm:
            self.outdir = self.alg_info.slurm_dir
        else:
            self.outdir = self.alg_info.mp_dir

        super().__init__()

        prev_hhnn = nearest_half_hour_utc()
        next_hhnn = None
        while True:
            if next_hhnn is None or next_hhnn != prev_hhnn:
                if next_hhnn is None:
                    # If None, set next_hhnn to the original value of prev_hhnn. This
                    # ensures that comparisons in the future will be valid time stamps
                    # instead of None vs a time stamp.
                    next_hhnn = prev_hhnn
                # This assumes the files will eventually arrive. If a
                # 'FileNotFoundError' is raised, we've been searching for too long and
                # the files most likely won't arrive. Reset our search to the next
                # timestep.
                try:
                    self.start_processing()
                except FileNotFoundError:
                    print(
                        f"All required files for timestep {next_hhnn} weren't found. "
                        "Skipping to next timestep.",
                    )

                prev_hhnn = next_hhnn
                hh = prev_hhnn[:2]
                nn = prev_hhnn[2:]

                if nn == "30":
                    hh = str((int(hh) + 1) % 24).zfill(2)
                    nn = "00"
                    if hh == "00":
                        raise NewJulianDateException(
                            "New date has started. Reinitialize this watcher.",
                        )
                else:
                    nn = "30"
                next_hhnn = f"{hh}{nn}"
            # next_hhnn = nearest_half_hour_utc()

    def start_processing(self):
        """Kick off processing of Stitched-Infrared imagery for arriving data."""
        times_searched = 0
        finfo = self.get_file_info()
        self.fl = FileLocator(finfo)
        ffound = self.fl.all_files_found()
        while not ffound:
            print("Waiting for data to be transferred to a temporary directory.")
            time.sleep(30)
            ffound = self.fl.all_files_found()
            times_searched += 1
            if times_searched >= 121:
                # We've been searching for longer than an hour. This means the files
                # we're expecting probably will not arrive. Raise an error and search
                # for the next time step.
                raise FileNotFoundError(
                    "Process has been searching for required hours for over an hour and"
                    " they haven't been found. Resetting search to next timestep.",
                )

        self.required_filepaths = self.fl.required_filepaths
        # List of file paths that end in '.bz2'. These need to be decompressed before
        # they can be used in GeoIPS (Himwarai9 AHI Files)
        bzipped_fpaths = list(
            filter(lambda f: Path(f).suffix == ".bz2", self.required_filepaths),
        )
        # All the other files used in this process are ready to go. No decompression
        # needed.
        ready_fpaths = list(
            filter(lambda f: Path(f).suffix != ".bz2", self.required_filepaths),
        )
        self.bzip_files_temporarily(bzipped_fpaths)
        # Make sure the files have decompressed fully
        time.sleep(6)
        # Copy all files over to a temporary directory
        for fpath in ready_fpaths:
            shutil.copy(fpath, self.watch_directory)
        # Make sure the files have copied over fully
        time.sleep(6)
        # Spawn your processes!
        self.spawn_processes()
        # Clean up output directory once processes have finished
        for fname in os.listdir(self.watch_directory):
            os.remove(f"{self.watch_directory}{fname}")

    def get_file_info(self) -> dict:
        """Return a dictionary of file information used to locate files needed.

        Where the dictionary takes on the form (can be repeated):
        {keyX: {'searchdir': fpath, 'fpatterns': list(str), 'num_expected_files': int}}
        """
        # NOTE: Probably need to update the calendar date to take into account hhnn
        # I.e. if the nearest half hour is '0000', but calendar date is on the day
        # before, say at 2347 hhnn, then we'll just reprocess the beginning hhnn of that
        # day. Need to wait for the new calendar date directory to be created or
        # consider this in our calendar day function.
        calendar_date = curr_calendar_date()
        jdate = calendar_to_julian()[-3:]
        year = calendar_date[0:4]
        month = calendar_date[4:6]
        day = calendar_date[6:8]
        hhnn = nearest_half_hour_utc()
        # If this watcher was just initialized and the watch directory doesn't exist,
        # wait for the directory to be created before initializing the actual watcher.
        finfo = {
            "GOES16": {
                "searchdir": f"/mnt/grb/goes16/{year}/{year}_{month}_{day}_{jdate}/abi/L1b/RadF",  # NOQA
                "fpatterns": [f"*M6C13*s{year}{jdate}{hhnn}*"],
                "num_expected_files": 1,
            },
            "GOES18": {
                "searchdir": f"/mnt/grb/goes18/{year}/{year}_{month}_{day}_{jdate}/abi/L1b/RadF",  # NOQA
                "fpatterns": [f"*M6C13*s{year}{jdate}{hhnn}*"],
                "num_expected_files": 1,
            },
            "M09": {
                "searchdir": f"/mnt/meteosat-09/{year}{month}{day}/MSG2",
                "fpatterns": [
                    f"H-000-MSG2__-MSG2_IODC___-_________-EPI______-{year}{month}{day}{hhnn}-__",
                    f"H-000-MSG2__-MSG2_IODC___-_________-PRO______-{year}{month}{day}{hhnn}-__",
                    f"H-000-MSG2__-MSG2_IODC___-IR_108___-00000[1-8]___-{year}{month}{day}{hhnn}-C_",
                ],
                "num_expected_files": 10,
            },
            "M10": {
                "searchdir": f"/mnt/meteosat-10/{year}{month}{day}/MSG3",
                "fpatterns": [
                    f"H-000-MSG3__-MSG3________-_________-EPI______-{year}{month}{day}{hhnn}-__",
                    f"H-000-MSG3__-MSG3________-_________-PRO______-{year}{month}{day}{hhnn}-__",
                    f"H-000-MSG3__-MSG3________-IR_108___-00000[1-8]___-{year}{month}{day}{hhnn}-C_",
                ],
                "num_expected_files": 10,
            },
            "GK2A": {
                "searchdir": f"/mnt/GK2A/AMI/L1B/FD/{year}{month}/{day}/{hhnn[0:2]}",
                "fpatterns": [f"*ir105*{year}{month}{day}{hhnn}*"],
                "num_expected_files": 1,
            },
            "H09": {
                "searchdir": f"/mnt/ahi/himawari9/{year}{month}{day}",
                "fpatterns": [f"*{year}{month}{day}_{hhnn}_B13_FLDK_*_S[01][0-9]10*"],
                "num_expected_files": 10,
            },
        }

        return finfo

    def spawn_processes(self):
        """Execute a series of processes using data found at file_path.

        If self.use_slurm is True, Submit a series jobfiles to slurm via 'sbatch' for
        the incoming CLAVR-X file.
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
        fpath: str
            - The path to the data file which just arrived.
        """
        script_paths = []
        for output_type in self.alg_info.output_types:
            for sector in ["overcast_georing"]:
                # Produce both imagery_annotated and imagery_clean outputs
                for product_name in self.alg_info.product_names:
                    # Do this for a set of CLAVR-X products that we want to produce
                    script_path = self.create_clavrx_script(
                        product_name,
                        output_type,
                        sector,
                    )
                    if self.use_slurm:
                        # Create, submit, and the job using sbatch (SLURM)
                        fname = f"infrared_stitched_{output_type}"
                        self.run_jobfile(fname, script_path)
                    else:
                        script_paths.append(script_path)
        if not self.use_slurm:
            self.parallel_process(script_paths)

    def create_clavrx_script(self, product_name, output_type, sector):
        """Generate a bash script for producing CLAVR-X products via GeoIPS.

        Where a clavrx bash script expects a filepath, product_name, and output_type.

        Parameters
        ----------
        product_name: str
            - The name of the product plugin we'll use in GeoIPS
        output_type: str
            - The name of the output_formatter plugin we'll use in GeoIPS
        sector: str
            - The name of the sector plugin we'll be using in the GeoIPS process
        """
        # Load the Jinja2 template
        env = Environment(loader=FileSystemLoader("."))
        template = env.get_template("templates/georing_infrared_template.j2")

        if output_type == "imagery_annotated":
            outdir_type = "annotated_imagery"
        else:
            outdir_type = "unprojected_imagery"
        # Define the context for the template
        context = {
            "output_type": output_type,
            "outdir_type": outdir_type,
        }

        # Render the template with the context
        clavrx_bash_script = template.render(context)
        script_path = (
            f"{self.outdir}/temp_scripts/{product_name}_{output_type}_{sector}.sh"
        )

        # Write the rendered template to a file
        with open(script_path, "w") as f:
            f.write(clavrx_bash_script)

        subprocess.run(["chmod", "+x", script_path], check=True)

        print(f"CLAVR-X Bash Script '{product_name}_{output_type}.sh' was created.")
        return script_path

    def parallel_process(self, script_paths):
        """Execute a set of processes in parallel for each script in 'script_paths'.

        Parameters
        ----------
        script_paths: list[str]
            - A list of file paths that are associated with GeoIPS bash scripts used
              for processing
        """
        with Pool(processes=4) as pool:
            results = pool.map(
                self.run_bash_script,
                script_paths,
            )
            # Save outputs to individual files
            for i, result in enumerate(results):
                # Create a unique output file name
                output_file = f"{self.outdir}/output/output_{os.path.basename(script_paths[i])[:-3]}.log"  # noqa
                with open(output_file, "w") as f:
                    f.write(result)
                    print(f"Output for script {script_paths[i]} saved to {output_file}")

    def run_bash_script(self, fpath):
        """Execute the bash script at 'fpath' while adhering to bandit protocols.

        Parameters
        ----------
        fpath: str
            - The path to the data file which just arrived.
        """
        try:
            # Run the bash script using subprocess.run
            result = subprocess.run(
                ["/bin/bash", fpath], check=True, capture_output=True, text=True,
            )
            return (
                result.stdout.strip()
            )  # Return output, stripped of leading/trailing whitespace
        except subprocess.CalledProcessError as e:
            return f"Error running script: {e}"
        except Exception as e:
            return f"Error: {e}"

    def run_jobfile(self, fname, script_path):
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
            LOG.exception(f"Failed to submit job for file: {fname}, error: {e}")

    def create_slurm_jobfile(self, job_name, executable, **kwargs):
        """Generate a slurm job file from the arguments provided using jinja.

        Where a job file expects a job_name, output_file, error_file, and an executable.
        Generated from a jinja slurm job template.

        Parameters
        ----------
        job_name: str
            - The name of the job we will submit to slurm.
        executable: str
            - The path to the file which we will be executing in slurm.
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
        """
        # Load the Jinja2 template
        env = Environment(loader=FileSystemLoader("."))
        template = env.get_template("templates/slurm_job_template.j2")

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

    def bzip_files_temporarily(self, orig_fpaths):
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

    def _decompress_bzip_file(self, input_fpath, output_fpath):
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


def start_watching():
    """Start up the daemon which produces GeoIPS Stitched-Infrared outputs.

    Uses data from satellite sensor pairs GOES16/18-ABI, M09/10-SEVIRI, GK2A-AMI, and
    H09-AHI on bands at or near 10.4 micron (Channel 13).
    """
    NASWatcher("StitchedInfrared", "ALL", "ALL", "ALL", use_slurm=False)
    # print(f"Started watching directory: {event_handler.watch_directory}")
    # # observer = Observer()
    # observer = PollingObserver()
    # observer.schedule(event_handler, event_handler.watch_directory, recursive=False)
    # observer.start()
    # # NOTE: Using a polling observer as the normal observer
    # # (which uses inotify under the hood) did not capture file system events w/ a NFS
    # print("Started polling observer.")

    # try:
    #     while observer.is_alive():
    #         time.sleep(1)
    # except (Exception, KeyboardInterrupt) as e:
    #     LOG.error(f"An error occurred: {e}")
    #     observer.stop()
    # finally:
    #     observer.join()


def main():
    """Start up the daemon and begin watching for data.

    When data is found, create and submit slurm jobfiles or execute bash scripts which
    will produce Stitched-Infrared Imagery on the overcast_georing grid.

    If the daemon crashes for some reason, catch that failure, wait 5 seconds, and
    attempt to restart it.
    """
    print("Initializing NASWatcher")
    while True:
        try:
            start_watching()
        except Exception and NewJulianDateException as e:
            if type(e).__name__ != "NewJulianDateException":
                LOG.exception(f"Watcher crashed with error: {e}")
            else:
                print(e)
            time.sleep(5)  # Wait a bit before restarting


if __name__ == "__main__":
    main()
