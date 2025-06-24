"""Daemon which listens for arriving data performs automatic processing on arrival.

The daemon is built using 'watchdog' and can either submit jobfiles for arriving data
or execute a set of processes in parallel if multiprocessing is selected.
"""

import argparse
import bz2
import logging
import os
import subprocess
import time
from glob import glob
from multiprocessing import Pool
from pathlib import Path

from jinja2 import Environment, FileSystemLoader
from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver

from geoips_driver.algorithm_info import (
    NewJulianDateException,
    algorithms,
    curr_calendar_date,
)

LOG = logging.getLogger(__name__)


class AhiGeoColorUtils:
    """Object providing utility functions for producing AHI GeoColor Imagery."""

    band_resolution_mapping = {
        "B01": "R10",
        "B02": "R10",
        "B03": "R05",
        "B04": "R10",
        "B07": "R20",
        "B13": "R20",
    }
    segment_divisions = [f"S{str(num).zfill(2)}10" for num in range(1, 11)]
    temp_savedir = f"{os.environ['GEOIPS_TESTDATA_DIR']}/test_data_ahi_day/temp_files"

    def required_geocolor_fnames(self, cdate, hhnn):
        """Generate a listing of all fnames needed to produce AHI GeoColor images exist.

        Generate all AHI fnames for Bands 1, 2, 3, 4, 7, & 13 at time <cdate>_<hhmm>.
        This will return a list of required files needed to produce AHI GeoColor
        Imagery.

        Parameters
        ----------
        cdate: str
            - The calendar date of the data used for GeoColor.
              Formatted: {yyyy}{mm}{dd}.
        hhmm: str
            - The hour and minute of 'cdate' that we'll use to produce GeoColor imagery.
        """
        required_fnames = []
        for band, res in self.band_resolution_mapping.items():
            for seg_div in self.segment_divisions:
                required_fnames.append(
                    f"HS_H09_{cdate}_{hhnn}_{band}_FLDK_{res}_{seg_div}.DAT.bz2",
                )
        return sorted(required_fnames)

    def get_full_geocolor_paths(self, datadir, cdate, hhnn):
        """Return a list of full paths to files needed to produce AHI GeoColor imagery.

        Check that all files for Bands 1, 2, 3, 4, 7, & 13 are found in 'datadir' at
        time <cdate>_<hhmm>. This will return full file paths to the required
        files if all exist, otherwise, a RuntimeError will be raised.

        Parameters
        ----------
        datadir: str
            - The path to the directory containing the files used for GeoColor.
        cdate: str
            - The calendar date of the data used for GeoColor.
              Formatted: {yyyy}{mm}{dd}.
        hhnn: str
            - The hour and minute of 'cdate' that we'll use to produce GeoColor imagery.
        """
        required_fnames = self.required_geocolor_fnames(cdate, hhnn)
        files_exist = [
            os.path.exists(f"{datadir}/{cdate}/{fname}") for fname in required_fnames
        ]
        full_paths = [f"{datadir}/{cdate}/{fname}" for fname in required_fnames]

        if not all(files_exist):
            raise RuntimeError(
                "Not all files for this timestep have been created yet. Please check "
                "that the information you provided is correct, and if so, wait for "
                "those files to be created.",
            )

        return full_paths

    def bzip_files_temporarily(self, datadir, cdate, hhnn):
        """Decompress .bz2 files to a new location temporarily for GeoColor processing.

        Check that all files for Bands 1, 2, 3, 4, 7, & 13 are found in 'datadir' at
        time <cdate>_<hhmm>. This will return either True, all files found, or false,
        some files missing.

        Parameters
        ----------
        datadir: str
            - The path to the directory containing the files used for GeoColor.
        cdate: str
            - The calendar date of the data used for GeoColor.
              Formatted: {yyyy}{mm}{dd}.
        hhnn: str
            - The hour and minute of 'cdate' that we'll use to produce GeoColor imagery.
        """
        full_paths = self.get_full_geocolor_paths(datadir, cdate, hhnn)
        in_out_fpaths = [
            (fpath, f"{self.temp_savedir}/{os.path.basename(fpath)[:-4]}")
            for fpath in full_paths
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


class NASWatcher(FileSystemEventHandler):
    """Daemon which watches the given directory and submits job files upon arrival.

    Submits jobfiles via slurm or executes multiple processes in parallel using python
    multiprocess for GeoIPS-based processing. This will likely need to be adjusted based
    on the hardware in which you are running on.
    """

    gc_utils = AhiGeoColorUtils()
    last_processed_hhnn = None

    def __init__(self, algorithm, sat, sensor, sector, calendar_date, use_slurm=True):
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
        calendar_date: str
            - The current julian date in which we are runnign this NASWatcher
            - Formatted: {yyyy}{mm}{dd}
        use_slurm: bool (default = True)
            - Whether or not we want to use slurm-based automated processing. If False,
              we'll use multiprocessing to automate.
        """
        if algorithm not in algorithms or algorithm != "AHI":
            raise NotImplementedError(
                f"Algorithm '{algorithm}' hasn't been implemented in "
                "geoips_driver.algorithm_info:algorithms. Please create an info "
                f"container for '{algorithm}' before instantiating a watcher for it.",
            )
        self.required_gc_files = set()
        # Initialize variables for the AHI GeoColor Watcher
        self.max_cpu_count = os.cpu_count() // 4
        self.alg_info = algorithms[algorithm]
        self.use_slurm = use_slurm
        # NOTE: most likely need to add a check here to ensure that
        # sat, sensor, and sector are valid keys to this information dictionary
        self.watch_directory = (
            f"{self.alg_info.paths[sat][sensor][sector]}/{calendar_date}/"
        )
        self.sector = self.alg_info.sector_mapping[sector]

        if self.use_slurm:
            self.outdir = self.alg_info.slurm_dir
        else:
            self.outdir = self.alg_info.mp_dir

        # If this watcher was just initialized and the watch directory doesn't exist,
        # wait for the directory to be created before initializing the actual watcher.
        while not os.path.exists(self.watch_directory):
            print(f"Waiting for data directory {self.watch_directory} to be created.")
            time.sleep(30)

        super().__init__()

    def on_created(self, event):
        """If a file in 'watch_directory' was created, call this func.

        If the created source path was an actual file, not a directory, then execute
        GeoIPS CLAVR-X processing.

        Parameters
        ----------
        event: FileSystemEvent
            - An event caught on the file system being watched.
        """
        # Called when a file is created
        file_path = event.src_path
        bname = os.path.basename(file_path)
        pl_path = Path(file_path)
        if (
            not event.is_directory
            and "FLDK" in file_path
            and pl_path.suffix == ".bz2"
            and any(band in bname for band in self.gc_utils.band_resolution_mapping)
        ):
            # AHI GeoColor input file structure
            # header_satellite_cdate_hhnn_band_sector_res_segdiv.DAT.bz2
            # HS_H09_20240719_1340_B08_FLDK_R20_S1010.DAT.bz2
            # 0__1___2________3____4___5____6___7.DAT.bz2
            fsplit = file_path.split("_")
            cdate = fsplit[2]
            hhnn = fsplit[3]
            print(f"Detected new file: {file_path}")
            try:
                self.gc_utils.get_full_geocolor_paths(
                    self.alg_info.basedir,
                    cdate,
                    hhnn,
                )
                files_found = True
            except RuntimeError:
                # Not all files exist yet for processing
                # print("Still waiting for a few files...")
                files_found = False

            if files_found and self.last_processed_hhnn != hhnn:
                success_str = (
                    "All files exist! Starting processing after files have been written"
                    "..."
                )
                print(success_str)
                time.sleep(30)
                print("Starting processing...")
                self.prepare_process_cleanup(cdate, hhnn)
                self.last_processed_hhnn = hhnn

    def prepare_process_cleanup(self, cdate, hhnn):
        """Prepare files for processing, process, then remove decompressed files.

        Parameters
        ----------
        cdate: str
            - The calendar date of the data used for GeoColor.
              Formatted: {yyyy}{mm}{dd}.
        hhnn: str
            - The hour and minute of 'cdate' that we'll use to produce GeoColor imagery.
        """
        # Decompress the required files to a temporary location
        print("Decompressing Files for AHI GeoColor Imagery")
        self.gc_utils.bzip_files_temporarily(
            self.alg_info.basedir,
            cdate,
            hhnn,
        )
        # Wait a bit to ensure the files have been decompressed fully. This might need
        # to be adjusted. Previously was getting errors that these files didn't exist,
        # although they did. Hopefully this fixes things.
        time.sleep(6)
        # Only start processing if all files needed have been found and
        # decompressed with bunzip2
        self.start_processing(f"{self.gc_utils.temp_savedir}/*")
        # Remove the temporarily decompressed files
        for fpath in glob(f"{self.gc_utils.temp_savedir}/*.DAT"):
            os.remove(fpath)
        # Reset found / required files for the next timestep of processing
        self.required_gc_files = set()

    def start_processing(self, fpath):
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
            for sector in ["himawari", "south_china_sea"]:
                # Produce both imagery_annotated and imagery_clean outputs
                for product_name in self.alg_info.product_names:
                    # Do this for a set of CLAVR-X products that we want to produce
                    script_path = self.create_clavrx_script(
                        fpath,
                        product_name,
                        output_type,
                        sector,
                    )
                    if self.use_slurm:
                        # Create, submit, and the job using sbatch (SLURM)
                        fname = f"ahi_gc_{output_type}"
                        self.run_jobfile(fname, fpath, script_path)
                    else:
                        script_paths.append(script_path)
                    # Remove the bash script after it has been executed.
                    # NOTE: Need to determine how long it will take to run that file
                    # after we've submitted it to slurm. Might just to clean up every
                    # once in a while for the time being.

                    # os.remove(script_path)
        if not self.use_slurm:
            self.parallel_process(script_paths)

    def create_clavrx_script(self, fpath, product_name, output_type, sector):
        """Generate a bash script for producing CLAVR-X products via GeoIPS.

        Where a clavrx bash script expects a filepath, product_name, and output_type.

        Parameters
        ----------
        fpath: str
            - The path to the data file which just arrived.
        product_name: str
            - The name of the product plugin we'll use in GeoIPS
        output_type: str
            - The name of the output_formatter plugin we'll use in GeoIPS
        sector: str
            - The name of the sector plugin we'll be using in the GeoIPS process
        """
        # Load the Jinja2 template
        env = Environment(loader=FileSystemLoader("."))
        template = env.get_template("templates/ahi_geocolor_template.j2")

        if output_type == "imagery_annotated":
            outdir_type = "annotated_imagery"
        else:
            outdir_type = "clean_imagery"
        # Define the context for the template
        context = {
            "file_path": fpath,
            "product_name": product_name,
            "output_type": output_type,
            "outdir_type": outdir_type,
            "sector": sector,
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
                ["/bin/bash", fpath],
                check=True,
                capture_output=True,
                text=True,
            )
            return (
                result.stdout.strip()
            )  # Return output, stripped of leading/trailing whitespace
        except subprocess.CalledProcessError as e:
            return f"Error running script: {e}"
        except Exception as e:
            return f"Error: {e}"

    def run_jobfile(self, fname, fpath, script_path):
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
            print(f"Submitted job for file: {fpath}")
        except subprocess.CalledProcessError as e:
            LOG.exception(f"Failed to submit job for file: {fpath}, error: {e}")

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


def start_watching(algorithm, sat, sensor, sector, use_slurm=True):
    """Invoke a NASWatcher to observe for incoming data files.

    When a data file arrives, generate and run a series of processes or slurm jobfiles
    for automated processing. The directory being watched depends on the algorithm, sat,
    sensor, and sector specified.

    Ie. if algorithm = "CLAVRX", sat = "GOES16", sensor = "ABI", and sector = "RadC",
    then start a watcher on the directory containing such information. If your data is
    stored in a different location than what is listed in 'geoips_driver'

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
    starting_cdate = curr_calendar_date()
    event_handler = NASWatcher(
        algorithm,
        sat,
        sensor,
        sector,
        starting_cdate,
        use_slurm=use_slurm,
    )
    print(f"Started watching directory: {event_handler.watch_directory}")
    # observer = Observer()
    observer = PollingObserver()
    observer.schedule(event_handler, event_handler.watch_directory, recursive=True)
    observer.start()
    # NOTE: Using a polling observer as the normal observer
    # (which uses inotify under the hood) did not capture file system events w/ a NFS
    print("Started polling observer.")

    try:
        while observer.is_alive():
            time.sleep(1)
            curr_cdate = curr_calendar_date()
            if int(curr_cdate) > int(starting_cdate):
                starting_cdate = curr_cdate
                observer.stop()
                observer.join()
                raise NewJulianDateException(
                    f"Reinitializing NASWatcher for calendar date = {starting_cdate}.",
                )
    # except KeyboardInterrupt:
    #     observer.stop()
    except Exception as e:
        LOG.exception(f"An error occurred: {e}")
        observer.stop()
    finally:
        observer.join()


def main():
    """Start up the daemon and begin watching for data at 'watch_directory'.

    When data is found, create and submit slurm jobfiles which will produce a set of
    CLAVR-X products for each data file via geoips and geoips_clavrx.

    If the daemon crashes for some reason, catch that failure, wait 5 seconds, and
    attempt to restart it.
    """
    parser = argparse.ArgumentParser("NASWatcher for automating GeoIPS Processing")
    parser.add_argument(
        "--algorithm",
        "-alg",
        type=str,
        default="AHI",
        help="The algorithm that you want your data to come from.",
    )
    parser.add_argument(
        "--satellite",
        "-sat",
        type=str,
        default="Himawari9",
        help="The satellite that you want your data to come from.",
    )
    parser.add_argument(
        "--sensor",
        "-sens",
        type=str,
        default="AHI",
        help="The sensor of the satellite that you want your data to come from.",
    )
    parser.add_argument(
        "--sector",
        "-sect",
        type=str,
        default="FLDK",
        help="The sector that you want your data to come from.",
    )
    ARGS = parser.parse_args()
    alg, sat, sensor, sector = ARGS.algorithm, ARGS.satellite, ARGS.sensor, ARGS.sector
    print("Initializing NASWatcher")
    while True:
        try:
            start_watching(alg, sat, sensor, sector, use_slurm=False)
            # start_watching("CLAVRX", "GOES16", "ABI", "RadF")
            # start_watching("CLAVRX", "GOES18", "ABI", "RadC")
            # start_watching("CLAVRX", "GOES18", "ABI", "RadF")
        except Exception and NewJulianDateException as e:
            if type(e).__name__ != "NewJulianDateException":
                LOG.exception(f"Watcher crashed with error: {e}")
            else:
                print(e)
            time.sleep(5)  # Wait a bit before restarting


if __name__ == "__main__":
    main()
