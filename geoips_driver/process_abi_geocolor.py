"""Daemon which listens for arriving data performs automatic processing on arrival.

The daemon is built using 'watchdog' and can either submit jobfiles for arriving data
or execute a set of processes in parallel if multiprocessing is selected.
"""

import argparse
import logging
import os
import subprocess
import time
from multiprocessing import Pool

from jinja2 import Environment, FileSystemLoader
from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver

from geoips_driver.algorithm_info import (
    NewJulianDateException,
    algorithms,
    calendar_to_julian,
    curr_calendar_date,
    julian_to_calendar,
)

LOG = logging.getLogger(__name__)


class NASWatcher(FileSystemEventHandler):
    """Daemon which watches the given directory and submits job files upon arrival.

    Submits jobfiles via slurm or executes multiple processes in parallel using python
    multiprocess for GeoIPS-based processing. This will likely need to be adjusted based
    on the hardware in which you are running on.
    """

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
        if algorithm not in algorithms or algorithm not in [
            "AHI",
            "GOES16_ABI",
            "GOES18_ABI",
        ]:
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
        yyyyjjj = calendar_to_julian()
        # now use the julian date to return a formatted directory where the data will
        # lie
        abi_date_str = julian_to_calendar(yyyyjjj)
        self.watch_directory = (
            f"{self.alg_info.basedir}/{yyyyjjj[:4]}/{abi_date_str}/abi/L1b/{sector}/"
        )
        self.satellite = sat
        self.orig_sector = sector
        self.sector = self.alg_info.sector_mapping[sector]
        self.bands = ["B01", "B02", "B03", "B07", "B13"]
        self.last_start_dt_processed = None

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
        if not event.is_directory and any(
            band.replace("B", "C") in bname for band in self.bands
        ):
            # ABI GeoColor input file structure
            # header_satellite_cdate_hhnn_band_sector_res_segdiv.DAT.bz2
            # OR_ABI-L1b-RadF-M6C16_G18_s20242631610209_e20242631619528_c20242631619575.nc  # NOQA
            # 0*_1*****************_2**_3**************_4**************_5*****************  # NOQA
            fsplit = bname.split("_")
            start_dt = fsplit[3]
            print(f"Detected new file: {file_path}")
            self.required_gc_files.add(file_path)
            if (
                all(
                    any(
                            band.replace("B", "C") in os.path.basename(fpath)
                                for fpath in self.required_gc_files
                        )
                        for band in self.bands
                )
                and self.last_start_dt_processed != start_dt
            ):
                success_str = (
                    "All files exist! Starting processing after files have been written"
                    "..."
                )
                print(success_str)
                time.sleep(30)
                print("Starting processing...")
                fpath_str = " ".join(sorted(self.required_gc_files))
                self.start_processing(fpath_str)
                self.last_start_dt_processed = start_dt
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
            for sector in ["goes_west", "conus"]:
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
                        fname = f"abi_gc_{output_type}"
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
        template = env.get_template("templates/abi_geocolor_template.j2")

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
                ["/bin/bash", fpath], check=True, capture_output=True, text=True,
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
        algorithm, sat, sensor, sector, starting_cdate, use_slurm=use_slurm,
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
        default="GOES16_ABI",
        help="The algorithm that you want your data to come from.",
    )
    parser.add_argument(
        "--satellite",
        "-sat",
        type=str,
        default="GOES16",
        help="The satellite that you want your data to come from.",
    )
    parser.add_argument(
        "--sensor",
        "-sens",
        type=str,
        default="ABI",
        help="The sensor of the satellite that you want your data to come from.",
    )
    parser.add_argument(
        "--sector",
        "-sect",
        type=str,
        default="RadF",
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
