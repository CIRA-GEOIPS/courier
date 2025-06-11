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
from pathlib import Path

from jinja2 import Environment, FileSystemLoader
from pyhdf.SD import SD, SDC
from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver

# The import below does not work because the data we are watching is on a NAS drive.
# Apparently, Observers only work on local disks. However, a PollingObserver works on
# NAS drives, and is just a little less efficient, to the point where the difference is
# neglegible. Works for our use case!
# from watchdog.observers import Observer
from geoips_driver.algorithm_info import (
    NewJulianDateException,
    algorithms,
    calendar_to_julian,
)

LOG = logging.getLogger(__name__)


class NASWatcher(FileSystemEventHandler):
    """Daemon which watches the given directory and submits job files upon arrival.

    Submits jobfiles via slurm or executes multiple processes in parallel using python
    multiprocess for GeoIPS-based processing. This will likely need to be adjusted based
    on the hardware in which you are running on.
    """

    def __init__(self, algorithm, sat, sensor, sector, julian_date, use_slurm=True):
        """Start up the NASWatcher to listen for arriving data at 'watch_directory'.

        Where 'watch_directory' is set based on the provided parameters. This will need
        to be adjusted based on where data is arriving on the machine you are watching.

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
        julian_date: str
            - The current julian date in which we are runnign this NASWatcher
            - Formatted: {year}{julian_date}
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
        self.max_cpu_count = os.cpu_count() // 4
        self.alg_info = algorithms[algorithm]
        self.use_slurm = use_slurm
        # NOTE: most likely need to add a check here to ensure that
        # sat, sensor, and sector are valid keys to this information dictionary
        self.watch_directory = (
            f"{self.alg_info.paths[sat][sensor][sector]}/{julian_date}/"
        )
        # If this watcher was just initialized and the watch directory doesn't exist,
        # wait for the directory to be created before initializing the actual watcher.
        while not os.path.exists(self.watch_directory):
            print(f"Waiting for data directory {self.watch_directory} to be created.")
            time.sleep(30)
        self.sector = self.alg_info.sector_mapping[sector]

        if self.use_slurm:
            self.outdir = self.alg_info.slurm_dir
        else:
            self.outdir = self.alg_info.mp_dir
        # Initialize the rest of the components from the parent FileSystemEventHandler
        super().__init__()

    def file_fully_written(self, fpath):
        """Determine if a file has been fully written to disk.

        Parameters
        ----------
        fpath: str
            - The path to the file we're checking to see if it's fully written.
        """
        # This seems to work for the time being. What's annoying about these Observer
        # classes is that they both recognize that a file has arrived in a directory
        # before it is necessarily fully written to disk. This is a workaround that
        # checks for file size after each minute, and if those match, then attempt to
        # read the attributes of the file as those generally are written last.
        initial_size = os.path.getsize(fpath)
        time.sleep(60)
        next_size = os.path.getsize(fpath)

        # Note that this is using pyhdf and will need to be changed if your file type
        # is of a different file format
        return bool(initial_size == next_size and len(SD(fpath, SDC.READ).attributes()))

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
        # NOTE: This will only work for GOES clavrx files right now. Need to add new
        # functionality which checks for the 'correct' file depending on algorithm,
        # satellite, sensor, and sector.
        print(f"EVENT TYPE = {event.event_type}")
        print(f"EVENT PATH = {file_path}")
        # This conditional filters out files we don't want
        if not event.is_directory and "ML" not in bname and pl_path.suffix == ".hdf":
            print(f"Detected new file: {file_path}")
            fully_written = False
            while not fully_written:
                print("Waiting for file be fully written to disc...")
                fully_written = self.file_fully_written(file_path)

            # Kick of your queue of jobs or multiprocesses
            self.start_processing(file_path)

    def start_processing(self, fpath):
        """Execute a series of processes using data found at file_path.

        If self.use_slurm is True, Submit a series jobfiles to slurm via 'sbatch' for
        the incoming CLAVR-X file. Otherwise, use multiprocessing to execute your
        processes in parallel.

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
        fname = os.path.basename(fpath)

        for output_type in self.alg_info.output_types:
            # Produce both imagery_annotated and imagery_clean outputs
            script_paths = []
            for product_name in self.alg_info.product_names:
                # Do this for a set of CLAVR-X products that we want to produce
                script_path = self.create_clavrx_script(
                    fpath,
                    product_name,
                    output_type,
                )
                if self.use_slurm:
                    # Create, submit, and the job using sbatch (SLURM)

                    # NOTE: I haven't yet tested this as I wasn't positive on the
                    # resource allocation I should provide for each job
                    self.run_jobfile(fname, fpath, script_path)
                else:
                    script_paths.append(script_path)
                # NOTE: No need to remove the bash script after it has been executed, as
                # the are overwritten for each file.
            if not self.use_slurm:
                self.parallel_process(script_paths, output_type)

    def create_clavrx_script(self, fpath, product_name, output_type):
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
        """
        # Load the Jinja2 template
        env = Environment(loader=FileSystemLoader("."))
        # Using clavrx template, replace if using a template for a different type of
        # product
        template = env.get_template("templates/geoips_clavrx_template.j2")

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
            "sector": self.sector,
        }

        # Render the template with the context
        clavrx_bash_script = template.render(context)
        script_path = f"{self.outdir}/temp_scripts/{product_name}_{output_type}.sh"

        # Write the rendered template to a file
        with open(script_path, "w") as f:
            f.write(clavrx_bash_script)

        # Modify the file permissions to be executable
        subprocess.run(["chmod", "+x", script_path], check=True)

        print(f"CLAVR-X Bash Script '{product_name}_{output_type}.sh' was created.")
        return script_path

    def parallel_process(self, script_paths, output_type):
        """Execute a set of processes in parallel for each script in 'script_paths'.

        Parameters
        ----------
        script_paths: list[str]
            - A list of file paths that are associated with GeoIPS bash scripts used
              for processing
        output_type: str
            - The name of the output_formatter plugin we'll use in GeoIPS
        """
        with Pool(
            processes=min(len(self.alg_info.product_names), self.max_cpu_count),
        ) as pool:
            # Execute your GeoIPS bash scripts in parallel
            results = pool.map(
                self.run_bash_script,
                script_paths,
            )
            # Save outputs to individual files
            for i, result in enumerate(results):
                product_name = self.alg_info.product_names[i]
                # Create a unique output file name
                output_file = (
                    f"{self.outdir}/output/output_{product_name}_{output_type}.log"
                )
                # Write the output log to disk
                with open(output_file, "w") as f:
                    f.write(result)
                    print(
                        f"Output for script {product_name}_{output_type} saved "
                        f"to {output_file}",
                    )

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
    starting_jdate = calendar_to_julian()
    # Need a julian date as that is the format of directory names for GOES-CLAVR-x data
    event_handler = NASWatcher(
        algorithm, sat, sensor, sector, starting_jdate, use_slurm=use_slurm,
    )
    print(f"Started watching directory: {event_handler.watch_directory}")
    observer = PollingObserver()
    observer.schedule(event_handler, event_handler.watch_directory, recursive=True)
    observer.start()
    # NOTE: Using a polling observer as the normal observer
    # (which uses inotify under the hood) did not capture file system events w/ a NFS
    print("Started polling observer.")

    try:
        while observer.is_alive():
            time.sleep(1)
            curr_jdate = calendar_to_julian()
            if int(curr_jdate) > int(starting_jdate):
                starting_jdate = curr_jdate
                observer.stop()
                observer.join()
                raise NewJulianDateException(
                    f"Reinitializing NASWatcher for julian date = {starting_jdate}.",
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
        default="CLAVRX",
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
        default="RadC",
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
