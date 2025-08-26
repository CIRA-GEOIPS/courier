"""Daemon which listens for arriving data performs automatic processing on arrival.

The daemon is built using 'watchdog' and can either submit jobfiles for arriving data
or execute a set of processes in parallel if multiprocessing is selected.
"""

import argparse
import logging
import os
import time
from datetime import UTC, datetime

from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver

from geoips_driver.algorithm_info import (
    NewJulianDateException,
    algorithms,
    calendar_to_julian,
)
from geoips_driver.driver_components import ProcessSpawner

LOG = logging.getLogger(__name__)


class OvercastRunner(ProcessSpawner):
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
            curr_jdate = calendar_to_julian()
            if (
                int(curr_jdate) > int(julian_date)
                # Offset the day by 3 hours (hr 02 today - hr 23 prev day)
                # as there is a delay of about 2hr 40 min for the data to come in
                and datetime.now(UTC).hour >= 2
            ):
                # Directory was never created. Most likely caused by data outages from
                # one or more satellites. Raise a julian date exception and move
                # on to the next date.
                raise NewJulianDateException(
                    f"Directory {self.watch_directory} was never created and a new "
                    "day has started. Exiting to watch the next directory.",
                )
            else:
                print(
                    f"Waiting for data directory {self.watch_directory} to be created.",
                )
                time.sleep(30)
        self.sector = self.alg_info.sector_mapping[sector]

        if self.use_slurm:
            self.outdir = self.alg_info.slurm_dir
        else:
            self.outdir = self.alg_info.mp_dir
        # Initialize the rest of the components from the parent FileSystemEventHandler
        FileSystemEventHandler().__init__()

    def on_created(self, event):
        """If a file in 'watch_directory' was created, call this func.

        If the created source path was an actual file, not a directory, then execute
        OVERCAST GEOring_3d processing.

        Parameters
        ----------
        event: FileSystemEvent
            - An event caught on the file system being watched.
        """
        # Called when a file is created
        file_path = event.src_path
        print(f"EVENT TYPE = {event.event_type}")
        print(f"EVENT PATH = {file_path}")
        # This conditional filters out files we don't want
        if not event.is_directory:
            print(f"Detected new file: {file_path}")
            fully_written = False
            while not fully_written:
                print("Waiting for file be fully written to disc...")
                fully_written = self.file_fully_written(file_path)

            # Kick of your queue of jobs or multiprocesses
            self.spawn_processes(
                fpath=file_path,
                template_name="unprojected_georing_template",
                sync=True,
            )


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
    event_handler = OvercastRunner(
        algorithm,
        sat,
        sensor,
        sector,
        starting_jdate,
        use_slurm=use_slurm,
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
            if (
                int(curr_jdate) > int(starting_jdate)
                # Offset the day by 3 hours (hr 02 today - hr 23 prev day)
                # as there is a delay of about 2hr 40 min for the data to come in
                and datetime.now(UTC).hour >= 2
            ):
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
    GEOring_3d products for each data file via geoips and geoips_clavrx.

    If the daemon crashes for some reason, catch that failure, wait 5 seconds, and
    attempt to restart it.
    """
    parser = argparse.ArgumentParser("NASWatcher for automating GeoIPS Processing")
    parser.add_argument(
        "--algorithm",
        "-alg",
        type=str,
        default="GEORING",
        choices=list(algorithms.keys()),
        help="The algorithm that you want your data to come from.",
    )
    parser.add_argument(
        "--satellite",
        "-sat",
        type=str,
        default="GEORING",
        help="The satellite that you want your data to come from.",
    )
    parser.add_argument(
        "--sensor",
        "-sens",
        type=str,
        default="GEORING",
        help="The sensor of the satellite that you want your data to come from.",
    )
    parser.add_argument(
        "--sector",
        "-sect",
        type=str,
        default="ALL",
        help="The sector that you want your data to come from.",
    )
    ARGS = parser.parse_args()
    alg, sat, sensor, sector = ARGS.algorithm, ARGS.satellite, ARGS.sensor, ARGS.sector
    print("Initializing NASWatcher")
    while True:
        try:
            start_watching(alg, sat, sensor, sector, use_slurm=False)
        except Exception and NewJulianDateException as e:
            if type(e).__name__ != "NewJulianDateException":
                LOG.exception(f"Watcher crashed with error: {e}")
            else:
                print(e)
            time.sleep(5)  # Wait a bit before restarting


if __name__ == "__main__":
    main()
