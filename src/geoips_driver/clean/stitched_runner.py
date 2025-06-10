"""Daemon which produces GeoIPS Stitched imagery.

Assumes that stitched data will be produced from a set of data stemming from different
satellite sensors.
"""

import argparse
import logging
import os
import shutil
import time

from geoips_driver.algorithm_info import (
    algorithms,
    calendar_to_julian,
    curr_calendar_date,
    NewJulianDateException,
)

from geoips_driver.driver_components import FileLocator, ProcessSpawner


LOG = logging.getLogger(__name__)


class StitchedRunner(ProcessSpawner):
    """Daemon which produces GeoIPS NRT Stitched outputs.

    Searches through multiple directories (denoted by satellite and sensor) for data
    closest to the nearest 30 minute mark. Once data from all satellites has arrived,
    copy the data over to a consolidated location to perform GeoIPS processing on.

    Submits jobfiles via slurm or executes multiple processes in parallel using python
    multiprocess for GeoIPS-based processing. This will likely need to be adjusted based
    on the hardware in which you are running on.
    """

    last_processed_hhnn = None

    def __init__(self, stitched_object, use_slurm=True) -> None:
        """Initialize the runner start watching for data coming from 1+ directories.

        Parameters
        ----------
        stitched_object: str
            - The stitched_object that denotes what product and data files are needed to
              produce continuous output.
        use_slurm: bool (default = True)
            - Whether or not we want to use slurm-based automated processing. If False,
              we'll use multiprocessing to automate.
        """
        if stitched_object not in algorithms:
            raise NotImplementedError(
                f"Object '{stitched_object}' hasn't been implemented in "
                "geoips_driver.algorithm_info:algorithms. Please create an info "
                f"container for '{stitched_object}' before instantiating a watcher for "
                "it."
            )
        self.alg_info = algorithms[stitched_object]
        self.use_slurm = use_slurm
        if self.use_slurm:
            self.outdir = self.alg_info.slurm_dir
        else:
            self.outdir = self.alg_info.mp_dir

        # prev_hhnn = nearest_half_hour_utc()
        calendar_date = curr_calendar_date()
        jdate = calendar_to_julian()[-3:]
        year = calendar_date[0:4]
        month = calendar_date[4:6]
        day = calendar_date[6:8]
        prev_hhnn = "0000"
        date_dict = {
            "jdate": jdate,
            "year": year,
            "month": month,
            "day": day,
            "hhnn": prev_hhnn,
        }
        # hh = prev_hhnn[:2]
        # nn = prev_hhnn[4:]
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
                    self.start_processing(date_dict)
                except FileNotFoundError:
                    print(
                        f"All required files for timestep {next_hhnn} weren't found. "
                        "Skipping to next timestep."
                    )

                prev_hhnn = next_hhnn
                hh = prev_hhnn[:2]
                nn = prev_hhnn[2:]

                if nn == "30":
                    hh = str((int(hh) + 1) % 24).zfill(2)
                    nn = "00"
                    if hh == "00":
                        raise NewJulianDateException(
                            "New date has started. Reinitialize this runner."
                        )
                else:
                    nn = "30"
                next_hhnn = f"{hh}{nn}"
                date_dict["hhnn"] = next_hhnn
            # next_hhnn = nearest_half_hour_utc()

    def start_processing(self, date_dict) -> None:
        """Kick off processing of Stitched imagery for arriving data.

        Parameters
        ----------
        date_dict: dict
            - Dictionary of information pertaining to the datetime that we want to
              operate on. Formatted {jdate, year, month, day, hhnn}, where all
              of those keys are string values representing their corresponding time.
        """
        times_searched = 0
        finfo = self.get_file_info(date_dict)
        self.fl = FileLocator(finfo)
        ffound = self.fl.all_files_found()
        while not ffound:
            print(f"Waiting for data to be transferred to a temporary directory.")
            print(f"{(times_searched * 30) / 60} minutes elapsed.")
            time.sleep(30)
            ffound = self.fl.all_files_found()
            times_searched += 1
            if times_searched >= 121:
                # We've been searching for longer than an hour. This means the files
                # we're expecting probably will not arrive. Raise an error and search
                # for the next time step.
                raise FileNotFoundError(
                    "Process has been searching for required hours for over an hour and"
                    " they haven't been found. Resetting search to next timestep."
                )

        self.required_filepaths = self.fl.required_filepaths
        # Copy all files over to a temporary directory
        for fpath in self.required_filepaths:
            shutil.copy(fpath, self.watch_directory)
        # Make sure the files have copied over fully
        time.sleep(6)
        # Spawn your processes!
        self.spawn_processes()
        # Clean up output directory once processes have finished
        for fname in os.listdir(self.watch_directory):
            os.remove(f"{self.watch_directory}{fname}")

    def get_file_info(self, date_dict) -> dict:
        """Return a dictionary of file information used to locate files needed.

        Where the dictionary takes on the form (can be repeated):
        {keyX: {'searchdir': fpath, 'fpatterns': list(str), 'num_expected_files': int}}

        Parameters
        ----------
        date_dict: dict
            - Dictionary of information pertaining to the datetime that we want to
              operate on. Formatted {jdate, year, month, day, hhnn}, where all
              of those keys are string values representing their corresponding time.
        """
        jdate = date_dict["jdate"]
        year = date_dict["year"]
        month = date_dict["month"]
        day = date_dict["day"]
        hhnn = date_dict["hhnn"]
        # If this watcher was just initialized and the watch directory doesn't exist,
        # wait for the directory to be created before initializing the actual watcher.
        finfo = self.alg_info.finfo

        def date_fill(val) -> str:
            """Replace date specific strings with the datetimes values above.

            Replacing "YYYY" with year, "MM", with month, "DD" with day,
            "HHNN" with hhnn, "HH" with hhnn[0:2], "JJJ" with jdate.

            Parameters
            ----------
            val: str
                - The string to replace date specific strings with.

            Returns
            -------
            filled: str
                - Filled string with correct datetime strings.
            """
            filled = (
                str(val)
                .replace("YYYY", year)
                .replace("MM", month)
                .replace("DD", day)
                .replace("HHNN", hhnn)
                .replace("HH", hhnn[0:2])
                .replace("JJJ", jdate)
            )
            return filled

        """
        NOTE: Each entry in finfo should look something like this.
        "GOES16": {
            "searchdir": f"/mnt/sat/grb/goes16/YYYY/YYYY_MM_DD_JJJ/abi/L1b/RadF",
            "fpatterns": [f"*M6C13*sYYYYJJJHHNN*"],
            "num_expected_files": 1,
        },
        """

        for key, val in finfo.items():
            for i, j in val.items():
                if isinstance(j, str):
                    finfo[key][i] = date_fill(j)
                elif isinstance(j, list):
                    filled_vals = []
                    for x in j:
                        # Assumes all elements of j are a string
                        filled_vals.append(date_fill(x))
                    finfo[key][i] = filled_vals
                else:
                    continue

        return finfo


def start_watching(stitched_object, use_slurm) -> None:
    """Start up the daemon which produces Stitched GeoIPS output.

    This can be data stemming from the GeoRing of satellites or a subset of those
    satellites.

    Parameters
    ----------
    stitched_object: str
        - The name (case sensitive) of the Stitched Object Class which will provide
          information on how to produce your stitched output.
    use_slurm: bool (default = True)
        - Whether or not we want to use slurm-based automated processing. If False,
            we'll use multiprocessing to automate.
    """
    StitchedRunner(stitched_object, "ALL", "ALL", "ALL", use_slurm=use_slurm)


def main() -> None:
    """Start up the daemon and begin watching for data.

    When data is found, create and submit slurm jobfiles or execute bash scripts which
    will produce Stitched-Infrared Imagery on the overcast_georing grid.

    If the daemon crashes for some reason, catch that failure, wait 5 seconds, and
    attempt to restart it.
    """
    parser = argparse.ArgumentParser("NASWatcher for automating GeoIPS Processing")
    parser.add_argument(
        "stitched_object",
        type=str,
        default="StitchedInfrared",
        help=(
            "The object which contains information about the stitched outputs that "
            "you want to create."
        ),
    )
    parser.add_argument(
        "--slurm",
        default=False,
        action="store_true",
        help=(
            "The object which contains information about the stitched outputs that "
            "you want to create."
        ),
    )
    ARGS = parser.parse_args()
    so = ARGS.stitched_object
    use_slurm = ARGS.slurm
    print("Initializing NASWatcher")
    while True:
        try:
            start_watching(so, use_slurm)
        except Exception and NewJulianDateException as e:
            if type(e).__name__ != "NewJulianDateException":
                LOG.error(f"Watcher crashed with error: {e}")
            else:
                print(e)
            time.sleep(5)  # Wait a bit before restarting


if __name__ == "__main__":
    main()
