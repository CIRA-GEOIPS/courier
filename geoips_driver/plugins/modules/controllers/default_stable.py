"""Daemon which produces GeoIPS Stitched imagery.

Assumes that stitched data will be produced from a set of data stemming from different
satellite sensors.
"""

import logging
import os
import shutil
import time
from datetime import datetime

from geoips.errors import PluginError

from geoips_driver.algorithm_info import (
    NewJulianDateException,
    calendar_to_julian,
    curr_calendar_date,
)
from geoips_driver.clean.driver_components import (
    DriverUtilities,
    FileLocator,
    ProcessSpawner,
)
from geoips_driver.interfaces import controller_configs as driver_configs

LOG = logging.getLogger(__name__)

interface = "drivers"
name = "default_stable"
family = "obp"


class DefaultDriver(ProcessSpawner):
    """Daemon which produces GeoIPS NRT outputs.

    Searches through one or more directories (denoted by satellite and sensor) for data
    closest to the provided cadence increment. Once data from requested sources has
    arrived, begin GeoIPS processing.

    Submits jobfiles via slurm or executes multiple processes in parallel using python
    multiprocess for GeoIPS-based processing. This will likely need to be adjusted based
    on the hardware in which you are running on.
    """

    last_processed_hhnn = None
    utils = DriverUtilities()

    def __init__(self, driver_config, start_time) -> None:
        """Initialize the runner to start watching for data coming from 1+ directories.

        Parameters
        ----------
        driver_config: GeoIPS driver_config yaml plugin
            - GeoIPS yaml plugin which describes the configuration for locating, processing,
            files, additionally specifying what products we want to create and how we
            should output them.
        start_time: Datetime object or None
            - If specified, begin your search for files and the specified time. If None,
            search the current calendar date starting at 0000 UTC.
            - Formatted: YYYYMMDDHHNN
        """
        try:
            dc = driver_configs.get_plugin(driver_config)
        except PluginError:
            raise NotImplementedError(
                f"No driver_config plugin under name '{driver_config}' could be found. "
                "If you're positive this plugin exists, please run "
                "'create_plugin_registries'.",
            )
        dc = self.utils.dict_to_namespace(dc)
        self.dispatchers = list(dc.spec.dispatchers)
        self.data_monitor = dc.spec.data_monitor.plugin

        cadence = self.dispatchers[0].arguments.cadence
        self.outdir = "/some/path"

        date_dict = self.get_starting_date_dict(start_time)
        datetime(
            date_dict["year"],
            date_dict["month"],
            date_dict["day"],
            hour=date_dict["hhnn"][:2],
            minute=date_dict["hhnn"][2:],
        )
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
                        "Skipping to next timestep.",
                    )

                prev_hhnn = next_hhnn
                hh = prev_hhnn[:2]
                nn = prev_hhnn[2:]
                # Try to make this more complex. It's pretty linear right now.
                if int(nn) + cadence >= 60:
                    # Hour has passed. Now we need to determine if it's one or more.
                    new_nn = int(nn) + cadence
                    hrs_passed = new_nn // 60
                    new_hh = int(hh) + hrs_passed
                    if new_hh >= 24:
                        datetime(
                            date_dict["year"],
                            date_dict["month"],
                            date_dict["day"],
                            hour=date_dict["hhnn"][:2],
                            minute=date_dict["hhnn"][2:],
                        )
                        raise NewJulianDateException(
                            "New date has started. Reinitialize this runner.",
                        )
                    hh = str((new_hh) % 24).zfill(2)
                    nn = str((new_nn) % 60).zfill(2)
                next_hhnn = f"{hh}{nn}"
                date_dict["hhnn"] = next_hhnn
            # next_hhnn = nearest_half_hour_utc()

    def get_starting_date_dict(self, start_time=None) -> dict:
        """Generate a dictionary of info about the start time of files to search for.

        Parameters
        ----------
        start_time: Datetime Object (Default=None)
            - The start datetime to begin our polling at. Should be formatted
              YYYYMMDDHHNN.
        """
        if start_time is None:
            # prev_hhnn = nearest_half_hour_utc()
            calendar_date = curr_calendar_date()
            jdate = calendar_to_julian()[-3:]
            year = calendar_date[0:4]
            month = calendar_date[4:6]
            day = calendar_date[6:8]
            prev_hhnn = "0000"
        else:
            year = str(start_time.year)
            month = str(start_time.month).zfill(2)
            day = str(start_time.day).zfill(2)
            hh = str(start_time.hour).zfill(2)
            nn = str(start_time.minute).zfill(2)
            jdate = calendar_to_julian(cal_dt=datetime(year, month, day))
            prev_hhnn = f"{hh}{nn}"

        date_dict = {
            "jdate": jdate,
            "year": year,
            "month": month,
            "day": day,
            "hhnn": prev_hhnn,
        }
        return date_dict

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
            print("Waiting for data to be transferred to a temporary directory.")
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
                    " they haven't been found. Resetting search to next timestep.",
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


def call(driver_config, start_time=None) -> None:
    """Start up the daemon and begin watching for data.

    When data is found, create and submit slurm jobfiles or execute bash scripts which
    will produce GeoIPS output based on the input driver_config plugin.

    If the daemon crashes for some reason, catch that failure, wait 5 seconds, and
    attempt to restart it.

    Parameters
    ----------
    driver_config: GeoIPS driver_config yaml plugin
        - GeoIPS yaml plugin which describes the configuration for locating, processing,
          files, additionally specifying what products we want to create and how we
          should output them.
    start_time: str, (Default=None)
        - If specified, begin your search for files and the specified time. If None,
          search the current calendar date starting at 0000 UTC.
        - Formatted: YYYYMMDDHHNN
    """
    if start_time:
        if (
            not isinstance(start_time, str)
            or len(start_time) != 12
            or not all(char.isdigit() for char in start_time)
        ):
            raise TypeError(
                f"Error: Argument 'start_time'={start_time} was provided but did not "
                "meet the format required to generate a valid datetime object. Please "
                "provide a string of only digits formatted YYYYMMDDHHNN.",
            )
        year = start_time[:4]
        month = start_time[4:6]
        day = start_time[6:8]
        hh = start_time[8:10]
        nn = start_time[10:]
        start_time = datetime(year, month, day, hh, nn)
    print("Initializing NASWatcher")
    while True:
        try:
            DefaultDriver(driver_config, start_time)
        except Exception and NewJulianDateException as e:
            if type(e).__name__ != "NewJulianDateException":
                LOG.exception(f"Watcher crashed with error: {e}")
                # NOTE: Implement logic here to calculate the new start time based on
                # the cadence provided in the driver config.
            else:
                print(e)
            time.sleep(5)  # Wait a bit before restarting
