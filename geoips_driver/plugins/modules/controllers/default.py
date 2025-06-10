"""Daemon which produces GeoIPS Stitched imagery.

Assumes that stitched data will be produced from a set of data stemming from different
satellite sensors.
"""

from datetime import datetime
from importlib.resources import files
import logging
import multiprocessing as mp
import time

import sqlite3 as sql

from geoips_driver.algorithm_info import NewJulianDateException
from geoips_driver.driver_components import ProcessSpawner, DriverUtilities

from geoips.errors import PluginError
from geoips_driver import interfaces

LOG = logging.getLogger(__name__)

interface = "controllers"
name = "out_of_date"
family = "obp"

# TODO: Develop interfaces for all of the new plugin types


class DispatcherError(Exception):
    """Error used when a dispatcher has encountered some exception.

    Where an exception could be missing files for some product, runtime error
    encountered via the dispatcher, etc.
    """

    pass


class DrivingConcluded(Exception):
    """Exception used when a dispatcher will not dispatch any more jobs.

    This should only be used when you've set a start datetime and end datetime to
    process within, and that you've processed all of the files within that time period
    matching your dispatcher's cadence.
    """

    pass


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

    def __init__(self, driver_config, start_time, end_time) -> None:
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
            dc = interfaces.driver_configs.get_plugin(driver_config)
        except PluginError:
            raise NotImplementedError(
                f"No driver_config plugin under name '{driver_config}' could be found. "
                "If you're positive this plugin exists, please run "
                "'create_plugin_registries'."
            )
        dc = self.utils.dict_to_namespace(dc)
        dispatchers = [plg for plg in dc.spec.dispatchers]
        data_monitor = dc.spec.data_monitor.plugin

        db_path = self._create_db(driver_config)

        data_monitor = interfaces.get_plugin(data_monitor.name)(
            db_path=db_path,
            **data_monitor.arguments,
        )

        # NOTE: Need to determine what this really consitutes. This could be that we've
        # found every file within some time period, but could also be that we've
        # finished processing every iteration of our cadence within some time period,
        # even though arriving files may persist.

        # Also, should the data monitor be doing this, or is this the drivers' job?
        # Need to talk with Jeremy about this.
        while data_monitor.operating_within_search_space():
            for dispatcher in dispatchers:
                try:
                    dispatcher(
                        files=self._filtered_by_source_names(
                            data_monitor.files,
                            dispatcher.arguments.source_names,
                        ),
                        time_window=(data_monitor.start_time, data_monitor.end_time),
                        **dispatcher.arguments,
                    )
                except DispatcherError as e:
                    LOG.warning(str(e))
                    continue
            # Wait 15 seconds before trying again
            time.sleep(15)

    def _filtered_by_source_names(self, files, source_names) -> dict:
        """Return a dictionary whose keys match those in source_names.

        For example, if I had a dictionary with keys A-Z, and a list whose values were
        [C, G, I, X], return a new dictionary who's keys are [C, G, I, X] and nothing
        more.

        Parameters
        ----------
        files: dict
            - A dictionary of files which come from satellite_sensor sources.
        source_names: list(str)
            - A list of source names when denote the sensors needed to produce a certain
              output.
        """
        filtered_files = {}
        for key in list(files.keys()):
            for sn in source_names:
                if sn in key:
                    filtered_files[key] = files[key]
        return filtered_files

    def _create_db(self, driver_config_name) -> str:
        """Create an sqlite database for file monitoring.

        Dispatchers will make use of the database filled by data_monitor plugins to
        determine whether or not they have the correct data to spawn a process.

        Parameters
        ----------
        driver_config_name: str
            - The name of the driver config plugin this driver is operating on.

        Returns
        -------
        db_path: str
            - The path to the database that was created
        """
        # Build a database for file searching
        db_path = str(files("geoips_driver") / "databases")
        conn = sql.connect(f"{db_path}/{driver_config_name}.db")
        cursor = conn.cursor()
        # Create a table for that database including the following columns
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS {} (
                _id INTEGER PRIMARY KEY AUTOINCREMENT,
                fpath TEXT NOT NULL,
                start_time DATETIME NOT NULL,
                end_time DATETIME NOT NULL,
                satellite TEXT NOT NULL,
                sensor TEXT NOT NULL,
                obs_area TEXT NOT NULL,
                product_name TEXT NOT NULL
            )
            """.format(
                driver_config_name
            )
        )
        conn.commit()
        conn.close()
        return db_path


# Product Name could look like: L1b-RadF-M6C0


def call(driver_config, start_time=None, end_time=None) -> None:
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
        - If specified, override the start_time argument of your data_monitor plugin
          (found in driver_config plugin). If not specified here, default to the
          start_time value specified in your data_monitor plugin. If not specified
          there, search for the current calendar date starting at 0000 UTC.
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
                "provide a string of only digits formatted YYYYMMDDHHNN."
            )
        start_time = datetime.strptime(start_time, "%Y%m%d%H%M")
    print("Initializing NASWatcher")
    while True:
        try:
            DefaultDriver(driver_config, start_time)
        except Exception or NewJulianDateException or DrivingConcluded as e:
            if type(e).__name__ != "NewJulianDateException":
                LOG.error(f"Watcher crashed with error: {e}")
                # NOTE: Implement logic here to calculate the new start time based on
                # the cadence provided in the driver config.
            if type(e).__name__ != "DrivingConcluded":
                LOG.info(f"Concluded driving {driver_config}'s processing.")
                break
            else:
                print(e)
            time.sleep(5)  # Wait a bit before restarting
