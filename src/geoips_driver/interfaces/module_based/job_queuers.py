"""Python class for the job_queuers geoips_driver interface."""

from datetime import datetime, timedelta
import fnmatch
from glob import glob
import threading
import time

import dateparser
from jinja2 import Template

# import geoips.interfaces.base as GeoIPSPlugin # TODO: actually.... import the class lol
from geoips_driver.interfaces import monitor_configs
from geoips_driver.interfaces.module_based.data_monitors import FILE_FOUND_QUEUE, File
from geoips_driver.interfaces.module_based.service import (
    ServicePlugin,
    log_execution,
    setup_logging,
)
from geoips_driver.utils.driver_components import date_utils

logger = setup_logging()

JOB_READY_QUEUE = "JobReadyQueue"


class JobGroup:
    def __init__(self, job_name, config) -> None:
        self.name = job_name
        self.config = config
        self.jobs = {}
        self.num_expected_files = 0
        self.expected_files = []

    def ready_jobs(self):
        return [job for job in self.jobs.values() if job.ready()]

    def file_is_relevant(self, file):
        return file in self.expected_files

    def get_job_id_from_file(self, file: File):
        return file.file

    def add_file(self, file):
        """YEAAAA."""
        if not isinstance(file, File):
            file = File(file, hostname="localhost")

        if not self.file_is_relevant(file):
            return False
        jid = self.get_job_id_from_file(file)

        if jid not in self.jobs:
            self.jobs[jid] = Job(self.name, jid, self.config)
        self.jobs[jid].add_file(file.file)

        return True

    def _replace_monitor_configs_with_datetime(
        self, monitor_configs: list[dict], source_names: list[str], dt: datetime
    ) -> dict:
        """Replace the template paths from monitor configs with the correct datetime.

        Uses Jinja2's 'Template' class to replace relevant datetime portions of a file
        path with the correct values from the datetime provided.

        Parameters
        ----------
        monitor_configs: list[dict]
            - A list of relevant monitor configs used to query our file database. All
            monitor configs provided should be needed to produce the end product.
        source_names: list[str] or None
            - List of source names needed for this dispatcher. Corresponds to the source
            in which the data comes from. Essentially, this acts as a filter to a larger
            set of data files for the dispatcher. If None, no filtering will occur.
        dt: datetime
            - The current datetime to query against.

        Returns
        -------
        query_dict: dict
            - A dictionary containing all filepaths needed to produce a certain product
            and the number of total expected files needed to produce this product.
            Additionally, includes a 'sensor_filepath_mapping' key, whose value is a
            dictionary which contains all relevant file paths mapped to the satellite
            and sensor which captured them.
            - All of the filepaths in this dictionary have been translated to regex in
            order to use the regex utilities which postgres supports
        """
        replaced = []
        total_expected = 0
        sensor_filepath_mapping = {}

        for idx, mc in enumerate(monitor_configs):
            mc_name = list(mc.keys())[0]
            parent_dir = mc[mc_name][0]["parent_dir"]
            patterns = mc[mc_name][0]["patterns"]
            num_expected = mc[mc_name][0]["num_expected"]

            template_dir = Template(str(parent_dir))
            base_dir = template_dir.render(
                YYYY=str(dt.year),
                MM=str(dt.month),
                DD=str(dt.day),
                HH=str(dt.day),
                NN=str(dt.minute),
                JJJ=date_utils.calendar_to_julian(cal_dt=dt),
            )

            # Should be 'abi', 'ahi', or another sensor along those lines
            if mc_name.split("_")[-1] in source_names or source_names is None:

                fpaths_for_sensor = []
                regex_fpaths = []
                total_expected += num_expected

                for pattern in patterns:
                    template_pattern = Template(str(pattern))
                    filled_pattern = template_pattern.render(
                        YYYY=str(dt.year),
                        MM=str(dt.month),
                        DD=str(dt.day),
                        HH=str(dt.day),
                        NN=str(dt.minute),
                        JJJ=date_utils.calendar_to_julian(cal_dt=dt),
                    )
                    filled_path = "/".join([base_dir, filled_pattern])
                    fpaths_for_sensor += glob(filled_path)
                    regex_path = fnmatch.translate(filled_path)
                    regex_fpaths.append(regex_path)

                replaced += regex_fpaths
                sensor_filepath_mapping[mc_name] = fpaths_for_sensor

        query_dict = {
            "regex_fpatterns": replaced,
            "total_expected": total_expected,
            "sensor_filepath_mapping": sensor_filepath_mapping,
        }

        return query_dict


class Job:
    def __init__(self, job_name, jid, config):
        self.name = job_name
        self.identifier = jid
        self.config = config
        self.files = set()
        self.last_modified = time.time()
        self.timeout = 60 * 60 * 24  # 24 hours

    def ready(self):
        return False

    def add_file(self, file):
        self.files.add(file)
        self.last_modified = time.time()

    def is_old(self):
        return time.time() - self.timeout < self.last_modified


class JobReady(ServicePlugin):
    """Base data filter plugin."""

    def __init__(self, service):
        self.parent_service = service
        self.queue = JOB_READY_QUEUE
        self._running = False
        self.job_groups = []

    def parse_timedelta(self, text: str) -> timedelta:
        """Parse dateparser's natural language time strings like '15 min' into timedelta.  # NOQA

        Parameters
        ----------
        text: str
            - The dateparser natural language formatted string to represent as a
            datetime timedelta
        """
        # set an arbitrary 'anchor time'
        base = datetime(2000, 1, 1)
        # parse the incoming time relative to the anchor time we set
        parsed = dateparser.parse(text, settings={"RELATIVE_BASE": base})
        if not parsed:
            raise ValueError(f"Could not parse duration: {text}")
        # NOTE: dateparser interprets strings as relative to the past, which is why we
        # do base - parsed
        return base - parsed

    def get_aligned_time(
        self, now: datetime, cadence: timedelta, offset: timedelta
    ) -> datetime:
        """Return the nearest start time ≤ now given cadence and offset.

        I.e. if the time was 11:55, offset was 15 minutes, and cadence was 30 minutes,
        the aligned datetime would be 11:45 rather than 12:15.

        Parameters
        ----------
        now: datetime
            - The current datetime
        cadence: timedelta
            - The interval timedelta. I.e. timedelta between queries
        offset: timedelta
            - The offset timedelta from the top of the hour
        """
        # Determine today's first offset time (00:00 + offset)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        first_time = day_start + offset

        # Compute how many cadence intervals have passed since first_time
        delta_since_first = now - first_time
        intervals = int(delta_since_first.total_seconds() // cadence.total_seconds())

        # The aligned time is the most recent cadence multiple after the first offset
        aligned = first_time + intervals * cadence

        # If this aligned time is in the future (can happen if offset > now),
        # go back one cadence interval
        if aligned > now:
            aligned -= cadence

        return aligned

    def handle_timeout(
        self,
        now: datetime,
        succeed_time: datetime,
        timeout: timedelta,
        cadence: timedelta,
        offset: timedelta,
    ) -> tuple[datetime, bool]:
        """
        Handle timeout logic for the querier.

        If the time since the last success exceeds timeout, return the next
        cadence-aligned time. Otherwise, keep the same.

        Parameters
        ----------
        now: datetime
            - The current datetime.
        succeed_time: datetime
            - The datetime of the last successful query.
        timeout: timedelta
            - The timedelta waiting time before we just quit and iterate to the next
            cadence datetime value.
        cadence: timedelta
            - The interval timedelta. I.e. timedelta between queries.
        offset: timedelta
            - The offset timedelta from the top of the hour.

        Returns
        -------
        search_time: tuple[datetime, bool]
            - The time to query for. If 'bool' is False, this means that a timeout has
            not occurred yet. If 'bool' is True, this mean a timeout has occurred and a
            new search_time to query for has been set.
        """
        if (now - succeed_time) > timeout:
            # Get the cadence-aligned time based on the last succeed_time
            new_time = self.get_aligned_time(succeed_time, cadence, offset) + cadence
            return (new_time, True)

        return (succeed_time, False)

    def _set_monitor_configs_by_observation_area(self) -> None:
        """Generate a set of monitor configs for the JobReady class.

        These configs direct this class how to know what files are relevant and what are
        not.
        """
        # Collect a list of monitor config entries (not plugins)
        monitor_cfgs = (
            self.config.get("spec", {})
            .get("data_monitor", {})
            .get("arguments", {})
            .get("monitor_configs")
        )
        # Collect all of their observation areas
        obs_area_map = {
            mcfg.get("name", "no_name"): mcfg.get("arguments", {}).get("obs_area")
            for mcfg in monitor_cfgs
        }
        # Load all of the monitor config plugins
        mcfg_plgs = [
            monitor_configs.get_plugin(mcfg.get("name")) for mcfg in monitor_cfgs
        ]
        mcfg_obs_areas = []
        idx = 0
        # Grab only the observation areas requested
        for name, obs_areas in obs_area_map.items():
            plg = mcfg_plgs[idx]
            filtered_obs_areas = {name: []}
            for obs_area in obs_areas:
                filtered_obs_areas[name].append(plg["spec"]["obs_areas"][obs_area])
            idx += 1
            mcfg_obs_areas.append(filtered_obs_areas)

        self.monitor_configs = mcfg_obs_areas

    def _set_search_time(self, search_time: datetime | None = None) -> None:
        """Set a search time to compare files against.

        This search time determines what files are relevant for the JobReady class and
        the JobGroups it references.

        Parameters
        ----------
        search_time: datetime or None
            - A datetime to compare files against or None. If None and self.start_time
            is also None, set search time to the closest datetime calculated from the
            offset and cadence.
        """
        if self.start_time is None and search_time is None:
            self.search_time = self.get_aligned_time(
                datetime.now(), self.cadence_td, self.offset_td
            )
        elif self.start_time is None:
            self.search_time = self.start_time
        else:
            self.search_time = search_time

        if self.end_time and self.search_time > self.end_time:
            logger.debug(
                "The search time is greater than the end time. Ending this process."
            )
            self.stop()

    def initialize(self, config, search_time: datetime | None = None):
        """Initialize JobReady based on input parameters.

        Parameters
        ----------
        config: dict
            - A dictionary representation of a config file used to inform this class as
            to when one or more jobs are ready.
        search_time: datetime or None
            - A datetime to compare files against or None. If None and self.start_time
            is also None, set search time to the closest datetime calculated from the
            offset and cadence.
        """
        # Set initial parameters to correctly generate file paths that adhere to a
        # single datetime
        self.config = config
        self.start_time = self.config.get("spec", {}).get("start_time")
        self.end_time = self.config.get("spec", {}).get("end_time")
        self.cadence = self.config.get("spec", {}).get("cadence")
        self.offset = self.config.get("spec", {}).get("offset")
        self.timeout = (
            self.config.get("spec", {})
            .get("querier", {})
            .get("arguments", {})
            .get("timeout", "1 hr")
        )

        self.cadence_td = self.parse_timedelta(self.cadence)
        self.offset_td = self.parse_timedelta(self.offset)
        self.timeout_td = self.parse_timedelta(self.timeout)

        self._set_search_time(search_time)
        self._set_monitor_configs_by_observation_area()

        for job_group in self.job_groups:
            query_dict = job_group._replace_monitor_configs_with_datetime(
                self.monitor_configs, None, self.search_time
            )
            for sat_sensor, filepaths in query_dict.get(
                "sensor_filepath_mapping", {}
            ).items():
                job_group.num_expected_files += len(filepaths)
                job_group.expected_files += filepaths

    def emit(self, job) -> None:
        message = job.files
        logger.info(f"Queueing job with message {message}")
        self.parent_service.emit(queue=self.queue, message=message)

    def handle_incoming_files(self) -> None:
        """Listen to incoming files and mark job as ready when appropriate."""
        logger.debug("Starting to handle incoming files")
        for file in self.parent_service.consume(FILE_FOUND_QUEUE):
            logger.debug(f"Received file {file} from file queue")
            for idx, job_group in enumerate(self.job_groups):
                fpath = file.split(",")[0].split(" ")[1]
                logger.debug(f"FILE = {fpath}")

                if job_group.add_file(fpath):  # aka file added
                    for ready_job in job_group.ready_jobs():
                        self.emit(ready_job)
                elif job_group.is_old():
                    self.job_groups.remove(job_group)
                    self.initialize(
                        self.config, search_time=self.search_time + self.cadence_td
                    )
                    self.job_groups.append(job_group)

    @log_execution
    def start(self) -> None:
        if self._running:
            return
        else:
            self._running = False
        self._main_thread = threading.Thread(
            target=self.handle_incoming_files,
            name=self.name,
            daemon=True,
        )
        self._running = True
        self._main_thread.start()
        return

    @log_execution
    def stop(self) -> None:
        """Stop main thread."""
        if self._main_thread and self._main_thread.is_alive():
            self._main_thread.join(timeout=5)


def call():
    raise NotImplementedError("You cannot call this plugin directly.")
