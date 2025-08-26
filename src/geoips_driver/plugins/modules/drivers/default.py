"""Default driver plugin module.

Takes in a querier plugin and dispatcher plugin used to drive NRT processing making use
of GeoIPS.
"""

from datetime import datetime, timedelta

from geoips_driver.clean.driver_components import date_utils
from geoips_driver.geoips_driver_utils import monitor_configs_to_finfo
from geoips_driver.interfaces import dispatchers, queriers

interface = "drivers"
name = "default"
family = "standard"


def convert_dateparser_string_to_timedelta(dp_str):
    """Convert a dateparser formatted string to a timedelta object.

    Parameters
    ----------
    dp_str: str
      - A dateparser formatted string. I.e. '30 min' '2 hr' '3 days'...

    Returns
    -------
    td: Timedelta object
      - A timedelta object that can be added or subtracted from a datetime object.
    """
    if " " not in dp_str:
        raise ValueError(
            "Error: Dateparser string must be formatted '<val> <unit>'. Please include "
            "a space in this string.",
        )
    val, unit = dp_str.split(" ")
    val = int(val)
    if "min" in unit:
        td = timedelta(minutes=val)
    elif "hr" or "hour" in unit:
        td = timedelta(hours=val)
    elif "day" in unit:
        td = timedelta(days=val)
    else:
        raise ValueError(
            "Error: cannot parse timeout string provided. Please use minutes, hours, or"
            "days as a unit for timeout.",
        )
    return td


# TODO: Figure out logic for cadence and offset
def get_starting_query_time(offset, custom_datetime=None):
    """Generate a datetime instance representing the appropriate time to query for.

    Parameters
    ----------
    offset: str
      - How many minutes off the top of the hour we should offset our datetime by.
        Formatted using dateparser's natural language format. See
        https://dateparser.readthedocs.io/en/latest/index.html for more info.
    custom_datetime: Datetime Object, default=None
      - If supplied, this represents a custom datetime to start querying for. This
        should only be used if you're trying to run processes for files that have
        already been created.

    Returns
    -------
    dt: Datetime object
      - The initial datetime the driver start querying for.
    """
    if isinstance(custom_datetime, datetime):
        yyyymmdd = f"{custom_datetime.year}{custom_datetime.month}{custom_datetime.day}"
        hhnn = f"{custom_datetime.hour}00"
    else:
        yyyymmdd = date_utils.curr_calendar_date()
        hhnn = date_utils.nearest_half_hour_utc()[:2] + "00"
    dt = datetime.strptime(f"{yyyymmdd}{hhnn}", "%Y%m%d%H%M") + timedelta(
        minutes=int(offset.split(" ")[0]),
    )
    return dt


def call(querier, dispatcher, monitor_configs, cadence, offset, start_dt, end_dt, port):
    """Drive GeoIPS NRT processing using the specified querier and dispatcher.

    Where the driver communicates with one or more data monitors on the port provided.

    Parameters
    ----------
    querier: pydantic-based Querier Object
      - An object containing information on what querier plugin to use, as well
        as the search parameters that querier will need to know to search an
        information storage system appropriately.
    dispatcher: pydantic-based Dispatcher Object
      - An object containing information on what dispatcher plugin to use, as
        well as additional arguments needed to appropriately spawn a job/process
        for GeoIPS to run
    monitor_configs: list[dict]
      - A list of dictionaries which that represent monitor config plugins. These will
        be used to construct a file_information dictionary to query the data monitor.
    cadence: str
      - How often a job should be dispatched. Formatted using dateparser's natural
        language format. See https://dateparser.readthedocs.io/en/latest/index.html
        for more information.
    offset: str
      - Time offset from the top of the hour to dispatch a process at. Formatted using
        dateparser's natural language format. See
        https://dateparser.readthedocs.io/en/latest/index.html for more information.
    start_dt: Datetime object or None
      - The start datetime to begin monitoring at. If None, default to the current
        datetime offset from the top of the hour via 'offset'.
    end_dt: Datetime object or None
      - The end datetime to stop driving at. If None, the driver will persist.
    port: int
      - The port number for microservices to reveive data over (no external access
        provided)
    """
    # server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # server.bind(("localhost", port))
    # # I think the int provided to listen is how many messages to go back for?
    # server.listen(5)
    # TODO: implement logic to query for appropriate files when a file is found via the
    # data_monitors that are running.

    queriers.get_plugin(querier.name)
    dispatchers.get_plugin(dispatcher.name)

    finfo = monitor_configs_to_finfo(monitor_configs)

    # when a file is found: implement the pseudo-code written below
    # required_files = qplg(finfo, querier.arguments.source_names)
    # if required_files:
    #   dplg(<some_info>)
    query_dt = get_starting_query_time(offset, custom_datetime=start_dt)

    timeout_str = querier.arguments.timeout
    timeout = convert_dateparser_string_to_timedelta(timeout_str)
    cadence = convert_dateparser_string_to_timedelta(cadence)
    offset = convert_dateparser_string_to_timedelta(offset)

    deadline = datetime.now() + timeout

    while True:
        # This is where the logic should occur for a file arriving to the data monitor
        # IF NEW ROW ADDED, QUERY
        year = str(query_dt.year)
        month = str(query_dt.month)
        day = str(query_dt.day)
        hour = str(query_dt.hour)
        minute = str(query_dt.minute)
        julian = date_utils.calendar_to_julian(cal_dt=query_dt)

        for key, val in finfo.items():
            finfo[key] = (
                val.replace("YYYY", year)
                .replace("MM", month)
                .replace("DD", day)
                .replace("HH", hour)
                .replace("NN", minute)
                .replace("JJJ", julian)
            )
        # when a file is found: implement the pseudo-code written below
        required_files = qplg(finfo, querier.arguments.source_names)
        if required_files:
            # Time to dispatch a job!
            # dplg(args)
            pass
        elif datetime.now() > deadline:
            # timeout has hit. Iterate to next datetime
            query_dt = query_dt + cadence
            deadline = datetime.now() + timeout
