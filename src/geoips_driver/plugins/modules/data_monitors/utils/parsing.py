"""Utility module used for parsing information out of data files.

This has been designed specifically to capture information that we'll use to fill a
database, which will be used by GeoIPS NRT dispatchers and data_monitors.
"""

import os
import re
from datetime import datetime, timedelta

# from pprint import pprint

interface = None

regex_strs = {
    "gk2a_ami_fd": (
        r"^(?P<satellite>gk2a)_(?P<sensor>ami)_(?P<level>le1b)_.*?_"
        r"(?P<resolution>fd)\d+ge_(?P<start_time>\d{12})\.nc$"
    ),
    "gk2a_ami_la": (
        r"^(?P<satellite>gk2a)_(?P<sensor>ami)_(?P<level>le1b)_.*?_"
        r"(?P<resolution>la)\d+ge_(?P<start_time>\d{12})\.nc$"
    ),
    "gk2a_ami_ela": (
        r"^(?P<satellite>gk2a)_(?P<sensor>ami)_(?P<level>le1b)_.*?_"
        r"(?P<resolution>ela)\d+lc_(?P<start_time>\d{12})\.nc$"
    ),
    "g16_abi_radf": (
        r"^OR_(?P<sensor>ABI)-(?P<level>L1b)-(?P<resolution>RadF)-.*?"
        r"_(?P<satellite>G\d+)_s(?P<start_time>\d+)_e\d+_c\d+\.nc$"
    ),
    "g16_abi_radc": (
        r"^OR_(?P<sensor>ABI)-(?P<level>L1b)-(?P<resolution>RadC)-.*?"
        r"_(?P<satellite>G\d+)_s(?P<start_time>\d+)_e\d+_c\d+\.nc$"
    ),
    "g16_abi_radm1": (
        r"^OR_(?P<sensor>ABI)-(?P<level>L1b)-(?P<resolution>RadM1)-.*?"
        r"_(?P<satellite>G\d+)_s(?P<start_time>\d+)_e\d+_c\d+\.nc$"
    ),
    "g16_abi_radm2": (
        r"^OR_(?P<sensor>ABI)-(?P<level>L1b)-(?P<resolution>RadM2)-.*?"
        r"_(?P<satellite>G\d+)_s(?P<start_time>\d+)_e\d+_c\d+\.nc$"
    ),
    "g18_abi_radf": (
        r"^OR_(?P<sensor>ABI)-(?P<level>L1b)-(?P<resolution>RadF)-.*?"
        r"_(?P<satellite>G\d+)_s(?P<start_time>\d+)_e\d+_c\d+\.nc$"
    ),
    "g18_abi_radc": (
        r"^OR_(?P<sensor>ABI)-(?P<level>L1b)-(?P<resolution>RadC)-.*?"
        r"_(?P<satellite>G\d+)_s(?P<start_time>\d+)_e\d+_c\d+\.nc$"
    ),
    "g18_abi_radm1": (
        r"^OR_(?P<sensor>ABI)-(?P<level>L1b)-(?P<resolution>RadM1)-.*?"
        r"_(?P<satellite>G\d+)_s(?P<start_time>\d+)_e\d+_c\d+\.nc$"
    ),
    "g18_abi_radm2": (
        r"^OR_(?P<sensor>ABI)-(?P<level>L1b)-(?P<resolution>RadM2)-.*?"
        r"_(?P<satellite>G\d+)_s(?P<start_time>\d+)_e\d+_c\d+\.nc$"
    ),
    "h09_ahi_fldk": (
        r"^HS_(?P<satellite>H\d+)_(?P<start_date>\d{8})"
        r"_(?P<start_hour>\d{2})(?P<start_min>\d{2})"
        r"_B\d+_(?P<resolution>FLDK)_.*\.DAT$"
    ),
    "h09_ahi_jp": (
        r"^HS_(?P<satellite>H\d+)_(?P<start_date>\d{8})"
        r"_(?P<start_hour>\d{2})(?P<start_min>\d{2})"
        r"_B\d+_(?P<resolution>JP)\d+_.*\.DAT$"
    ),
    "h09_ahi_r3": (
        r"^HS_(?P<satellite>H\d+)_(?P<start_date>\d{8})"
        r"_(?P<start_hour>\d{2})(?P<start_min>\d{2})"
        r"_B\d+_(?P<resolution>R3)\d+_.*\.DAT$"
    ),
    "msg2_seviri_full-disk": (
        r"^H-\d{3}-(?P<satellite>MSG2)__-MSG2_IODC___-"
        r"(?P<channel>[A-Z]+_\d{3}|VIS\d{3})___-"
        r"(?P<valid_number>00000[1-8])___-(?P<start_time>\d{12})-C_$"
    ),
    "msg2_seviri_full-disk_epi_pro": (
        r"^H-\d{3}-(?P<satellite>MSG2)__-MSG2_IODC___-_*-(?P<type>EPI|PRO)_*"
        r"-(?P<start_time>\d{12})-__$"
    ),
    "msg3_seviri_full-disk": (
        r"^H-\d{3}-(?P<satellite>MSG3)__-MSG3_*"
        r"-(?P<channel>[A-Z]+_\d{3}|VIS\d{3})___-"
        r"(?P<valid_number>00000[1-8])___-(?P<start_time>\d{12})-C_$"
    ),
    "msg3_seviri_full-disk_epi_pro": (
        r"^H-\d{3}-(?P<satellite>MSG3)__-MSG3_*"
        r"-_*-(?P<type>EPI|PRO)_*-(?P<start_time>\d{12})-__$"
    ),
}

intervals = {
    "gk2a": {
        "ami": {
            "l1b": {
                "fd": 10,
                "la": 2,
                "ela": 2,
            },
        },
    },
    "g16": {
        "abi": {
            "l1b": {
                "radf": 10,
                "radc": 5,
                "radm1": 1,
                "radm2": 1,
            },
        },
    },
    "g18": {
        "abi": {
            "l1b": {
                "radf": 10,
                "radc": 5,
                "radm1": 1,
                "radm2": 1,
            },
        },
    },
    "msg2": {
        "seviri": {
            "l1b": {
                "full-disk": 15,
            },
        },
    },
    "msg3": {
        "seviri": {
            "l1b": {
                "full-disk": 15,
            },
        },
    },
    "h09": {
        "ahi": {
            "l1b": {
                "fldk": 10,
                "jp": 10,
                "r3": 10,
            },
        },
    },
}


def get_scanning_interval(sat, sensor, level, resolution) -> int:
    """Determine the scanning interval of the incoming data.

    Where the scanning interval is determined based on the satellite performing the
    retrieval, the sensor of that satellite, the level of the data, and the resolution
    of the data being scanned.

    Parameters
    ----------
    satellite: str
        - The name of the satellite which the data is coming from
    sensor: str
        - The sensor of the satellite which the data is coming from
    level: str
        - The level of the data (i.e. L1b)
    resolution: str
        - The resolution of the data (i.e. fd020ge)

    Returns
    -------
    interval: int
        - The number of minutes between each scan for the provided sat, sensor, level,
          and resolution
    """
    return intervals[sat][sensor][level][resolution]


def pick_associated_regex(filename) -> str:
    """Given the provided filename, choose the regex that can parse info out of it.

    This choice is determined based off of special characters found in the filename.
    For example, a combination of satellite and resolution is usually enough to
    determine what regex will be needed to parse out information of the filename for
    the database.

    Parameters
    ----------
    filename: str
        - The name of the file which we'll be parsing

    Returns
    -------
    regex: str
        - A raw regex string used to parse filename
    """
    for regex in regex_strs:
        match = re.match(regex_strs[regex], filename)
        if match:
            return regex_strs[regex]
    raise re.error(
        f"Error: The input file '{filename}' could not be associated with any regex "
        "pattern we have on file. Please provide a different filename.",
    )


def parse_fpath_with_regex(fpath) -> dict:
    """Parse a filepath using regex to obtain info that will be placed in a database.

    This function will parse the basename of the filepath provided, using the regex
    determined to be associated with the incoming file. The regex pattern should be
    able to capture the following variables.

    Variables
    ---------
    satellite: str
        - The name of the satellite which the data is coming from
    sensor: str
        - The sensor of the satellite which the data is coming from
    level_res: str
        - The level of the data (i.e. L1b) and the resolution of the data (i.e. fd020ge)
    start_time: str
        - The start datetime of the incoming data. Formatted "%Y%m%d%H%M"
    end_time: str
        - The end datetime of the incoming data. Formatted "%Y%m%d%H%M"

    Parameters
    ----------
    fpath: str
        - The path to the file whose name we'll be parsing

    Returns
    -------
    parsed: dict[str]
        - A dictionary containing the keys mentioned in 'Variables' which we'll use to
          update a database.

    Raises
    ------
    re.error:
        - A regex error when the provided regex string cannot be used to parse the
          basename of 'fpath'.
    """
    filename = os.path.basename(fpath)
    regex = pick_associated_regex(filename)
    match = re.match(regex, filename)

    if match:
        satellite = match.group("satellite").lower()
        level = None
        resolution = None

        if satellite in ["h09"]:
            sensor = "ahi"
            level = "l1b"
            # Extract start time components
            start_date = match.group("start_date")  # YYYYMMDD
            start_hour = match.group("start_hour")  # HH
            start_min = match.group("start_min")  # MM
        elif satellite in ["msg2", "msg3"]:
            sensor = "seviri"
            level = "l1b"
            resolution = "full-disk"
            start_time_str = match.group("start_time")
        else:
            sensor = match.group("sensor").lower()
            start_time_str = match.group("start_time")

        if level is None:
            level = match.group("level").lower()
            if level == "le1b":
                level = "l1b"
        if resolution is None:
            resolution = match.group("resolution").lower()

        scanning_interval = get_scanning_interval(satellite, sensor, level, resolution)

        if sensor in ["abi"]:
            # Convert start time from Year-Day-of-Year-HHMM to datetime
            start_time = datetime.strptime(start_time_str[:7], "%Y%j") + timedelta(
                hours=int(start_time_str[7:9]),
                minutes=int(start_time_str[9:11]),
            )
            start_time_str = start_time.strftime("%Y%m%d%H%M")
        elif sensor in ["ahi"]:
            # Convert start time to datetime
            start_time = datetime.strptime(
                f"{start_date}{start_hour}{start_min}",
                "%Y%m%d%H%M",
            )
            start_time_str = start_time.strftime("%Y%m%d%H%M")
        else:
            # Convert start_time to datetime
            start_time = datetime.strptime(start_time_str, "%Y%m%d%H%M")

        # Calculate end_time (10 minutes later)
        end_time = start_time + timedelta(minutes=scanning_interval)
        end_time_str = end_time.strftime("%Y%m%d%H%M")

        if resolution in ["radf", "fldk", "fd"]:
            resolution = "full-disk"
        elif resolution in ["radc"]:
            resolution = "conus"
        elif resolution in ["radm1", "radm2"]:
            resolution = resolution.replace("radm", "meso")
        elif resolution in ["jp"]:
            resolution = "japan"
        elif resolution in ["r3"]:
            resolution = "meso1"
        elif resolution in ["la", "ela"]:
            resolution = resolution.replace("e", "extended-").replace(
                "la",
                "local-area",
            )

        parsed = {
            "satellite": satellite,
            "sensor": sensor,
            "level": level,
            "obs_area": resolution,
            "start_time": start_time_str,
            "end_time": end_time_str,
            "fpath": fpath,
        }
        return parsed
    else:
        raise re.error(
            f"Error: Could't parse the filename '{filename}' using the supplied regex: "
            f"{regex}",
        )


test_paths = [
    "H-000-MSG3__-MSG3________-_________-EPI______-202501250700-__",
    "H-000-MSG3__-MSG3________-_________-PRO______-202501250815-__",
    "H-000-MSG3__-MSG3________-IR_039___-000005___-202501252230-C_",
    "H-000-MSG3__-MSG3________-VIS008___-000005___-202501252230-C_",
    "H-000-MSG2__-MSG2_IODC___-WV_073___-000008___-202501252300-C_",
    "H-000-MSG2__-MSG2_IODC___-VIS006___-000008___-202501252300-C_",
    "H-000-MSG2__-MSG2_IODC___-_________-EPI______-202501252200-__",
    "H-000-MSG2__-MSG2_IODC___-_________-PRO______-202501250615-__",
    "OR_ABI-L1b-RadF-M6C08_G18_s20250252240211_e20250252249519_c20250252249575.nc",
    "OR_ABI-L1b-RadC-M6C15_G18_s20250250151177_e20250250153556_c20250250154034.nc",
    "OR_ABI-L1b-RadM1-M6C01_G18_s20250250005255_e20250250005313_c20250250005358.nc",
    "OR_ABI-L1b-RadM2-M6C16_G18_s20250252355557_e20250252356026_c20250252356061.nc",
    "OR_ABI-L1b-RadF-M6C05_G16_s20250251200207_e20250251209515_c20250251209551.nc",
    "OR_ABI-L1b-RadC-M6C13_G16_s20250250741172_e20250250743557_c20250250744041.nc",
    "OR_ABI-L1b-RadM1-M6C08_G16_s20250251200280_e20250251200337_c20250251200387.nc",
    "OR_ABI-L1b-RadM2-M6C01_G16_s20250250006551_e20250250007008_c20250250007066.nc",
    "gk2a_ami_le1b_ir096_fd020ge_202501252300.nc",
    "gk2a_ami_le1b_wv069_la020ge_202501252346.nc",
    "gk2a_ami_le1b_nr013_ela020lc_202501252314.nc",
    "HS_H09_20250125_0600_B03_FLDK_R05_S0910.DAT",
    "HS_H09_20250125_1150_B14_R302_R20_S0101.DAT",
    "HS_H09_20250125_1800_B03_JP03_R05_S0101.DAT",
]

