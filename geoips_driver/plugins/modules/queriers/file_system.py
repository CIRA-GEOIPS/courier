"""Querier plugin which searches a file_system to determine if a job can be ran."""

from geoips_driver.clean.driver_components import FileLocator

interface = "queriers"
name = "file_system"
family = "standard"


"""
See 'example_finfo' for an example of how to set up your search space. Information
needed is the 'parent_dir' (search directory) for where your data comes from,
'patterns' (file patterns to match), and 'num_expected' (number of expected
files to find).

Once this data is provided, you can use this class to search through inputted
directories for your files.

example_finfo = {
    "GOES16": {
        "parent_dir": "/mnt/grb/goes16/2024/2024_11_21_326/abi/L1b/RadF",
        "patterns": ["*M6C13*s20243261600*"],
        "num_expected": 1,
    },
    "GOES18": {
        "parent_dir": "/mnt/grb/goes18/2024/2024_11_21_326/abi/L1b/RadF",
        "patterns": ["*M6C13*s20243261600*"],
        "num_expected": 1,
    },
    "M09": {
        "parent_dir": "/mnt/meteosat-09/20241121/MSG2",
        "patterns": [
            "H-000-MSG2__-MSG2_IODC___-_________-EPI______-202411211600-__",
            "H-000-MSG2__-MSG2_IODC___-_________-PRO______-202411211600-__",
            "H-000-MSG2__-MSG2_IODC___-IR_108___-00000[1-8]___-202411211600-C_",
        ],
        "num_expected": 10,
    },
    "M10": {
        "parent_dir": "/mnt/meteosat-10/20241121/MSG3",
        "patterns": [
            "H-000-MSG3__-MSG3_IODC___-_________-EPI______-202411211600-__",
            "H-000-MSG3__-MSG3_IODC___-_________-PRO______-2024112116000-__",
            "H-000-MSG3__-MSG3_IODC___-IR_108___-00000[1-8]___-202411211600-C_",
        ],
        "num_expected": 10,
    },
    "GK2A": {
        "parent_dir": "/mnt/GK2A/AMI/L1B/FD/202411/21/16",
        "patterns": ["*ir105*202411211600*"],
        "num_expected": 1,
    },
    "H09": {
        "parent_dir": "/mnt/ahi/himawari9/20241121",
        "patterns": ["*20241121_1600_B13_FLDK_*_S[01][0-9]10*"],
        "num_expected": 10,
    },
}
"""


def filter_by_source_names(finfo, source_names):
    """Filter finfo using the source names provided.

    Where 'filter' denotes that we are choosing only certain key, value pairs from
    finfo, based on the source_names provided. See 'parameters' for more information.

    Parameters
    ----------
    finfo: dict
        - A dictionary of file information needed to query a file system. Each entry
          should be the name of a satellite, whose value is a dictionary containing
          keys ['parent_dir', 'patterns', 'num_expected']. See 'example_finfo'
          above for more information.
    source_names: List[str]
        - A list of strings used to filter what files are actually needed. For example,
          if this function was provided the dictionary above, and source_names was
          equal to ['abi', 'ahi'], then only the GOES and Himawari dictionaries would
          be used in the query.

    Raises
    ------
    ValueError:
        - Raised if a key could not be associated with a valid GeoIPS source_name.
    """
    filtered_finfo = {}
    for sat in finfo:
        if sat.startswith("GOES"):
            src = "abi"
        elif sat == "GK2A":
            src = "ami"
        elif sat in ["M09", "M10"]:
            src = "seviri"
        elif sat in ["H08", "H09"]:
            src = "ahi"
        elif sat.startswith("MTG"):
            src = "fci"
        else:
            raise ValueError(
                f"Error: satellite key '{sat}' couldn't be associated with a valid "
                "GeoIPS source_name. Run 'geoips list source-names' for more info.",
            )
        if src in source_names:
            filtered_finfo[sat] = finfo[sat]
    return filtered_finfo


def call(finfo, source_names):
    """Query a file system to determine whether or not a job/process can be ran.

    Where information in this case are data files needed for a certain process to
    execute properly.

    Parameters
    ----------
    finfo: dict
        - A dictionary of file information needed to query a file system. Each entry
          should be the name of a satellite, whose value is a dictionary containing
          keys ['parent_dir', 'patterns', 'num_expected']. See 'example_finfo'
          above for more information.
    source_names: List[str]
        - A list of strings used to filter what files are actually needed. For example,
          if this function was provided the dictionary above, and source_names was
          equal to ['abi', 'ahi'], then only the GOES and Himawari dictionaries would
          be used in the query.

    Returns
    -------
    list:
        - A list of filepaths required to run your process. If empty, this means that
          one or more required files for your process were missing.
    """
    filtered_finfo = filter_by_source_names(finfo, source_names)
    file_locator = FileLocator(filtered_finfo)
    if file_locator.all_files_found():
        return file_locator.generate_required_filepaths()
    else:
        return []
