finfo = {
    "GOES16": {
        "searchdir": f"/mnt/sat/grb/goes16/YYYY/YYYY_MM_DD_JJJ/abi/L1b/RadF",  # NOQA
        "fpatterns": [f"*M6C13*sYYYYJJJHHNN*"],
        "num_expected_files": 1,
    },
    "GOES18": {
        "searchdir": f"/mnt/sat/grb/goes18/YYYY/YYYY_MM_DD_JJJ/abi/L1b/RadF",  # NOQA
        "fpatterns": [f"*M6C13*sYYYYJJJHHNN*"],
        "num_expected_files": 1,
    },
    "M09": {
        "searchdir": f"/mnt/sat/meteosat/meteosat-09/YYYYMMDD/MSG2",
        "fpatterns": [
            f"H-000-MSG2__-MSG2_IODC___-_________-EPI______-YYYYMMDDHHNN-__",  # NOQA
            f"H-000-MSG2__-MSG2_IODC___-_________-PRO______-YYYYMMDDHHNN-__",  # NOQA
            f"H-000-MSG2__-MSG2_IODC___-IR_108___-00000[1-8]___-YYYYMMDDHHNN-C_",  # NOQA
        ],
        "num_expected_files": 10,
    },
    "M10": {
        "searchdir": f"/mnt/sat/meteosat/meteosat-10/YYYYMMDD/MSG3",
        "fpatterns": [
            f"H-000-MSG3__-MSG3________-_________-EPI______-YYYYMMDDHHNN-__",  # NOQA
            f"H-000-MSG3__-MSG3________-_________-PRO______-YYYYMMDDHHNN-__",  # NOQA
            f"H-000-MSG3__-MSG3________-IR_108___-00000[1-8]___-YYYYMMDDHHNN-C_",  # NOQA
        ],
        "num_expected_files": 10,
    },
    "GK2A": {
        "searchdir": f"/mnt/GK2A/AMI/L1B/FD/YYYYMM/DD/HH",
        "fpatterns": [f"*ir105_fd020ge_*YYYYMMDDHHNN*"],
        "num_expected_files": 1,
    },
    "H09": {
        "searchdir": f"/mnt/sat/ahi-unzip/himawari9/YYYYMMDD",
        "fpatterns": [f"*YYYYMMDD_HHNN_B13_FLDK_*_S[01][0-9]10*"],
        "num_expected_files": 10,
    },
}

date_dict = {"jdate": "178", "year": "2024", "month": "06", "day": "26", "hhnn": "2230"}


def get_file_info(date_dict, finfo) -> dict:
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


filled_finfo = get_file_info(date_dict, finfo)


for key, val in filled_finfo.items():
    for i, j in val.items():
        print(key)
        print(f"\t{i}: {j}")
