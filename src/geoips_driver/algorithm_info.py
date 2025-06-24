"""Generic module containing info needed to automate processing of satellite data."""

# NOTE: I'd like to modify the AlgorithmInfo-based classes to YAML. I think that would
# be easier to support in the long run.

import abc
from datetime import UTC, datetime, timedelta
from os import environ
from types import SimpleNamespace


def nearest_half_hour_utc():
    """Return the nearest half hour increment in UTC time.

    Formatted: hhnn. I.e. '2030' or '0100', ...
    """
    now = datetime.now(UTC)
    minute = now.minute
    if minute < 25:
        # Round down to the previous hour
        result = now.replace(minute=0, second=0, microsecond=0)
    elif minute < 55:
        # Round to the nearest half-hour
        result = now.replace(minute=30, second=0, microsecond=0)
    else:
        # Round up to the next hour
        result = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    return f"{str(result.hour).zfill(2)}{str(result.minute).zfill(2)}"


def curr_calendar_date():
    """Return the current calendar date in string format {year}{month}{day}."""
    curr_dt = datetime.now(UTC)
    year, month, day = curr_dt.year, curr_dt.month, curr_dt.day
    return f"{year}{str(month).zfill(2)}{str(day).zfill(2)}"


def calendar_to_julian(cal_dt=None) -> str:
    """Convert the provided calendar date into a julian date (len=3).

    Parameters
    ----------
    cal_dt: datetime object, default=None
        - The calendar date to be converted to a julian date. If None, uses the
            current calendar date.

    Returns
    -------
    full_jdate: str
        - A string julian date converted from cal_dt.
        - Formatted: 'YYYYJJJ'
    """
    if cal_dt is None:
        cal_dt = datetime.now(UTC)
    year, month, day = cal_dt.year, cal_dt.month, cal_dt.day
    date_obj = datetime(year, month, day)
    epoch = datetime(year, 1, 1)
    jdate = (date_obj - epoch).days + 1
    full_jdate = f"{year}{str(jdate).zfill(3)}"  # insert 0's if len != 3
    return full_jdate


def julian_to_calendar(julian_date, fmt="%Y_%m_%d"):
    """Convert the julian date formatted YYYYJJJ to a calendar date using format fmt."""
    # Extract the year and the day of the year (jjj)
    year = int(str(julian_date)[:4])
    day_of_year = int(str(julian_date)[4:])

    # Create a datetime object for the first day of the year
    date = datetime(year, 1, 1) + timedelta(days=day_of_year - 1)

    return f"{date.strftime(fmt)}_{day_of_year}"  # Return in a standard format like YYYY_MM_DD_JJJ  # NOQA


# If we eventually support Near Real Time (NRT) processing in the main GeoIPS package,
# we'll move this class into geoips.errors
class NewJulianDateException(Exception):
    """A new julian date has started and the directory being watched needs to change."""

    pass


class AlgorithmInfo(abc.ABC):
    """Abstract container of info needed to automate processing of satellite data."""

    slurm_dir = f"{environ['GEOIPS_PACKAGES_DIR']}/geoips_driver/tests/slurm_jobs"
    mp_dir = f"{environ['GEOIPS_PACKAGES_DIR']}/geoips_driver/tests/multiprocessing"

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """The name of the algorithm container."""
        pass

    @property
    @abc.abstractmethod
    def basedir(self) -> str:
        """The base directory that will contain the data from name_sensor_mesosector."""
        pass

    @property
    @abc.abstractmethod
    def paths(self) -> dict:
        """A dictionary of data paths structured as satellite.sensor.sector."""
        # """A namespace of data paths structured as satellite.sensor.sector."""
        pass

    @property
    @abc.abstractmethod
    def product_names(self) -> list[str]:
        """A listing of products that can be made from GeoIPS CLAVRX."""
        pass

    @property
    @abc.abstractmethod
    def output_types(self) -> list[str]:
        """A listing of output types that we want to produce for this algorithm."""
        pass

    @property
    @abc.abstractmethod
    def sector_mapping(self) -> dict[str]:
        """A mapping of satellite-sensor-sector naming to GeoIPS sectors."""
        pass

    def dict_to_namespace(self, iter) -> SimpleNamespace:
        """Recursively transform an iterable into a namespace.

        If a dictionary, run this again on each if its values.
        If a list, run this function on each of its items.
        Otherwise return the value provided.

        Parameters
        ----------
        iter: iterable or value
            - If an iterable, run this function on each of its items. Otherwise return
              this value
        """
        if isinstance(iter, dict):
            return SimpleNamespace(
                **{key: self.dict_to_namespace(val) for key, val in iter.items()},
            )
        elif isinstance(iter, list):
            return [self.dict_to_namespace(item) for item in iter]
        else:
            return iter


class CLAVRX(AlgorithmInfo):
    """Container of information needed for automating geoips_clavrx processing."""

    name = "CLAVR-x"
    basedir = "/mnt/overcastnas1/GEO_clavrx"
    output_types = ["imagery_annotated", "imagery_clean"]
    product_names = [
        "Cloud-Top-Height",
        "Cloud-Base-Height",
        "Cloud-Depth",
        "Cloud-Phase",
        "Cloud-Optical-Depth",
        "Effective-Radius",
        "Cloud-Water-Path",
    ]
    sector_mapping = {"RadC": "conus", "RadF": "goes_east"}

    @property
    def paths(self):
        """A namespace of data paths structured as satellite.sensor.sector."""
        if not hasattr(self, "_paths"):
            self._paths = {
                "GOES16": {
                    "ABI": {
                        "RadC": f"{self.basedir}/GOES16_ABI/RadC/output",
                        "RadF": f"{self.basedir}/GOES16_ABI/RadF/output",
                    },
                },
                "GOES18": {
                    "ABI": {
                        "RadC": f"{self.basedir}/GOES18_ABI/RadC/output",
                        "RadF": f"{self.basedir}/GOES18_ABI/RadF/output",
                    },
                },
                "Himawari8": {
                    "AHI": {
                        "RadF": f"{self.basedir}/Himawari8_AHI/RadF/output",
                    },
                },
                "Himawari9": {
                    "AHI": {
                        "RadF": f"{self.basedir}/Himawari9_AHI/RadF/output",
                    },
                },
            }
            # self._paths = self.dict_to_namespace(self._paths)
        return self._paths


class AHI(AlgorithmInfo):
    """Container of information needed for automating H09 AHI GeoColor processing."""

    name = "Himawari9-AHI"
    basedir = "/mnt/ahi/himawari9"
    output_types = ["imagery_annotated", "imagery_clean"]
    product_names = ["GeoColor"]
    sector_mapping = {"FLDK": "himawari", "SCS": "south_china_sea"}

    @property
    def paths(self):
        """A namespace of data paths structured as satellite.sensor.sector."""
        if not hasattr(self, "_paths"):
            self._paths = {
                "Himawari9": {
                    "AHI": {
                        "FLDK": f"{self.basedir}",
                        "SCS": f"{self.basedir}",
                    },
                },
            }
            # self._paths = self.dict_to_namespace(self._paths)
        return self._paths


class GOES18_ABI(AlgorithmInfo):
    """Container of information needed for automating GOES18 ABI GeoColor processing."""

    name = "GOES18_ABI"
    basedir = "/mnt/grb/goes18"
    output_types = ["imagery_annotated", "imagery_clean"]
    product_names = ["GeoColor"]
    sector_mapping = {"RadF": "goes_west", "RadC": "conus"}

    @property
    def paths(self):
        """A namespace of data paths structured as satellite.sensor.sector."""
        if not hasattr(self, "_paths"):
            self._paths = {
                "GOES18": {
                    "ABI": {
                        "RadF": f"{self.basedir}",
                        "RadC": f"{self.basedir}",
                    },
                },
            }
            # self._paths = self.dict_to_namespace(self._paths)
        return self._paths


class GOES16_ABI(AlgorithmInfo):
    """Container of information needed for automating GOES16 ABI GeoColor processing."""

    name = "GOES16_ABI"
    basedir = "/mnt/grb/goes16"
    output_types = ["imagery_annotated", "imagery_clean"]
    product_names = ["GeoColor"]
    sector_mapping = {"RadF": "goes_east", "RadC": "conus"}

    @property
    def paths(self):
        """A namespace of data paths structured as satellite.sensor.sector."""
        if not hasattr(self, "_paths"):
            self._paths = {
                "GOES16": {
                    "ABI": {
                        "RadF": f"{self.basedir}",
                        "RadC": f"{self.basedir}",
                    },
                },
            }
            # self._paths = self.dict_to_namespace(self._paths)
        return self._paths


class StitchedInfrared(AlgorithmInfo):
    """Container of information needed for automating stitched infrared processing.

    This will use data from G16/18 ABI, M09/10 SEVIRI, and H09 AHI to create the
    stitched infrared output which will be interpolated to the OVERCAST GeoRing grid.

    Will use B13 for this use case, but this can be modified as needed.
    """

    name = "StitchedInfrared"
    basedir = f"{environ['GEOIPS_TESTDATA_DIR']}/temp_infrared_data"

    output_types = ["unprojected_image"]
    product_names = ["Stitched-Infrared"]
    sector_mapping = {"ALL": "overcast_georing"}

    @property
    def paths(self):
        """A namespace of data paths structured as satellite.sensor.sector."""
        if not hasattr(self, "_paths"):
            self._paths = {
                "ALL": {
                    "ALL": {
                        "ALL": f"{self.basedir}",
                    },
                },
            }
        return self._paths


class StitchedGeoColor(AlgorithmInfo):
    """Container of information needed for automating stitched infrared processing.

    This will use data from G16/18 ABI, M12 FCI, and H09 AHI to create the
    stitched infrared output which will be interpolated to the OVERCAST GeoRing grid.
    """

    name = "StitchedGeoColor"
    basedir = f"{environ['GEOIPS_TESTDATA_DIR']}/temp_geocolor_data"

    output_types = ["unprojected_image"]
    product_names = ["Stitched-GeoColor"]
    sector_mapping = {"ALL": "overcast_georing"}

    @property
    def paths(self):
        """A namespace of data paths structured as satellite.sensor.sector."""
        if not hasattr(self, "_paths"):
            self._paths = {
                "ALL": {
                    "ALL": {
                        "ALL": f"{self.basedir}",
                    },
                },
            }
        return self._paths


class GEORING(AlgorithmInfo):
    """Container of information needed for automating GEOring_3d processing."""

    name = "OVERCAST"
    basedir = "/mnt/overcastnas1/GEOring_3d/output"
    # basedir = "/home/erose/geoips/geoips_packages/test_data/test_data_overcast"
    # output_types = ["imagery_annotated", "imagery_clean"]
    output_types = ["unprojected_image"]
    # product_names = [
    #     "Binary_Cloud_Mask",
    #     "Cloud_Base_Height",
    #     "Cloud_Top_Height",
    #     "Cloud_Depth_Height",
    #     "Cloud_Type",
    #     "Cloud_Water_Content",
    # ]
    product_names = [
        "Unprojected-Cloud-Type",
        "Unprojected-Binary-Cloud-Mask",
        "Unprojected-Cloud-Top-Height",
        "Unprojected-Cloud-Base-Height",
        "Unprojected-Cloud-Depth",
        "Unprojected-Cloud-Water-Content",
    ]
    sector_mapping = {"ALL": "overcast_georing"}

    @property
    def paths(self):
        """A namespace of data paths structured as satellite.sensor.sector."""
        if not hasattr(self, "_paths"):
            self._paths = {
                "GEORING": {
                    "GEORING": {
                        "ALL": f"{self.basedir}",
                    },
                },
            }
            # self._paths = self.dict_to_namespace(self._paths)
        return self._paths


algorithms = {
    "CLAVRX": CLAVRX(),
    "AHI": AHI(),
    "GEORING": GEORING(),
    "GOES16_ABI": GOES16_ABI(),
    "GOES18_ABI": GOES18_ABI(),
    "StitchedInfrared": StitchedInfrared(),
}
