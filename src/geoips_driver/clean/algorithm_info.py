"""Generic module containing info needed to automate processing of satellite data."""

# NOTE: I'd like to modify the AlgorithmInfo-based classes to YAML. I think that would
# be easier to support in the long run.

import abc
from os import environ
from types import SimpleNamespace


# If we eventually support Near Real Time (NRT) processing in the main GeoIPS package,
# we'll move this class into geoips.errors
class NewJulianDateException(Exception):
    """A new julian date has started and the directory being watched needs to change."""

    pass


class AlgorithmInfo(abc.ABC):
    """Abstract container of info needed to automate processing of satellite data."""

    slurm_dir = f"{environ['GEOIPS_PACKAGES_DIR']}/geoips_driver/tests/slurm_jobs"
    mp_dir = f"{environ['GEOIPS_PACKAGES_DIR']}/geoips_driver/tests/multiprocessing"
    finfo = {}

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

    finfo = {
        "GOES16": {
            "searchdir": f"/mnt/sat/grb/goes16/YYYY/YYYY_MM_DD_JJJ/abi/L1b/RadF",  # NOQA
            "fpatterns": ["*M6C13*sYYYYJJJHHNN*"],
            "num_expected_files": 1,
            # "reader": "abi_netcdf",
        },
        "GOES18": {
            "searchdir": f"/mnt/sat/grb/goes18/YYYY/YYYY_MM_DD_JJJ/abi/L1b/RadF",  # NOQA
            "fpatterns": ["*M6C13*sYYYYJJJHHNN*"],
            "num_expected_files": 1,
        },
        "M09": {
            "searchdir": "/mnt/sat/meteosat/meteosat-09/YYYYMMDD/MSG2",
            "fpatterns": [
                f"H-000-MSG2__-MSG2_IODC___-_________-EPI______-YYYYMMDDHHNN-__",  # NOQA
                f"H-000-MSG2__-MSG2_IODC___-_________-PRO______-YYYYMMDDHHNN-__",  # NOQA
                f"H-000-MSG2__-MSG2_IODC___-IR_108___-00000[1-8]___-YYYYMMDDHHNN-C_",  # NOQA
            ],
            "num_expected_files": 10,
        },
        "M10": {
            "searchdir": "/mnt/sat/meteosat/meteosat-10/YYYYMMDD/MSG3",
            "fpatterns": [
                f"H-000-MSG3__-MSG3________-_________-EPI______-YYYYMMDDHHNN-__",  # NOQA
                f"H-000-MSG3__-MSG3________-_________-PRO______-YYYYMMDDHHNN-__",  # NOQA
                f"H-000-MSG3__-MSG3________-IR_108___-00000[1-8]___-YYYYMMDDHHNN-C_",  # NOQA
            ],
            "num_expected_files": 10,
        },
        "GK2A": {
            "searchdir": "/mnt/GK2A/AMI/L1B/FD/YYYYMM/DD/HH",
            "fpatterns": ["*ir105_fd020ge_*YYYYMMDDHHNN*"],
            "num_expected_files": 1,
        },
        "H09": {
            "searchdir": "/mnt/sat/ahi-unzip/himawari9/YYYYMMDD",
            "fpatterns": ["*YYYYMMDD_HHNN_B13_FLDK_*_S[01][0-9]10*"],
            "num_expected_files": 10,
        },
    }


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
