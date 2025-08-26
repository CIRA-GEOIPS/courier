"""Python class for the data_monitors geoips_driver interface."""

from geoips.interfaces.base import BaseModuleInterface


class DataMonitorsInterface(BaseModuleInterface):
    """Interface used for data monitoring a file system in different manners."""

    name = "data_monitors"
    apiVersion = "geoips_driver/v1"  # noqa: N815


data_monitors = DataMonitorsInterface()
