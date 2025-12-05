"""Python class for the data_monitor_configs geoips_driver interface."""

from geoips.interfaces.base import BaseYamlInterface  # type: ignore

from geoips_driver.pydantic.data_monitor_configs import DataMonitorConfig


class DataMonitorConfigsInterface(BaseYamlInterface):
    """Templated file paths used for searching file systems."""

    name = "data_monitor_configs"
    apiVersion = "geoips_driver/v1"  # noqa: N815
    validator = DataMonitorConfig


monitor_configs = DataMonitorConfigsInterface()
