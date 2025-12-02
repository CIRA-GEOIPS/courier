"""Python class for the data_monitor_configs geoips_driver interface."""

from geoips.interfaces.base import BaseYamlInterface  # type: ignore

from geoips_driver.pydantic.monitor_configs import MonitorConfigPlugin


class DataMonitorConfigsInterface(BaseYamlInterface):
    """Templated file paths used for searching file systems."""

    name = "data_monitor_configs"
    apiVersion = "geoips_driver/v1"  # noqa: N815
    validator = MonitorConfigPlugin


monitor_configs = DataMonitorConfigsInterface()
