"""Python class for the monitor_configs geoips_driver interface."""

from geoips.interfaces.base import BaseYamlInterface

from geoips_driver.pydantic.monitor_configs import MonitorConfigPlugin


class MonitorConfigsInterface(BaseYamlInterface):
    """Templated file paths used for searching file systems."""

    name = "monitor_configs"
    apiVersion = "geoips_driver/v1"  # noqa: N815
    validator = MonitorConfigPlugin


monitor_configs = MonitorConfigsInterface()
