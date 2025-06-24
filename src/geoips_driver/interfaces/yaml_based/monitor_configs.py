"""Python class for the monitor_configs geoips_driver interface."""

from geoips.interfaces.base import BaseYamlInterface

from geoips_driver.pydantic.monitor_configs import MonitorConfigPlugin
from geoips_driver.clean.driver_components import driver_utils
from argparse import Namespace


class MonitorConfigsInterface(BaseYamlInterface):
    """Templated file paths used for searching file systems."""

    name = "monitor_configs"
    apiVersion = "geoips_driver/v1"
    validator = MonitorConfigPlugin

    def get_plugin(self, name: str) -> Namespace:
        """Retrieve a monitor_config plugin and convert it to a nested namespace."""
        plg = super().get_plugin(name)
        return driver_utils.dict_to_namespace(plg)


monitor_configs = MonitorConfigsInterface()
