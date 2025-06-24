"""Python class for the controller_configs geoips_driver interface."""

from geoips.interfaces.base import BaseYamlInterface

from geoips_driver.pydantic.controller_configs import ControllerConfigPlugin
from geoips_driver.clean.driver_components import driver_utils


class ControllerConfigsInterface(BaseYamlInterface):
    """Configuration protocol for controlling GeoIPS NRT processing."""

    name = "controller_configs"
    apiVersion = "geoips_driver/v1"
    validator = ControllerConfigPlugin

    def get_plugin(self, name):
        """Retrieve a controller_config plugin and convert it to a nested namespace."""
        plg = super().get_plugin(name)
        return driver_utils.dict_to_namespace(plg)


controller_configs = ControllerConfigsInterface()
