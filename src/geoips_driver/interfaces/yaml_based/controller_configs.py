"""Python class for the controller_configs geoips_driver interface."""

from argparse import Namespace

from geoips.interfaces.base import BaseYamlInterface

from geoips_driver.clean.driver_components import driver_utils
from geoips_driver.pydantic.controller_configs import ControllerConfigPlugin


class ControllerConfigsInterface(BaseYamlInterface):
    """Configuration protocol for controlling GeoIPS NRT processing."""

    name = "controller_configs"
    apiVersion = "geoips_driver/v1"  # noqa: N815
    validator = ControllerConfigPlugin

    def get_plugin(self, name: str) -> Namespace:
        """Retrieve a controller_config plugin and convert it to a nested namespace."""
        plg = super().get_plugin(name)
        return driver_utils.dict_to_namespace(plg)


controller_configs = ControllerConfigsInterface()
