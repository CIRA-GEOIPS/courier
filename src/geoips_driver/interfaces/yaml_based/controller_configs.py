"""Python class for the controller_configs geoips_driver interface."""

from geoips.interfaces.base import BaseYamlInterface
from geoips_driver.pydantic.controller_configs import ControllerConfigPlugin


class ControllerConfigsInterface(BaseYamlInterface):
    """Configuration protocol for controlling GeoIPS NRT processing."""

    name = "controller_configs"
    apiVersion = "geoips_driver/v1"  # noqa: N815
    validator = ControllerConfigPlugin


controller_configs = ControllerConfigsInterface()
