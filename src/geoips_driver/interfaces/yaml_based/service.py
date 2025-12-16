"""Python class for the service geoips_driver interface."""

from geoips.interfaces.base import BaseYamlInterface  # type: ignore

from geoips_driver.pydantic.service_config import ServiceConfigModel


class ServiceConfigInterface(BaseYamlInterface):
    """Configuration protocol for controlling GeoIPS NRT processing."""

    name = "service"
    apiVersion = "geoips_driver/v1"  # noqa: N815
    validator = ServiceConfigModel


controller_configs = ServiceConfigInterface()
