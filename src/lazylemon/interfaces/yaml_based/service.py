"""Python class for the service lazylemon interface."""

from geoips.interfaces.base import BaseYamlInterface  # type: ignore

from lazylemon.pydantic.service_config import ServiceConfigModel


class ServiceConfigInterface(BaseYamlInterface):
    """Configuration protocol for controlling GeoIPS NRT processing."""

    name = "service"
    apiVersion = "lazylemon/v1"  # noqa: N815
    validator = ServiceConfigModel


controller_configs = ServiceConfigInterface()
