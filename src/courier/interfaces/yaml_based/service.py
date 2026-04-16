"""Python class for the service courier interface."""

from geoips.interfaces.base import BaseYamlInterface  # type: ignore

from courier.schema import ServiceConfigModel


class ServiceConfigInterface(BaseYamlInterface):
    """Configuration protocol for controlling GeoIPS NRT processing."""

    name = "service"
    apiVersion = "courier.dev/v1alpha1"  # noqa: N815
    validator = ServiceConfigModel


controller_configs = ServiceConfigInterface()
