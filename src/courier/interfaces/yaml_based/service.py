"""Python class for the service courier interface."""

from pluginify.interfaces.base import BaseYamlInterface

from courier.schema import ServiceConfigModel


class ServiceConfigInterface(BaseYamlInterface):
    """Configuration protocol for controlling courier NRT processing."""

    name = "service"
    apiVersion = "runcourier.dev/v1alpha1"  # noqa: N815
    validator = ServiceConfigModel


controller_configs = ServiceConfigInterface()
