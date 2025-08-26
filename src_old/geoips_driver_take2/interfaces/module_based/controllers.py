"""Python class for the controllers geoips_driver interface."""

from geoips.interfaces.base import BaseModuleInterface


class ControllersInterface(BaseModuleInterface):
    """Interface for module plugins used to drive GeoIPS processing."""

    name = "controllers"
    apiVersion = "geoips_driver/v1"  # noqa: N815


controllers = ControllersInterface()
