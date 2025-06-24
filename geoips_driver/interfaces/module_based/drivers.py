"""Python class for the drivers geoips_driver interface."""

from geoips.interfaces.base import BaseModuleInterface


class DriversInterface(BaseModuleInterface):
    """Interface for module plugins used to drive GeoIPS processing."""

    name = "drivers"
    apiVersion = "geoips_driver/v1"


drivers = DriversInterface()
