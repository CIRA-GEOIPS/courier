"""Python class for the dispatchers geoips_driver interface."""

from geoips.interfaces.base import BaseModuleInterface


class DispatchersInterface(BaseModuleInterface):
    """Interface for module plugins which can dispatch processes and/or jobs.

    Dispatchers are the method in which we spawn processes for driving GeoIPS.
    """

    name = "dispatchers"
    apiVersion = "geoips_driver/v1"


dispatchers = DispatchersInterface()
