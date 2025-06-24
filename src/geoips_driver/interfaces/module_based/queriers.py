"""Python class for the queriers geoips_driver interface."""

from geoips.interfaces.base import BaseModuleInterface


class QueriersInterface(BaseModuleInterface):
    """Interface for module plugins which can query information storage systems.

    Queriers are the method in which we can determine whether we have enough information
    to dispatch a certain process or job while driving GeoIPS.

    For example, a querier could search a file system, query a database, ping NOAA AWS,
    and more to determine if enough information exists to run a certain process.
    """

    name = "queriers"
    apiVersion = "geoips_driver/v1"  # noqa: N815


queriers = QueriersInterface()
