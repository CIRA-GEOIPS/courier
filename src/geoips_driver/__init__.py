"""Template repository demonstrating a basic GeoIPS plugin example."""

# NOTE: _version.py is generated automatically during build/install

from geoips_driver import interfaces

from ._version import __version__, __version_tuple__

__all__ = ["__version__", "__version_tuple__", "interfaces"]
