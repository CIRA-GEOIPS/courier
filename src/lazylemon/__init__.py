"""Template repository demonstrating a basic GeoIPS plugin example."""

# NOTE: _version.py is generated automatically during build/install

from lazylemon import interfaces
from lazylemon._version import __version__, __version_tuple__

__all__ = ["__version__", "__version_tuple__", "interfaces"]
