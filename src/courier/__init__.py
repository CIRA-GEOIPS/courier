"""Template repository demonstrating a basic GeoIPS plugin example."""

# NOTE: _version.py is generated automatically during build/install

from courier import interfaces
from courier._version import __version__, __version_tuple__

__all__ = ["__version__", "__version_tuple__", "interfaces"]
