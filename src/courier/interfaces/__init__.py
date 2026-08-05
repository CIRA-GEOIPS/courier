"""Lazy Lemon Interface Module."""

from courier.interfaces.configs import data_monitor_configs
from courier.interfaces.data_monitors import data_monitors
from courier.interfaces.dispatchers import dispatchers
from courier.interfaces.job_builders import job_builders
from courier.interfaces.plugin_protocol import ServicePlugin
from courier.service import Service, create_service_with_plugins

# These lists are the "master" lists of the interface names, and each is also
# the suffix of that interface's entry-point group (``courier.<name>``).
# Plugin interfaces hand back classes; config interfaces hand back validated
# model instances.
plugin_interfaces: list[str] = [
    "data_monitors",
    "dispatchers",
    "job_builders",
]
config_interfaces: list[str] = [
    "data_monitor_configs",
]
# Note due to the fact that we are including all of the imported packages
# in __all__ via variables rather than the actual strings, flake8 does
# not recognize the above imports as being used.
# No QA this line because many linters will complain about
# this not "only" containing strings
__all__ = (  # noqa: PLE0605 # type: ignore
    plugin_interfaces
    + config_interfaces
    + [
        "ServicePlugin",
        "Service",
        "create_service_with_plugins",
    ]
)
