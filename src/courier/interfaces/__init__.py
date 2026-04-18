"""Lazy Lemon Interface Module."""

from courier.interfaces.module_based.data_monitors import data_monitors
from courier.interfaces.module_based.dispatchers import dispatchers
from courier.interfaces.module_based.job_builders import job_builders
from courier.interfaces.plugin_protocol import ServicePlugin
from courier.interfaces.yaml_based.data_monitor_configs import (
    data_monitor_configs,
)
from courier.service import Service, create_service_with_plugins

# These lists are the "master" lists of the interface names.
# These are used in validating the plugins (ie, so we will catch a typo
# in an interface name)
module_based_interfaces: list[str] = [
    "data_monitors",
    "dispatchers",
    "job_builders",
]
yaml_based_interfaces: list[str] = [
    "data_monitor_configs",
]
# Note due to the fact that we are including all of the imported packages
# in __all__ via variables rather than the actual strings, flake8 does
# not recognize the above imports as being used.
# No QA this line because many linters will complain about
# this not "only" containing strings
__all__ = (  # noqa: PLE0605 # type: ignore
    module_based_interfaces
    + yaml_based_interfaces
    + [
        "ServicePlugin",
        "Service",
        "create_service_with_plugins",
    ]
)
