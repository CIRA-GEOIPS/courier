"""GeoIPS Driver Interface Module."""

from geoips_driver.interfaces.module_based.data_monitors import DataMonitor
from geoips_driver.interfaces.module_based.dispatchers import Dispatcher
from geoips_driver.interfaces.module_based.job_builders import JobBuilder
from geoips_driver.interfaces.yaml_based.data_monitor_configs import (
    DataMonitorConfigsInterface,
)

# These lists are the "master" lists of the interface names.
# These are used in validating the plugins (ie, so we will catch a typo
# in an interface name)
module_based_interfaces = [
    "data_monitors",
    "dispatchers",
    "job_builders",
]
yaml_based_interfaces = [
    "data_monitor_configs",
]
# Note due to the fact that we are including all of the imported packages
# in __all__ via variables rather than the actual strings, flake8 does
# not recognize the above imports as being used.  F401 ignored via
# per-file ignore in geoips/.config/flake8 config.  See comment above
# for more information.
__all__ = module_based_interfaces + yaml_based_interfaces  # noqa: PLE0605
