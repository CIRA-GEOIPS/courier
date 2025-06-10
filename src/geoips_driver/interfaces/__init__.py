# # # This source code is protected under the license referenced at
# # # https://github.com/NRLMMD-GEOIPS.

"""GeoIPS Driver Interface Module."""

from geoips_driver.interfaces.module_based.data_monitors import data_monitors
from geoips_driver.interfaces.module_based.dispatchers import dispatchers
from geoips_driver.interfaces.module_based.drivers import drivers
from geoips_driver.interfaces.module_based.queriers import queriers
from geoips_driver.interfaces.yaml_based.controller_configs import controller_configs
from geoips_driver.interfaces.yaml_based.monitor_configs import monitor_configs

# These lists are the "master" lists of the interface names.
# These are used in validating the plugins (ie, so we will catch a typo
# in an interface name)
module_based_interfaces = [
    "data_monitors",
    "dispatchers",
    "drivers",
    "queriers",
]
yaml_based_interfaces = [
    "controller_configs",
    "monitor_configs",
]
# Note due to the fact that we are including all of the imported packages
# in __all__ via variables rather than the actual strings, flake8 does
# not recognize the above imports as being used.  F401 ignored via
# per-file ignore in geoips/.config/flake8 config.  See comment above
# for more information.
__all__ = module_based_interfaces + yaml_based_interfaces
