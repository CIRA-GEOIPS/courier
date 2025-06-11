"""Test that data monitor validation is working."""

from pprint import pprint

import yaml
from pydantic import ValidationError

from geoips_driver.pydantic.driver_configs import DriverConfigPlugin
from geoips_driver.pydantic.monitor_configs import MonitorConfigPlugin

dc = yaml.safe_load(open("../plugins/yaml/driver_configs/StitchedInfrared.yaml"))
mc = yaml.safe_load(open("../plugins/yaml/monitor_configs/goes16_abi.yaml"))

dc_obj = None
mc_obj = None
try:
    dc_obj = DriverConfigPlugin(**dc)
    mc_obj = MonitorConfigPlugin(**mc)
except ValidationError as e:
    print(e)
    print(e.errors())

# val = DriverConfigPlugin(**yam)


pprint(dc_obj)
print()
pprint(mc_obj)
