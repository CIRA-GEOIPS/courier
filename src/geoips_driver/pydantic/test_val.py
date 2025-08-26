"""Test that data monitor validation is working."""

from pathlib import Path
from pprint import pprint

import yaml
from pydantic import ValidationError

from geoips_driver.pydantic.driver_configs import DriverConfigPlugin
from geoips_driver.pydantic.monitor_configs import MonitorConfigPlugin

dc_path = Path("../plugins/yaml/driver_configs/StitchedInfrared.yaml")
mc_path = Path("../plugins/yaml/monitor_configs/goes16_abi.yaml")

dc = yaml.safe_load(dc_path.open())
mc = yaml.safe_load(mc_path.open())

dc_obj = None
mc_obj = None
try:
    dc_obj = DriverConfigPlugin(**dc)
    mc_obj = MonitorConfigPlugin(**mc)
except ValidationError as e:
    print(e)
    print(e.errors())


pprint(dc_obj)
print()
pprint(mc_obj)
