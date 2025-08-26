"""Utility methods that can be used throughout geoips_driver code."""

from geoips_driver import interfaces


def monitor_configs_to_finfo(monitor_configs):
    """Convert a list of monitor config dictionaries to a file info dictionary."""
    finfo = {}
    for mc in monitor_configs:
        mc_plg = interfaces.monitor_configs.get_plugin(mc.name)
        for obs_area in mc.arguments.obs_area:
            obs_area_config = getattr(mc_plg.spec.obs_areas, obs_area)
            finfo[f"{obs_area}_{mc_plg.name}"] = obs_area_config

    return finfo
