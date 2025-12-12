"""Python class for the data_monitor_configs geoips_driver interface."""

from geoips.interfaces.base import BaseYamlInterface  # type: ignore


class DataMonitorConfigsInterface(BaseYamlInterface):
    """Templated file paths used for searching file systems."""

    name = "data_monitor_configs"
    # apiVersion = "geoips_driver/v1"
    # validator = DataMonitorConfig
    use_pydantic = False


data_monitor_configs = DataMonitorConfigsInterface()
