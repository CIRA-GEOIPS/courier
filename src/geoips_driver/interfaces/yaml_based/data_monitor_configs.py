"""Python class for the data_monitor_configs geoips_driver interface."""

from typing import ClassVar

from geoips.interfaces.base import BaseYamlInterface  # type: ignore

from geoips_driver.pydantic.data_monitor_configs import DataMonitorConfig


class DataMonitorConfigsInterface(BaseYamlInterface):
    """Templated file paths used for searching file systems."""

    name = "data_monitor_configs"
    # ignoring odd capitalization to match existing code style in GeoIPS
    # which itself is matching Kubernetes conventions
    apiVersion: ClassVar[str] = "geoips_driver/v1"  # noqa: N815
    validator = DataMonitorConfig
    # use_pydantic = True


data_monitor_configs = DataMonitorConfigsInterface()
