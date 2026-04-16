"""Python class for the data_monitor_configs courier interface."""

from typing import ClassVar

from geoips.interfaces.base import BaseYamlInterface  # type: ignore

from courier.schema import DataMonitorConfig


class DataMonitorConfigsInterface(BaseYamlInterface):
    """Templated file paths used for searching file systems."""

    name = "data_monitor_configs"
    # ignoring odd capitalization to match existing code style in GeoIPS
    # which itself is matching Kubernetes conventions
    apiVersion: ClassVar[str] = "courier.dev/v1alpha1"  # noqa: N815
    validator = DataMonitorConfig


data_monitor_configs = DataMonitorConfigsInterface()
