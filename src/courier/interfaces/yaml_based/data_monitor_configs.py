"""Python class for the data_monitor_configs courier interface."""

from typing import ClassVar

from pluginify.interfaces.base import BaseYamlInterface

from courier.schema import DataMonitorConfig


class DataMonitorConfigsInterface(BaseYamlInterface):
    """Templated file paths used for searching file systems."""

    name = "data_monitor_configs"
    # ignoring odd capitalization to match Kubernetes apiVersion conventions
    apiVersion: ClassVar[str] = "runcourier.dev/v1alpha1"  # noqa: N815
    validator = DataMonitorConfig


data_monitor_configs = DataMonitorConfigsInterface()
