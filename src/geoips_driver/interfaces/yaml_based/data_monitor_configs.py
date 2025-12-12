"""Python class for the data_monitor_configs geoips_driver interface."""

from typing import ClassVar

from geoips.interfaces.base import BaseYamlInterface  # type: ignore


class DataMonitorConfigsInterface(BaseYamlInterface):
    """Templated file paths used for searching file systems."""

    name = "data_monitor_configs"
    # ignoring odd capitalization to match existing code style in GeoIPS
    # which itself is matching Kubernetes conventions
    apiVersion: ClassVar[str] = "geoips_driver/v1"  # noqa: N815
    # required_args: ClassVar[dict[str, list[str]]] = {"standard": []}
    # required_kwargs: ClassVar[dict[str, list[str]]] = {"standard": []}
    # validator = DataMonitorConfig
    use_pydantic = False


data_monitor_configs = DataMonitorConfigsInterface()
