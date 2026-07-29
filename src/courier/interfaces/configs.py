"""The ``data_monitor_configs`` interface.

Metadata configs are data rather than behaviour, so this registry hands back
validated :class:`~courier.schema.DataMonitorConfig` instances rather than
classes. Each is constructed at import time by the module declaring it, so a
malformed config fails discovery instead of quietly matching no files.
"""

from courier.interfaces.discovery import ENTRY_POINT_PREFIX, ConfigPluginRegistry
from courier.schema import DataMonitorConfig

data_monitor_configs: ConfigPluginRegistry[DataMonitorConfig] = ConfigPluginRegistry(
    name="data_monitor_configs",
    group=f"{ENTRY_POINT_PREFIX}.data_monitor_configs",
    model=DataMonitorConfig,
)
