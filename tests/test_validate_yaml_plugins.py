"""Tests for plugin registration and retrieval."""

from collections.abc import Iterator

import pytest
from pluginify.interfaces.base import BaseClassInterface, BaseYamlInterface

from courier.interfaces import (
    data_monitor_configs,
    data_monitors,
    dispatchers,
    job_builders,
)

PLUGIN_REGISTRIES: dict[str, BaseYamlInterface | BaseClassInterface] = {
    "data_monitor_configs": data_monitor_configs,
    "data_monitors": data_monitors,
    "dispatchers": dispatchers,
    "job_builders": job_builders,
}


def _discover_plugins() -> Iterator[tuple[str, str]]:
    """Discover all plugins by querying each interface's registry."""
    for kind, registry in PLUGIN_REGISTRIES.items():
        for plugin in registry.get_plugins():
            yield plugin.name, kind


@pytest.mark.parametrize(("plugin_name", "kind"), list(_discover_plugins()))
def test_get_plugin(plugin_name: str, kind: str) -> None:
    """Test that each discovered plugin can be retrieved from its registry."""
    registry: BaseYamlInterface | BaseClassInterface = PLUGIN_REGISTRIES[kind]
    plugin = registry.get_plugin(plugin_name)
    assert plugin is not None, f"Failed to retrieve plugin '{plugin_name}' from {kind}"
