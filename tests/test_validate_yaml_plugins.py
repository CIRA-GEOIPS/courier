"""Tests for plugin registration and retrieval."""

import pathlib
from collections.abc import Iterator

import pytest
from geoips.interfaces.base import (  # type: ignore[import-untyped]
    BaseModuleInterface, BaseYamlInterface)

from geoips_driver.interfaces import (data_monitor_configs, data_monitors,
                                      dispatchers, job_builders)

PLUGIN_REGISTRIES: dict[str, BaseYamlInterface | BaseModuleInterface] = {
    "data_monitor_configs": data_monitor_configs,
    "data_monitors": data_monitors,
    "dispatchers": dispatchers,
    "job_builders": job_builders,
}

PLUGINS_ROOT = pathlib.Path(__file__).parent.parent / "src" / "geoips_driver" / "plugins"


def _discover_plugins() -> Iterator[tuple[str, str]]:
    """Discover all plugins by scanning yaml and module directories."""
    for kind in PLUGIN_REGISTRIES:
        is_yaml_plugin = kind.endswith("configs")
        subdir = "yaml" if is_yaml_plugin else "modules"
        glob_pattern = "*.yaml" if is_yaml_plugin else "*.py"

        plugin_dir = PLUGINS_ROOT / subdir / kind
        if not plugin_dir.exists():
            continue

        for plugin_file in plugin_dir.glob(glob_pattern):
            if plugin_file.stem.startswith("_"):
                continue
            yield plugin_file.stem, kind


@pytest.mark.parametrize(("plugin_name", "kind"), list(_discover_plugins()))
def test_get_plugin(plugin_name: str, kind: str) -> None:
    """Test that each discovered plugin can be retrieved from its registry."""
    registry: BaseYamlInterface | BaseModuleInterface = PLUGIN_REGISTRIES[kind]
    plugin = registry.get_plugin(plugin_name)
    assert plugin is not None, f"Failed to retrieve plugin '{plugin_name}' from {kind}"
