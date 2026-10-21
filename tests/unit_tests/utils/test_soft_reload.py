from pathlib import Path
from threading import Event

import pytest
from unittest.mock import MagicMock
import tempfile

from courier.cli.feedback import load_config_or_exit
from courier.managers.plugin_manager import PluginManager
from courier.schema import ServiceConfigModel
from courier.service import Service
from courier.utils.config_monitor import ConfigMonitor

example_service = """apiVersion: runcourier.dev/v1alpha1
kind: Service
metadata:
  name: service
  namespace: service
  description: 'A courier service: service'
  docstring: null
  labels: {}
  annotations: {}
spec:
  broker:
    transport: memory
    max_retries: 5
  run:
  - identifier: data-monitor-file-system-poller-watchdog
    spec:
      kind: data_monitor
      name: file_system_poller_watchdog
      config:
        path: ./incoming
        hostname: localhost
"""

@pytest.fixture
def reload_config_file(tmp_path: Path) -> Path:
    dummy_file = tmp_path / "service.yaml"
    dummy_file.write_text(example_service)
    return dummy_file


@pytest.fixture
def reload_service_config(
    reload_config_file: Path,
) -> ServiceConfigModel:
    return load_config_or_exit(reload_config_file)

@pytest.fixture
def reload_plugin_manager(reload_service_config: ServiceConfigModel) -> PluginManager: 
    service_dummy = Service(reload_service_config.spec.service_config)
    plugin_manager_dummy = PluginManager(
            reload_service_config.spec.service_config, service_dummy)
    return plugin_manager_dummy


class TestConstruction:
    def test_construction(self, reload_service_config: ServiceConfigModel):
        service_dummy = Service(reload_service_config.spec.service_config)
        plugin_manager_dummy = PluginManager(
                reload_service_config.spec.service_config, service_dummy)
    def test_pm_start(self, reload_plugin_manager: PluginManager):
        reload_plugin_manager.start()
        reload_plugin_manager.stop()

class TestFileMonitor:
    def test_detects_file_change(self,
                                 reload_config_file: Path,
                                 monkeypatch: pytest.MonkeyPatch):
        monitor = ConfigMonitor(
                config_file=reload_config_file,
                parent_service=MagicMock
        )

        changed = Event()

        monkeypatch.setattr(
            monitor,
            "_on_config_changed",
            lambda event: changed.set(),
        )

        monitor.start()

        try:
            with reload_config_file.open("a") as f:
                f.write("\n# Config modified\n")

            assert changed.wait(timeout=5), (
                    "ConfigMonitor did not detect file modification"
            )
        finally:
            monitor.stop()

