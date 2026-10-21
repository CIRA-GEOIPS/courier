from pathlib import Path
from threading import Event

import pytest
from unittest.mock import MagicMock
import tempfile

from courier.cli.feedback import load_config_or_exit
from courier.managers.plugin_manager import PluginManager
from courier.schema import ServiceConfigModel
from courier.service import Service
from courier.utils.config_monitor import ConfigMonitor, get_diff_lines, get_spec_from_line


@pytest.fixture
def reload_config_file(tmp_path: Path) -> Path:
    dummy_file = tmp_path / "service.yaml"
    example_service = f"""apiVersion: runcourier.dev/v1alpha1
kind: Service
metadata:
  name: service
  namespace: service
  description: 'A courier service: service'
  docstring: null
  labels: {{}}
  annotations: {{}}
spec:
  broker:
    transport: memory
    max_retries: 5
  service_config: 
      live_reload: True
      config_path: {dummy_file}
  run:
  - identifier: data-monitor-file-system-poller-watchdog
    spec:
      kind: data_monitor
      name: file_system_poller_watchdog
      config:
        path: ./incoming
        hostname: localhost
"""
    dummy_file.write_text(example_service)
    return dummy_file


@pytest.fixture
def reload_service_config(
    reload_config_file: Path,
) -> ServiceConfigModel:
    config = load_config_or_exit(reload_config_file)
    return config

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

class TestBasicUtils:
    def test_diff_lines(self):
        old_src = """1. one
2. two
3. three
4. four
5. five
"""
        new_src = """1. one
2. two
3. new_three
4. four
5. new_five
6. hello world
"""
        diff_lines = get_diff_lines(old_src, new_src)
        assert len(diff_lines) == 3
        assert diff_lines == [2, 4, 5]
    def test_spec_from_line(self):
        ln = """- identifier: data-monitor-file-system-poller-watchdog
    spec:
      kind: data_monitor
      name: file_system_poller_watchdog
      config:
        path: ./incoming
        hostname: localhost"""
        assert ln.splitlines()[3].lstrip() == "name: file_system_poller_watchdog"
        upper, lower = get_spec_from_line(ln, 3)
        assert upper == 0
        assert lower == 6

    def test_two_spec_from_line(self):
        ln = """- identifier: data-monitor-file-system-poller-watchdog
    spec:
      kind: data_monitor
      name: file_system_poller_watchdog
      config:
        path: ./incoming
        hostname: localhost

  - identifier: data-monitor-file-system-poller-watchdog
    spec:
      kind: data_monitor
      name: file_system_poller_watchdog
      config:
        path: ./incoming
        hostname: localhost"""
        assert ln.splitlines()[3].lstrip() == "name: file_system_poller_watchdog"
        upper, lower = get_spec_from_line(ln, 3)
        assert upper == 0
        assert lower == 6

    def test_two_spec_from_line_start_on_id(self):
        ln = """- identifier: data-monitor-file-system-poller-watchdog
    spec:
      kind: data_monitor
      name: file_system_poller_watchdog
      config:
        path: ./incoming
        hostname: localhost

  - identifier: data-monitor-file-system-poller-watchdog
    spec:
      kind: data_monitor
      name: file_system_poller_watchdog
      config:
        path: ./incoming
        hostname: localhost"""
        assert ln.splitlines()[3].lstrip() == "name: file_system_poller_watchdog"
        upper, lower = get_spec_from_line(ln, 8)
        assert upper == 8
        assert lower == 14


class TestFileMonitor:
    def test_detects_file_change(self,
                                 reload_config_file: Path,
                                 monkeypatch: pytest.MonkeyPatch):
        monitor = ConfigMonitor(
                config_file=str(reload_config_file),
                parent_service=MagicMock
        )

        changed = Event()

        monkeypatch.setattr(
            monitor,
            "_on_config_changed",
            changed.set()
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

