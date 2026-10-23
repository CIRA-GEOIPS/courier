from pathlib import Path
from threading import Event
import pytest
from unittest.mock import MagicMock
import tempfile

from courier.cli.feedback import load_config_or_exit
from courier.cli.plugins import PLUGIN_REGISTRIES
from courier.managers.plugin_manager import PluginManager
from courier.schema import ServiceConfigModel
from courier.schema.v1alpha1.service_config import MicroserviceModel
from courier.service import Service
from courier.utils.config_monitor import ConfigMonitor, get_diff_lines, get_spec_from_buffer, get_spec_from_line


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
def reload_nested_config() -> str:
    example_service = (
"""
kind: Service
metadata:
  name: tmp-vvkvek7pth
  config_path: null
  namespace: tmp-vvkvek7pth
  description: 'A courier service: tmp-vvkvek7pth'
  docstring: null
  labels: {}
  annotations: {}
spec:
  broker:
    transport: memory
    max_retries: 5
  run:
  - identifier: job-builder-dummyjobbuilder
    spec:
      kind: job_builder
      name: DummyJobBuilder
      config:
        payload:
          identifier: payload-bash-payload
          spec:
            kind: payload
            name: bash_payload
            config:
              script: echo "hi"

"""
)
    return example_service


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
    
    def test_get_spec_from_buffer(self) -> None:
        spec = """- identifier: data-monitor-file-system-poller-watchdog
  spec:
    kind: data_monitor
    name: file_system_poller_watchdog
    config:
      path: ./incoming
      hostname: localhost"""

        model = get_spec_from_buffer(spec)
        assert model.identifier == "data-monitor-file-system-poller-watchdog"

    def test_get_parent_buffer_from_nested_plugin(self, reload_nested_config: str) -> None:    
        example_service = (
"""kind: Service
metadata:
  name: tmp-vvkvek7pth
  config_path: null
  namespace: tmp-vvkvek7pth
  description: 'A courier service: tmp-vvkvek7pth'
  docstring: null
  labels: {}
  annotations: {}
spec:
  broker:
    transport: memory
    max_retries: 5
  run:
  - identifier: job-builder-dummyjobbuilder
    spec:
      kind: job_builder
      name: DummyJobBuilder
      config:
        payload:
          identifier: payload-bash-payload
          spec:
            kind: payload
            name: bash_payload
            config:
              script: echo "hi"
""")

        upper, lower = get_spec_from_line(example_service, 22)
        assert upper == 14
        assert lower == 25

    def test_get_parent_buffer_from_two_nested_plugin(self, reload_nested_config: str) -> None:    
        example_service = (
"""kind: Service
metadata:
  name: tmp-vvkvek7pth
  config_path: null
  namespace: tmp-vvkvek7pth
  description: 'A courier service: tmp-vvkvek7pth'
  docstring: null
  labels: {}
  annotations: {}
spec:
  broker:
    transport: memory
    max_retries: 5
  run:
  - identifier: job-builder-dummyjobbuilder
    spec:
      kind: job_builder
      name: DummyJobBuilder
      config:
        payload:
          identifier: payload-bash-payload
          spec:
            kind: payload
            name: bash_payload
            config:
              script: echo "hi"
  - identifier: job-builder-dummyjobbuilder
    spec:
      kind: job_builder
      name: DummyJobBuilder
      config:
        payload:
          identifier: payload-bash-payload
          spec:
            kind: payload
            name: bash_payload
            config:
              script: echo "hi"
""")

        upper, lower = get_spec_from_line(example_service, 22)
        assert upper == 14
        assert lower == 25


    def test_get_parent_buffer_from_two_nested_plugin_bottom(self, reload_nested_config: str) -> None:    
        example_service = (
"""kind: Service
metadata:
  name: tmp-vvkvek7pth
  config_path: null
  namespace: tmp-vvkvek7pth
  description: 'A courier service: tmp-vvkvek7pth'
  docstring: null
  labels: {}
  annotations: {}
spec:
  broker:
    transport: memory
    max_retries: 5
  run:
  - identifier: job-builder-dummyjobbuilder
    spec:
      kind: job_builder
      name: DummyJobBuilder
      config:
        payload:
          identifier: payload-bash-payload
          spec:
            kind: payload
            name: bash_payload
            config:
              script: echo "hi"
  - identifier: job-builder-dummyjobbuilder
    spec:
      kind: job_builder
      name: DummyJobBuilder
      config:
        payload:
          identifier: payload-bash-payload
          spec:
            kind: payload
            name: bash_payload
            config:
              script: echo "hi"
""")

        upper, lower = get_spec_from_line(example_service, 27)
        assert upper == 26
        assert lower == 37

    def test_get_parent_spec_from_nested_plugin(self, reload_nested_config: str) -> None:    
        example_buffer = (
"""
  - identifier: job-builder-dummyjobbuilder
    spec:
      kind: job_builder
      name: DummyJobBuilder
      config:
        payload:
          identifier: payload-bash-payload
          spec:
            kind: payload
            name: bash_payload
            config:
              script: echo "hi"
""")

        spec = get_spec_from_buffer(example_buffer)
        assert spec.spec.config == {
                "payload": {
                    "identifier": "payload-bash-payload",
                    "spec": {
                        "kind": "payload",
                        "name": "bash_payload",
                        "config": {"script": "echo \"hi\""}
                        }
                    }
                }
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

class TestFullWorkflow:
    def test_replace_running_plugin(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        identifier = "data-monitor-file-system-poller-watchdog"

        old_plugin = MagicMock()

        manager = MagicMock()
        manager._plugins = {
            identifier: old_plugin,
        }

        new_plugin_info = MagicMock()

        def mock_register_plugin(plugin_class, config, plugin_id):
            manager._plugins[plugin_id] = new_plugin_info

        manager.register_plugin.side_effect = mock_register_plugin

        new_plugin_class = MagicMock()
        registry = MagicMock()
        registry.get_plugin.return_value = new_plugin_class

        monkeypatch.setitem(
            PLUGIN_REGISTRIES,
            "data_monitors",
            registry,
        )

        service = MicroserviceModel.model_validate({
            "identifier": identifier,
            "spec": {
                "kind": "data_monitor",
                "name": "file_system_poller_watchdog",
                "config": {
                    "path": "./new_incoming",
                    "hostname": "localhost",
                },
            },
        })

        config_file = tmp_path / "service.yaml"
        config_file.write_text("")

        monitor = ConfigMonitor(
            config_file=str(config_file),
            parent_service=manager,
        )

        monitor._replace_running_plugin(service)

        manager._stop_plugin.assert_called_once_with(old_plugin)

        registry.get_plugin.assert_called_once_with(
            "file_system_poller_watchdog"
        )

        manager.register_plugin.assert_called_once_with(
            new_plugin_class,
            {
                "path": "./new_incoming",
                "hostname": "localhost",
            },
            identifier,
        )
