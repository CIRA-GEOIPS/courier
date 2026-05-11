"""Unit tests for courier.cli.init — non-interactive functions."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
import yaml

from courier.cli.init import (
    PluginSelection,
    _coerce_value,
    _make_identifier,
    build_service_config,
    validate_config,
    write_yaml,
)
from courier.plugins.classes.data_monitors.file_system_poller_watchdog import (
    FileSystemPoller,
    FileSystemPollerConfig,
)
from courier.plugins.classes.dispatchers.serial_bash import (
    SerialBashDispatcher,
    SerialBashConfig,
)
from courier.plugins.classes.job_builders.dummy_job_builder import (
    DummyJobBuilder,
    DummyJobBuilderConfig,
)


class TestMakeIdentifier:
    """Tests for _make_identifier()."""

    def test_replaces_underscores(self):
        """Underscores should be converted to hyphens."""
        result = _make_identifier("data_monitor", "rabbit_mq_watcher")
        assert result == "data-monitor-rabbit-mq-watcher"

    def test_lowercases(self):
        """Should output only lowercase."""
        result = _make_identifier("DataMonitor", "S3Poller")
        assert result == "datamonitor-s3poller"

    def test_strips_non_dns_chars(self):
        """Should remove characters that aren't alphanumeric or hyphens."""
        result = _make_identifier("data_monitor", "plugin@test!")
        assert result == "data-monitor-plugintest"

    def test_truncates_long_names(self):
        """Should truncate to 63 chars and not end with hyphen."""
        long_name = "a" * 100
        result = _make_identifier("data_monitor", long_name)
        assert len(result) <= 63
        assert not result.endswith("-")


class TestCoerceValue:
    """Tests for _coerce_value()."""

    def test_int(self):
        assert _coerce_value("42", "int") == 42

    def test_float(self):
        assert _coerce_value("3.14", "float") == 3.14

    def test_bool_true(self):
        assert _coerce_value("true", "bool") is True
        assert _coerce_value("yes", "bool") is True
        assert _coerce_value("1", "bool") is True

    def test_bool_false(self):
        assert _coerce_value("false", "bool") is False
        assert _coerce_value("no", "bool") is False

    def test_list_str(self):
        result = _coerce_value("a, b, c", "list[str]")
        assert result == ["a", "b", "c"]

    def test_empty_string_returns_sentinel(self):
        result = _coerce_value("", "str")
        assert result is ...


class TestBuildServiceConfig:
    """Tests for build_service_config()."""

    @staticmethod
    def _make_selection(plugin_class, plugin_name, yaml_kind, config_values=None):
        return PluginSelection(
            plugin_class=plugin_class,
            plugin_name=plugin_name,
            interface_kind="data_monitors",
            yaml_kind=yaml_kind,
            display_label="Data Monitor",
            config_model=None,
            config_values=config_values or {},
        )

    def test_basic_structure(self):
        sel = self._make_selection(FileSystemPoller, "file_system_poller_watchdog", "data_monitor")
        config = build_service_config(
            metadata={"name": "test-svc", "description": "test"},
            selections=[sel],
        )
        assert config["apiVersion"] == "runcourier.dev/v1alpha1"
        assert config["kind"] == "Service"
        assert config["metadata"]["name"] == "test-svc"
        assert len(config["spec"]["run"]) == 1

    def test_identifier_generation(self):
        sel = self._make_selection(FileSystemPoller, "file_system_poller_watchdog", "data_monitor")
        config = build_service_config(
            metadata={"name": "test", "description": "test"},
            selections=[sel],
        )
        assert config["spec"]["run"][0]["identifier"] == "data-monitor-file-system-poller-watchdog"

    def test_kind_is_singular(self):
        sel = self._make_selection(FileSystemPoller, "file_system_poller_watchdog", "data_monitor")
        config = build_service_config(
            metadata={"name": "test", "description": "test"},
            selections=[sel],
        )
        assert config["spec"]["run"][0]["spec"]["kind"] == "data_monitor"

    def test_config_values_included(self):
        sel = PluginSelection(
            plugin_class=FileSystemPoller,
            plugin_name="file_system_poller_watchdog",
            interface_kind="data_monitors",
            yaml_kind="data_monitor",
            display_label="Data Monitor",
            config_model=FileSystemPollerConfig,
            config_values={"path": "/tmp/watch", "hostname": "myhost"},
        )
        config = build_service_config(
            metadata={"name": "test", "description": "test"},
            selections=[sel],
        )
        assert config["spec"]["run"][0]["spec"]["config"]["path"] == "/tmp/watch"
        assert config["spec"]["run"][0]["spec"]["config"]["hostname"] == "myhost"

    def test_config_omitted_when_empty(self):
        sel = self._make_selection(FileSystemPoller, "file_system_poller_watchdog", "data_monitor")
        config = build_service_config(
            metadata={"name": "test", "description": "test"},
            selections=[sel],
        )
        assert "config" not in config["spec"]["run"][0]["spec"]

    def test_duplicate_names_add_suffix(self):
        sel = self._make_selection(FileSystemPoller, "file_system_poller_watchdog", "data_monitor")
        config = build_service_config(
            metadata={"name": "test", "description": "test"},
            selections=[sel, sel],
        )
        ids = [e["identifier"] for e in config["spec"]["run"]]
        assert ids[0] == "data-monitor-file-system-poller-watchdog"
        assert ids[1] == "data-monitor-file-system-poller-watchdog-2"
        assert len(set(ids)) == 2

    def test_multiple_plugins_different_types(self):
        sel_dm = PluginSelection(
            plugin_class=FileSystemPoller,
            plugin_name="file_system_poller_watchdog",
            interface_kind="data_monitors",
            yaml_kind="data_monitor",
            display_label="Data Monitor",
            config_model=FileSystemPollerConfig,
            config_values={"path": "/tmp"},
        )
        sel_jb = PluginSelection(
            plugin_class=DummyJobBuilder,
            plugin_name="DummyJobBuilder",
            interface_kind="job_builders",
            yaml_kind="job_builder",
            display_label="Job Builder",
            config_model=DummyJobBuilderConfig,
            config_values={},
        )
        sel_dp = PluginSelection(
            plugin_class=SerialBashDispatcher,
            plugin_name="serial_bash",
            interface_kind="dispatchers",
            yaml_kind="dispatcher",
            display_label="Dispatcher",
            config_model=SerialBashConfig,
            config_values={"bash_script": "echo hello"},
        )
        config = build_service_config(
            metadata={"name": "test", "description": "test"},
            selections=[sel_dm, sel_jb, sel_dp],
        )
        assert len(config["spec"]["run"]) == 3
        assert config["spec"]["run"][0]["spec"]["kind"] == "data_monitor"
        assert config["spec"]["run"][1]["spec"]["kind"] == "job_builder"
        assert config["spec"]["run"][2]["spec"]["kind"] == "dispatcher"


class TestValidateConfig:
    """Tests for validate_config()."""

    def test_valid_config_passes(self):
        sel = PluginSelection(
            plugin_class=FileSystemPoller,
            plugin_name="file_system_poller_watchdog",
            interface_kind="data_monitors",
            yaml_kind="data_monitor",
            display_label="Data Monitor",
            config_model=FileSystemPollerConfig,
            config_values={"path": "/tmp"},
        )
        config_dict = build_service_config(
            metadata={"name": "test", "description": "test"},
            selections=[sel],
        )
        validated = validate_config(config_dict)
        assert validated.metadata.name == "test"

    def test_invalid_config_raises(self):
        """Missing required fields should raise."""
        with pytest.raises(Exception):
            validate_config({"apiVersion": "bad", "kind": "Service", "metadata": {}, "spec": {}})

    def test_empty_run_raises(self):
        """Empty run list should be rejected."""
        with pytest.raises(Exception):
            validate_config({
                "apiVersion": "runcourier.dev/v1alpha1",
                "kind": "Service",
                "metadata": {
                    "name": "test",
                    "namespace": "test",
                    "description": "test",
                },
                "spec": {"run": []},
            })


class TestWriteYaml:
    """Tests for write_yaml()."""

    def test_roundtrip(self):
        """Generated YAML should round-trip through validation."""
        from rich.console import Console

        sel = PluginSelection(
            plugin_class=FileSystemPoller,
            plugin_name="file_system_poller_watchdog",
            interface_kind="data_monitors",
            yaml_kind="data_monitor",
            display_label="Data Monitor",
            config_model=FileSystemPollerConfig,
            config_values={"path": "/tmp"},
        )
        config_dict = build_service_config(
            metadata={"name": "test-roundtrip", "description": "roundtrip test"},
            selections=[sel],
        )
        validated = validate_config(config_dict)

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as tmp:
            tmp_path = Path(tmp.name)

        try:
            console = Console(file=None, width=80)  # quiet console
            write_yaml(validated, tmp_path, console)

            # Read back and re-validate
            with open(tmp_path) as f:
                written = yaml.safe_load(f)

            revalidated = validate_config(written)
            assert revalidated.metadata.name == "test-roundtrip"
        finally:
            tmp_path.unlink(missing_ok=True)
