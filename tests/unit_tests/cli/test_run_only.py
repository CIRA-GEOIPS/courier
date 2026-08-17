"""Tests for run_service --only flag filtering and validation logic."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import ANY, MagicMock, patch

import pytest

from courier.cli.run import run as cli_run
from courier.cli.run import run_service
from courier.config import ServiceConfig

def _make_entry(identifier: str, kind: str, name: str, config: dict | None = None):
    """Build a mock config run entry with the given attributes."""
    entry = MagicMock()
    entry.identifier = identifier
    entry.spec.kind = kind
    entry.spec.name = name
    entry.spec.config = config or {}
    return entry


def _make_config(entries, **extra_spec_attrs):
    """Build a mock config from a list of run entries."""
    config = MagicMock()

    config.spec.run = entries
    config.spec.broker.to_url.return_value = "memory://"
    config.spec.broker.max_retries = 5
    config.spec.allow_implicit_target = True
    config.spec.service_config = ServiceConfig(heartbeat_interval=30)
    
    config.metadata.namespace = "test"
    config.metadata.name = "test-service"
    for attr, value in extra_spec_attrs.items():
        setattr(config.spec, attr, value)
    return config


def _make_registry():
    """Build a mock plugin registry whose get_plugin returns a mock class."""
    mock_plugin = MagicMock()
    mock_plugin.__class__ = type("FakePlugin", (), {})
    registry = MagicMock()
    registry.get_plugin.return_value = mock_plugin
    return registry


def _plugin_registries_fixture():
    """Return a dict mapping the three runnable kinds to the same mock registry.

    Keys must be the *plural* interface names, matching the real
    ``courier.cli.plugins.PLUGIN_REGISTRIES``: ``run_service`` looks up
    ``PLUGIN_REGISTRIES[normalize_kind(entry.spec.kind)]``, and
    ``normalize_kind`` maps the singular YAML kinds onto these plural keys.
    """
    reg = _make_registry()
    return {
        "data_monitors": reg,
        "job_builders": reg,
        "dispatchers": reg,
    }


class TestOnlyFlag:
    """Tests for the --only flag on run_service (filtering + validation)."""

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _entries(extra=None):
        """Default three-entry run list used by most tests."""
        entries = [
            _make_entry("my-dm", "data_monitor", "rabbit_mq_watcher"),
            _make_entry("my-jb", "job_builder", "filter_and_group"),
            _make_entry("my-dp", "dispatcher", "serial_bash"),
        ]
        if extra:
            entries.extend(extra)
        return entries

    # ------------------------------------------------------------------
    # Test 1 — None runs everything
    # ------------------------------------------------------------------

    @patch("courier.cli.run.create_service_with_plugins")
    @patch("courier.cli.run.PLUGIN_REGISTRIES", _plugin_registries_fixture())
    def test_only_none_runs_all_plugins(self, mock_create_svc):
        """only_set=None should register every runnable plugin unconditionally."""
        entries = self._entries()
        config = _make_config(entries)

        run_service(config, only_set=None)

        expected_count = 3  # all three entries are runnable kinds
        registrations = mock_create_svc.call_args[0][1]
        assert len(registrations) == expected_count, (
            f"Expected 3 registrations, got {len(registrations)}"
        )

        registered_ids = {r[2] for r in registrations}
        assert registered_ids == {"my-dm", "my-jb", "my-dp"}

        svc = mock_create_svc.return_value
        svc.configure_routing.assert_called_once()
        _, kwargs = svc.configure_routing.call_args
        # dispatcher_ids only includes dispatcher-kind entries
        assert kwargs["dispatcher_identifiers"] == {"my-dp"}

    # ------------------------------------------------------------------
    # Test 2 — single valid identifier
    # ------------------------------------------------------------------

    @patch("courier.cli.run.create_service_with_plugins")
    @patch("courier.cli.run.PLUGIN_REGISTRIES", _plugin_registries_fixture())
    def test_only_single_valid_filters_to_one(self, mock_create_svc):
        """only_set={'my-dm'} should register exactly the data_monitor plugin."""
        entries = self._entries()
        config = _make_config(entries)

        run_service(config, only_set={"my-dm"})

        registrations = mock_create_svc.call_args[0][1]
        assert len(registrations) == 1
        assert registrations[0][2] == "my-dm"

        svc = mock_create_svc.return_value
        svc.configure_routing.assert_called_once()
        _, kwargs = svc.configure_routing.call_args
        # No dispatcher in only_set, so dispatcher_ids is empty
        assert kwargs["dispatcher_identifiers"] == set()

    # ------------------------------------------------------------------
    # Test 3 — unknown identifier
    # ------------------------------------------------------------------

    @patch("courier.cli.run.create_service_with_plugins")
    @patch("courier.cli.run.PLUGIN_REGISTRIES", _plugin_registries_fixture())
    def test_only_unknown_identifier_raises(self, mock_create_svc):
        """An identifier not present in the config must raise ValueError."""
        entries = self._entries()
        config = _make_config(entries)

        with pytest.raises(ValueError, match="Unknown plugin identifiers"):
            run_service(config, only_set={"nonexistent"})

        mock_create_svc.assert_not_called()

    # ------------------------------------------------------------------
    # Test 4 — data_monitor_configs in only_set
    # ------------------------------------------------------------------

    @patch("courier.cli.run.create_service_with_plugins")
    @patch("courier.cli.run.PLUGIN_REGISTRIES", _plugin_registries_fixture())
    def test_only_with_data_monitor_configs_raises(self, mock_create_svc):
        """data_monitor_configs entries are not runnable — must raise."""
        entry = _make_entry("my-dmc-config", "data_monitor_configs", "some_yaml")
        entries = [entry]
        config = _make_config(entries)

        with pytest.raises(ValueError, match="data_monitor_configs"):
            run_service(config, only_set={"my-dmc-config"})

        mock_create_svc.assert_not_called()

    # ------------------------------------------------------------------
    # Test 5 — multiple valid identifiers
    # ------------------------------------------------------------------

    @patch("courier.cli.run.create_service_with_plugins")
    @patch("courier.cli.run.PLUGIN_REGISTRIES", _plugin_registries_fixture())
    def test_only_multiple_valid_registers_two(self, mock_create_svc):
        """only_set with two valid IDs registers exactly those two plugins."""
        entries = self._entries()
        config = _make_config(entries)

        run_service(config, only_set={"my-dm", "my-jb"})

        expected_count = 2
        registrations = mock_create_svc.call_args[0][1]
        assert len(registrations) == expected_count
        registered_ids = {r[2] for r in registrations}
        assert registered_ids == {"my-dm", "my-jb"}

    # ------------------------------------------------------------------
    # Test 6 — builder targets union into dispatcher_ids
    # ------------------------------------------------------------------

    @patch("courier.cli.run.create_service_with_plugins")
    @patch("courier.cli.run.PLUGIN_REGISTRIES", _plugin_registries_fixture())
    def test_only_builder_without_dispatcher_unions_targets(self, mock_create_svc):
        """Dispatcher targeted by builder joins dispatcher_ids even when absent."""
        entries = [
            _make_entry("dm-1", "data_monitor", "rabbit_mq_watcher"),
            _make_entry(
                "jb-1", "job_builder", "filter_and_group",
                config={"targets": ["dp-remote"]},
            ),
            _make_entry("dp-remote", "dispatcher", "serial_bash"),
        ]
        config = _make_config(entries)

        run_service(config, only_set={"dm-1", "jb-1"})

        expected_count = 2
        registrations = mock_create_svc.call_args[0][1]
        assert len(registrations) == expected_count, (
            f"Expected 2 registrations, got {len(registrations)}"
        )
        registered_ids = {r[2] for r in registrations}
        assert registered_ids == {"dm-1", "jb-1"}

        svc = mock_create_svc.return_value
        svc.configure_routing.assert_called_once()
        _, kwargs = svc.configure_routing.call_args
        # dispatcher_ids is the union of (dispatchers in only_set) and builder targets
        assert "dp-remote" in kwargs["dispatcher_identifiers"], (
            f"Expected dp-remote in dispatcher_identifiers, "
            f"got {kwargs['dispatcher_identifiers']}"
        )

    # ------------------------------------------------------------------
    # Test 7 — dispatcher-only
    # ------------------------------------------------------------------

    @patch("courier.cli.run.create_service_with_plugins")
    @patch("courier.cli.run.PLUGIN_REGISTRIES", _plugin_registries_fixture())
    def test_only_dispatcher_only_registers_dispatcher(self, mock_create_svc):
        """only_set={'my-dp'} registers just the dispatcher plugin."""
        entries = self._entries()
        config = _make_config(entries)

        run_service(config, only_set={"my-dp"})

        registrations = mock_create_svc.call_args[0][1]
        assert len(registrations) == 1
        assert registrations[0][2] == "my-dp"

        svc = mock_create_svc.return_value
        svc.configure_routing.assert_called_once()
        _, kwargs = svc.configure_routing.call_args
        assert kwargs["dispatcher_identifiers"] == {"my-dp"}

    # ------------------------------------------------------------------
    # Test 8 — empty only_set edge case
    # ------------------------------------------------------------------

    @patch("courier.cli.run.create_service_with_plugins")
    @patch("courier.cli.run.PLUGIN_REGISTRIES", _plugin_registries_fixture())
    def test_only_empty_set_registers_nothing(self, mock_create_svc):
        """only_set=set() should skip every entry. create_service still called."""
        entries = self._entries()
        config = _make_config(entries)

        run_service(config, only_set=set())

        registrations = mock_create_svc.call_args[0][1]
        assert registrations == [], (
            f"Expected empty registrations list, got {registrations}"
        )

        # create_service_with_plugins is still called (with empty list)
        mock_create_svc.assert_called_once()

    # ------------------------------------------------------------------
    # Test 9 — mixed valid + unknown identifiers (fail-fast)
    # ------------------------------------------------------------------

    @patch("courier.cli.run.create_service_with_plugins")
    @patch("courier.cli.run.PLUGIN_REGISTRIES", _plugin_registries_fixture())
    def test_only_mixed_valid_and_unknown_raises_before_registration(
        self, mock_create_svc,
    ):
        """Validation fails before any plugin is registered when any ID is unknown."""
        entries = self._entries()
        config = _make_config(entries)

        with pytest.raises(ValueError, match="Unknown plugin identifiers"):
            run_service(config, only_set={"my-dm", "nonexistent"})

        mock_create_svc.assert_not_called()


class TestRunCLIOnlyParsing:
    """Tests for the ``run()`` CLI entry-point --only parsing layer."""

    # ------------------------------------------------------------------
    # Test 1 — empty string maps to None
    # ------------------------------------------------------------------

    @patch("courier.cli.run.run_service")
    @patch("courier.cli.run.load_config_or_exit")
    def test_only_empty_string_maps_to_none(
        self, mock_load_config, mock_run_service,
    ):
        """An empty --only string is treated like --only was never supplied."""
        mock_load_config.return_value = MagicMock()
        ctx = MagicMock()
        ctx.obj = {}
        config_file = MagicMock(spec=Path)
        config_file.exists.return_value = True

        cli_run(ctx, config_file, only="")

        mock_run_service.assert_called_once_with(
            ANY, log_level=None, only_set=None,
        )

    # ------------------------------------------------------------------
    # Test 2 — lowercase normalization
    # ------------------------------------------------------------------

    @patch("courier.cli.run.run_service")
    @patch("courier.cli.run.load_config_or_exit")
    def test_only_lowercase_normalization(
        self, mock_load_config, mock_run_service,
    ):
        """Uppercase-only identifiers are lowercased before reaching run_service."""
        mock_load_config.return_value = MagicMock()
        ctx = MagicMock()
        ctx.obj = {}
        config_file = MagicMock(spec=Path)
        config_file.exists.return_value = True

        cli_run(ctx, config_file, only="MY-DM")

        mock_run_service.assert_called_once_with(
            ANY, log_level=None, only_set={"my-dm"},
        )

    # ------------------------------------------------------------------
    # Test 3 — strips spaces
    # ------------------------------------------------------------------

    @patch("courier.cli.run.run_service")
    @patch("courier.cli.run.load_config_or_exit")
    def test_only_strips_spaces(
        self, mock_load_config, mock_run_service,
    ):
        """Surrounding and inter-word spaces are stripped from each part."""
        mock_load_config.return_value = MagicMock()
        ctx = MagicMock()
        ctx.obj = {}
        config_file = MagicMock(spec=Path)
        config_file.exists.return_value = True

        cli_run(ctx, config_file, only=" my-dm , my-jb ")

        mock_run_service.assert_called_once_with(
            ANY, log_level=None, only_set={"my-dm", "my-jb"},
        )

    # ------------------------------------------------------------------
    # Test 4 — deduplicates
    # ------------------------------------------------------------------

    @patch("courier.cli.run.run_service")
    @patch("courier.cli.run.load_config_or_exit")
    def test_only_deduplicates(
        self, mock_load_config, mock_run_service,
    ):
        """Duplicate identifiers are collapsed into a single entry."""
        mock_load_config.return_value = MagicMock()
        ctx = MagicMock()
        ctx.obj = {}
        config_file = MagicMock(spec=Path)
        config_file.exists.return_value = True

        cli_run(ctx, config_file, only="my-dm,my-dm")

        mock_run_service.assert_called_once_with(
            ANY, log_level=None, only_set={"my-dm"},
        )

    # ------------------------------------------------------------------
    # Test 5 — trailing comma ignored (empty parts filtered)
    # ------------------------------------------------------------------

    @patch("courier.cli.run.run_service")
    @patch("courier.cli.run.load_config_or_exit")
    def test_only_trailing_comma_ignored(
        self, mock_load_config, mock_run_service,
    ):
        """Trailing commas produce no empty identifier in the resulting set."""
        mock_load_config.return_value = MagicMock()
        ctx = MagicMock()
        ctx.obj = {}
        config_file = MagicMock(spec=Path)
        config_file.exists.return_value = True

        cli_run(ctx, config_file, only="my-dm,")

        mock_run_service.assert_called_once_with(
            ANY, log_level=None, only_set={"my-dm"},
        )
