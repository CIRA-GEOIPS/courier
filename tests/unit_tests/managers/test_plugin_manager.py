"""Unit tests for PluginManager (ISSUE 10, 13, 14)."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

import pytest

from courier.config import ServiceConfig
from courier.constants import PluginRunState
from courier.interfaces.plugin_protocol import ServicePlugin
from courier.managers.plugin_manager import PluginManager, PluginStateInfo

# ── helpers ─────────────────────────────────────────────────────────────────


def _make_config(**overrides) -> ServiceConfig:
    """Build a ServiceConfig with safe defaults for unit tests."""
    defaults = {
        "service_id": "test-svc",
        "namespace": "test-ns",
        "plugin_health_check_interval": 1.0,
        "plugin_max_restart_attempts": 3,
        "plugin_restart_delay": 0,
        "loki_enabled": False,
    }
    defaults.update(overrides)
    return ServiceConfig(**defaults)


def _make_plugin(name: str = "test-plugin", healthy: bool = True) -> MagicMock:
    """Build a mock ServicePlugin with *name* and *healthy*."""
    plugin = MagicMock(spec=ServicePlugin)
    plugin.name = name
    plugin.version = "0.1.0"
    plugin.is_healthy.return_value = healthy
    plugin.get_metrics.return_value = {}
    return plugin


def _plugin_cls_for(mock: MagicMock) -> type:
    """Return a real class that instantiates to *mock*.

    ``register_plugin`` calls ``issubclass(plugin_cls, Dispatcher)``
    which requires a real class — MagicMock with ``return_value=``
    will not work.
    """

    class _FakeCls:
        def __new__(cls, *args, **kwargs):  # noqa: ARG004
            return mock

    return _FakeCls


def _failing_plugin_cls(name: str) -> type:
    """Return a real class whose instances have start() that raises."""

    class _FakeFailingCls:
        def __new__(cls, *args, **kwargs):  # noqa: ARG004
            instance = MagicMock(spec=ServicePlugin)
            instance.name = name
            instance.version = "0.1.0"
            instance.is_healthy.return_value = False
            instance.get_metrics.return_value = {}
            instance.start.side_effect = RuntimeError("crash")
            return instance

    return _FakeFailingCls


# ═════════════════════════════════════════════════════════════════════════════
# _plugin_identifier static method (ISSUE 8)
# ═════════════════════════════════════════════════════════════════════════════


class TestPluginIdentifier:
    """Tests for the _plugin_identifier helper."""

    def test_returns_config_identifier_when_present(self) -> None:
        """When plugin has an 'identifier' attr, it is returned."""
        plugin = _make_plugin("field-name")
        plugin.identifier = "yaml-id"
        assert PluginManager._plugin_identifier(plugin) == "yaml-id"

    def test_falls_back_to_plugin_name(self) -> None:
        """When plugin has no 'identifier' attr, name is used."""
        plugin = _make_plugin("fallback-name")
        assert PluginManager._plugin_identifier(plugin) == "fallback-name"


# ═════════════════════════════════════════════════════════════════════════════
# register_plugin — immediate metrics on registration (ISSUE 10)
# ═════════════════════════════════════════════════════════════════════════════


class TestRegisterPlugin:
    """Tests for register_plugin (ISSUE 10 — lifecycle metrics)."""

    def test_registration_sets_state_to_starting(self) -> None:
        """After registration the STARTING metric is emitted."""
        config = _make_config()
        parent = MagicMock()
        manager = PluginManager(config, parent_service=parent)

        mock_instance = _make_plugin("reg-test")
        clazz = _plugin_cls_for(mock_instance)

        with patch.object(manager, "_plugin_state_metric") as mock_state, \
             patch.object(manager, "_plugin_health_metric"):
            manager.register_plugin(clazz, {}, identifier="reg-test")

        mock_state.labels.assert_called_once()
        _, kwargs = mock_state.labels.call_args
        assert kwargs["plugin_name"] == "reg-test"
        assert "plugin_identifier" in kwargs

    def test_registration_sets_health_to_zero(self) -> None:
        """Registration sets health metric to 0 (not yet healthy)."""
        config = _make_config()
        parent = MagicMock()
        manager = PluginManager(config, parent_service=parent)

        mock_instance = _make_plugin("reg-test-2")
        clazz = _plugin_cls_for(mock_instance)

        with patch.object(manager, "_plugin_state_metric"), \
             patch.object(manager, "_plugin_health_metric") as mock_health:
            manager.register_plugin(clazz, {}, identifier="reg-test-2")

        mock_health.labels.return_value.set.assert_called_once_with(0)

    def test_duplicate_key_increments_registration_failures(self) -> None:
        """Second registration with same key increments failures counter."""
        config = _make_config()
        parent = MagicMock()
        manager = PluginManager(config, parent_service=parent)

        mock1 = _make_plugin("b1")
        clazz1 = _plugin_cls_for(mock1)
        manager.register_plugin(clazz1, {}, identifier="dup-key")

        mock2 = _make_plugin("b2")
        clazz2 = _plugin_cls_for(mock2)
        mock2.identifier = "dup-key"

        with patch.object(
            manager, "_registration_failures_metric"
        ) as mock_reg_fail:
            with pytest.raises(ValueError, match="already registered"):
                manager.register_plugin(clazz2, {}, identifier="dup-key")

        mock_reg_fail.labels.assert_called_once()
        _, kwargs = mock_reg_fail.labels.call_args
        assert kwargs["reason"] == "duplicate_key"


# ═════════════════════════════════════════════════════════════════════════════
# Eager health check after plugin.start() (ISSUE 14)
# ═════════════════════════════════════════════════════════════════════════════


class TestStartPluginEagerHealth:
    """Tests for _start_plugin eager health gate (ISSUE 14)."""

    def test_healthy_plugin_transitions_to_running(self) -> None:
        """When start() returns and is_healthy() is True → RUNNING."""
        config = _make_config(plugin_health_check_interval=2.0)
        manager = PluginManager(config, parent_service=MagicMock())
        plugin = _make_plugin("healthy-one")
        plugin.start = MagicMock()
        plugin.is_healthy.return_value = True

        info = PluginStateInfo(plugin=plugin)
        manager._state = PluginRunState.RUNNING
        manager._plugins["healthy-one"] = info

        manager._start_plugin(info)
        info.ready.wait(timeout=5.0)
        info.thread.join(timeout=5.0)  # type: ignore[union-attr]

        assert info.state == PluginRunState.RUNNING

    def test_unhealthy_plugin_still_proceeds_to_running(self) -> None:
        """When is_healthy() is always False, eventually proceeds to RUNNING."""
        config = _make_config(plugin_health_check_interval=0.5)
        manager = PluginManager(config, parent_service=MagicMock())
        plugin = _make_plugin("slow-one")
        plugin.start = MagicMock()
        plugin.is_healthy.return_value = False

        info = PluginStateInfo(plugin=plugin)
        manager._state = PluginRunState.RUNNING
        manager._plugins["slow-one"] = info

        manager._start_plugin(info)
        info.ready.wait(timeout=5.0)
        info.thread.join(timeout=5.0)  # type: ignore[union-attr]

        assert info.state == PluginRunState.RUNNING

    def test_start_raises_plugin_goes_failed(self) -> None:
        """When plugin.start() raises, state transitions to FAILED."""
        config = _make_config()
        manager = PluginManager(config, parent_service=MagicMock())
        plugin = _make_plugin("crashy")
        plugin.start.side_effect = RuntimeError("boom")

        info = PluginStateInfo(plugin=plugin)
        manager._state = PluginRunState.RUNNING
        manager._plugins["crashy"] = info

        manager._start_plugin(info)
        info.thread.join(timeout=5.0)  # type: ignore[union-attr]

        assert info.state == PluginRunState.FAILED
        assert "boom" in (info.error_message or "")


# ═════════════════════════════════════════════════════════════════════════════
# start() — bulk start with verify (ISSUE 13)
# ═════════════════════════════════════════════════════════════════════════════


class TestStartAll:
    """Tests for start() bulk verify (ISSUE 13)."""

    def test_no_plugins_registered_succeeds(self) -> None:
        """start() with no plugins succeeds (monitor thread runs briefly)."""
        config = _make_config()
        manager = PluginManager(config, parent_service=MagicMock())

        manager.start()
        manager.stop()  # type: ignore[unused-coroutine]

    def test_all_plugins_failed_raises_runtime_error(self) -> None:
        """If every plugin fails to start, RuntimeError is raised."""
        config = _make_config(plugin_health_check_interval=0.5)
        manager = PluginManager(config, parent_service=MagicMock())

        c1 = _failing_plugin_cls("fail-1")
        manager.register_plugin(c1, {}, identifier="f1")

        c2 = _failing_plugin_cls("fail-2")
        manager.register_plugin(c2, {}, identifier="f2")

        with pytest.raises(RuntimeError, match="All plugins failed"):
            manager.start()

        manager.stop()  # type: ignore[unused-coroutine]


# ═════════════════════════════════════════════════════════════════════════════
# is_healthy (ISSUE 14)
# ═════════════════════════════════════════════════════════════════════════════


class TestIsHealthy:
    """Tests for is_healthy() (ISSUE 14)."""

    def test_empty_plugins_is_healthy(self) -> None:
        """No registered plugins → healthy."""
        config = _make_config()
        manager = PluginManager(config, parent_service=MagicMock())
        assert manager.is_healthy()

    def test_not_running_with_plugins_is_unhealthy(self) -> None:
        """Plugins registered but not started → not healthy."""
        config = _make_config()
        manager = PluginManager(config, parent_service=MagicMock())

        p = _make_plugin("p1")
        clazz = _plugin_cls_for(p)
        manager.register_plugin(clazz, {}, identifier="p1")

        assert not manager.is_healthy()

    def test_running_with_healthy_plugin_is_healthy(self) -> None:
        """Running + healthy plugin → healthy."""
        config = _make_config()
        manager = PluginManager(config, parent_service=MagicMock())

        p = _make_plugin("p-healthy", healthy=True)
        clazz = _plugin_cls_for(p)
        manager.register_plugin(clazz, {}, identifier="p-healthy")

        info = manager.get_plugins()["p-healthy"]
        info.state = PluginRunState.RUNNING
        # Long-lived thread so it's still alive during is_healthy check
        barrier = threading.Barrier(2)
        info.thread = threading.Thread(target=barrier.wait)
        info.thread.start()
        manager._state = PluginRunState.RUNNING

        assert manager.is_healthy()

        barrier.wait()  # release the thread
        info.thread.join()


# ═════════════════════════════════════════════════════════════════════════════
# _stop_plugin
# ═════════════════════════════════════════════════════════════════════════════


class TestStopPlugin:
    """Tests for _stop_plugin."""

    def test_running_plugin_transitions_to_stopped(self) -> None:
        """A RUNNING plugin becomes STOPPED after _stop_plugin."""
        config = _make_config()
        manager = PluginManager(config, parent_service=MagicMock())

        plugin = _make_plugin("to-stop")
        info = PluginStateInfo(plugin=plugin, state=PluginRunState.RUNNING)
        info.thread = threading.Thread(target=lambda: None)

        with patch.object(manager, "_plugin_state_metric") as mock_state:
            manager._stop_plugin(info)

        assert info.state == PluginRunState.STOPPED


# ═════════════════════════════════════════════════════════════════════════════
# get_plugins snapshots
# ═════════════════════════════════════════════════════════════════════════════


class TestGetPlugins:
    """Tests for get_plugins() thread-safe snapshot."""

    def test_mutating_snapshot_does_not_affect_internal_state(self) -> None:
        """Pop from returned dict doesn't remove from internal store."""
        config = _make_config()
        manager = PluginManager(config, parent_service=MagicMock())

        p = _make_plugin("snapshot-p")
        clazz = _plugin_cls_for(p)
        manager.register_plugin(clazz, {}, identifier="snapshot-p")

        snap = manager.get_plugins()
        assert "snapshot-p" in snap
        snap.pop("snapshot-p")
        assert "snapshot-p" in manager.get_plugins()
