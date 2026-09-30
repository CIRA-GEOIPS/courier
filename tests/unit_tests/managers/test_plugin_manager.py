"""Unit tests for PluginManager (ISSUE 10, 13, 14)."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

import pytest
from prometheus_client import REGISTRY

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


def _non_threaded_plugin_cls(name: str) -> type:
    """Return a real class whose instances are healthy thread-less sub-plugins."""
    instance = _make_plugin(name, healthy=True)
    instance.threaded = False
    return _plugin_cls_for(instance)


def _state_gauge(name: str, identifier: str) -> float | None:
    """Read ``courier_plugin_state`` for one plugin from the registry."""
    return REGISTRY.get_sample_value(
        "courier_plugin_state",
        {"plugin_name": name, "plugin_identifier": identifier},
    )


def _run_one_monitor_pass(manager: PluginManager) -> None:
    """Run exactly one iteration of the monitor loop, synchronously."""

    def _stop_after_pass(_seconds: float) -> None:
        manager._state = PluginRunState.STOPPED

    manager._state = PluginRunState.RUNNING
    with patch("courier.managers.plugin_manager.time.sleep", _stop_after_pass):
        manager._monitor_plugins()


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

        with (
            patch.object(manager, "_plugin_state_metric") as mock_state,
            patch.object(manager, "_plugin_health_metric"),
        ):
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

        with (
            patch.object(manager, "_plugin_state_metric"),
            patch.object(manager, "_plugin_health_metric") as mock_health,
        ):
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

        with patch.object(manager, "_registration_failures_metric") as mock_reg_fail:
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


class TestNonThreadedSubPlugin:
    """Payloads and other sub-plugins have no run loop of their own."""

    def test_non_threaded_plugin_runs_without_a_thread(self) -> None:
        config = _make_config()
        manager = PluginManager(config, parent_service=MagicMock())
        plugin = _make_plugin("payload-one")
        plugin.threaded = False
        plugin.start = MagicMock()

        info = PluginStateInfo(plugin=plugin, threaded=False)
        manager._state = PluginRunState.RUNNING
        manager._plugins["payload-one"] = info

        manager._start_plugin(info)

        assert info.thread is None
        assert info.state == PluginRunState.RUNNING
        plugin.start.assert_called_once()

    def test_start_failure_records_the_error_and_exports_failed(self) -> None:
        """A failed sub-plugin reads FAILED everywhere, not STARTING forever."""
        config = _make_config()
        manager = PluginManager(config, parent_service=MagicMock())
        plugin = _make_plugin("bad-payload")
        plugin.identifier = "bad-payload-id"
        plugin.threaded = False
        plugin.start.side_effect = RuntimeError("missing license file")
        manager.register_plugin(
            _plugin_cls_for(plugin),
            {},
            identifier="bad-payload-id",
        )
        info = manager.get_plugins()["bad-payload-id"]
        manager._state = PluginRunState.RUNNING

        manager._start_plugin(info)

        assert info.thread is None
        assert info.state == PluginRunState.FAILED
        assert "missing license file" in (info.error_message or "")
        assert not info.ready.is_set()
        assert _state_gauge("bad-payload", "bad-payload-id") == (
            PluginRunState.FAILED.value
        )

    def test_monitor_does_not_treat_a_thread_less_sub_plugin_as_dead(self) -> None:
        """No thread is the normal state of a payload, not a crashed one."""
        config = _make_config()
        manager = PluginManager(config, parent_service=MagicMock())
        plugin = _make_plugin("payload-mon", healthy=True)
        plugin.threaded = False
        info = PluginStateInfo(
            plugin=plugin,
            state=PluginRunState.RUNNING,
            threaded=False,
        )
        manager._plugins["payload-mon"] = info

        _run_one_monitor_pass(manager)

        assert info.state == PluginRunState.RUNNING
        assert info.restart_count == 0
        plugin.stop.assert_not_called()
        plugin.is_healthy.assert_called()

    def test_monitor_still_reports_a_dead_threaded_plugin(self) -> None:
        """The exemption is for sub-plugins only; a builder's thread counts."""
        config = _make_config(plugin_max_restart_attempts=0)
        manager = PluginManager(config, parent_service=MagicMock())
        plugin = _make_plugin("builder-mon")
        info = PluginStateInfo(plugin=plugin, state=PluginRunState.RUNNING)
        manager._plugins["builder-mon"] = info

        _run_one_monitor_pass(manager)

        assert info.state == PluginRunState.FAILED


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

    def test_a_running_sub_plugin_does_not_mask_a_failed_builder(self) -> None:
        """A payload is always RUNNING, so it must not count as a survivor.

        Otherwise every builder container (a builder plus its payload) keeps
        running with no consumer when the builder refuses to start.
        """
        config = _make_config(plugin_health_check_interval=0.5)
        manager = PluginManager(config, parent_service=MagicMock())
        manager.register_plugin(_failing_plugin_cls("builder"), {}, identifier="jb")
        manager.register_plugin(
            _non_threaded_plugin_cls("payload"),
            {},
            identifier="pl",
        )

        try:
            with pytest.raises(
                RuntimeError,
                match="All plugins failed.*builder=",
            ) as err:
                manager.start()
            assert "payload" not in str(err.value)
            assert manager.get_plugins()["pl"].state == PluginRunState.RUNNING
        finally:
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

    def test_a_running_sub_plugin_does_not_mask_a_failed_builder(self) -> None:
        """Health reflects runnable plugins; a payload has no work of its own."""
        config = _make_config()
        manager = PluginManager(config, parent_service=MagicMock())
        manager.register_plugin(
            _plugin_cls_for(_make_plugin("builder", healthy=True)),
            {},
            identifier="jb",
        )
        manager.register_plugin(
            _non_threaded_plugin_cls("payload"),
            {},
            identifier="pl",
        )
        plugins = manager.get_plugins()
        plugins["jb"].state = PluginRunState.FAILED
        plugins["pl"].state = PluginRunState.RUNNING
        manager._state = PluginRunState.RUNNING

        assert not manager.is_healthy()

    def test_sub_plugins_alone_leave_nothing_to_be_unhealthy(self) -> None:
        """With no runnable plugin registered there is nothing to fail."""
        config = _make_config()
        manager = PluginManager(config, parent_service=MagicMock())
        manager.register_plugin(
            _non_threaded_plugin_cls("p-payload"),
            {},
            identifier="p-payload",
        )
        manager.get_plugins()["p-payload"].state = PluginRunState.RUNNING
        manager._state = PluginRunState.RUNNING

        assert manager.is_healthy()


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
