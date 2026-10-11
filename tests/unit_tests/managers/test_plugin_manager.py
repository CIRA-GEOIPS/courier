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
        try:
            assert info.ready.wait(timeout=5.0)
            assert info.state == PluginRunState.RUNNING
        finally:
            # The plugin thread loops while the manager is RUNNING; leaving it
            # alive leaks a thread into later tests that patch time.sleep.
            manager._state = PluginRunState.STOPPED
            info.thread.join(timeout=5.0)  # type: ignore[union-attr]
        assert not info.thread.is_alive()  # type: ignore[union-attr]

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
        try:
            assert info.ready.wait(timeout=5.0)
            assert info.state == PluginRunState.RUNNING
        finally:
            manager._state = PluginRunState.STOPPED
            info.thread.join(timeout=5.0)  # type: ignore[union-attr]
        assert not info.thread.is_alive()  # type: ignore[union-attr]

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


# ═════════════════════════════════════════════════════════════════════════════
# monitor loop: health checks, restart scheduling
# ═════════════════════════════════════════════════════════════════════════════


def _live_thread() -> threading.Thread:
    """Return a started thread that stays alive until the test ends."""
    stop = threading.Event()
    thread = threading.Thread(target=stop.wait, daemon=True)
    thread.start()
    thread.stop_event = stop  # type: ignore[attr-defined]
    return thread


def _running_info(plugin: MagicMock) -> PluginStateInfo:
    return PluginStateInfo(
        plugin=plugin,
        state=PluginRunState.RUNNING,
        thread=_live_thread(),
    )


class TestMonitorChecks:
    """One pass of the monitor loop, driven by hand."""

    def test_an_unhealthy_plugin_is_marked_failed(self) -> None:
        manager = PluginManager(_make_config(), parent_service=MagicMock())
        plugin = _make_plugin("sick", healthy=False)
        info = _running_info(plugin)

        assert manager._check_plugin("sick", info) is True
        assert info.state == PluginRunState.FAILED
        assert info.last_health_check is not None

    def test_a_dead_thread_is_failed_without_asking_the_plugin(self) -> None:
        manager = PluginManager(_make_config(), parent_service=MagicMock())
        plugin = _make_plugin("dead")
        dead = threading.Thread(target=lambda: None)
        dead.start()
        dead.join()
        info = PluginStateInfo(plugin=plugin, state=PluginRunState.RUNNING, thread=dead)

        assert manager._check_plugin("dead", info) is True
        assert info.state == PluginRunState.FAILED
        plugin.is_healthy.assert_not_called()

    def test_a_check_inside_the_interval_is_skipped(self) -> None:
        manager = PluginManager(
            _make_config(plugin_health_check_interval=60.0),
            parent_service=MagicMock(),
        )
        plugin = _make_plugin("fresh", healthy=False)
        info = _running_info(plugin)

        assert manager._check_plugin("fresh", info) is True
        plugin.is_healthy.reset_mock()
        info.state = PluginRunState.RUNNING

        assert manager._check_plugin("fresh", info) is False
        plugin.is_healthy.assert_not_called()

    def test_a_health_check_that_raises_does_not_fail_the_plugin(self) -> None:
        manager = PluginManager(_make_config(), parent_service=MagicMock())
        plugin = _make_plugin("flaky")
        plugin.is_healthy.side_effect = RuntimeError("probe broke")
        info = _running_info(plugin)

        assert manager._check_plugin("flaky", info) is False
        assert info.state == PluginRunState.RUNNING

    def test_the_lock_is_not_held_during_the_health_check(self) -> None:
        """A slow ``is_healthy`` must not block registration or shutdown."""
        manager = PluginManager(_make_config(), parent_service=MagicMock())
        plugin = _make_plugin("slow")
        lock_free: list[bool] = []

        def probe() -> bool:
            acquired = manager._lock.acquire(timeout=1.0)
            lock_free.append(acquired)
            if acquired:
                manager._lock.release()
            return True

        plugin.is_healthy.side_effect = probe
        manager._check_plugin("slow", _running_info(plugin))

        assert lock_free == [True]


class TestRestartScheduling:
    """Failed plugins are restarted from the monitor loop, not by sleeping."""

    def test_a_failed_plugin_is_scheduled_not_slept(self) -> None:
        manager = PluginManager(
            _make_config(plugin_restart_delay=30.0),
            parent_service=MagicMock(),
        )
        manager._state = PluginRunState.RUNNING
        plugin = _make_plugin("restartable")
        info = PluginStateInfo(plugin=plugin, state=PluginRunState.FAILED)

        with patch("courier.managers.plugin_manager.time.sleep") as sleep:
            manager._handle_failed_plugin(info)

        sleep.assert_not_called()
        assert info.state == PluginRunState.RESTARTING
        assert info.restart_count == 1
        assert info.last_restart is not None
        assert info.restart_due is not None
        assert manager._due_restarts([("restartable", info)]) == []

    def test_a_due_restart_is_returned_once(self) -> None:
        manager = PluginManager(_make_config(), parent_service=MagicMock())
        manager._state = PluginRunState.RUNNING
        plugin = _make_plugin("due")
        info = PluginStateInfo(plugin=plugin, state=PluginRunState.FAILED)

        manager._handle_failed_plugin(info)

        snapshot = [("due", info)]
        assert manager._due_restarts(snapshot) == [info]
        assert info.restart_due is None
        assert manager._due_restarts(snapshot) == []

    def test_max_attempts_leaves_the_plugin_failed(self) -> None:
        manager = PluginManager(
            _make_config(plugin_max_restart_attempts=1),
            parent_service=MagicMock(),
        )
        manager._state = PluginRunState.RUNNING
        plugin = _make_plugin("exhausted")
        info = PluginStateInfo(
            plugin=plugin,
            state=PluginRunState.FAILED,
            restart_count=1,
        )

        manager._handle_failed_plugin(info)

        assert info.state == PluginRunState.FAILED
        assert info.restart_due is None

    def test_a_restart_too_soon_after_the_last_is_refused(self) -> None:
        """The interval is measured on the monotonic clock, not wall time."""
        manager = PluginManager(
            _make_config(plugin_restart_delay=60.0),
            parent_service=MagicMock(),
        )
        manager._state = PluginRunState.RUNNING
        plugin = _make_plugin("hasty")
        info = PluginStateInfo(plugin=plugin, state=PluginRunState.FAILED)

        manager._handle_failed_plugin(info)
        assert info.state == PluginRunState.RESTARTING
        info.state = PluginRunState.FAILED
        info.restart_due = None

        manager._handle_failed_plugin(info)

        assert info.state == PluginRunState.FAILED
        assert info.restart_count == 1

    def test_the_monitor_restarts_a_crashed_plugin(self) -> None:
        """End to end: unhealthy → stop → scheduled → started again."""
        manager = PluginManager(_make_config(), parent_service=MagicMock())
        manager._state = PluginRunState.RUNNING
        plugin = _make_plugin("phoenix", healthy=False)
        info = _running_info(plugin)
        manager._plugins["phoenix"] = info
        started = threading.Event()

        def fake_start(plugin_info: PluginStateInfo) -> None:
            plugin_info.state = PluginRunState.RUNNING
            started.set()
            manager._state = PluginRunState.STOPPED

        with (
            patch.object(manager, "_start_plugin", side_effect=fake_start),
            patch("courier.managers.plugin_manager.time.sleep"),
        ):
            manager._monitor_plugins()

        assert started.is_set()
        plugin.stop.assert_called_once()
        assert info.restart_count == 1
        assert info.state == PluginRunState.RUNNING
