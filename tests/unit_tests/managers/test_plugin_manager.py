"""Unit tests for PluginManager (ISSUE 10, 13, 14)."""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

from courier.config import ServiceConfig
from courier.constants import PluginRunState
from courier.interfaces.plugin_protocol import ServicePlugin
from courier.managers.plugin_manager import PluginManager, PluginStateInfo

if TYPE_CHECKING:
    from collections.abc import Callable

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
        assert info.ready.wait(timeout=5.0)
        assert info.state == PluginRunState.RUNNING
        assert info.running_since is not None
        # The starter thread exits by itself while the manager stays RUNNING.
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
        assert info.ready.wait(timeout=5.0)
        assert info.state == PluginRunState.RUNNING
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
    """A RUNNING plugin the monitor has already looked at once."""
    return PluginStateInfo(
        plugin=plugin,
        state=PluginRunState.RUNNING,
        thread=_live_thread(),
        last_health_check=time.monotonic() - 3600,
        running_since=time.monotonic() - 3600,
    )


def _lock_free_from_another_thread(lock: threading.RLock) -> bool:
    """Try the lock from a second thread: an RLock re-acquires on its own."""
    outcome: list[bool] = []

    def attempt() -> None:
        acquired = lock.acquire(timeout=1.0)
        if acquired:
            lock.release()
        outcome.append(acquired)

    thread = threading.Thread(target=attempt)
    thread.start()
    thread.join()
    return outcome[0]


class TestMonitorChecks:
    """One pass of the monitor loop, driven by hand."""

    def test_an_unhealthy_plugin_is_marked_failed(self) -> None:
        manager = PluginManager(_make_config(), parent_service=MagicMock())
        plugin = _make_plugin("sick", healthy=False)
        info = _running_info(plugin)

        assert manager._check_plugin("sick", info) is True
        assert info.state == PluginRunState.FAILED
        assert info.last_health_check is not None

    def test_a_failed_start_is_handed_back_for_restart(self) -> None:
        """A plugin whose start() raised is FAILED, and still gets its retries."""
        manager = PluginManager(_make_config(), parent_service=MagicMock())
        plugin = _make_plugin("stillborn")
        info = PluginStateInfo(plugin=plugin, state=PluginRunState.FAILED)

        assert manager._check_plugin("stillborn", info) is True
        plugin.is_healthy.assert_not_called()

    def test_a_terminally_failed_plugin_is_left_alone(self) -> None:
        manager = PluginManager(
            _make_config(plugin_max_restart_attempts=2),
            parent_service=MagicMock(),
        )
        plugin = _make_plugin("gone")
        info = PluginStateInfo(
            plugin=plugin,
            state=PluginRunState.FAILED,
            restart_count=2,
        )

        assert manager._check_plugin("gone", info) is False
        assert manager.exhausted_plugins() == []  # not registered
        manager._plugins["gone"] = info
        assert manager.exhausted_plugins() == ["gone"]

    def test_restarts_are_forgiven_after_a_healthy_window(self) -> None:
        manager = PluginManager(
            _make_config(plugin_restart_window=100),
            parent_service=MagicMock(),
        )
        plugin = _make_plugin("recovered", healthy=True)
        info = _running_info(plugin)
        info.restart_count = 2
        info.running_since = time.monotonic() - 50

        assert manager._check_plugin("recovered", info) is False
        assert info.restart_count == 2, "window not yet elapsed"

        info.last_health_check = None
        info.running_since = time.monotonic() - 101
        assert manager._check_plugin("recovered", info) is False
        assert info.restart_count == 0

    def test_a_raising_health_check_earns_no_forgiveness(self) -> None:
        manager = PluginManager(
            _make_config(plugin_restart_window=1),
            parent_service=MagicMock(),
        )
        plugin = _make_plugin("broken-probe")
        plugin.is_healthy.side_effect = RuntimeError("probe broke")
        info = _running_info(plugin)
        info.restart_count = 2

        assert manager._check_plugin("broken-probe", info) is False
        assert info.state == PluginRunState.RUNNING
        assert info.restart_count == 2

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
            lock_free.append(_lock_free_from_another_thread(manager._lock))
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
        plugin.stop.assert_called_once()  # told to stop, left FAILED

    def test_a_second_failure_inside_the_delay_is_still_restarted(self) -> None:
        """Failing again soon after a restart costs an attempt, not the plugin.

        The old "too soon since last restart" rule turned a second failure
        inside ``plugin_restart_delay`` into a permanent one. The delay is
        enforced by ``restart_due``; nothing else needs to.
        """
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

        assert info.state == PluginRunState.RESTARTING
        assert info.restart_count == 2
        assert info.restart_due is not None

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


class TestManagerHealth:
    """Manager health is every plugin's health, not any plugin's."""

    def _manager_with(self, *infos: PluginStateInfo) -> PluginManager:
        manager = PluginManager(
            _make_config(plugin_max_restart_attempts=1),
            parent_service=MagicMock(),
        )
        manager._state = PluginRunState.RUNNING
        for info in infos:
            manager._plugins[info.plugin.name] = info
        return manager

    def test_one_unhealthy_plugin_makes_the_manager_unhealthy(self) -> None:
        good = _running_info(_make_plugin("good", healthy=True))
        bad = _running_info(_make_plugin("bad", healthy=False))

        assert not self._manager_with(good, bad).is_healthy()
        assert self._manager_with(good).is_healthy()

    def test_a_terminally_failed_plugin_makes_the_manager_unhealthy(self) -> None:
        good = _running_info(_make_plugin("good", healthy=True))
        dead = PluginStateInfo(
            plugin=_make_plugin("dead"),
            state=PluginRunState.FAILED,
            restart_count=1,
        )
        manager = self._manager_with(good, dead)

        assert not manager.is_healthy()
        assert manager.exhausted_plugins() == ["dead"]
        dead.plugin.is_healthy.assert_not_called()

    def test_a_plugin_awaiting_restart_does_not_count(self) -> None:
        good = _running_info(_make_plugin("good", healthy=True))
        waiting = PluginStateInfo(
            plugin=_make_plugin("waiting"),
            state=PluginRunState.RESTARTING,
            restart_count=1,
            restart_due=time.monotonic() + 10,
        )
        manager = self._manager_with(good, waiting)

        assert manager.is_healthy()
        assert manager.exhausted_plugins() == []

    def test_a_running_plugin_not_yet_examined_does_not_count(self) -> None:
        """A plugin past the start gate but unchecked is still in transit."""
        plugin = _make_plugin("slow-start", healthy=False)
        unchecked = PluginStateInfo(plugin=plugin, state=PluginRunState.RUNNING)
        manager = self._manager_with(unchecked)

        assert manager.is_healthy()
        plugin.is_healthy.assert_not_called()

        unchecked.last_health_check = time.monotonic()
        assert not manager.is_healthy()

    def test_plugin_health_is_asked_once_and_outside_the_lock(self) -> None:
        plugin = _make_plugin("slow")
        lock_free: list[bool] = []

        def probe() -> bool:
            lock_free.append(_lock_free_from_another_thread(plugin_manager._lock))
            return True

        plugin.is_healthy.side_effect = probe
        plugin_manager = self._manager_with(_running_info(plugin))

        assert plugin_manager.is_healthy()
        assert lock_free == [True]
        assert plugin.is_healthy.call_count == 1


class TestRestartsThroughTheRealLoop:
    """start() plus the monitor thread, with plugins that fail to start."""

    @staticmethod
    def _wait_for(predicate: Callable[[], bool], timeout: float = 10.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return True
            time.sleep(0.05)
        return predicate()

    def test_a_start_that_fails_once_is_retried_after_startup(self) -> None:
        manager = PluginManager(
            _make_config(plugin_health_check_interval=0.2),
            parent_service=MagicMock(),
        )
        good = _make_plugin("good")
        manager.register_plugin(_plugin_cls_for(good), {}, identifier="good")
        flaky = _make_plugin("flaky")
        flaky.start.side_effect = [RuntimeError("boom"), None]
        manager.register_plugin(_plugin_cls_for(flaky), {}, identifier="flaky")

        manager.start()
        try:
            info = manager.get_plugins()["flaky"]
            assert self._wait_for(
                lambda: (
                    info.state == PluginRunState.RUNNING and info.restart_count == 1
                ),
            )
            assert flaky.start.call_count == 2
            assert manager.exhausted_plugins() == []
            assert manager.is_healthy()
        finally:
            manager.stop()

    def test_a_start_that_always_fails_is_exhausted(self) -> None:
        manager = PluginManager(
            _make_config(
                plugin_health_check_interval=0.2,
                plugin_max_restart_attempts=1,
            ),
            parent_service=MagicMock(),
        )
        good = _make_plugin("good")
        manager.register_plugin(_plugin_cls_for(good), {}, identifier="good")
        dead = _make_plugin("dead")
        dead.start.side_effect = RuntimeError("boom")
        manager.register_plugin(_plugin_cls_for(dead), {}, identifier="dead")

        manager.start()
        try:
            assert self._wait_for(lambda: manager.exhausted_plugins() == ["dead"])
            assert dead.start.call_count == 2
            assert not manager.is_healthy()
        finally:
            manager.stop()

    def test_all_starts_failing_raises_before_the_monitor_runs(self) -> None:
        """Deterministic: the monitor cannot reschedule a plugin first."""
        manager = PluginManager(
            _make_config(plugin_health_check_interval=0.5),
            parent_service=MagicMock(),
        )
        for name in ("a", "b"):
            manager.register_plugin(_failing_plugin_cls(name), {}, identifier=name)

        with pytest.raises(RuntimeError, match="All plugins failed"):
            manager.start()

        assert manager._monitor_thread is None
        manager.stop()


def _blocked_starter(plugin: MagicMock, info: PluginStateInfo) -> threading.Event:
    """Run ``plugin.start`` on a starter thread that blocks until released."""
    release = threading.Event()
    plugin.start.side_effect = release.wait
    info.state = PluginRunState.STARTING
    info.starting_since = time.monotonic()
    info.thread = threading.Thread(target=plugin.start, name="stuck-start", daemon=True)
    info.thread.start()
    return release


class TestStuckStart:
    """A start() that never returns is given up on, not retried."""

    def test_a_fresh_start_is_left_alone(self) -> None:
        manager = PluginManager(_make_config(), parent_service=MagicMock())
        plugin = _make_plugin("warming-up")
        info = PluginStateInfo(plugin=plugin)
        release = _blocked_starter(plugin, info)
        try:
            assert manager._check_plugin("warming-up", info) is False
            assert info.state == PluginRunState.STARTING
            manager._state = PluginRunState.RUNNING
            manager._plugins["warming-up"] = info
            assert manager.is_healthy()
        finally:
            release.set()

    def test_an_overdue_start_is_failed_exhausted_and_told_to_stop(self) -> None:
        manager = PluginManager(
            _make_config(plugin_start_timeout=1, plugin_max_restart_attempts=3),
            parent_service=MagicMock(),
        )
        plugin = _make_plugin("stuck")
        info = PluginStateInfo(plugin=plugin)
        release = _blocked_starter(plugin, info)
        stuck_thread = info.thread
        manager._state = PluginRunState.RUNNING
        manager._plugins["stuck"] = info
        try:
            info.starting_since = time.monotonic() - 2
            assert not manager.is_healthy(), "overdue start is unhealthy"

            assert manager._check_plugin("stuck", info) is False

            assert info.state == PluginRunState.FAILED
            assert info.restart_count == 3
            assert "did not return within 1s" in (info.error_message or "")
            assert info.thread is None
            assert info.abandoned_threads == [stuck_thread]
            assert manager.exhausted_plugins() == ["stuck"]
            plugin.stop.assert_called_once()
            # A second tick does nothing more: not retried, not re-failed.
            info.last_health_check = None
            assert manager._check_plugin("stuck", info) is False
            assert info.restart_count == 3
        finally:
            release.set()

    def test_the_log_names_what_the_thread_is_blocked_on(self) -> None:
        manager = PluginManager(
            _make_config(plugin_start_timeout=1),
            parent_service=MagicMock(),
        )
        plugin = _make_plugin("stuck")
        info = PluginStateInfo(plugin=plugin)
        release = _blocked_starter(plugin, info)
        try:
            time.sleep(0.05)  # let the thread reach Event.wait
            info.starting_since = time.monotonic() - 2
            with patch.object(manager, "_logger") as logger:
                manager._abandon_stuck_start(info)
            message = logger.error.call_args.args[0] % logger.error.call_args.args[1:]
            assert "blocked at" in message
            assert "wait" in message
        finally:
            release.set()

    def test_a_late_return_does_not_resurrect_the_plugin(self) -> None:
        manager = PluginManager(
            _make_config(plugin_start_timeout=1),
            parent_service=MagicMock(),
        )
        plugin = _make_plugin("tardy", healthy=True)
        info = PluginStateInfo(plugin=plugin)
        manager._plugins["tardy"] = info
        release = threading.Event()
        plugin.start.side_effect = release.wait

        manager._start_plugin(info)
        starter = info.thread
        info.starting_since = time.monotonic() - 2
        manager._abandon_stuck_start(info)
        assert info.state == PluginRunState.FAILED

        release.set()
        starter.join(timeout=5)  # type: ignore[union-attr]

        assert info.state == PluginRunState.FAILED
        assert not info.ready.is_set()
        assert manager.exhausted_plugins() == ["tardy"]

    def test_the_real_loop_gives_up_and_stop_returns(self) -> None:
        manager = PluginManager(
            _make_config(plugin_health_check_interval=0.2, plugin_start_timeout=0.5),
            parent_service=MagicMock(),
        )
        good = _make_plugin("good")
        manager.register_plugin(_plugin_cls_for(good), {}, identifier="good")
        stuck = _make_plugin("stuck")
        release = threading.Event()
        stuck.start.side_effect = release.wait
        manager.register_plugin(_plugin_cls_for(stuck), {}, identifier="stuck")

        manager.start()
        try:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline and not manager.exhausted_plugins():
                time.sleep(0.05)
            assert manager.exhausted_plugins() == ["stuck"]
            assert not manager.is_healthy()
            assert manager.get_plugin_status()["stuck"]["abandoned_threads"] == 1

            started = time.monotonic()
            manager.stop()
            assert time.monotonic() - started < 10
        finally:
            release.set()


class TestStopOutlivingItsJoin:
    def test_a_thread_that_will_not_stop_is_recorded(self) -> None:
        manager = PluginManager(_make_config(), parent_service=MagicMock())
        plugin = _make_plugin("clingy")
        release = threading.Event()
        info = PluginStateInfo(plugin=plugin, state=PluginRunState.RUNNING)
        info.thread = threading.Thread(target=release.wait, daemon=True)
        info.thread.start()
        clingy = info.thread
        try:
            with patch.object(
                threading.Thread, "join", lambda self, timeout=None: None
            ):
                manager._stop_plugin(info)
            assert info.state == PluginRunState.STOPPED
            assert info.thread is None
            assert info.abandoned_threads == [clingy]
        finally:
            release.set()
