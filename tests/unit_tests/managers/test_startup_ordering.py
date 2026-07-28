"""Startup-ordering guarantees for the file-found fanout exchange.

A fanout exchange discards any message published while no queue is bound to
it. Data monitors are producers and several emit immediately on start
(``cron_glob`` with ``run_on_start``, or a watchdog seeing a pre-existing
file), so a monitor that runs before the job builders have bound loses those
files silently — no error, no metric, no log line.

These tests pin the invariant rather than the timing. The natural race window
is short enough that a wall-clock test passes on an idle machine whether or
not the guard exists, which would make it worse than no test at all.
"""

from __future__ import annotations

import threading
import time
from typing import Any, ClassVar

import pytest

from courier.config import ServiceConfig
from courier.constants import PluginRunState
from courier.managers.plugin_manager import PluginManager


class _FakePlugin:
    """Minimal plugin double implementing just what PluginManager touches."""

    version: ClassVar[str] = "0"

    def __init__(self, name: str, interface: str, timeline: list[str]) -> None:
        self.name = name
        self.identifier = name
        self.interface = interface
        self._timeline = timeline
        self._subscribed = threading.Event()
        self.started = threading.Event()

    def start(self) -> None:
        self._timeline.append(f"start:{self.name}")
        self.started.set()

    def stop(self) -> None:
        pass

    def is_healthy(self) -> bool:
        return True

    def get_metrics(self) -> dict[str, Any]:
        return {}


class _FakeConsumer(_FakePlugin):
    """A consumer that binds to its queue *after* a delay, like a real one."""

    def __init__(
        self,
        name: str,
        timeline: list[str],
        bind_delay: float = 0.0,
    ) -> None:
        super().__init__(name, "job_builders", timeline)
        self._bind_delay = bind_delay

    def start(self) -> None:
        super().start()

        def _bind() -> None:
            time.sleep(self._bind_delay)
            self._timeline.append(f"subscribed:{self.name}")
            self._subscribed.set()

        threading.Thread(target=_bind, daemon=True).start()

    def wait_until_subscribed(self, timeout: float) -> bool:
        return self._subscribed.wait(timeout=timeout)


class _FakeProducer(_FakePlugin):
    """A data monitor, which publishes as soon as it starts."""

    def __init__(self, name: str, timeline: list[str]) -> None:
        super().__init__(name, "data_monitors", timeline)


@pytest.fixture
def manager_config() -> ServiceConfig:
    """Config with a health-check interval long enough to cover binding."""
    return ServiceConfig(
        broker_url="memory://",
        prometheus_port=0,
        plugin_health_check_interval=3,
        tracing_enabled=False,
    )


def _make_manager(config: ServiceConfig, plugins: list[_FakePlugin]) -> PluginManager:
    manager = PluginManager(config, parent_service=None)
    for plugin in plugins:
        manager.register_plugin(type(plugin), {}, identifier=plugin.name)
        # register_plugin instantiates the class; swap in our prepared double.
        manager._plugins[plugin.name].plugin = plugin  # noqa: SLF001
    return manager


class TestProducerConsumerOrdering:
    """Producers must not start until every consumer has bound its queue."""

    def test_consumers_subscribe_before_producers_start(
        self,
        manager_config: ServiceConfig,
    ) -> None:
        """The whole point: no producer runs while the fanout has no binding."""
        timeline: list[str] = []
        consumer = _FakeConsumer("builder", timeline, bind_delay=0.25)
        producer = _FakeProducer("monitor", timeline)

        manager = PluginManager(manager_config, parent_service=None)
        # Registration order deliberately puts the producer first — that is
        # how configs are written (monitor, builder, dispatcher) and is what
        # made the race likely rather than rare.
        for plugin in (producer, consumer):
            manager._plugins[plugin.name] = _state_info(plugin)  # noqa: SLF001

        try:
            manager.start()
        finally:
            manager._state = PluginRunState.STOPPED  # noqa: SLF001

        assert "subscribed:builder" in timeline, "consumer never bound"
        assert timeline.index("subscribed:builder") < timeline.index(
            "start:monitor",
        ), f"producer started before consumer bound: {timeline}"

    def test_startup_proceeds_when_a_consumer_never_binds(
        self,
        manager_config: ServiceConfig,
    ) -> None:
        """A stuck consumer must not block the service from coming up.

        The wait is bounded; the manager logs a warning and starts producers
        anyway rather than hanging forever.
        """
        timeline: list[str] = []
        stuck = _FakeConsumer("stuck-builder", timeline, bind_delay=999.0)
        producer = _FakeProducer("monitor", timeline)

        config = ServiceConfig(
            broker_url="memory://",
            prometheus_port=0,
            plugin_health_check_interval=1,
            tracing_enabled=False,
        )
        manager = PluginManager(config, parent_service=None)
        for plugin in (stuck, producer):
            manager._plugins[plugin.name] = _state_info(plugin)  # noqa: SLF001

        started = time.time()
        try:
            manager.start()
        finally:
            manager._state = PluginRunState.STOPPED  # noqa: SLF001
        elapsed = time.time() - started

        assert producer.started.is_set(), "producer never started"
        assert elapsed < 30, f"startup blocked on a stuck consumer ({elapsed:.1f}s)"

    def test_partition_by_role_classifies_every_interface(
        self,
        manager_config: ServiceConfig,
    ) -> None:
        """Only data monitors are producers; everything else consumes."""
        timeline: list[str] = []
        plugins = [
            _FakeProducer("dm", timeline),
            _FakeConsumer("jb", timeline),
            _FakePlugin("dp", "dispatchers", timeline),
        ]
        manager = PluginManager(manager_config, parent_service=None)
        for plugin in plugins:
            manager._plugins[plugin.name] = _state_info(plugin)  # noqa: SLF001

        consumers, producers = manager._partition_by_role()  # noqa: SLF001

        assert [p.plugin.name for p in producers] == ["dm"]
        assert sorted(c.plugin.name for c in consumers) == ["dp", "jb"]


def _state_info(plugin: _FakePlugin):
    """Build a PluginStateInfo wrapping *plugin* without touching the registry."""
    from courier.managers.plugin_manager import PluginStateInfo

    return PluginStateInfo(plugin=plugin)  # type: ignore[arg-type]


class TestConsumeSubscriptionSignal:
    """``Service.consume`` must signal binding before it reads any message."""

    def test_on_subscribed_fires_before_the_first_yield(self) -> None:
        """The callback is the barrier's evidence that the queue is bound.

        If it fired after the first message arrived it would be useless: the
        producer would already have published into an unbound fanout.
        """
        import uuid

        from courier.constants import FILE_FOUND_EXCHANGE
        from courier.service import Service

        service = Service(
            ServiceConfig(
                broker_url="memory://",
                prometheus_port=0,
                namespace=f"sub-{uuid.uuid4().hex[:8]}",
                tracing_enabled=False,
            ),
        )
        service._broker_manager.start()  # noqa: SLF001

        events: list[str] = []
        stop = threading.Event()
        subscribed = threading.Event()

        def _consume() -> None:
            for _body, _ctx in service.consume(
                FILE_FOUND_EXCHANGE,
                stop_event=stop,
                on_subscribed=lambda: (
                    events.append("subscribed"),
                    subscribed.set(),
                ),
            ):
                events.append("message")
                break

        worker = threading.Thread(target=_consume, daemon=True)
        worker.start()
        try:
            assert subscribed.wait(timeout=10), "on_subscribed never fired"
            service.emit(FILE_FOUND_EXCHANGE, '{"file": "/data/x.nc"}')
            deadline = time.time() + 10
            while "message" not in events and time.time() < deadline:
                time.sleep(0.05)
        finally:
            stop.set()
            worker.join(timeout=10)
            service._broker_manager.stop()  # noqa: SLF001

        assert events[0] == "subscribed", f"unexpected order: {events}"
        assert "message" in events, "file published after binding was still lost"
