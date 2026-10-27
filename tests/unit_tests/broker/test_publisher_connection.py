"""Publishers reuse one broker connection per thread.

``Service.emit`` once opened a fresh ``kombu.Connection`` per call: a TCP,
TLS and AMQP handshake for every file found, job emitted and execution log.
:meth:`MessageBrokerManager.publisher_connection` keeps one per thread
instead, and drops it when a publish raises so the retry starts clean.
"""

from __future__ import annotations

import threading
import uuid
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from kombu.exceptions import OperationalError

from courier.broker.kombu import MessageBrokerManager
from courier.config import ServiceConfig
from courier.constants import DISPATCHER_QUEUE
from courier.errors import TransientBrokerError
from courier.metrics import BROKER_CONNECTED
from courier.service import Service

if TYPE_CHECKING:
    import kombu


def _manager(**overrides: object) -> MessageBrokerManager:
    return MessageBrokerManager(
        ServiceConfig(
            broker_url="memory://",
            prometheus_port=0,
            namespace=f"pc-{uuid.uuid4().hex[:8]}",
            tracing_enabled=False,
            **overrides,  # type: ignore[arg-type]
        ),
    )


def _gauge() -> float:
    return BROKER_CONNECTED._value.get()


def test_the_same_thread_gets_the_same_connection() -> None:
    manager = _manager()
    with manager.publisher_connection() as first:
        pass
    with manager.publisher_connection() as second:
        pass
    assert first is second
    assert first.connected


def test_each_thread_gets_its_own_connection() -> None:
    """Kombu connections are not thread-safe; sharing one would interleave frames."""
    manager = _manager()
    seen: dict[str, kombu.Connection] = {}

    def grab(key: str) -> None:
        with manager.publisher_connection() as conn:
            seen[key] = conn

    grab("main")
    worker = threading.Thread(target=grab, args=("worker",))
    worker.start()
    worker.join(timeout=5)

    assert seen["main"] is not seen["worker"]


def test_a_failing_publish_drops_the_connection() -> None:
    """A transport left mid-frame must not be reused by the retry."""
    manager = _manager()
    with manager.publisher_connection() as first:
        pass
    with pytest.raises(RuntimeError), manager.publisher_connection():
        raise RuntimeError("publish failed")
    assert not first.connected
    with manager.publisher_connection() as second:
        pass
    assert second is not first
    assert second.connected


def test_stop_closes_every_threads_connection() -> None:
    manager = _manager()
    opened: list[kombu.Connection] = []

    def grab() -> None:
        with manager.publisher_connection() as conn:
            opened.append(conn)

    grab()
    worker = threading.Thread(target=grab)
    worker.start()
    worker.join(timeout=5)
    assert len(opened) == 2

    manager.stop()

    assert all(not conn.connected for conn in opened)
    assert manager._publisher_connections == {}


def test_registered_queues_are_declared_once_on_the_cached_connection() -> None:
    manager = _manager()
    name = manager.add_queue("declared-once", durable=True, exclusive=False)
    with patch("courier.broker.kombu.declare_queue") as declare:
        with manager.publisher_connection() as first:
            pass
        with manager.publisher_connection() as second:
            pass
    assert first is second
    assert name in manager._created_queues
    declare.assert_called_once_with(first, name, durable=True, exclusive=False)


def test_emit_reuses_the_publisher_connection() -> None:
    """Two emits from one thread open one connection, not two."""
    service = Service(
        ServiceConfig(
            broker_url="memory://",
            prometheus_port=0,
            namespace=f"pc-{uuid.uuid4().hex[:8]}",
            tracing_enabled=False,
        ),
    )
    manager = service._broker_manager
    try:
        with patch.object(
            manager,
            "_open_publisher_connection_impl",
            wraps=manager._open_publisher_connection_impl,
        ) as opener:
            service.emit(message="one", queue=DISPATCHER_QUEUE)
            service.emit(message="two", queue=DISPATCHER_QUEUE)
        assert opener.call_count == 1
        assert len(manager._publisher_connections) == 1
    finally:
        manager.stop()


def test_health_and_the_gauge_follow_start_and_stop() -> None:
    manager = _manager()
    assert not manager.is_healthy()

    manager.start()
    assert manager.is_healthy()
    assert _gauge() == 1

    manager.stop()
    assert not manager.is_healthy()
    assert _gauge() == 0


def test_start_twice_on_one_thread_keeps_one_connection() -> None:
    manager = _manager()
    manager.start()
    first = manager._cached_publisher_connection()
    manager.start()

    assert manager._cached_publisher_connection() is first
    assert len(manager._publisher_connections) == 1
    manager.stop()


def test_discarding_the_last_connection_zeroes_the_gauge() -> None:
    manager = _manager()
    with manager.publisher_connection() as conn:
        pass
    assert _gauge() == 1

    manager._discard_publisher_connection(conn)

    assert _gauge() == 0
    assert not manager.is_healthy()


def test_only_start_retries_around_the_connection_attempt() -> None:
    """A publish-path reopen gets one attempt; the thread may hold a message."""
    manager = _manager(broker_max_retries=2)
    with (
        patch(
            "courier.broker.kombu._open_connection",
            side_effect=OperationalError("down"),
        ) as opener,
        patch("courier.utils.decorators.time.sleep"),
    ):
        with pytest.raises(OperationalError), manager.publisher_connection():
            pass
        assert opener.call_count == 1

        opener.reset_mock()
        with pytest.raises(OperationalError):
            manager.start()
        assert opener.call_count == 2


def test_a_failed_queue_declaration_does_not_cache_the_connection() -> None:
    """A connection whose adoption failed is closed, not reused."""
    manager = _manager()
    manager.add_queue("fails-to-declare", durable=True, exclusive=False)
    real_open = manager._open_publisher_connection_impl
    opened: list[kombu.Connection] = []

    def open_and_record() -> kombu.Connection:
        conn = real_open()
        opened.append(conn)
        return conn

    with (
        patch(
            "courier.broker.kombu.declare_queue",
            side_effect=OperationalError("declare failed"),
        ),
        patch.object(
            manager,
            "_open_publisher_connection_impl",
            side_effect=open_and_record,
        ),
        pytest.raises(OperationalError),
        manager.publisher_connection(),
    ):
        pass
    assert manager._publisher_connections == {}
    assert not opened[0].connected

    with manager.publisher_connection() as conn:
        pass
    assert conn.connected
    assert len(manager._publisher_connections) == 1


def test_a_dead_threads_connection_is_closed() -> None:
    """A thread that exits does not leave its connection open and tracked."""
    manager = _manager()
    opened: list[kombu.Connection] = []

    def grab() -> None:
        with manager.publisher_connection() as conn:
            opened.append(conn)

    worker = threading.Thread(target=grab)
    worker.start()
    worker.join(timeout=5)
    assert len(manager._publisher_connections) == 1

    assert not manager.is_healthy()
    assert not opened[0].connected
    assert manager._publisher_connections == {}
    assert _gauge() == 0


def test_no_connection_is_adopted_after_stop() -> None:
    """A job still running after shutdown cannot reopen an unmanaged connection."""
    manager = _manager()
    manager.start()
    manager.stop()

    with (
        patch.object(manager, "_open_publisher_connection_impl") as opener,
        pytest.raises(TransientBrokerError),
        manager.publisher_connection(),
    ):
        pass
    opener.assert_not_called()
    assert manager._publisher_connections == {}


def test_a_connection_opened_across_stop_is_closed() -> None:
    """stop() landing between open and adopt closes the new connection."""
    manager = _manager()
    real_open = manager._open_publisher_connection_impl
    opened: list[kombu.Connection] = []

    def open_then_stop() -> kombu.Connection:
        conn = real_open()
        opened.append(conn)
        manager.stop()
        return conn

    with (
        patch.object(
            manager,
            "_open_publisher_connection_impl",
            side_effect=open_then_stop,
        ),
        pytest.raises(TransientBrokerError),
        manager.publisher_connection(),
    ):
        pass
    assert not opened[0].connected
    assert manager._publisher_connections == {}


def test_start_after_stop_allows_publishing_again() -> None:
    """The stop gate is lifted by the next start."""
    manager = _manager()
    manager.start()
    manager.stop()
    manager.start()
    with manager.publisher_connection() as conn:
        pass
    assert conn.connected
    manager.stop()
