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

import pytest

from courier.broker.kombu import MessageBrokerManager
from courier.config import ServiceConfig
from courier.constants import DISPATCHER_QUEUE
from courier.service import Service

if TYPE_CHECKING:
    import kombu


def _manager() -> MessageBrokerManager:
    return MessageBrokerManager(
        ServiceConfig(
            broker_url="memory://",
            prometheus_port=0,
            namespace=f"pc-{uuid.uuid4().hex[:8]}",
            tracing_enabled=False,
        ),
    )


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
    assert manager._publisher_connections == []


def test_registered_queues_are_declared_once_on_the_cached_connection() -> None:
    manager = _manager()
    name = manager.add_queue("declared-once", durable=True, exclusive=False)
    with manager.publisher_connection():
        pass
    assert name in manager._created_queues
    before = set(manager._created_queues)
    with manager.publisher_connection():
        pass
    assert manager._created_queues == before


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
        service.emit(message="one", queue=DISPATCHER_QUEUE)
        service.emit(message="two", queue=DISPATCHER_QUEUE)
        assert len(manager._publisher_connections) == 1
    finally:
        manager.stop()
