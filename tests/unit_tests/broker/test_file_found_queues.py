"""Durable per-builder file-found queues (issue #44).

The in-memory transport covers half of the bug. It ignores ``exclusive`` and
never deletes a queue when a connection closes, so the "builder was down and
its queue vanished" half lives in ``tests/rabbitmq/``. The half covered here is
that a fanout exchange with nothing bound to it discards everything published
to it, with no error.
"""

from __future__ import annotations

import threading
import uuid
from typing import Any
from unittest.mock import ANY, MagicMock, patch

import pytest

import kombu

from courier.broker.kombu import declare_fanout_exchange, declare_queue
from courier.config import ServiceConfig
from courier.constants import DISPATCHER_QUEUE, FILE_FOUND_EXCHANGE
from courier.errors import ConfigurationError
from courier.service import Service
from tests._helpers import poll_until


def _service(namespace: str | None = None) -> Service:
    """Return a Service on the in-memory transport with a unique namespace."""
    return Service(
        ServiceConfig(
            broker_url="memory://",
            prometheus_port=0,
            namespace=namespace or f"ff-{uuid.uuid4().hex[:8]}",
            tracing_enabled=False,
        ),
    )


def _declared_queues() -> dict[str, Any]:
    """Return the in-memory transport's declared queues."""
    from kombu.transport import memory

    return memory.Channel.queues


def _consume_once(
    service: Service,
    subscriber: str,
    received: list[str],
    stop: threading.Event,
    subscribed: threading.Event,
) -> threading.Thread:
    """Start a consumer thread that records one message and returns."""

    def _run() -> None:
        for body, _ctx in service.consume(
            FILE_FOUND_EXCHANGE,
            stop_event=stop,
            on_subscribed=subscribed.set,
            subscriber=subscriber,
        ):
            received.append(body)
            break

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    return worker


def test_cold_start_publish_before_any_consumer_is_retained() -> None:
    """A file published before any builder exists is still delivered.

    The split-deployment failure from issue #44: a container running only a
    data monitor has no job builders of its own, so nothing was bound to the
    fanout exchange and every file it published was discarded until a builder
    container had started once.

    Reverted check: keep the ``builder_identifiers`` plumbing and remove the
    predeclaration loop in ``Service._predeclare_target_queues``. The publish
    then reaches an exchange with no bindings and the consumer below times out.
    """
    namespace = f"cold-{uuid.uuid4().hex[:8]}"
    producer = _service(namespace)
    # The shape `courier run --only <monitor>` produces: no dispatchers, no
    # builder targets, all builder identifiers known.
    producer.configure_routing(
        dispatcher_identifiers=set(),
        builder_targets={},
        builder_identifiers={"jb"},
    )
    producer.preflight_check()

    producer.emit(FILE_FOUND_EXCHANGE, '{"file": "/data/x.nc", "hostname": "h"}')

    consumer = _service(namespace)
    received: list[str] = []
    stop = threading.Event()
    subscribed = threading.Event()
    worker = _consume_once(consumer, "jb", received, stop, subscribed)
    try:
        assert subscribed.wait(timeout=10), "consumer never subscribed"
        assert poll_until(lambda: bool(received), timeout=10), (
            "the file published before any consumer existed was lost"
        )
    finally:
        stop.set()
        worker.join(timeout=10)

    assert "/data/x.nc" in received[0]


def test_preflight_declares_a_bound_durable_queue_for_every_builder() -> None:
    """Preflight declares and binds one queue per builder, durably."""
    svc = _service("t")
    svc.configure_routing(
        dispatcher_identifiers=["only"],
        builder_targets={},
        builder_identifiers=["build-a", "build-b"],
    )
    svc.preflight_check()

    for builder in ("build-a", "build-b"):
        name = f"t-FilesFound-{builder}"
        registered = svc._broker_manager._queues[name]  # noqa: SLF001
        assert registered["durable"] is True
        assert registered["exclusive"] is False
        assert registered["auto_delete"] is False
        assert registered["exchange"].name == "t-FilesFoundExchange"
        assert registered["exchange"].type == "fanout"
        assert name in _declared_queues()


def test_preflight_declares_the_dispatcher_queue() -> None:
    """The shared dispatcher queue is declared during preflight.

    It used to be registered after the connection context closed, so it stayed
    undeclared until something else opened a connection.
    """
    svc = _service("t")
    svc.configure_routing(dispatcher_identifiers=["only"], builder_targets={})
    svc.preflight_check()

    assert "t-DispatcherQueue" in _declared_queues()


def test_consume_requires_a_subscriber_on_the_fanout_path() -> None:
    """Consuming the file-found exchange without a subscriber is refused.

    The error is raised when ``consume`` is called, before the first
    ``next()``, so it points at the call site. There is no anonymous fallback:
    that was the per-connection queue whose deletion lost files.
    """
    svc = _service()
    with pytest.raises(ConfigurationError, match="subscriber"):
        svc.consume(FILE_FOUND_EXCHANGE)

    assert not any("-fanout-" in name for name in _declared_queues())


def test_consume_names_the_queue_after_the_subscriber() -> None:
    """The durable queue is named from the subscribing builder."""
    namespace = f"nm-{uuid.uuid4().hex[:8]}"
    svc = _service(namespace)
    received: list[str] = []
    stop = threading.Event()
    subscribed = threading.Event()
    worker = _consume_once(svc, "my-builder", received, stop, subscribed)
    try:
        assert subscribed.wait(timeout=10)
        assert f"{namespace}-FilesFound-my-builder" in _declared_queues()
    finally:
        stop.set()
        worker.join(timeout=10)


def test_subscriber_is_ignored_on_direct_queues() -> None:
    """A subscriber on a directly-addressed queue is accepted and ignored."""
    namespace = f"dir-{uuid.uuid4().hex[:8]}"
    svc = _service(namespace)
    stop = threading.Event()
    subscribed = threading.Event()

    def _run() -> None:
        for _body, _ctx in svc.consume(
            DISPATCHER_QUEUE,
            stop_event=stop,
            on_subscribed=subscribed.set,
            subscriber="ignored",
        ):
            break

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    try:
        assert subscribed.wait(timeout=10)
        assert f"{namespace}-DispatcherQueue" in _declared_queues()
    finally:
        stop.set()
        worker.join(timeout=10)


def test_distinct_builders_each_receive_every_file() -> None:
    """Two different builder identifiers each get their own copy.

    Multi-builder configurations depend on this fan-out. A single shared queue
    for all builders would break it with no error.
    """
    namespace = f"fan-{uuid.uuid4().hex[:8]}"
    consumers = [_service(namespace) for _ in range(2)]
    received: list[list[str]] = [[], []]
    stops = [threading.Event(), threading.Event()]
    subscribed = [threading.Event(), threading.Event()]
    workers = [
        _consume_once(svc, name, box, stop, sub)
        for svc, name, box, stop, sub in zip(
            consumers,
            ("builder-a", "builder-b"),
            received,
            stops,
            subscribed,
            strict=True,
        )
    ]
    try:
        assert all(event.wait(timeout=10) for event in subscribed)
        _service(namespace).emit(
            FILE_FOUND_EXCHANGE,
            '{"file": "/data/both.nc", "hostname": "h"}',
        )
        assert poll_until(lambda: all(received), timeout=10), (
            f"not every builder received the file: {received}"
        )
    finally:
        for stop in stops:
            stop.set()
        for worker in workers:
            worker.join(timeout=10)


def test_the_file_found_queue_is_durable_and_shared() -> None:
    """The AMQP flags are asserted structurally, because memory ignores them.

    The in-memory transport discards ``durable``, ``exclusive`` and
    ``auto_delete``, so the assertions are on the resulting ``kombu.Queue``
    object. The flags come from that class's defaults, so asserting on the
    call kwargs would prove nothing.
    """
    conn = kombu.Connection("memory://")
    exchange = declare_fanout_exchange(conn, "ns-FilesFoundExchange")
    queue = declare_queue(conn, "ns-FilesFound-b", exchange=exchange)

    assert queue.durable is True, "a transient queue loses the backlog (#44)"
    assert queue.exclusive is False, "an exclusive queue dies with its consumer"
    assert queue.auto_delete is False, "auto-delete is the #44 topology"
    # Compared by name: kombu rebinds the Exchange to the queue's channel.
    assert queue.exchange.name == exchange.name
    assert queue.exchange.type == "fanout"
    assert queue.routing_key == ""


def test_the_consume_declare_matches_the_registered_config() -> None:
    """The consumer's declare and the registered config describe one queue.

    ``get_connection_context`` declares the file-found queue from
    ``_file_found_queue_config``; the consumer redeclares it on its own
    connection. A broker compares a durable queue's properties on
    redeclaration and answers 406 if they disagree. The two descriptions used
    to be written out separately, in ``declare_bound_queue`` and in that
    config.
    """
    manager = _service()._broker_manager
    registered = manager._file_found_queue_config()

    conn = kombu.Connection("memory://")
    exchange = declare_fanout_exchange(
        conn,
        manager.get_queue_name(FILE_FOUND_EXCHANGE),
    )
    consumed = declare_queue(conn, "ns-FilesFound-b", exchange=exchange)

    assert consumed.durable == registered["durable"]
    assert consumed.exclusive == registered["exclusive"]
    assert consumed.auto_delete == registered["auto_delete"]
    assert consumed.routing_key == registered["routing_key"]
    assert consumed.exchange.name == registered["exchange"].name
    assert consumed.exchange.type == registered["exchange"].type
    assert consumed.exchange.durable == registered["exchange"].durable


def test_the_exclusive_queue_helper_is_gone() -> None:
    """The anonymous exclusive-queue helper is gone.

    It provided a fallback that loses files. The in-memory transport cannot
    show that behaviourally, so the check here is structural.
    """
    from courier.broker import kombu as broker

    assert not hasattr(broker, "declare_fanout_queue")
