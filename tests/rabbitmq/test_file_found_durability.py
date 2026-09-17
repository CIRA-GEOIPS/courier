"""The half of issue #44 that only a real broker can demonstrate.

The in-memory transport ignores ``exclusive`` and never deletes a queue when a
connection closes, so a test written against it passes identically before and
after the fix. On RabbitMQ the old subscription queue was
``durable=True, exclusive=True``, and exclusivity makes the broker delete the
queue -- and everything buffered in it -- the instant the consumer disconnects.
"""

from __future__ import annotations

import threading

import kombu
import pytest

from courier.broker.kombu import declare_bound_queue, declare_fanout_exchange
from courier.config import ServiceConfig
from courier.constants import FILE_FOUND_EXCHANGE
from courier.errors import FatalBrokerError
from courier.service import Service
from tests._helpers import poll_until
from tests.rabbitmq.conftest import queue_depth


def _drain(service: Service, subscriber: str, received: list[str]) -> threading.Thread:
    """Start a consumer thread, and return it so the caller can stop it."""
    stop = threading.Event()
    subscribed = threading.Event()

    def _run() -> None:
        for body, _ctx in service.consume(
            FILE_FOUND_EXCHANGE,
            stop_event=stop,
            on_subscribed=subscribed.set,
            subscriber=subscriber,
        ):
            received.append(body)

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    assert subscribed.wait(timeout=30), "consumer never subscribed"
    worker.stop_event = stop  # type: ignore[attr-defined]
    return worker


def _stop(worker: threading.Thread) -> None:
    """Stop a consumer thread started by :func:`_drain`."""
    worker.stop_event.set()  # type: ignore[attr-defined]
    worker.join(timeout=30)


def test_files_published_while_the_consumer_is_gone_are_delivered_later(
    amqp_config: ServiceConfig,
    namespace: str,
    raw_conn: kombu.Connection,
) -> None:
    """A builder that goes away and comes back loses nothing.

    This is the headline of issue #44. With the previous exclusive queue the
    broker deleted the subscription the moment the builder disconnected, and
    every file published while it was away was discarded with no error, no
    metric and no log line anywhere.

    Reverted check: change ``declare_bound_queue`` back to ``exclusive=True``.
    The queue disappears on disconnect, the three publishes below go nowhere,
    and the depth assertion fails.
    """
    service = Service(amqp_config)
    service.configure_routing(
        dispatcher_identifiers=set(),
        builder_targets={},
        builder_identifiers={"jb"},
    )
    service.preflight_check()

    # Attach once, then disconnect: this is where the old queue died.
    first: list[str] = []
    worker = _drain(service, "jb", first)
    _stop(worker)

    for index in range(3):
        service.emit(
            FILE_FOUND_EXCHANGE,
            f'{{"file": "/data/away-{index}.nc", "hostname": "h"}}',
        )

    queue = f"{namespace}-FilesFound-jb"
    assert poll_until(lambda: queue_depth(raw_conn, queue) == 3, timeout=30), (
        f"expected 3 messages held for the absent builder, "
        f"found {queue_depth(raw_conn, queue)}"
    )

    received: list[str] = []
    worker = _drain(service, "jb", received)
    try:
        assert poll_until(lambda: len(received) == 3, timeout=30), (
            f"only {len(received)} of 3 buffered files were delivered"
        )
    finally:
        _stop(worker)


def test_the_queue_survives_its_consumer_and_is_not_exclusive(
    amqp_config: ServiceConfig,
    namespace: str,
    raw_conn: kombu.Connection,
) -> None:
    """After the consumer disconnects the queue is still there, and durable.

    Redeclaring with the same properties must not raise: if the queue had been
    declared exclusive or auto-delete, this passive check would fail with
    ``NOT_FOUND`` because the broker would have removed it.
    """
    service = Service(amqp_config)
    service.configure_routing(
        dispatcher_identifiers=set(),
        builder_targets={},
        builder_identifiers={"jb"},
    )
    service.preflight_check()

    worker = _drain(service, "jb", [])
    _stop(worker)

    queue = f"{namespace}-FilesFound-jb"
    assert queue_depth(raw_conn, queue) == 0

    # Same properties: idempotent, no precondition failure.
    exchange = declare_fanout_exchange(raw_conn, f"{namespace}-FilesFoundExchange")
    declare_bound_queue(raw_conn, exchange, queue)


@pytest.mark.parametrize(
    ("label", "conflict"),
    [
        ("auto_delete", {"auto_delete": True}),
        ("arguments", {"queue_arguments": {"x-message-ttl": 60000}}),
    ],
)
def test_a_conflicting_redeclare_is_a_fatal_error_naming_the_queue(
    amqp_config: ServiceConfig,
    namespace: str,
    raw_conn: kombu.Connection,
    label: str,
    conflict: dict,
) -> None:
    """A property mismatch fails loudly with a remedy, not a raw traceback.

    A 406 used to match none of the broker layer's except clauses, escape as a
    raw amqp exception, and take the process down through the plugin's
    catch-all.

    The conflict is deliberately *not* on ``durable``, which is what this
    planted before. A transient non-exclusive queue is RabbitMQ's deprecated
    ``transient_nonexcl_queues`` feature, refused by default since 4.x with a
    541 -- so the setup died before courier was ever called and the test
    asserted nothing. ``declare_bound_queue`` sets
    ``durable=True, exclusive=False, auto_delete=False`` and no arguments, so
    either of the properties below is a genuine mismatch on a broker that still
    permits the declare.

    The ``arguments`` case is the one ADR-0010 leans on: its whole argument for
    dead-lettering from the consumer side rather than with an
    ``x-dead-letter-exchange`` is that adding an argument to a durable queue an
    existing deployment already declared is a 406. That is pinned here.
    """
    del label  # names the case in test output
    queue = f"{namespace}-FilesFound-jb"
    with raw_conn.channel() as channel:
        kombu.Queue(queue, durable=True, channel=channel, **conflict).declare()

    service = Service(amqp_config)
    service.configure_routing(
        dispatcher_identifiers=set(),
        builder_targets={},
        builder_identifiers={"jb"},
    )

    with pytest.raises(FatalBrokerError) as caught:
        service.preflight_check()

    message = str(caught.value)
    assert queue in message
    assert "406" in message or "PRECONDITION" in message.upper()
    assert "prune" in message


def test_prefetch_bounds_unacknowledged_deliveries(
    amqp_url: str,
    namespace: str,
    raw_conn: kombu.Connection,
) -> None:
    """The broker holds messages back instead of pushing the whole backlog.

    The only behavioural proof that a quality-of-service frame is actually
    sent: the unit tier can only show that the value reached the consumer
    object. Without it the broker default is unlimited, so a builder attaching
    to a backlog would have all of it pushed at once, unacknowledged.
    """
    config = ServiceConfig(
        broker_url=amqp_url,
        prometheus_port=0,
        namespace=namespace,
        tracing_enabled=False,
        broker_max_retries=1,
        broker_prefetch_count=1,
    )
    service = Service(config)
    service.configure_routing(
        dispatcher_identifiers=set(),
        builder_targets={},
        builder_identifiers={"jb"},
    )
    service.preflight_check()

    total = 5
    for index in range(total):
        service.emit(
            FILE_FOUND_EXCHANGE,
            f'{{"file": "/data/burst-{index}.nc", "hostname": "h"}}',
        )

    queue = f"{namespace}-FilesFound-jb"
    assert poll_until(lambda: queue_depth(raw_conn, queue) == total, timeout=30)

    release = threading.Event()
    stop = threading.Event()
    subscribed = threading.Event()
    handled: list[str] = []

    def _run() -> None:
        for body, _ctx in service.consume(
            FILE_FOUND_EXCHANGE,
            stop_event=stop,
            on_subscribed=subscribed.set,
            subscriber="jb",
        ):
            handled.append(body)
            release.wait(timeout=60)

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    try:
        assert subscribed.wait(timeout=30)
        # One message is in flight and unacknowledged; with no prefetch the
        # broker would have handed over all five and left none ready.
        assert poll_until(
            lambda: queue_depth(raw_conn, queue) == total - 1,
            timeout=30,
        ), (
            f"expected {total - 1} still ready with prefetch=1, "
            f"found {queue_depth(raw_conn, queue)}"
        )
    finally:
        release.set()
        stop.set()
        worker.join(timeout=30)
