"""The half of issue #44 that only a real broker can demonstrate.

The in-memory transport ignores ``exclusive`` and never deletes a queue when a
connection closes, so a test written against it passes the same before and
after the fix. On RabbitMQ the old subscription queue was
``durable=True, exclusive=True``, and the broker deletes an exclusive queue,
along with everything buffered in it, when the consumer disconnects.
"""

from __future__ import annotations

import threading

import kombu
import pytest

from courier.broker.kombu import declare_fanout_exchange, declare_queue
from courier.config import ServiceConfig
from courier.constants import FILE_FOUND_EXCHANGE
from courier.errors import FatalBrokerError
from courier.service import Service
from tests._helpers import poll_until
from tests.rabbitmq.conftest import queue_depth

#: Messages published in one burst to build a backlog the consumer
#: must then drain.
BURST_SIZE = 5


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

    With the previous exclusive queue the broker deleted the subscription when
    the builder disconnected, and every file published while it was away was
    discarded with no error, metric or log line.

    Reverted check: pass ``exclusive=True`` from
    ``MessageBrokerManager._file_found_queue_config``. The queue disappears on
    disconnect, the three publishes below go nowhere, and the depth assertion
    fails.
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
        assert poll_until(
            lambda: len(received) == 3, timeout=30
        ), f"only {len(received)} of 3 buffered files were delivered"
    finally:
        _stop(worker)


def test_the_queue_survives_its_consumer_and_is_not_exclusive(
    amqp_config: ServiceConfig,
    namespace: str,
    raw_conn: kombu.Connection,
) -> None:
    """After the consumer disconnects the queue is still there, and durable.

    An exclusive or auto-delete queue is removed on disconnect, and the
    passive depth check below then fails with ``NOT_FOUND``. Redeclaring with
    the same properties must not raise.
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
    declare_queue(raw_conn, queue, exchange=exchange)


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
    """A property mismatch raises a fatal error naming the queue and a remedy.

    A 406 used to match none of the broker layer's except clauses, escape as a
    raw amqp exception, and take the process down through the plugin's
    catch-all.

    The planted conflict is on ``auto_delete`` or on queue arguments, not on
    ``durable``. A transient non-exclusive queue is RabbitMQ's deprecated
    ``transient_nonexcl_queues`` feature, refused by default since 4.x with a
    541, so a ``durable`` conflict fails in the setup before courier is called.
    The file-found queue is ``durable=True, exclusive=False, auto_delete=False``
    with no arguments, so both properties below mismatch on a broker that
    permits the declare.

    ADR-0010 relies on the ``arguments`` case: courier dead-letters from the
    consumer side because adding an argument to a durable queue an existing
    deployment already declared is a 406.
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

    The unit tier can only show that the prefetch value reached the consumer
    object. This test shows the quality-of-service frame reaching the broker.
    The broker default is unlimited, so without it a builder attaching to a
    backlog receives the whole backlog at once, unacknowledged.
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

    for index in range(BURST_SIZE):
        service.emit(
            FILE_FOUND_EXCHANGE,
            f'{{"file": "/data/burst-{index}.nc", "hostname": "h"}}',
        )

    queue = f"{namespace}-FilesFound-jb"
    assert poll_until(lambda: queue_depth(raw_conn, queue) == BURST_SIZE, timeout=30)

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
        # One message is in flight and unacknowledged, so the rest stay ready.
        assert poll_until(
            lambda: queue_depth(raw_conn, queue) == BURST_SIZE - 1,
            timeout=30,
        ), (
            f"expected {BURST_SIZE - 1} still ready with prefetch=1, "
            f"found {queue_depth(raw_conn, queue)}"
        )
    finally:
        release.set()
        stop.set()
        worker.join(timeout=30)
