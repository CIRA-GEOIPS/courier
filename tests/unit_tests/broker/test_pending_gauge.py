"""The pending-messages gauge counts the same series up and down.

A fanout publish was counted once, labelled with the exchange; every consumer
decremented labelled with the queue it read from. The two halves were
different time series, so the exchange series climbed forever and each queue
series went negative as its backlog drained.
"""

from __future__ import annotations

import threading
import uuid

from prometheus_client import REGISTRY

from courier.config import ServiceConfig
from courier.constants import FILE_FOUND_EXCHANGE
from courier.service import Service
from tests._helpers import poll_until

_GAUGE = "courier_broker_messages_pending"


def _service(namespace: str, builders: frozenset[str]) -> Service:
    """Return a Service on the in-memory transport that knows *builders*."""
    service = Service(
        ServiceConfig(
            broker_url="memory://",
            prometheus_port=0,
            namespace=namespace,
            tracing_enabled=False,
        ),
    )
    service.configure_routing((), builder_identifiers=builders)
    return service


def _pending(queue_name: str) -> float:
    """Return the gauge's current value for *queue_name*, or 0.0 if unset."""
    value = REGISTRY.get_sample_value(_GAUGE, {"queue_name": queue_name})
    return 0.0 if value is None else value


def test_a_fanout_publish_is_counted_against_every_bound_queue() -> None:
    """Each builder's queue gets one increment, and the exchange gets none.

    Reverted check: count once against the exchange name instead. Both
    per-builder assertions then read zero.
    """
    namespace = f"pg-{uuid.uuid4().hex[:8]}"
    service = _service(namespace, frozenset({"alpha", "beta"}))
    alpha, beta = service._file_found_queue_names()  # noqa: SLF001
    exchange = service._broker_manager.get_queue_name(
        FILE_FOUND_EXCHANGE
    )  # noqa: SLF001

    service.emit(FILE_FOUND_EXCHANGE, "one")

    assert _pending(alpha) == 1.0
    assert _pending(beta) == 1.0
    assert _pending(exchange) == 0.0


def test_consuming_returns_the_gauge_to_zero() -> None:
    """The decrement lands on the series the increment raised.

    The consumer runs to acknowledgement instead of ``break``-ing on the first
    message. A message still in hand when the loop is abandoned counts as a
    failed attempt and is requeued, which raises the gauge again.
    """
    namespace = f"pg-{uuid.uuid4().hex[:8]}"
    service = _service(namespace, frozenset({"solo"}))
    (queue_name,) = service._file_found_queue_names()  # noqa: SLF001
    before = _pending(queue_name)

    stop = threading.Event()
    subscribed = threading.Event()
    received: list[str] = []

    def _run() -> None:
        for body, _ctx in service.consume(
            FILE_FOUND_EXCHANGE,
            stop_event=stop,
            on_subscribed=subscribed.set,
            subscriber="solo",
        ):
            received.append(body)

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    try:
        assert subscribed.wait(timeout=10)
        service.emit(FILE_FOUND_EXCHANGE, "one")
        assert poll_until(lambda: bool(received), timeout=10)
        assert poll_until(lambda: _pending(queue_name) == before, timeout=10)
    finally:
        stop.set()
        worker.join(timeout=10)
