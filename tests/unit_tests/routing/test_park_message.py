"""``Service.park_message``: parking a message no retry can ever fix.

A dispatcher that cannot execute a job at all (no payload, a payload plugin
not installed here, no compatible representation) must neither drop it nor
retry it forever. It parks it on the dead-letter queue its consumer already
declared, with the reason in a header, and acknowledges the original.
"""

from __future__ import annotations

import threading
import uuid
from unittest.mock import patch

import kombu
import pytest

from courier.broker.kombu import DELIVERY_ATTEMPT_HEADER, PARK_REASON_HEADER
from courier.config import ServiceConfig
from courier.constants import job_ready_queue_for
from courier.errors import FatalBrokerError
from courier.service import Service


def _service() -> Service:
    """Return a Service on the in-memory transport with a unique namespace."""
    return Service(
        ServiceConfig(
            broker_url="memory://",
            prometheus_port=0,
            namespace=f"park-{uuid.uuid4().hex[:8]}",
            tracing_enabled=False,
        ),
    )


def _drain(name: str) -> list[kombu.Message]:
    """Remove and return every message on queue *name*."""
    drained: list[kombu.Message] = []
    with kombu.Connection("memory://") as conn:
        queue = kombu.Queue(name, channel=conn.channel())
        while (message := queue.get(no_ack=True)) is not None:
            drained.append(message)
    return drained


def test_message_lands_on_the_consumers_dead_letter_queue_with_its_reason() -> None:
    svc = _service()
    ns = svc.config.namespace

    svc.park_message(job_ready_queue_for("d1"), '{"job": 1}', "carries no payload")

    [parked] = _drain(f"{ns}-JobReady-d1-DeadLetter")
    assert parked.decode() == '{"job": 1}'
    assert parked.headers[PARK_REASON_HEADER] == "carries no payload"
    # Parked outright, not after spending retries.
    assert DELIVERY_ATTEMPT_HEADER not in parked.headers
    assert _drain(f"{ns}-JobReady-d1") == []


def test_a_dispatcher_parks_a_job_it_cannot_execute() -> None:
    """End to end: a job with no payload (a pre-upgrade builder's) is kept.

    It lands on the dispatcher's dead-letter queue with a reason instead of
    being acknowledged and dropped, and the original is acknowledged.
    """
    from courier.plugins.dispatchers.local_dispatcher import LocalDispatcher
    from courier.types.job import Job

    svc = _service()
    ns = svc.config.namespace
    svc.register_plugin(LocalDispatcher, {}, identifier="d1")
    dispatcher = svc._plugin_manager.get_plugins()["d1"].plugin
    body = str(Job("legacy", "job-1", {}))
    svc.emit(job_ready_queue_for("d1"), body)
    parked = threading.Event()
    park = svc.park_message

    def _park_then_stop(*args: object, **kwargs: object) -> None:
        park(*args, **kwargs)  # type: ignore[arg-type]
        parked.set()
        dispatcher._stop_event.set()

    with patch.object(svc, "park_message", side_effect=_park_then_stop):
        worker = threading.Thread(target=dispatcher.handle_incoming_jobs, daemon=True)
        worker.start()
        assert parked.wait(timeout=10)
        worker.join(timeout=10)

    assert not worker.is_alive()
    [message] = _drain(f"{ns}-JobReady-d1-DeadLetter")
    assert message.decode() == body
    assert "payload" in message.headers[PARK_REASON_HEADER]
    assert _drain(f"{ns}-JobReady-d1") == []


def test_a_long_reason_is_capped_in_the_header_and_logged_in_full(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A long reason must not be able to make the park itself fail."""
    svc = _service()
    ns = svc.config.namespace
    reason = "invalid payload config: " + "x" * 10_000
    logger = svc._logger.logger
    logger.addHandler(caplog.handler)
    try:
        svc.park_message(job_ready_queue_for("d1"), "job", reason)
    finally:
        logger.removeHandler(caplog.handler)

    [parked] = _drain(f"{ns}-JobReady-d1-DeadLetter")
    header = parked.headers[PARK_REASON_HEADER]
    assert reason.startswith(header)
    assert 0 < len(header) < len(reason)
    assert any(reason in r.getMessage() for r in caplog.records)


def test_a_failed_publish_propagates() -> None:
    """The caller must not return to the loop, so the original is redelivered."""
    svc = _service()

    with (
        patch("courier.service.publish", side_effect=FatalBrokerError("refused")),
        pytest.raises(FatalBrokerError),
    ):
        svc.park_message(job_ready_queue_for("d1"), "job", "no payload")
