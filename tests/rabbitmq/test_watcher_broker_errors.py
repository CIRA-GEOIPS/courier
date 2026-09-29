"""What the queue-watching data monitor does when the broker refuses it.

``RabbitMQWatcher`` consumes a queue somebody else owns -- the shipped
``config.yaml`` points it at ``nrt_file_notif_queue``, "set by the data
inventory" -- and declares it ``durable=True`` with no configuration knob for
``exclusive``, ``auto_delete`` or ``arguments``. If the owner declared it any
other way the watcher's own declare draws a 406, and the operator cannot
configure their way out of it.

That declare used to sit outside ``broker_error_triage``, and every amqp
exception is an ``AMQPError`` -- which is neither ``OperationalError`` nor
``OSError``/``ValueError``/``RuntimeError``, the only things
``_listen_to_broker`` caught. So the 406 escaped the retry loop, killed the
listener thread with the error queue empty, and surfaced as ``find_file``'s
generic "listener thread exited unexpectedly without an error on the queue" --
which the data-monitor catch-all turned into ``os._exit(1)``.

A 406 is a real-broker property: the memory transport ignores ``durable``,
``exclusive`` and ``auto_delete`` entirely, so nothing below can be expressed
one tier down.
"""

from __future__ import annotations

import contextlib
import threading
import uuid
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import kombu
import pytest

from courier.errors import FatalBrokerError
from courier.plugins.data_monitors.rabbit_mq_watcher import RabbitMQWatcher
from tests._helpers import poll_until

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture
def watched_queue(raw_conn: kombu.Connection, namespace: str) -> Iterator[str]:
    """Return a queue name unique to one test, deleted on teardown."""
    name = f"{namespace}-watched-{uuid.uuid4().hex[:8]}"
    try:
        yield name
    finally:
        with raw_conn.channel() as channel, contextlib.suppress(Exception):
            # Teardown is best effort.
            channel.queue_delete(name)


@pytest.fixture
def stub_service() -> MagicMock:
    """Service stub with the minimum attributes every plugin reads.

    The equivalent fixture in ``tests/unit_tests/plugins/conftest.py`` does not
    reach this package.
    """
    service = MagicMock()
    service._config = MagicMock()
    service._config.log_level = "DEBUG"
    service._config.loki_enabled = False
    service._config.namespace = "test-ns"
    service.config = service._config
    return service


def _watcher(amqp_url: str, queue_name: str, service: object) -> RabbitMQWatcher:
    """Return a watcher pointed at *queue_name* on the test broker."""
    parsed = kombu.Connection(amqp_url)
    return RabbitMQWatcher(
        service,
        {
            "rabbitmq_host": parsed.hostname or "localhost",
            "rabbitmq_port": parsed.port or 5672,
            "rabbitmq_username": parsed.userid or "guest",
            "rabbitmq_password": parsed.password or "guest",
            "rabbitmq_virtual_host": parsed.virtual_host or "/",
            "rabbitmq_queue": queue_name,
            # The shipped config's policy: reconnect forever. A fatal error has
            # to stop anyway, and that is the claim under test.
            "max_retries": -1,
            "retry_delay_seconds": 0.01,
        },
    )


def test_a_queue_the_watcher_cannot_declare_is_reported_not_retried_forever(
    amqp_url: str,
    watched_queue: str,
    raw_conn: kombu.Connection,
    stub_service: MagicMock,
) -> None:
    """A property mismatch reaches the operator, with the queue name and a remedy.

    Reverted check, verified: dropping either half -- the
    ``broker_error_triage`` block around the declare, or the
    ``FatalBrokerError`` clause in ``_listen_to_broker`` -- fails this with
    ``RuntimeError('RabbitMQ listener thread exited unexpectedly without an
    error on the queue.')``. Both leave an exception that matches no clause in
    the retry loop, so the thread dies with the error queue empty and the real
    406 is lost. Both halves are needed: the wrapper produces a
    ``FatalBrokerError``, the clause is what catches it.
    """
    # Somebody else owns this queue and declared it auto-delete. The watcher
    # always asks for auto_delete=False, so its declare cannot succeed.
    with raw_conn.channel() as channel:
        kombu.Queue(
            watched_queue,
            durable=True,
            auto_delete=True,
            channel=channel,
        ).declare()

    watcher = _watcher(amqp_url, watched_queue, stub_service)
    raised: list[BaseException] = []

    def _run() -> None:
        try:
            list(watcher.find_file())
        except BaseException as exc:  # the assertion below is on the type
            raised.append(exc)

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    try:
        assert poll_until(lambda: bool(raised), timeout=30), (
            "the watcher never reported the mismatched queue; it is still "
            "reconnecting into an error that cannot succeed"
        )
    finally:
        watcher.stop()
        worker.join(timeout=30)

    error = raised[0]
    assert isinstance(
        error, FatalBrokerError
    ), f"expected a FatalBrokerError naming the queue, got {error!r}"
    message = str(error)
    assert watched_queue in message
    assert "406" in message or "PRECONDITION" in message.upper()
    assert "prune" in message


def test_a_queue_the_watcher_can_declare_keeps_the_listener_running(
    amqp_url: str,
    watched_queue: str,
    raw_conn: kombu.Connection,
    stub_service: MagicMock,
) -> None:
    """The guard must not fire on a queue that is simply fine.

    Without this the previous test would pass against a watcher that treated
    every declare as fatal.
    """
    with raw_conn.channel() as channel:
        kombu.Queue(watched_queue, durable=True, channel=channel).declare()

    watcher = _watcher(amqp_url, watched_queue, stub_service)
    raised: list[BaseException] = []
    started = threading.Event()

    def _run() -> None:
        started.set()
        try:
            list(watcher.find_file())
        except BaseException as exc:  # the assertion below is on the type
            raised.append(exc)

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    try:
        assert started.wait(timeout=30)
        # It stays up: the queue matches, so there is nothing to report.
        assert not poll_until(
            lambda: bool(raised), timeout=5
        ), f"a healthy queue was reported as a broker failure: {raised}"
    finally:
        watcher.stop()
        worker.join(timeout=30)

    assert not raised
