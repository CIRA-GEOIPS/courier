"""One message a consumer cannot get past must not stop every message behind it.

``Service._relay`` used to answer any failure with ``reject(requeue=True)``,
which returns the message to the *head* of its queue. The shipped
``broker_prefetch_count`` is 1, so the broker immediately handed the same
message back to the same consumer, which failed on it again, forever. Nothing
behind it was ever delivered, and nothing said so: no dead-letter, no cap, no
metric, no log line. A run against a deliberately broken consumer produced
~1.6M log lines in under four minutes with zero messages processed.

The in-memory transport cannot express any of this. It has no redelivery
semantics to speak of, so a test written against it passes identically before
and after the fix -- which is why this tier exists.
"""

from __future__ import annotations

import threading
import uuid

import kombu
import pytest
from prometheus_client import REGISTRY

from courier.config import ServiceConfig
from courier.constants import FILE_FOUND_EXCHANGE, dead_letter_queue_for
from courier.service import Service
from tests._helpers import poll_until, stays_false
from tests.rabbitmq.conftest import queue_depth

_DEAD_LETTERED = "courier_broker_messages_dead_lettered_total"
_REDELIVERED = "courier_broker_messages_redelivered_total"


class _PluginExplodedError(RuntimeError):
    """What a plugin raising on one message looks like from the consume loop."""


def _counter(name: str, queue_name: str) -> float:
    """Return a labelled counter's current value, or 0.0 if it has none yet."""
    value = REGISTRY.get_sample_value(name, {"queue_name": queue_name})
    return 0.0 if value is None else value


def _service(amqp_url: str, namespace: str, max_redeliveries: int) -> Service:
    """Return a routed, preflighted service with one job builder, ``jb``."""
    service = Service(
        ServiceConfig(
            broker_url=amqp_url,
            prometheus_port=0,
            namespace=namespace,
            tracing_enabled=False,
            broker_max_retries=1,
            broker_max_redeliveries=max_redeliveries,
        ),
    )
    service.configure_routing(
        dispatcher_identifiers=set(),
        builder_targets={},
        builder_identifiers={"jb"},
    )
    service.preflight_check()
    return service


class _Consumer:
    """A consumer that raises on chosen bodies and records every delivery.

    Modelled on :meth:`courier.interfaces.dispatchers.Dispatcher.handle_incoming_jobs`
    -- a ``while not stopped`` loop around ``consume`` -- because that shape is
    what makes the bug survivable enough to observe. The exception is raised in
    the ``for`` body, so Python abandons the generator rather than throwing into
    it, and the failure reaches ``_relay`` as ``GeneratorExit``. That is the path
    a real plugin failure takes.
    """

    def __init__(self, service: Service, poison: frozenset[str]) -> None:
        self.service = service
        self.poison = poison
        self.delivered: list[str] = []
        self.stop = threading.Event()
        self.subscribed = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        while not self.stop.is_set():
            try:
                for body, _ctx in self.service.consume(
                    FILE_FOUND_EXCHANGE,
                    stop_event=self.stop,
                    on_subscribed=self.subscribed.set,
                    subscriber="jb",
                ):
                    self.delivered.append(body)
                    if body in self.poison:
                        raise _PluginExplodedError(
                            body
                        )  # noqa: TRY301 -- inline is the point: this is a plugin failing mid-loop
            except _PluginExplodedError:
                continue

    def deliveries_of(self, body: str) -> int:
        """Return how many times *body* has been handed to this consumer."""
        return self.delivered.count(body)

    def __enter__(self) -> _Consumer:
        self._thread.start()
        assert self.subscribed.wait(timeout=30), "consumer never subscribed"
        return self

    def __exit__(self, *_exc: object) -> None:
        self.stop.set()
        self._thread.join(timeout=30)


@pytest.fixture
def poison_body() -> str:
    """Return a body unique to one test, so counters cannot bleed across runs."""
    return f'{{"file": "/data/poison-{uuid.uuid4().hex[:8]}.nc", "hostname": "h"}}'


def test_messages_behind_an_unprocessable_one_are_still_delivered(
    amqp_url: str,
    namespace: str,
    raw_conn: kombu.Connection,
    poison_body: str,
) -> None:
    """The queue keeps moving when its head is a message nobody can handle.

    This is the head-of-line block itself. Reverted check: put
    ``reject()`` back in ``_relay``'s ``GeneratorExit`` branch unconditionally.
    The poison message is redelivered to the head forever and not one of the
    three files behind it is ever seen, so the assertion below times out.
    """
    service = _service(amqp_url, namespace, max_redeliveries=2)
    service.emit(FILE_FOUND_EXCHANGE, poison_body)
    behind = [
        f'{{"file": "/data/behind-{index}.nc", "hostname": "h"}}' for index in range(3)
    ]
    for body in behind:
        service.emit(FILE_FOUND_EXCHANGE, body)

    with _Consumer(service, frozenset({poison_body})) as consumer:
        assert poll_until(
            lambda: all(body in consumer.delivered for body in behind),
            timeout=30,
        ), (
            "the files queued behind an unprocessable message were never "
            f"delivered; {len(consumer.delivered)} deliveries so far, "
            f"{consumer.deliveries_of(poison_body)} of them the same "
            "unprocessable message"
        )

    # The healthy messages were acknowledged, not parked alongside the poison.
    dead_letter = dead_letter_queue_for(f"{namespace}-FilesFound-jb")
    assert poll_until(lambda: queue_depth(raw_conn, dead_letter) == 1, timeout=30), (
        "exactly the unprocessable message should have been parked, found "
        f"{queue_depth(raw_conn, dead_letter)} parked"
    )


def test_an_unprocessable_message_is_parked_rather_than_retried_forever(
    amqp_url: str,
    namespace: str,
    raw_conn: kombu.Connection,
    poison_body: str,
) -> None:
    """Attempts are bounded by ``broker_max_redeliveries``, and then it stops.

    Reverted check: the unconditional ``reject()`` never stops retrying, so the
    settle window below sees the delivery count climb past the bound.
    """
    max_redeliveries = 2
    service = _service(amqp_url, namespace, max_redeliveries=max_redeliveries)
    service.emit(FILE_FOUND_EXCHANGE, poison_body)

    queue = f"{namespace}-FilesFound-jb"
    expected = max_redeliveries + 1

    with _Consumer(service, frozenset({poison_body})) as consumer:
        assert poll_until(
            lambda: consumer.deliveries_of(poison_body) == expected,
            timeout=30,
        ), (
            f"expected {expected} attempts at the poison message, saw "
            f"{consumer.deliveries_of(poison_body)}"
        )
        # And then it stops: retrying is bounded, not merely slowed down.
        assert stays_false(
            lambda: consumer.deliveries_of(poison_body) > expected,
            window=5,
        ), (
            f"the poison message was still being retried after {expected} "
            f"attempts ({consumer.deliveries_of(poison_body)} so far)"
        )

    assert queue_depth(raw_conn, queue) == 0, (
        "the poison message is still on its original queue, where it will "
        "block the next consumer to attach"
    )


def test_a_parked_message_is_kept_intact_on_the_dead_letter_queue(
    amqp_url: str,
    namespace: str,
    raw_conn: kombu.Connection,
    poison_body: str,
) -> None:
    """Giving up on a message must not mean discarding it.

    Acking-and-dropping would also unblock the queue, and would lose the file
    silently -- the same failure mode as the exclusive queue this branch
    replaced. The body has to still be there, readable, for an operator to
    triage and replay.
    """
    service = _service(amqp_url, namespace, max_redeliveries=1)
    service.emit(FILE_FOUND_EXCHANGE, poison_body)
    dead_letter = dead_letter_queue_for(f"{namespace}-FilesFound-jb")

    with _Consumer(service, frozenset({poison_body})):
        assert poll_until(
            lambda: queue_depth(raw_conn, dead_letter) == 1,
            timeout=30,
        ), "the message the service gave up on was not parked anywhere"

    with raw_conn.channel() as channel:
        message = kombu.Queue(dead_letter, channel=channel).get(no_ack=True)
    assert message is not None
    body = message.body
    assert (body.decode() if isinstance(body, bytes) else body) == poison_body


def test_giving_up_on_a_message_is_counted_and_not_silent(
    amqp_url: str,
    namespace: str,
    raw_conn: kombu.Connection,
    poison_body: str,
) -> None:
    """Being stuck has to be visible to something other than a log tail.

    The original failure was diagnosable only by noticing that a log file had
    grown by a million lines. Both halves are counted here: retries, which are
    normal in small numbers, and parked messages, which never are.
    """
    max_redeliveries = 2
    service = _service(amqp_url, namespace, max_redeliveries=max_redeliveries)
    queue = f"{namespace}-FilesFound-jb"
    before_parked = _counter(_DEAD_LETTERED, queue)
    before_retried = _counter(_REDELIVERED, queue)

    service.emit(FILE_FOUND_EXCHANGE, poison_body)
    with _Consumer(service, frozenset({poison_body})):
        assert poll_until(
            lambda: _counter(_DEAD_LETTERED, queue) == before_parked + 1,
            timeout=30,
        ), "a message was abandoned without the dead-letter counter moving"

    assert _counter(_REDELIVERED, queue) == before_retried + max_redeliveries
    assert queue_depth(raw_conn, queue) == 0, (
        "the counters say the message was dealt with, but it is still on the "
        "queue where it can block the next consumer"
    )


def test_shutting_down_mid_message_does_not_spend_a_retry(
    amqp_url: str,
    namespace: str,
    raw_conn: kombu.Connection,
    poison_body: str,
) -> None:
    """A message in flight at shutdown was never tried, so it is not a failure.

    ``GeneratorExit`` reaches ``_relay`` both when the caller gives up on a
    message and when the consumer is stopping with the message untouched --
    the dispatcher's daemon thread is documented as being abandoned mid-job if
    it outlives the join timeout. Counting the second as an attempt would spend
    redelivery budget on every rolling restart and eventually park healthy
    messages. ``max_redeliveries=0`` makes that immediate and so observable: if
    shutdown were treated as a failure this message would be parked.

    Driving the generator by hand is the point rather than an implementation
    detail: closing it while a message is outstanding is the only way to reach
    the branch, and it is what abandoning the consume loop does.

    Unlike the rest of this module this test passes against the unfixed code
    too, because that rejected on every ``GeneratorExit``. It guards a risk the
    fix introduces rather than the bug the fix removes, and it is the reason
    ``_relay`` consults *stop_event* instead of counting every close.
    """
    service = _service(amqp_url, namespace, max_redeliveries=0)
    service.emit(FILE_FOUND_EXCHANGE, poison_body)

    queue = f"{namespace}-FilesFound-jb"
    dead_letter = dead_letter_queue_for(queue)
    stop = threading.Event()
    stream = service.consume(
        FILE_FOUND_EXCHANGE,
        stop_event=stop,
        subscriber="jb",
    )
    try:
        body, _ctx = next(stream)
        assert body == poison_body
        # The message is in hand and unacknowledged. Now the service stops and
        # the consumer walks away from it.
        stop.set()
    finally:
        stream.close()

    assert poll_until(
        lambda: queue_depth(raw_conn, queue) == 1, timeout=30
    ), "a message held at shutdown was not returned to its queue"
    assert queue_depth(raw_conn, dead_letter) == 0, (
        "a message that was merely interrupted by shutdown was parked as if "
        "the consumer had failed on it"
    )
