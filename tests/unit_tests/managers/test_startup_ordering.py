"""``Service.consume`` signals its binding before it reads anything.

Plugins used to be started consumers-first, with the manager blocking until
each reported its subscription, so that no data monitor could publish into a
fanout exchange with nothing bound to it. That ordering is gone: each builder
now consumes a durable ``FilesFound-<identifier>`` queue that every container
declares during preflight, so a file published before its builder attaches
waits on the broker instead of being discarded.

What survives is the signal itself. ``wait_until_subscribed`` and the
``on_subscribed`` callback are still how the broker-backed tests in
``tests/rabbitmq/`` synchronise on a consumer being attached, and a callback
that fired *after* the first message would be useless for that. Survival
across a builder being down is covered there too, because it needs a real
broker -- the in-memory transport ignores exclusivity entirely.
"""

from __future__ import annotations

import threading
import time
import uuid

from courier.config import ServiceConfig
from courier.constants import FILE_FOUND_EXCHANGE
from courier.service import Service


class TestConsumeSubscriptionSignal:
    """``Service.consume`` must signal binding before it reads any message."""

    def test_on_subscribed_fires_before_the_first_yield(self) -> None:
        """The callback is the barrier's evidence that the queue is bound.

        If it fired after the first message arrived it would be useless as an
        ordering signal: the producer would already have been released.
        """
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
                subscriber="builder",
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
