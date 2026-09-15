"""Consumer prefetch reaches the broker.

Without a prefetch count no quality-of-service frame is sent at all, which
means the broker default of *unlimited*. That was harmless while the
file-found queue was deleted whenever a consumer disconnected, because it was
always empty on attach. Against a durable queue holding a backlog it means the
whole backlog is pushed to the first consumer to attach, unacknowledged.
"""

from __future__ import annotations

import threading
import uuid
from unittest.mock import MagicMock, patch

import kombu
import pytest

from courier.broker.kombu import messages
from courier.config import ServiceConfig
from courier.constants import DISPATCHER_QUEUE, FILE_FOUND_EXCHANGE
from courier.errors import ConfigurationError
from courier.service import Service


def _drain(prefetch_count: int | None) -> MagicMock:
    """Run one no-op consume and return the patched Consumer class."""
    conn = kombu.Connection("memory://")
    queue = kombu.Queue(f"q-{uuid.uuid4().hex[:8]}", channel=conn.channel())
    stop = threading.Event()
    stop.set()  # exit after the first drain window

    with patch("courier.broker.kombu.kombu.Consumer") as consumer_cls:
        consumer_cls.return_value.__enter__ = MagicMock()
        consumer_cls.return_value.__exit__ = MagicMock(return_value=False)
        list(messages(conn, queue, stop_event=stop, prefetch_count=prefetch_count))
    return consumer_cls


def test_prefetch_count_reaches_the_consumer() -> None:
    """The configured window is handed to the broker consumer."""
    consumer_cls = _drain(7)

    assert consumer_cls.call_args.kwargs["prefetch_count"] == 7


def test_omitting_prefetch_sends_no_qos() -> None:
    """``None`` is passed through, which is the library's "send no QoS" value.

    Documents the default that made an unbounded push possible, so a change
    to it is deliberate rather than accidental.
    """
    consumer_cls = _drain(None)

    assert consumer_cls.call_args.kwargs["prefetch_count"] is None


@pytest.mark.parametrize(
    ("queue", "kwargs"),
    [
        (FILE_FOUND_EXCHANGE, {"subscriber": "jb"}),
        (DISPATCHER_QUEUE, {}),
    ],
)
def test_service_passes_its_prefetch_on_both_consume_paths(
    queue: str,
    kwargs: dict[str, str],
) -> None:
    """Both the file-found and the direct consume paths bound their window."""
    service = Service(
        ServiceConfig(
            broker_url="memory://",
            prometheus_port=0,
            namespace=f"pf-{uuid.uuid4().hex[:8]}",
            tracing_enabled=False,
            broker_prefetch_count=5,
        ),
    )
    stop = threading.Event()
    subscribed = threading.Event()
    seen: dict[str, object] = {}

    def _fake_messages(*_args: object, **call_kwargs: object) -> list[object]:
        seen.update(call_kwargs)
        return []

    with patch("courier.service.broker_messages", side_effect=_fake_messages):
        worker = threading.Thread(
            target=lambda: list(
                service.consume(
                    queue,
                    stop_event=stop,
                    on_subscribed=subscribed.set,
                    **kwargs,
                ),
            ),
            daemon=True,
        )
        worker.start()
        assert subscribed.wait(timeout=10)
        worker.join(timeout=10)

    assert seen["prefetch_count"] == 5


def test_a_prefetch_below_one_is_refused() -> None:
    """Zero means unlimited in AMQP, so it is rejected rather than accepted."""
    with pytest.raises(ConfigurationError, match="broker_prefetch_count"):
        ServiceConfig(broker_url="memory://", broker_prefetch_count=0)
