"""Behavioural tests for ``Dispatcher._emit_queue_depth``.

Assertions read the *exported sample* from the Prometheus registry rather than
inspecting the gauge object or a mock. The previous version of this file
patched ``DISPATCHER_QUEUE_DEPTH.labels``, then called it directly from the
test body and asserted the mock had been called -- ``_emit_queue_depth`` was
never invoked, so all four tests passed with the method deleted.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from kombu.exceptions import OperationalError
from prometheus_client import REGISTRY

from courier.interfaces.dispatchers import Dispatcher

_METRIC = "courier_dispatcher_queue_depth"


def _depth(identifier: str) -> float | None:
    """Read the gauge as Prometheus would scrape it."""
    return REGISTRY.get_sample_value(
        _METRIC,
        {"dispatcher_identifier": identifier},
    )


def _dispatcher(mock_service: MagicMock, identifier: str) -> Dispatcher:
    return Dispatcher(mock_service, {}, identifier=identifier)


def _wire_broker(
    mock_service: MagicMock,
    *,
    connected: bool,
    message_count: int = 0,
    channel_error: Exception | None = None,
) -> MagicMock:
    """Point the service's broker manager at a stub channel.

    The probe takes a connection of its own from
    ``open_private_connection`` rather than borrowing the service's shared
    ``_connection``, so that is what is stubbed here.  A broker that cannot
    be reached raises from there, which is the ``connected=False`` case.
    """
    channel = MagicMock()
    if channel_error is not None:
        channel.queue_declare.side_effect = channel_error
    else:
        # py-amqp returns (queue_name, message_count, consumer_count).
        channel.queue_declare.return_value = ("q", message_count, 0)

    connection = MagicMock()
    connection.connected = True
    connection.channel.return_value.__enter__.return_value = channel

    broker = mock_service._broker_manager
    broker.reset_mock()
    if connected:
        broker.open_private_connection.side_effect = None
        broker.open_private_connection.return_value = connection
    else:
        broker.open_private_connection.side_effect = OperationalError("no broker")
    broker.get_queue_name.side_effect = lambda base: f"test-ns-{base}"
    return channel


class TestEmitQueueDepth:
    """The gauge must reflect what the broker actually reports."""

    def test_reports_broker_depth_when_connected(
        self,
        mock_service: MagicMock,
    ) -> None:
        _wire_broker(mock_service, connected=True, message_count=42)
        dispatcher = _dispatcher(mock_service, "depth-connected")

        dispatcher._emit_queue_depth()

        assert _depth("depth-connected") == 42

    def test_reports_zero_when_broker_disconnected(
        self,
        mock_service: MagicMock,
    ) -> None:
        """A broker that cannot be reached zeroes the gauge, it does not raise."""
        _wire_broker(mock_service, connected=False)
        dispatcher = _dispatcher(mock_service, "depth-disconnected")

        dispatcher._emit_queue_depth()

        assert _depth("depth-disconnected") == 0

    def test_reports_zero_when_the_broker_query_raises(
        self,
        mock_service: MagicMock,
    ) -> None:
        """A broker that refuses passive declare must not break dispatch."""
        _wire_broker(
            mock_service,
            connected=True,
            channel_error=RuntimeError("NOT_FOUND - no queue"),
        )
        dispatcher = _dispatcher(mock_service, "depth-error")

        dispatcher._emit_queue_depth()  # must not raise

        assert _depth("depth-error") == 0

    def test_queries_the_namespaced_queue_for_this_dispatcher(
        self,
        mock_service: MagicMock,
    ) -> None:
        """Depth must come from *this* dispatcher's queue, namespaced."""
        channel = _wire_broker(mock_service, connected=True, message_count=7)
        dispatcher = _dispatcher(mock_service, "depth-scoped")

        dispatcher._emit_queue_depth()

        channel.queue_declare.assert_called_once_with(
            queue="test-ns-JobReady-depth-scoped",
            passive=True,
        )

    def test_depth_updates_on_each_call(self, mock_service: MagicMock) -> None:
        """The gauge tracks the current value, it does not accumulate.

        The broker's answer is changed on the channel already in use rather
        than by rewiring a second connection: the dispatcher keeps one
        connection for its whole life, so a replacement stub would never be
        reached and the second assertion would pass on a stale reading.
        """
        channel = _wire_broker(mock_service, connected=True, message_count=10)
        dispatcher = _dispatcher(mock_service, "depth-updating")
        dispatcher._emit_queue_depth()
        assert _depth("depth-updating") == 10

        channel.queue_declare.return_value = ("q", 3, 0)
        dispatcher._emit_queue_depth()
        assert _depth("depth-updating") == 3

    def test_a_failed_probe_does_not_poison_every_later_one(
        self,
        mock_service: MagicMock,
    ) -> None:
        """After a broker error the next probe reports a real depth again.

        A connection whose reply never arrived may be left part-way through a
        frame, and reusing it would make every subsequent probe fail too --
        the gauge would read 0 for the rest of the process while the queue
        filled up. The failed connection is therefore dropped and the next
        probe opens a fresh one.
        """
        broken = MagicMock()
        broken.connected = True
        broken.channel.side_effect = OSError("timed out reading reply")

        recovered_depth = 12
        healthy_channel = MagicMock()
        healthy_channel.queue_declare.return_value = ("q", recovered_depth, 0)
        healthy = MagicMock()
        healthy.connected = True
        healthy.channel.return_value.__enter__.return_value = healthy_channel

        broker = mock_service._broker_manager
        broker.reset_mock()
        broker.open_private_connection.side_effect = [broken, healthy]
        broker.get_queue_name.side_effect = lambda base: f"test-ns-{base}"

        dispatcher = _dispatcher(mock_service, "depth-recovers")
        dispatcher._emit_queue_depth()
        assert _depth("depth-recovers") == 0

        dispatcher._emit_queue_depth()
        assert _depth("depth-recovers") == recovered_depth

    @pytest.mark.parametrize("count", [0, 1, 1000])
    def test_reports_the_exact_count(
        self,
        mock_service: MagicMock,
        count: int,
    ) -> None:
        _wire_broker(mock_service, connected=True, message_count=count)
        dispatcher = _dispatcher(mock_service, f"depth-exact-{count}")

        dispatcher._emit_queue_depth()

        assert _depth(f"depth-exact-{count}") == count
