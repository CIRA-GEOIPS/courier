"""Publisher confirms are armed where py-amqp actually honours them.

``Channel.__init__`` rebinds ``basic_publish`` to the waiting
``basic_publish_confirm`` if and only if ``self.connection.confirm_publish`` is
true. Calling ``confirm_select()`` on the channel instead puts the *broker*
into confirm mode but leaves the client not waiting, so returning Ack/Nack
frames dispatch into an empty handler set and are dropped -- while
:func:`courier.broker.kombu.redeliver_or_park` acknowledges the original
delivery on the strength of that confirm.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from courier.broker.kombu import _CONFIRM_TIMEOUT, _open_connection


def _transport_options(**kwargs: object) -> dict:
    """Return the transport options `_open_connection` builds."""
    with patch("kombu.Connection") as conn_cls:
        conn_cls.return_value = MagicMock()
        _open_connection("memory://", **kwargs)  # type: ignore[arg-type]
    return conn_cls.call_args.kwargs["transport_options"]


def test_connection_arms_publisher_confirms() -> None:
    """Every connection waits for the broker's ack, with no opt-out."""
    assert _transport_options()["confirm_publish"] is True


def test_read_timeout_does_not_displace_the_confirm_flag() -> None:
    """A caller asking for a read timeout still gets confirms."""
    options = _transport_options(read_timeout=5.0)
    assert options["read_timeout"] == 5.0
    assert options["confirm_publish"] is True


def test_confirm_wait_is_bounded() -> None:
    """A broker that accepts and then goes quiet must not park the thread."""
    assert _CONFIRM_TIMEOUT > 0
