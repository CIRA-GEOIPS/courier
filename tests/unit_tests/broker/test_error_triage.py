"""Classification of transport errors into transient and fatal broker errors.

Before this existed, a 406 ``PRECONDITION_FAILED`` -- the answer a broker gives
when a queue already exists with different properties -- matched none of the
except clauses in the broker layer. It escaped as a raw amqp exception into the
plugin's catch-all and took the whole process down with a traceback and no
remedy.

These tests deliberately construct the amqp exceptions directly rather than
provoking them from a live broker, so the classification is pinned in the tier
mutation testing actually scores.
"""

from __future__ import annotations

import amqp.exceptions
import kombu
import pytest

from courier.broker.kombu import (
    broker_error_triage,
    classify_broker_error,
    declare_fanout_exchange,
    declare_queue,
)
from courier.errors import FatalBrokerError, TransientBrokerError


@pytest.fixture
def amqp_conn() -> kombu.Connection:
    """Return an unconnected AMQP connection, for its error tuples only."""
    return kombu.Connection("pyamqp://guest:guest@localhost//")


@pytest.fixture
def memory_conn() -> kombu.Connection:
    """Return an in-memory connection, whose error tuples are far broader."""
    return kombu.Connection("memory://")


@pytest.mark.parametrize(
    ("exc", "fragment"),
    [
        (amqp.exceptions.AccessRefused("denied"), "permission"),
        (amqp.exceptions.NotFound("gone"), "no longer exists"),
        (amqp.exceptions.ResourceLocked("locked"), "exclusively"),
        (amqp.exceptions.PreconditionFailed("mismatch"), "already exists"),
        (amqp.exceptions.InternalError("deprecated feature"), "refused"),
    ],
)
def test_irrecoverable_reply_codes_are_fatal_and_carry_a_remedy(
    amqp_conn: kombu.Connection,
    exc: Exception,
    fragment: str,
) -> None:
    """403, 404, 405, 406 and 541 are fatal, and say what to do about it.

    405 and 541 are the load-bearing cases, for opposite reasons. py-amqp
    classes ``ResourceLocked`` as *recoverable*, so classifying by tuple
    membership alone would retry a queue held exclusively by another client
    forever. ``InternalError`` is an irrecoverable *connection* error rather
    than a channel error, so before 541 was listed it reached fatal only by
    accident of class membership, and carried no remedy at all.
    """
    mapped = classify_broker_error(amqp_conn, exc, "ns-q", "declaring queue")

    assert isinstance(mapped, FatalBrokerError)
    assert "ns-q" in str(mapped)
    assert fragment in str(mapped)


@pytest.mark.parametrize(
    "exc",
    [
        amqp.exceptions.ContentTooLarge("too big"),
        amqp.exceptions.ConnectionForced("bounced"),
        OSError("socket went away"),
        TimeoutError("timed out"),
    ],
)
def test_recoverable_errors_are_transient(
    amqp_conn: kombu.Connection,
    exc: Exception,
) -> None:
    """Errors a retry could survive are classified transient."""
    mapped = classify_broker_error(amqp_conn, exc, "ns-q", "publishing to")

    assert isinstance(mapped, TransientBrokerError)


def test_irrecoverable_connection_errors_are_fatal(
    amqp_conn: kombu.Connection,
) -> None:
    """An irrecoverable connection error is not retried.

    The connection-error tuple is the base of both the recoverable and the
    irrecoverable families, so treating the whole tuple as transient would
    quietly retry conditions that can never succeed.
    """
    mapped = classify_broker_error(
        amqp_conn,
        amqp.exceptions.NotAllowed("vhost refused"),
        "ns-q",
        "declaring queue",
    )

    assert isinstance(mapped, FatalBrokerError)


def test_channel_error_on_the_memory_transport_is_fatal(
    memory_conn: kombu.Connection,
) -> None:
    """A bare channel error is fatal even where the error tuples are loose.

    On the in-memory transport ``recoverable_connection_errors`` falls back to
    every connection *and* channel error, so checking the transient tuples
    first would classify a genuine precondition failure as retryable -- on the
    very transport the unit tier runs against.
    """
    mapped = classify_broker_error(
        memory_conn,
        kombu.exceptions.ChannelError("inequivalent arg 'durable'"),
        "ns-q",
        "declaring queue",
    )

    assert isinstance(mapped, FatalBrokerError)


def test_an_unrelated_exception_is_left_alone(memory_conn: kombu.Connection) -> None:
    """A non-broker exception is not reclassified, and propagates unchanged."""
    assert classify_broker_error(memory_conn, KeyError("x"), "q", "declaring") is None

    with pytest.raises(KeyError):
        with broker_error_triage(memory_conn, "q", "declaring queue"):
            raise KeyError("x")


def test_declare_paths_translate_a_precondition_failure(
    memory_conn: kombu.Connection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Declaring a queue or an exchange surfaces a 406 as a fatal error."""

    def _raise(*_args: object, **_kwargs: object) -> None:
        raise amqp.exceptions.PreconditionFailed("inequivalent arg 'durable'")

    monkeypatch.setattr(kombu.Queue, "declare", _raise)
    with pytest.raises(FatalBrokerError, match="ns-q"):
        declare_queue(memory_conn, "ns-q", durable=True)

    monkeypatch.setattr(kombu.Exchange, "declare", _raise)
    with pytest.raises(FatalBrokerError, match="ns-ex"):
        declare_fanout_exchange(memory_conn, "ns-ex")


def test_a_541_is_fatal_on_the_memory_transport_too(
    memory_conn: kombu.Connection,
) -> None:
    """The unit tier must not disagree with production about this one.

    ``InternalError`` is an irrecoverable *connection* error. The memory
    transport has no ``recoverable_connection_errors`` of its own, so kombu
    falls back to ``connection_errors + channel_errors`` -- which contains it,
    and used to classify a 541 as retryable here while pyamqp called it fatal.
    A test written against ``memory://`` therefore asserted the opposite of
    what a deployment would see. Listing 541 in ``_FATAL_REPLY_CODES`` settles
    it before either fallback is consulted.
    """
    mapped = classify_broker_error(
        memory_conn,
        amqp.exceptions.InternalError("deprecated feature"),
        "ns-q",
        "declaring queue",
    )

    assert isinstance(mapped, FatalBrokerError)
    assert "ns-q" in str(mapped)
    assert "refused" in str(mapped)
