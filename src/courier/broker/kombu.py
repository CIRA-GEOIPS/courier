"""Kombu-based message broker: connection functions and MessageBrokerManager."""

import queue as stdlib_queue
import threading
from collections.abc import Callable, Generator
from contextlib import contextmanager, suppress
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import kombu
import kombu.exceptions
from kombu.exceptions import (
    ChannelError,
    KombuError,
    OperationalError,
)
from kombu.exceptions import ConnectionError as KombuConnectionError

from courier.config import ServiceConfig
from courier.constants import FILE_FOUND_EXCHANGE, file_found_queue_for
from courier.errors import FatalBrokerError, TransientBrokerError
from courier.managers.base import ServiceManager
from courier.metrics import (
    BROKER_CONNECTED,
    BROKER_CONNECTIONS,
    BROKER_MESSAGES_DEAD_LETTERED,
    BROKER_MESSAGES_PENDING,
    BROKER_MESSAGES_REDELIVERED,
)
from courier.utils.decorators import log_execution, retry_with_backoff
from courier.utils.logging import get_logger

# Memory transport does not implement broker-level publisher confirms;
# passing ``confirm=True`` to it is a silent no-op.
_MEMORY_TRANSPORT_SCHEMES: frozenset[str] = frozenset({"memory"})

#: Everything a transport can raise while declaring or publishing. Kombu
#: re-exports amqp's ``ChannelError`` and ``ConnectionError``, so classifying
#: a channel error needs no direct dependency on the lower-level library.
_BROKER_ERRORS: tuple[type[BaseException], ...] = (
    KombuError,
    ChannelError,
    KombuConnectionError,
    OSError,
)

#: Header carrying how many times courier has handed one message to a consumer.
#: It travels with the message rather than living in the consumer's memory
#: because the plugin interfaces treat an unhandled exception as fatal and call
#: ``os._exit``: an in-process counter would be reset by the very crash it is
#: meant to be counting, and the message would be retried forever across
#: container restarts.
DELIVERY_ATTEMPT_HEADER = "x-courier-delivery-attempt"

#: Bytes of a retried or parked body written to the log. Enough to identify the
#: message without shipping a whole payload on every retry.
_PARKED_BODY_PREVIEW = 512

#: Position of the consumer count in an AMQP queue.declare-ok reply,
#: which is (queue name, message count, consumer count).
_CONSUMER_COUNT_FIELD = 2

#: AMQP reply codes that retrying the same operation can never fix.
#:
#: Membership here is what makes the classification transport-independent, and
#: that is the point of listing a code rather than relying on its exception
#: class. 405 is a *Recoverable*ChannelError in amqp and 541 an
#: IrrecoverableConnectionError, so the class-based fallbacks disagree about
#: both -- 541 classified as transient on the memory transport and fatal on
#: pyamqp, which meant the unit tier asserted the opposite of production. This
#: set is checked first, so it settles the question the same way everywhere.
_FATAL_REPLY_CODES: frozenset[int] = frozenset({403, 404, 405, 406, 541})

#: Operator-actionable remedy per fatal reply code.
_FATAL_HINTS: dict[int, str] = {
    403: "the broker user lacks configure or write permission for this name",
    404: "the exchange or queue no longer exists; restart courier to redeclare it",
    405: "another connection holds this queue exclusively; stop that client first",
    406: (
        "a queue with this name already exists with different "
        "durable/exclusive/auto_delete/arguments; drain and delete it "
        "(courier queues prune CONFIG --candidate <name> --apply), then restart"
    ),
    541: (
        "the broker refused the operation outright; on RabbitMQ 4.x this is "
        "usually a deprecated feature that is no longer permitted (see the "
        "reply text), which no retry and no courier setting can work around"
    ),
}

_logger = get_logger("module", "broker.kombu", None)


def redact_broker_url(url: str) -> str:
    """Return *url* with any embedded password replaced by ``***``.

    Broker URLs carry credentials in their userinfo section and the default
    log level is DEBUG, so logging one verbatim writes the password to the
    console and — when Loki shipping is enabled — into the log store.
    """
    try:
        parsed = urlsplit(url)
    except ValueError:
        return "<unparseable broker url>"
    if parsed.password is None:
        return url
    userinfo = parsed.username or ""
    host = parsed.hostname or ""
    if parsed.port is not None:
        host = f"{host}:{parsed.port}"
    netloc = f"{userinfo}:***@{host}" if userinfo else f":***@{host}"
    return urlunsplit(
        (parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment),
    )


def _reply_code(exc: BaseException) -> int | None:
    """Return the AMQP reply code carried by *exc*, if any.

    Parameters
    ----------
    exc : BaseException
        Exception raised by a transport.

    Returns
    -------
    int or None
        The reply code, or ``None`` when the exception carries none.
    """
    code = getattr(exc, "reply_code", None) or getattr(exc, "code", None)
    return code if isinstance(code, int) and code > 0 else None


def _error_types(
    conn: "kombu.Connection",
    attr: str,
) -> tuple[type[BaseException], ...]:
    """Return one of the connection's transport-specific error tuples.

    Parameters
    ----------
    conn : kombu.Connection
        Connection whose transport defines the tuple.
    attr : str
        Attribute name, e.g. ``"channel_errors"``.

    Returns
    -------
    tuple[type[BaseException], ...]
        Exception classes, filtered so a stub connection degrades gracefully.
    """
    raw = getattr(conn, attr, None) or ()
    return tuple(t for t in raw if isinstance(t, type))


def _is_fatal_channel_error(conn: "kombu.Connection", exc: BaseException) -> bool:
    """Return whether *exc* is a channel error that retrying cannot fix.

    Parameters
    ----------
    conn : kombu.Connection
        Connection the error came from.
    exc : BaseException
        Exception to classify.

    Returns
    -------
    bool
        ``True`` for an irrecoverable channel error.
    """
    if _reply_code(exc) in _FATAL_REPLY_CODES:
        return True
    return isinstance(exc, _error_types(conn, "channel_errors")) and not isinstance(
        exc,
        _error_types(conn, "recoverable_channel_errors"),
    )


def _is_transient(conn: "kombu.Connection", exc: BaseException) -> bool:
    """Return whether *exc* is worth retrying.

    Parameters
    ----------
    conn : kombu.Connection
        Connection the error came from.
    exc : BaseException
        Exception to classify.

    Returns
    -------
    bool
        ``True`` when a retry could succeed.
    """
    if isinstance(exc, OperationalError):
        return True
    if _is_fatal_channel_error(conn, exc):
        return False
    recoverable = (
        *_error_types(conn, "recoverable_channel_errors"),
        *_error_types(conn, "recoverable_connection_errors"),
        # Genuine socket failures, which every transport can raise.
        ConnectionError,
        TimeoutError,
        OSError,
    )
    return isinstance(exc, recoverable)


def classify_broker_error(
    conn: "kombu.Connection",
    exc: BaseException,
    target_name: str,
    action: str,
) -> Exception | None:
    """Map a transport error onto courier's broker error types.

    Three ordering decisions matter here, each of which was a real defect:

    * the explicit fatal reply codes are checked first, because py-amqp
      classes ``ResourceLocked`` (405) as *recoverable* and retrying it
      forever is not what an operator wants;
    * the fatal-channel check runs before the transient tuples, because on the
      in-memory transport ``recoverable_connection_errors`` falls back to
      including every channel error, which would make a 406 look retryable;
    * the connection-error tuple is deliberately *not* treated as transient,
      because it is the base of both the recoverable and the irrecoverable
      connection errors. Genuine socket failures are still caught by the
      builtin ``(ConnectionError, TimeoutError, OSError)`` arm.

    Parameters
    ----------
    conn : kombu.Connection
        Connection the error came from; supplies the transport's error tuples.
    exc : BaseException
        Exception raised by the transport.
    target_name : str
        Queue or exchange the operation targeted.
    action : str
        Present participle describing the operation, e.g. ``"declaring queue"``.

    Returns
    -------
    Exception or None
        A :class:`TransientBrokerError` or :class:`FatalBrokerError`, or
        ``None`` when *exc* is not a broker error and should propagate as-is.
    """
    if _is_transient(conn, exc):
        return TransientBrokerError(
            f"transient failure while {action} {target_name!r}: {exc}",
        )
    known = (
        KombuError,
        *_error_types(conn, "channel_errors"),
        *_error_types(conn, "connection_errors"),
    )
    if not isinstance(exc, known):
        return None
    hint = _FATAL_HINTS.get(_reply_code(exc) or 0)
    tail = f"; {hint}" if hint else ""
    return FatalBrokerError(
        f"fatal failure while {action} {target_name!r}: {exc}{tail}",
    )


@contextmanager
def broker_error_triage(
    conn: "kombu.Connection",
    target_name: str,
    action: str,
) -> Generator[None, None, None]:
    """Translate transport errors raised inside the block into broker errors.

    Parameters
    ----------
    conn : kombu.Connection
        Connection the operation runs on.
    target_name : str
        Queue or exchange the operation targets.
    action : str
        Present participle describing the operation.

    Yields
    ------
    None
        Control returns to the caller's block.

    Raises
    ------
    TransientBrokerError
        On a retryable failure.
    FatalBrokerError
        On a failure retrying cannot fix, with an operator-actionable hint.
    """
    try:
        yield
    except _BROKER_ERRORS as exc:
        mapped = classify_broker_error(conn, exc, target_name, action)
        if mapped is None:
            raise
        raise mapped from exc


def _normalize_headers(raw_headers: dict | None) -> dict[str, str]:
    """Normalize Kombu message headers for W3C trace context extraction.

    - None → {}
    - bytes → str via UTF-8 decode (drop non-UTF-8)
    - int/float/bool → str via str()
    - Drop dict, list, and non-serializable values
    """
    if raw_headers is None:
        return {}
    normalized: dict[str, str] = {}
    for key, val in raw_headers.items():
        if isinstance(val, str):
            normalized[key] = val
        elif isinstance(val, bytes):
            try:
                normalized[key] = val.decode("utf-8")
            except UnicodeDecodeError:
                continue  # drop non-UTF-8 bytes
        elif isinstance(val, (int, float, bool)):
            normalized[key] = str(val)
    return normalized


# ---------------------------------------------------------------------------
# Pure connection / messaging functions
# ---------------------------------------------------------------------------


def _open_connection(
    url: str,
    max_retries: int = 5,
    read_timeout: float | None = None,
) -> kombu.Connection:
    """Open and return a connected ``kombu.Connection``.

    Unlike ``Connection(url)`` (lazy), explicitly calls ``ensure_connection``
    to open so callers can detect failures immediately.

    Parameters
    ----------
    url : str
        Broker URL (e.g. ``amqp://user:pass@host:5672/``, ``redis://…``).
    max_retries : int, default=5
        Maximum connection retry attempts passed to Kombu's
        ``ensure_connection``.  Set to -1 to retry forever.
    read_timeout : float or None, optional
        Seconds a socket read on this connection may block before raising.
        ``None``, the default, blocks indefinitely, which is what a consumer
        wants.  A caller that only issues short synchronous RPCs should set
        it: without a timeout a reply that never arrives parks the calling
        thread forever, and a thread parked inside a broker call is invisible
        to every health check courier has.  Accepted by the amqp, redis and
        memory transports alike; transports with no socket to time out
        ignore it.

    Returns
    -------
    kombu.Connection
        An open, connected broker connection.

    Raises
    ------
    OperationalError
        If the broker is unreachable.
    """
    options = {"read_timeout": read_timeout} if read_timeout is not None else None
    conn = kombu.Connection(url, transport_options=options)
    conn.ensure_connection(
        max_retries=max_retries,
        interval_start=1,
        interval_step=1,
        interval_max=30,
    )
    return conn


@contextmanager
def broker_connection(
    url: str,
    max_retries: int = 5,
) -> Generator["kombu.Connection", None, None]:
    """Context manager that opens a broker connection and closes it on exit.

    Parameters
    ----------
    url : str
        Broker URL passed directly to :func:`_open_connection`.
    max_retries : int, default=5
        Maximum connection retry attempts.  Set to -1 to retry forever.

    Yields
    ------
    kombu.Connection
        An open connection that is closed when the block exits.
    """
    conn = _open_connection(url, max_retries=max_retries)
    try:
        yield conn
    finally:
        conn.close()


def declare_queue(
    conn: "kombu.Connection",
    name: str,
    **kwargs: Any,
) -> "kombu.Queue":
    """Declare a queue on *conn* and return the bound Queue object.

    Parameters
    ----------
    conn : kombu.Connection
        An open broker connection.
    name : str
        Queue name.
    **kwargs : Any
        Extra keyword arguments forwarded to ``kombu.Queue`` (e.g.
        ``durable=True``, ``exclusive=False``). Passing ``exchange=`` and
        ``routing_key=`` is meaningful: one ``declare()`` then declares the
        exchange, the queue *and* the binding between them.

    Returns
    -------
    kombu.Queue
        A queue object bound to *conn*'s channel and already declared on the
        broker.

    Raises
    ------
    TransientBrokerError
        On a retryable declaration failure.
    FatalBrokerError
        On a failure retrying cannot fix, such as a 406 from redeclaring an
        existing queue with different properties.
    """
    with broker_error_triage(conn, name, "declaring queue"):
        q: kombu.Queue = kombu.Queue(name, channel=conn.channel(), **kwargs)
        q.declare()
    return q


def publish(
    conn: "kombu.Connection",
    queue: "kombu.Queue",
    body: str,
    confirm: bool = False,
    headers: dict | None = None,
) -> None:
    """Publish *body* to *queue* using *conn*.

    Parameters
    ----------
    conn : kombu.Connection
        An open broker connection.
    queue : kombu.Queue
        Target queue (already declared).
    body : str
        Message body string.
    confirm : bool, optional
        When ``True`` and the transport supports publisher confirms (AMQP),
        block until the broker acknowledges the message. Silently ignored on
        transports that lack the concept (e.g. memory). Default ``False``.
    headers : dict or None, optional
        Message headers to attach (used for trace context propagation).

    Raises
    ------
    TransientBrokerError
        On retryable failures (connection drops, channel errors, timeouts).
    FatalBrokerError
        On non-retryable failures (access refused, message too large,
        permission denied).
    """
    scheme = (conn.transport_cls or "").split("+", 1)[0].lower()
    use_confirm = confirm and scheme not in _MEMORY_TRANSPORT_SCHEMES
    with broker_error_triage(conn, queue.name, "publishing to"):
        producer_cls = kombu.Producer
        with producer_cls(conn) as producer:
            if use_confirm:
                channel = producer.channel
                confirm_select = getattr(channel, "confirm_select", None)
                if confirm_select is not None:
                    confirm_select()
            producer.publish(
                body,
                routing_key=queue.name,
                exchange="",
                declare=[queue],
                headers=headers or {},
            )
            # Best-effort tracking: may drift on requeue or restart.
            BROKER_MESSAGES_PENDING.labels(queue_name=queue.name).inc()


def declare_dead_letter_queue(
    conn: "kombu.Connection",
    name: str,
) -> "kombu.Queue":
    """Declare the durable queue that parks messages a consumer gave up on.

    Bound to nothing and reachable only through the default exchange, so the
    only thing that ever writes to it is :func:`redeliver_or_park`. It is
    declared when a consumer subscribes rather than when the first message is
    parked: a name that is too long, or a broker user without ``configure``
    permission, is then a startup failure instead of a failure at exactly the
    moment there is a poison message with nowhere to put it.

    Parameters
    ----------
    conn : kombu.Connection
        An open broker connection.
    name : str
        Dead-letter queue name, from
        :func:`courier.constants.dead_letter_queue_for`.

    Returns
    -------
    kombu.Queue
        The declared queue.

    Raises
    ------
    TransientBrokerError
        On a retryable declaration failure.
    FatalBrokerError
        On a failure retrying cannot fix.
    """
    with broker_error_triage(conn, name, "declaring dead-letter queue"):
        q = kombu.Queue(
            name,
            durable=True,
            exclusive=False,
            auto_delete=False,
            channel=conn.channel(),
        )
        q.declare()
    return q


def delivery_attempt(headers: dict[str, str]) -> int:
    """Return how many times *headers*' message has already been delivered.

    Parameters
    ----------
    headers : dict[str, str]
        Normalized headers as yielded by :func:`messages`.

    Returns
    -------
    int
        1 for a message courier has not handed to a consumer before, which
        covers both a fresh publish and every message already sitting on a
        queue when this version is deployed. A header that is missing,
        unparseable or below 1 is treated the same way: the count is a bound
        on retries, so an unreadable one must not be able to exhaust it.
    """
    try:
        attempt = int(headers.get(DELIVERY_ATTEMPT_HEADER, "1"))
    except (TypeError, ValueError):
        return 1
    return max(attempt, 1)


def redeliver_or_park(  # noqa: PLR0913, PLR0917 -- one delivery's worth of state
    conn: "kombu.Connection",
    queue: "kombu.Queue",
    dead_letter: "kombu.Queue",
    body: str,
    headers: dict[str, str],
    max_redeliveries: int,
) -> bool:
    """Republish *body* for another attempt, or park it once attempts run out.

    The caller acknowledges the original delivery afterwards, so a message is
    never both queued for retry and still outstanding. Publishing before
    acknowledging means a crash in between duplicates the message rather than
    losing it, and the dispatcher already skips a job identifier it has just
    seen.

    A retry is republished to the *tail* of its own queue rather than rejected
    back to the head. That is the part that unblocks the pipeline: everything
    queued behind an unprocessable message is delivered while that message
    waits its next turn, instead of never being reached at all.

    Parameters
    ----------
    conn : kombu.Connection
        An open broker connection.
    queue : kombu.Queue
        The queue the message came from, and the target of a retry. Published
        through the default exchange, so a queue bound to the file-found
        fanout is retried on its own without every other builder receiving a
        copy.
    dead_letter : kombu.Queue
        Where the message goes once it has used up its attempts.
    body : str
        The message body, unchanged.
    headers : dict[str, str]
        The delivered message's headers. Carried across so the retry stays
        part of the same trace.
    max_redeliveries : int
        Further attempts allowed after the first. 0 parks immediately.

    Returns
    -------
    bool
        ``True`` if the message was parked, ``False`` if it was requeued for
        another attempt.

    Raises
    ------
    TransientBrokerError
        If the republish fails and is worth retrying.
    FatalBrokerError
        If the republish fails in a way retrying cannot fix. The caller has
        not acknowledged yet, so the message is redelivered rather than lost.
    """
    attempt = delivery_attempt(headers)
    if attempt > max_redeliveries:
        publish(conn, dead_letter, body, confirm=True, headers=dict(headers))
        BROKER_MESSAGES_DEAD_LETTERED.labels(queue_name=queue.name).inc()
        _logger.error(
            "Parked a message on %r after %d attempt(s) on %r: it is no longer "
            "blocking the queue and no longer being retried, and needs an "
            "operator. First %d bytes: %r",
            dead_letter.name,
            attempt,
            queue.name,
            _PARKED_BODY_PREVIEW,
            body[:_PARKED_BODY_PREVIEW],
        )
        return True

    retry_headers = {**headers, DELIVERY_ATTEMPT_HEADER: str(attempt + 1)}
    publish(conn, queue, body, confirm=True, headers=retry_headers)
    BROKER_MESSAGES_REDELIVERED.labels(queue_name=queue.name).inc()
    _logger.warning(
        "Requeued a message on %r for attempt %d of %d after the consumer "
        "raised; it goes behind the current backlog. First %d bytes: %r",
        queue.name,
        attempt + 1,
        max_redeliveries + 1,
        _PARKED_BODY_PREVIEW,
        body[:_PARKED_BODY_PREVIEW],
    )
    return False


def messages(
    conn: "kombu.Connection",
    queue: "kombu.Queue",
    stop_event: threading.Event | None = None,
    prefetch_count: int | None = None,
) -> Generator[
    tuple[str, Callable[[], None], Callable[[], None], dict[str, str]],
    None,
    None,
]:
    """Yield ``(body, ack, reject)`` tuples from *queue* until *stop_event* is set.

    Drains the broker connection in 0.5-second windows so that a
    ``stop_event`` check can interrupt consuming promptly.  Socket timeouts
    from an idle broker are silently swallowed.

    Parameters
    ----------
    conn : kombu.Connection
        An open broker connection.
    queue : kombu.Queue
        Queue to consume from (must already be declared).
    stop_event : threading.Event or None, optional
        When set, the generator exits after delivering any already-buffered
        messages.  Pass ``None`` to run until the caller closes the generator.
    prefetch_count : int or None, optional
        Maximum unacknowledged messages the broker may push (AMQP
        ``basic.qos``).  ``None`` sends no quality-of-service frame at all,
        which is the broker default of *unlimited* -- so the first consumer to
        attach to a queue holding a backlog would have the whole backlog
        pushed to it at once, unacknowledged.

    Yields
    ------
    tuple[str, Callable[[], None], Callable[[], None], dict[str, str]]
        ``(body, ack, reject, headers)`` where *body* is the decoded message string,
        *ack* acknowledges successful processing, *reject* re-queues the
        message for retry, and *headers* are the normalized message headers.

    Notes
    -----
    ``reject`` requeues (``requeue=True``) and counts nothing. It returns the
    message to the head of the queue unchanged, which is right for a consumer
    that is shutting down and wrong for one that just failed on the message --
    see :meth:`courier.service.Service._relay`, which uses ``reject`` only on
    ``GeneratorExit`` and routes a genuine failure through
    :func:`redeliver_or_park` instead.

    Prefetch bounds broker-side memory and how many messages are redelivered
    if a consumer dies mid-drain.  It does not make a drain faster: the caller
    acknowledges one message at a time, so processing stays serial.  A larger
    window also lengthens shutdown, because the buffered messages are drained
    before ``stop_event`` is rechecked.
    """
    buffer: stdlib_queue.Queue[tuple[Any, kombu.Message]] = stdlib_queue.Queue()

    def _on_message(body: Any, message: kombu.Message) -> None:
        buffer.put((body, message))

    with kombu.Consumer(
        conn,
        queues=[queue],
        callbacks=[_on_message],
        prefetch_count=prefetch_count,
    ):
        while stop_event is None or not stop_event.is_set():
            with suppress(TimeoutError):
                conn.drain_events(timeout=0.5)
            while not buffer.empty():
                raw_body, msg = buffer.get_nowait()
                decoded = (
                    raw_body if isinstance(raw_body, str) else raw_body.decode("utf-8")
                )

                def _reject(msg: Any = msg) -> None:
                    msg.reject(requeue=True)

                headers = _normalize_headers(msg.headers)
                # Best-effort tracking: may drift on requeue or restart.
                BROKER_MESSAGES_PENDING.labels(queue_name=queue.name).dec()
                yield decoded, msg.ack, _reject, headers


def declare_fanout_exchange(
    conn: "kombu.Connection",
    name: str,
) -> "kombu.Exchange":
    """Declare and return a durable fanout Exchange on *conn*.

    Parameters
    ----------
    conn : kombu.Connection
        An open broker connection.
    name : str
        Namespaced exchange name.

    Returns
    -------
    kombu.Exchange
        The declared exchange.

    Raises
    ------
    TransientBrokerError
        On a retryable declaration failure.
    FatalBrokerError
        On a failure retrying cannot fix.
    """
    with broker_error_triage(conn, name, "declaring fanout exchange"):
        exchange = kombu.Exchange(
            name,
            type="fanout",
            durable=True,
            channel=conn.channel(),
        )
        exchange.declare()
    return exchange


def publish_fanout(
    conn: "kombu.Connection",
    exchange: "kombu.Exchange",
    body: str,
    confirm: bool = False,
    headers: dict | None = None,
) -> None:
    """Publish *body* to a fanout *exchange* using *conn*.

    Same error-handling strategy as :func:`publish`.

    Parameters
    ----------
    conn : kombu.Connection
        An open broker connection.
    exchange : kombu.Exchange
        Fanout exchange to publish to.
    body : str
        Message body.
    confirm : bool, optional
        Wait for a publisher confirm where the transport supports it.
    headers : dict or None, optional
        Message headers to attach (used for trace context propagation).

    Notes
    -----
    The pending-message gauge is *not* incremented here. A fanout publish
    lands in every bound queue, and the consumer side decrements against the
    queue it read from, so counting one exchange-labelled increment here left
    the two halves as different series -- one only ever rising, the other only
    ever falling. The caller, which knows the bound queues, does the counting.
    """
    scheme = (conn.transport_cls or "").split("+", 1)[0].lower()
    use_confirm = confirm and scheme not in _MEMORY_TRANSPORT_SCHEMES
    with broker_error_triage(conn, exchange.name, "publishing to"):
        producer_cls = kombu.Producer
        with producer_cls(conn) as producer:
            if use_confirm:
                channel = producer.channel
                confirm_select = getattr(channel, "confirm_select", None)
                if confirm_select is not None:
                    confirm_select()
            producer.publish(
                body,
                exchange=exchange,
                routing_key="",
                declare=[exchange],
                headers=headers or {},
            )


def declare_bound_queue(
    conn: "kombu.Connection",
    exchange: "kombu.Exchange",
    name: str,
) -> "kombu.Queue":
    """Declare durable queue *name* and bind it to fanout *exchange*.

    Declared ``durable``, non-exclusive and non-auto-delete, so the queue and
    its binding outlive the consumer's connection. That is the whole point:
    with the previous exclusive queue the broker deleted the subscription the
    moment a job builder disconnected, and every file published while it was
    down was discarded with no error anywhere (issue #44).

    Every replica of one builder identifier shares this queue, so replicas are
    competing consumers rather than each receiving a copy.

    No queue arguments are set, and none may be added. A durable queue's
    arguments are part of what the broker compares on redeclaration, so
    introducing one -- ``x-dead-letter-exchange`` being the obvious
    candidate -- would answer 406 on every deployment that has already
    declared this queue, and courier would refuse to start until each was
    drained and deleted. Poison messages are therefore bounded from the
    consumer side instead, by :func:`redeliver_or_park`, which needs no
    argument on this queue and so needs no migration.

    Parameters
    ----------
    conn : kombu.Connection
        An open broker connection.
    exchange : kombu.Exchange
        The fanout exchange to bind to.
    name : str
        Namespaced queue name, from
        :func:`courier.constants.file_found_queue_for`. It never starts with
        ``amq.``, which RabbitMQ reserves.

    Returns
    -------
    kombu.Queue
        The declared, bound queue.

    Raises
    ------
    TransientBrokerError
        On a retryable declaration failure.
    FatalBrokerError
        On a failure retrying cannot fix, such as a 406 from an existing queue
        with different properties.
    """
    with broker_error_triage(conn, name, "declaring file-found queue"):
        q = kombu.Queue(
            name,
            exchange=exchange,
            routing_key="",
            durable=True,
            exclusive=False,
            auto_delete=False,
            channel=conn.channel(),
        )
        q.declare()
    return q


# ---------------------------------------------------------------------------
# MessageBrokerManager
# ---------------------------------------------------------------------------


class MessageBrokerManager(ServiceManager):
    """Manages broker connections and queue registry for the service.

    Handles broker connection lifecycle with retry logic, provides context
    managers for independent connections, and maintains queue configuration
    for connection establishment.  Transport-agnostic: the backend is
    determined by the URL scheme in ``config.broker_url`` (``amqp://``,
    ``redis://``, ``sqs://``, etc.).

    Parameters
    ----------
    config : ServiceConfig
        Service configuration containing the broker URL and retry settings.
    stop_event : threading.Event or None, optional
        Event that is set when a shutdown signal is received.

    Attributes
    ----------
    _config : ServiceConfig
        Service configuration.
    _connection : kombu.Connection or None
        Active broker connection.
    _queues : dict[str, dict[str, Any]]
        Registered queue configurations.
    _created_queues : set[str]
        Set of queues that have been declared on the broker.
    _namespace : str
        Service namespace for queue naming.

    Methods
    -------
    get_connection_context()
        Provide an independent broker connection context.
    get_queue_name(base_name)
        Generate full queue name with namespace prefix.
    add_queue(queue_name, **queue_config)
        Register queue configuration.

    Examples
    --------
    >>> config = ServiceConfig()
    >>> manager = MessageBrokerManager(config)
    >>> manager.is_healthy()
    False
    >>> manager.add_queue("test_queue", durable=True)
    'default-test_queue'
    >>> len(manager._queues)
    1
    """

    def __init__(
        self,
        config: ServiceConfig,
        stop_event: threading.Event | None = None,
    ) -> None:
        """Initialize the broker manager with configuration.

        Parameters
        ----------
        config : ServiceConfig
            Service configuration.
        stop_event : threading.Event or None, optional
            Event that is set when a shutdown signal is received.
        """
        self._config = config
        self._stop_event = stop_event
        self._logger = get_logger("manager", "MessageBrokerManager", config)
        self._connection: kombu.Connection | None = None
        self._queues: dict[str, dict[str, Any]] = {}
        self._created_queues: set[str] = set()
        self._namespace = config.namespace

        self._establish_connection = retry_with_backoff(
            max_retries=self._config.broker_max_retries,
            exceptions=(OperationalError,),
            stop_event=self._stop_event,
        )(self._establish_connection_impl)

    def _establish_connection_impl(self) -> kombu.Connection:
        """Establish a new broker connection.

        Returns
        -------
        kombu.Connection
            An open broker connection.

        Raises
        ------
        OperationalError
            If the connection attempt fails.
        """
        self._logger.debug(
            f"Attempting to connect to broker at "
            f"{redact_broker_url(self._config.broker_url)}",
        )
        try:
            conn = _open_connection(
                self._config.broker_url,
                max_retries=self._config.broker_max_retries,
            )
            _logger.debug("Successfully connected to broker")
            BROKER_CONNECTIONS.labels(status="success").inc()
            BROKER_CONNECTED.set(1)
        except OperationalError:
            BROKER_CONNECTIONS.labels(status="failure").inc()
            BROKER_CONNECTED.set(0)
            self._logger.exception("Failed to connect to broker")
            raise
        else:
            return conn

    @log_execution
    def start(self) -> None:
        """Initialize broker connection if not already healthy."""
        if not self.is_healthy():
            self._connection = self._establish_connection()

    def stop(self) -> None:
        """Close the broker connection safely and reset connection state."""
        if self._connection and self._connection.connected:
            try:
                self._connection.close()
                self._logger.info("Broker connection closed")
            except OSError as e:
                self._logger.warning(f"Error closing broker connection: {e}")

        self._connection = None
        BROKER_CONNECTED.set(0)

    def is_healthy(self) -> bool:
        """Check whether the broker connection is active.

        Returns
        -------
        bool
            True if a connection exists and is open, False otherwise.

        Examples
        --------
        >>> config = ServiceConfig()
        >>> manager = MessageBrokerManager(config)
        >>> manager.is_healthy()
        False
        """
        return self._connection is not None and self._connection.connected

    @contextmanager
    def get_connection_context(
        self,
    ) -> Generator["kombu.Connection", None, None]:
        """Provide an independent broker connection for isolated operations.

        Opens a temporary connection separate from the main connection,
        declares all registered queues on it, and ensures cleanup regardless
        of operation success or failure.

        Yields
        ------
        kombu.Connection
            An open connection with all registered queues declared.

        Raises
        ------
        OperationalError
            If unable to establish a connection.
        """
        with broker_connection(
            self._config.broker_url,
            max_retries=self._config.broker_max_retries,
        ) as conn:
            for queue_name, cfg in list(self._queues.items()):
                if queue_name not in self._created_queues:
                    self._logger.debug(
                        f"Declaring queue {queue_name} with config {cfg}",
                    )
                    declare_queue(conn, queue_name, **cfg)
                    self._created_queues.add(queue_name)
            yield conn

    def open_private_connection(
        self,
        read_timeout: float | None = None,
    ) -> "kombu.Connection":
        """Open a connection for the exclusive use of one thread.

        ``self._connection`` is the service's single shared connection, and
        kombu/py-amqp connections are not thread-safe: two threads issuing a
        synchronous RPC on one connection each wait on the same socket, and
        whichever reads first consumes the other's reply frame.  Both then
        block in ``read_frame`` for a frame that has already been taken, with
        no timeout and no exception — so no ``except`` clause anywhere sees
        it, every health check keeps reporting healthy, and any message the
        parked thread was handling stays unacknowledged.  With
        ``broker_prefetch_count`` at its default of 1 that one unacknowledged
        message is enough to stop the broker delivering to that consumer ever
        again.

        A caller that needs to talk to the broker from its own thread takes a
        connection of its own from here and closes it when that thread ends,
        rather than borrowing the shared one.  Unlike
        :meth:`get_connection_context` this does not close the connection for
        you and declares nothing on it: it is for a caller that reuses one
        connection across many short calls, where a fresh connect per call
        would put a TCP and AMQP handshake on a hot path.

        Parameters
        ----------
        read_timeout : float or None, optional
            Seconds a socket read may block before raising.  Pass a value
            whenever the connection is used only for short synchronous
            RPCs, so a stalled broker surfaces as an error the caller can
            handle instead of parking the thread.

        Returns
        -------
        kombu.Connection
            An open connection the caller owns and must close.

        Raises
        ------
        OperationalError
            If the broker is unreachable.
        """
        return _open_connection(
            self._config.broker_url,
            max_retries=self._config.broker_max_retries,
            read_timeout=read_timeout,
        )

    def get_queue_name(self, base_name: str) -> str:
        """Generate a full queue name with the service namespace prefix.

        Parameters
        ----------
        base_name : str
            Base name of the queue without namespace.

        Returns
        -------
        str
            Full queue name with namespace prefix.

        Examples
        --------
        >>> config = ServiceConfig()
        >>> manager = MessageBrokerManager(config)
        >>> manager.get_queue_name("my_queue")
        'default-my_queue'
        """
        return f"{self._namespace}-{base_name}"

    def add_queue(self, queue_name: str, **queue_config: Any) -> str:
        """Register a queue for automatic declaration on connections.

        Parameters
        ----------
        queue_name : str
            Base name of the queue (without namespace prefix).
        **queue_config : Any
            Keyword arguments forwarded to :func:`declare_queue`.

        Returns
        -------
        str
            Full queue name with namespace prefix.

        Examples
        --------
        >>> config = ServiceConfig()
        >>> manager = MessageBrokerManager(config)
        >>> full_name = manager.add_queue("my_queue", durable=True, exclusive=False)
        >>> full_name in manager._queues
        True
        >>> manager._queues[full_name]["durable"]
        True
        """
        new_queue_name = self.get_queue_name(queue_name)
        if new_queue_name not in self._queues:
            self._queues[new_queue_name] = queue_config
        return new_queue_name

    def consumer_count(self, queue_name: str) -> int:
        """Return how many consumers are attached to *queue_name*.

        A passive declare reports the queue's consumer count. It reports
        nothing about durability or exclusivity, so this is used only to
        answer "am I alone on this queue?".

        Parameters
        ----------
        queue_name : str
            Fully namespaced queue name.

        Returns
        -------
        int
            Attached consumers, or 0 when the queue does not exist yet or the
            transport does not report it.
        """
        try:
            with self.get_connection_context() as conn, conn.channel() as channel:
                result = kombu.Queue(queue_name, channel=channel).queue_declare(
                    passive=True,
                )
        except Exception:  # unknown queue, or a transport that cannot answer
            return 0
        count = getattr(result, "consumer_count", None)
        if count is None and isinstance(result, (tuple, list)):
            # queue.declare-ok is (name, message_count, consumer_count).
            count = (
                result[_CONSUMER_COUNT_FIELD]
                if len(
                    result,
                )
                > _CONSUMER_COUNT_FIELD
                else 0
            )
        return int(count or 0)

    def _file_found_queue_config(self) -> dict[str, Any]:
        """Return the kwargs that make a queue durable and fanout-bound.

        Returns
        -------
        dict[str, Any]
            Keyword arguments for ``kombu.Queue``. Declaring with an
            ``exchange`` and ``routing_key`` creates the exchange, the queue
            and the binding in one call.
        """
        return {
            "durable": True,
            "exclusive": False,
            "auto_delete": False,
            "routing_key": "",
            "exchange": kombu.Exchange(
                self.get_queue_name(FILE_FOUND_EXCHANGE),
                type="fanout",
                durable=True,
            ),
        }

    def add_file_found_queue(self, builder_identifier: str) -> str:
        """Register a job builder's durable file-found queue.

        One helper feeds both producer-side predeclaration and the consumer,
        so :meth:`add_queue`'s first-registration-wins behaviour cannot end up
        storing two different sets of arguments for the same queue.

        Parameters
        ----------
        builder_identifier : str
            The job builder's ``spec.run[*].identifier`` value.

        Returns
        -------
        str
            The namespaced queue name.
        """
        return self.add_queue(
            file_found_queue_for(builder_identifier),
            **self._file_found_queue_config(),
        )
