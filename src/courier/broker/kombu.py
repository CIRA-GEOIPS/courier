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
    KombuError,
    OperationalError,
)

from courier.config import ServiceConfig
from courier.constants import (
    FILE_FOUND_EXCHANGE,
    file_found_queue_for,
    namespaced_queue_name,
)
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

#: Seconds a publisher confirm may block before the publish raises. Without a
#: bound, a broker that accepts a message and then never answers parks the
#: publishing thread forever, and a thread parked inside a broker call is
#: invisible to every health check courier has.
_CONFIRM_TIMEOUT = 30.0

#: Header carrying how many times courier has handed one message to a consumer.
#: It travels with the message rather than living in the consumer's memory
#: because a plugin thread that raises is torn down and restarted by the
#: plugin manager: an in-process counter would be reset by the very failure it
#: is meant to be counting, and the message would be retried forever across
#: restarts.
DELIVERY_ATTEMPT_HEADER = "x-courier-delivery-attempt"

#: Header recording why a message was parked on a dead-letter queue without
#: being retried, set by :meth:`courier.service.Service.park_message`. A message
#: parked for running out of attempts carries :data:`DELIVERY_ATTEMPT_HEADER`
#: instead.
PARK_REASON_HEADER = "x-courier-park-reason"

#: Bytes of a retried or parked body written to the log. Enough to identify the
#: message without shipping a whole payload on every retry.
_PARKED_BODY_PREVIEW = 512

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
    except Exception as exc:
        # Deliberately not narrowed. Which classes a transport raises is the
        # transport's business, and ``classify_broker_error`` already asks the
        # live connection -- a fixed tuple here could only disagree with it.
        # It did: ``redis.exceptions.ConnectionError`` inherits ``RedisError``,
        # so a dropped redis connection never reached the classifier, nothing
        # mapped it to ``TransientBrokerError``, and the watcher's reconnect
        # arm never fired. Anything the classifier does not recognise is
        # re-raised untouched, so widening the catch changes no behaviour for
        # a class that was already handled.
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

    Notes
    -----
    ``confirm_publish`` is set on the *connection*, which is the only place
    py-amqp honours it: ``Channel.__init__`` rebinds ``basic_publish`` to the
    waiting ``basic_publish_confirm`` if and only if
    ``self.connection.confirm_publish`` is true.  Calling ``confirm_select()``
    on the channel instead puts the *broker* into confirm mode but leaves the
    client not waiting, so the returning Ack/Nack frames dispatch into an
    empty handler set and are dropped -- which is what courier did, while
    :func:`redeliver_or_park` acknowledged the original on the strength of a
    confirm that had never been waited for.  Transports without publisher
    confirms ignore the option.
    """
    options: dict[str, object] = {"confirm_publish": True}
    if read_timeout is not None:
        options["read_timeout"] = read_timeout
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
    action: str = "declaring queue",
    **kwargs: Any,
) -> "kombu.Queue":
    """Declare a queue on *conn* and return the bound Queue object.

    Parameters
    ----------
    conn : kombu.Connection
        An open broker connection.
    name : str
        Queue name.
    action : str, optional
        Present participle naming what is being declared, used only in the
        error message a failure raises.
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

    Notes
    -----
    ``durable=True``, ``exclusive=False`` and ``auto_delete=False`` are
    ``kombu.Queue`` class defaults, so the two queue families courier declares
    need no kwargs of their own beyond the file-found queue's ``exchange``.
    Both used to have a hand-written wrapper spelling those defaults out; the
    file-found one was a second copy of what
    :meth:`MessageBrokerManager._file_found_queue_config` already returns and
    ``get_connection_context`` already declares, which is exactly the drift
    that answers 406.

    **No queue arguments are set on the file-found queue, and none may be
    added.** A durable queue's arguments are part of what the broker compares
    on redeclaration, so introducing one -- ``x-dead-letter-exchange`` being
    the obvious candidate -- would answer 406 on every deployment that has
    already declared it, and courier would refuse to start until each was
    drained and deleted. Poison messages are bounded from the consumer side
    instead, by :func:`redeliver_or_park`, which needs no argument on the
    queue and so needs no migration.

    The queue is durable, non-exclusive and non-auto-delete so that it and its
    binding outlive the consumer's connection. That is the whole point: with
    the previous exclusive queue the broker deleted the subscription the
    moment a job builder disconnected, and every file published while it was
    down was discarded with no error anywhere (issue #44). Every replica of one
    builder identifier shares the queue, so replicas are competing consumers
    rather than each receiving a copy.

    The dead-letter queue is bound to nothing and reachable only through the
    default exchange, so the only things that ever write to it are
    :func:`redeliver_or_park` and :meth:`courier.service.Service.park_message`.
    It is declared when a consumer subscribes rather than when the first
    message is parked: a name that is too long, or a broker user without
    ``configure`` permission, is then a startup failure instead of a failure at
    exactly the moment there is a poison message with nowhere to put it.
    """
    # Bound to the connection's default channel rather than a fresh
    # ``conn.channel()``: publishers reuse one connection per thread for the
    # life of the service, and a new channel per declare was never closed, so
    # every publish leaked one until the broker's channel limit ended them.
    with broker_error_triage(conn, name, action):
        q: kombu.Queue = kombu.Queue(name, channel=conn.default_channel, **kwargs)
        q.declare()
    return q


def publish(
    conn: "kombu.Connection",
    queue: "kombu.Queue",
    body: str,
    headers: dict | None = None,
) -> None:
    """Publish *body* to *queue* using *conn*, waiting for the broker's ack.

    Every publish is confirmed. There is no per-call opt-out because every
    caller that mattered already asked for one, and a publish courier does not
    wait on is a publish it cannot safely acknowledge the original of -- see
    :func:`_open_connection` for why the confirm has to be armed on the
    connection rather than the channel.

    Parameters
    ----------
    conn : kombu.Connection
        An open broker connection.
    queue : kombu.Queue
        Target queue (already declared).
    body : str
        Message body string.
    headers : dict or None, optional
        Message headers to attach (used for trace context propagation).

    Raises
    ------
    TransientBrokerError
        On retryable failures (connection drops, channel errors, timeouts),
        including a confirm that does not arrive within
        :data:`_CONFIRM_TIMEOUT`.
    FatalBrokerError
        On non-retryable failures (access refused, message too large,
        permission denied).
    """
    with (
        broker_error_triage(conn, queue.name, "publishing to"),
        kombu.Producer(conn) as producer,
    ):
        producer.publish(
            body,
            routing_key=queue.name,
            exchange="",
            declare=[queue],
            headers=headers or {},
            confirm_timeout=_CONFIRM_TIMEOUT,
        )
        # Best-effort tracking: may drift on requeue or restart.
        BROKER_MESSAGES_PENDING.labels(queue_name=queue.name).inc()


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
        publish(conn, dead_letter, body, headers=dict(headers))
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
    publish(conn, queue, body, headers=retry_headers)
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
            channel=conn.default_channel,
        )
        exchange.declare()
    return exchange


def publish_fanout(
    conn: "kombu.Connection",
    exchange: "kombu.Exchange",
    body: str,
    headers: dict | None = None,
) -> None:
    """Publish *body* to a fanout *exchange* using *conn*.

    Same confirm and error-handling strategy as :func:`publish`.

    Parameters
    ----------
    conn : kombu.Connection
        An open broker connection.
    exchange : kombu.Exchange
        Fanout exchange to publish to.
    body : str
        Message body.
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
    with (
        broker_error_triage(conn, exchange.name, "publishing to"),
        kombu.Producer(conn) as producer,
    ):
        producer.publish(
            body,
            exchange=exchange,
            routing_key="",
            declare=[exchange],
            headers=headers or {},
            confirm_timeout=_CONFIRM_TIMEOUT,
        )


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
    _publisher_connections : list[kombu.Connection]
        Every thread's cached publisher connection; see
        :meth:`publisher_connection`.
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
        self._queues: dict[str, dict[str, Any]] = {}
        self._created_queues: set[str] = set()
        self._namespace = config.namespace
        # One publisher connection per thread, kept for the life of the
        # service; see publisher_connection(). The list lets stop() close
        # every thread's connection, whichever thread opened it.
        self._publisher_local = threading.local()
        self._publisher_connections: list[kombu.Connection] = []
        self._publisher_lock = threading.Lock()

        self._open_publisher_connection = retry_with_backoff(
            max_retries=self._config.broker_max_retries,
            exceptions=(OperationalError,),
            stop_event=self._stop_event,
        )(self._open_publisher_connection_impl)

    def _open_publisher_connection_impl(self) -> kombu.Connection:
        """Open a new broker connection, recording the outcome in metrics.

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
            self._logger.debug("Successfully connected to broker")
            BROKER_CONNECTIONS.labels(status="success").inc()
        except OperationalError:
            BROKER_CONNECTIONS.labels(status="failure").inc()
            self._logger.exception("Failed to connect to broker")
            raise
        else:
            return conn

    @log_execution
    def start(self) -> None:
        """Open the calling thread's publisher connection, with retries.

        Startup fails here, with the broker's error, rather than at the
        first publish. This is the only place a connection is opened under
        :func:`retry_with_backoff`; publish-path reopens get the single
        retry loop inside :func:`_open_connection`, since the thread doing
        them may be holding an unacknowledged message.

        ``Service.start`` calls this from the main thread, which afterwards
        only runs the heartbeat loop and never publishes. That connection
        stays cached and idle, and between publishes from other threads it
        is what keeps :meth:`is_healthy` true.
        """
        conn = getattr(self._publisher_local, "connection", None)
        if conn is not None and conn.connected:
            return
        if conn is not None:
            self._discard_publisher_connection(conn)
        self._adopt_publisher_connection(self._open_publisher_connection())

    def stop(self) -> None:
        """Close every broker connection this manager owns and reset state."""
        self._close_publisher_connections()
        self._logger.info("Broker connections closed")

    def _close_publisher_connections(self) -> None:
        """Close the cached publisher connection of every thread."""
        with self._publisher_lock:
            connections, self._publisher_connections = (
                self._publisher_connections,
                [],
            )
            self._update_connected_gauge_locked()
        for conn in connections:
            with suppress(Exception):
                conn.close()
        # Each thread's cached reference is now closed; the next publish from
        # any thread reopens. Only this thread's slot can be cleared from here.
        self._publisher_local.connection = None

    def is_healthy(self) -> bool:
        """Return whether any thread holds an open publisher connection.

        The connections asked are the ones threads publish on, plus the one
        :meth:`start` opened on the service's main thread, which is idle and
        normally the one answering. ``connected`` reports the client's view;
        a drop the broker has not yet signalled shows up at the next publish,
        which closes that connection and reopens it.

        Returns
        -------
        bool
            True if at least one publisher connection is open.

        Examples
        --------
        >>> config = ServiceConfig()
        >>> manager = MessageBrokerManager(config)
        >>> manager.is_healthy()
        False
        """
        with self._publisher_lock:
            connections = list(self._publisher_connections)
        return any(conn.connected for conn in connections)

    @contextmanager
    def get_connection_context(
        self,
    ) -> Generator["kombu.Connection", None, None]:
        """Provide an independent broker connection for isolated operations.

        Opens a temporary connection separate from the publisher connections,
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
            self._declare_registered_queues(conn)
            yield conn

    @contextmanager
    def publisher_connection(self) -> Generator["kombu.Connection", None, None]:
        """Yield the calling thread's long-lived publisher connection.

        :meth:`get_connection_context` opens a connection per call, which
        put a TCP, TLS and AMQP handshake on every publish: one per file
        found, per job emitted and per execution log. This keeps one
        connection per thread instead. Per thread because kombu connections
        are not thread-safe (see :meth:`open_private_connection`); for the
        life of the service because a publisher's own thread lives that long.

        Registered queues are declared on the connection the first time it is
        used, as :meth:`get_connection_context` does. If the block raises,
        the connection is closed and dropped, so the next publish from this
        thread (a retry included) starts from a fresh connection rather than
        reusing a transport left mid-frame.

        A dropped connection is noticed on write, not in advance:
        ``connected`` reflects the client's state, so a socket the broker has
        closed is handed out as live, the publish on it fails, and the
        caller's retry gets a fresh connection. Each thread therefore pays one
        failed publish per broker outage, up to the confirm timeout.

        Yields
        ------
        kombu.Connection
            An open connection used by no other thread.

        Raises
        ------
        OperationalError
            If a connection has to be opened and the broker is unreachable.
        """
        conn = self._thread_publisher_connection()
        try:
            yield conn
        except BaseException:
            self._discard_publisher_connection(conn)
            raise

    def _thread_publisher_connection(self) -> "kombu.Connection":
        """Return this thread's cached connection, opening one if needed."""
        conn = getattr(self._publisher_local, "connection", None)
        if conn is not None and conn.connected:
            return conn
        if conn is not None:
            self._discard_publisher_connection(conn)
        # One retry layer only: _open_connection already retries inside
        # ensure_connection, and this thread may be holding a message.
        conn = self._open_publisher_connection_impl()
        self._adopt_publisher_connection(conn)
        return conn

    def _adopt_publisher_connection(self, conn: "kombu.Connection") -> None:
        """Cache *conn* as this thread's publisher connection."""
        with self._publisher_lock:
            self._publisher_connections.append(conn)
            self._update_connected_gauge_locked()
        self._publisher_local.connection = conn
        self._declare_registered_queues(conn)

    def _update_connected_gauge_locked(self) -> None:
        """Set ``BROKER_CONNECTED`` from the connection list. Lock held."""
        BROKER_CONNECTED.set(1 if self._publisher_connections else 0)

    def _discard_publisher_connection(self, conn: "kombu.Connection") -> None:
        """Close *conn* and forget it, so the thread reopens on next use."""
        with self._publisher_lock:
            with suppress(ValueError):
                self._publisher_connections.remove(conn)
            self._update_connected_gauge_locked()
        if getattr(self._publisher_local, "connection", None) is conn:
            self._publisher_local.connection = None
        with suppress(Exception):
            conn.close()

    def _declare_registered_queues(self, conn: "kombu.Connection") -> None:
        """Declare every registered queue not yet declared by this process."""
        for queue_name, cfg in list(self._queues.items()):
            if queue_name not in self._created_queues:
                self._logger.debug(
                    f"Declaring queue {queue_name} with config {cfg}",
                )
                declare_queue(conn, queue_name, **cfg)
                self._created_queues.add(queue_name)

    def open_private_connection(
        self,
        read_timeout: float | None = None,
    ) -> "kombu.Connection":
        """Open a connection for the exclusive use of one thread.

        Publisher connections are cached one per thread, never shared, because
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
        rather than borrowing another thread's publisher connection.  Unlike
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

        Raises
        ------
        InvalidIdentifierError
            If the namespaced name exceeds the AMQP limit. Enforced here
            because this is the one place a namespaced name is built, so a
            long ``--namespace`` or ``SERVICE_NAMESPACE`` cannot slip past on
            a path that skipped a separate preflight check.
        """
        return namespaced_queue_name(self._namespace, base_name)

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
        # Every queue_declare kombu ships -- amqp, virtual and qpid -- returns
        # a queue_declare_ok_t, so the attribute is always there. A transport
        # that answers with something else is caught by the except above.
        return int(getattr(result, "consumer_count", 0) or 0)

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
