"""Kombu-based message broker: connection functions and MessageBrokerManager."""

import queue as stdlib_queue
import threading
from collections.abc import Callable, Generator
from contextlib import contextmanager, suppress
from typing import Any

import kombu
import kombu.exceptions
from kombu.exceptions import (
    KombuError,
    OperationalError,
)

from courier.config import ServiceConfig
from courier.errors import FatalBrokerError, TransientBrokerError
from courier.managers.base import ServiceManager
from courier.metrics import BROKER_CONNECTED, BROKER_CONNECTIONS
from courier.utils.decorators import log_execution, retry_with_backoff
from courier.utils.logging import get_logger

# Memory transport does not implement broker-level publisher confirms;
# passing ``confirm=True`` to it is a silent no-op.
_MEMORY_TRANSPORT_SCHEMES: frozenset[str] = frozenset({"memory"})

_logger = get_logger("module", "broker.kombu", None)


def _normalize_publish_error(
    exc: "kombu.exceptions.KombuError",
    target_name: str,
) -> "Exception":
    """Classify a Kombu publish/declare error as transient or fatal.

    Parameters
    ----------
    exc : kombu.exceptions.KombuError
        The exception raised by Kombu.
    target_name : str
        Name of the queue or exchange for error messages.

    Returns
    -------
    Exception
        :class:`TransientBrokerError` for recoverable failures,
        :class:`FatalBrokerError` otherwise.
    """
    if isinstance(exc, OperationalError):
        return TransientBrokerError(
            f"transient publish failure to {target_name!r}: {exc}"
        )
    if isinstance(exc, (ConnectionError, TimeoutError, OSError)):
        return TransientBrokerError(
            f"transient publish failure to {target_name!r}: {exc}"
        )
    return FatalBrokerError(
        f"fatal publish failure to {target_name!r}: {exc}"
    )


# ---------------------------------------------------------------------------
# Pure connection / messaging functions
# ---------------------------------------------------------------------------


def _open_connection(url: str) -> kombu.Connection:
    """Return an established kombu Connection for *url*.

    kombu connections are lazy by default; this function forces the connection
    to open so callers can detect failures immediately.

    Parameters
    ----------
    url : str
        Broker URL (e.g. ``amqp://user:pass@host:5672/``, ``redis://…``).

    Returns
    -------
    kombu.Connection
        An open, connected broker connection.

    Raises
    ------
    OperationalError
        If the broker is unreachable.
    """
    conn = kombu.Connection(url)
    conn.ensure_connection(max_retries=1, interval_start=0, interval_step=0)
    return conn


@contextmanager
def broker_connection(url: str) -> Generator["kombu.Connection", None, None]:
    """Context manager that opens a broker connection and closes it on exit.

    Parameters
    ----------
    url : str
        Broker URL passed directly to :func:`_open_connection`.

    Yields
    ------
    kombu.Connection
        An open connection that is closed when the block exits.
    """
    conn = _open_connection(url)
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
        ``durable=True``, ``exclusive=False``).

    Returns
    -------
    kombu.Queue
        A queue object bound to *conn*'s channel and already declared on the
        broker.
    """
    q: kombu.Queue = kombu.Queue(name, channel=conn.channel(), **kwargs)
    q.declare()
    return q


def publish(
    conn: "kombu.Connection",
    queue: "kombu.Queue",
    body: str,
    confirm: bool = False,
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
    try:
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
            )
    except KombuError as exc:
        raise _normalize_publish_error(exc, queue.name) from exc


def messages(
    conn: "kombu.Connection",
    queue: "kombu.Queue",
    stop_event: threading.Event | None = None,
) -> Generator[tuple[str, Callable[[], None], Callable[[], None]], None, None]:
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

    Yields
    ------
    tuple[str, Callable[[], None], Callable[[], None]]
        ``(body, ack, reject)`` where *body* is the decoded message string,
        *ack* acknowledges successful processing, and *reject* re-queues the
        message for retry.

    Notes
    -----
    ``reject`` always requeues (``requeue=True``).
    """
    buffer: stdlib_queue.Queue[tuple[Any, kombu.Message]] = stdlib_queue.Queue()

    def _on_message(body: Any, message: kombu.Message) -> None:
        buffer.put((body, message))

    with kombu.Consumer(conn, queues=[queue], callbacks=[_on_message]):
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

                yield decoded, msg.ack, _reject


def declare_fanout_exchange(
    conn: "kombu.Connection",
    name: str,
) -> "kombu.Exchange":
    """Declare and return a durable fanout Exchange on *conn*."""
    try:
        exchange = kombu.Exchange(name, type="fanout", durable=True, channel=conn.channel())
        exchange.declare()
        return exchange
    except OperationalError as exc:
        raise TransientBrokerError(
            f"transient failure declaring fanout exchange {name!r}: {exc}",
        ) from exc
    except (ConnectionError, TimeoutError, OSError) as exc:
        raise TransientBrokerError(
            f"transient failure declaring fanout exchange {name!r}: {exc}",
        ) from exc
    except KombuError as exc:
        raise FatalBrokerError(
            f"fatal failure declaring fanout exchange {name!r}: {exc}",
        ) from exc


def publish_fanout(
    conn: "kombu.Connection",
    exchange: "kombu.Exchange",
    body: str,
    confirm: bool = False,
) -> None:
    """Publish *body* to a fanout *exchange* using *conn*.

    Same error-handling strategy as :func:`publish`.
    """
    scheme = (conn.transport_cls or "").split("+", 1)[0].lower()
    use_confirm = confirm and scheme not in _MEMORY_TRANSPORT_SCHEMES
    try:
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
            )
    except KombuError as exc:
        raise _normalize_publish_error(exc, exchange.name) from exc


def declare_fanout_queue(
    conn: "kombu.Connection",
    exchange: "kombu.Exchange",
) -> "kombu.Queue":
    """Declare and return an anonymous exclusive queue bound to *exchange*.

    The queue name is server-generated (empty string). ``exclusive=True``
    ensures the queue is auto-deleted when the consumer disconnects, and
    the fanout binding guarantees every consumer's queue receives a copy
    of each published message.
    """
    # NOTE: Opens a new channel (conn.channel()) separate from the exchange's
    # channel.  In service.py:consume(), kombu.Consumer opens yet another
    # channel internally.  All channels share the same connection and are
    # closed when conn is closed — no leak.
    try:
        q = kombu.Queue("", exchange=exchange, exclusive=True, channel=conn.channel())
        q.declare()
        return q
    except OperationalError as exc:
        raise TransientBrokerError(
            f"transient failure declaring fanout queue: {exc}",
        ) from exc
    except (ConnectionError, TimeoutError, OSError) as exc:
        raise TransientBrokerError(
            f"transient failure declaring fanout queue: {exc}",
        ) from exc
    except KombuError as exc:
        raise FatalBrokerError(
            f"fatal failure declaring fanout queue: {exc}",
        ) from exc


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
            max_retries=max(self._config.broker_max_retries, 1),
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
            f"Attempting to connect to broker at {self._config.broker_url}",
        )
        try:
            conn = _open_connection(self._config.broker_url)
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
        with broker_connection(self._config.broker_url) as conn:
            for queue_name, cfg in list(self._queues.items()):
                if queue_name not in self._created_queues:
                    self._logger.debug(
                        f"Declaring queue {queue_name} with config {cfg}",
                    )
                    declare_queue(conn, queue_name, **cfg)
                    self._created_queues.add(queue_name)
            yield conn

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
