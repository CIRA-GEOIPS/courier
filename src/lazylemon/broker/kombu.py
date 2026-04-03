"""Kombu-based message broker: connection functions and MessageBrokerManager."""

import queue as stdlib_queue
import threading
from collections.abc import Callable, Generator
from contextlib import contextmanager, suppress
from typing import Any

import kombu  # type: ignore[import-untyped]
import prometheus_client
from kombu.exceptions import OperationalError  # type: ignore[import-untyped]

from lazylemon.config import ServiceConfig
from lazylemon.managers.base import ServiceManager
from lazylemon.utils.decorators import log_execution, retry_with_backoff
from lazylemon.utils.logging import get_logger

_logger = get_logger("module", "broker.kombu", None)


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
    """
    with kombu.Producer(conn) as producer:
        producer.publish(
            body,
            routing_key=queue.name,
            exchange="",
            declare=[queue],
        )


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
            exceptions=(OperationalError,),
            stop_event=self._stop_event,
        )(self._establish_connection_impl)

        # Prometheus metrics
        self._broker_connections_total = prometheus_client.Counter(
            "broker_connections_total",
            "Total number of broker connection attempts",
            ["status"],
        )
        self._broker_messages_sent_total = prometheus_client.Counter(
            "broker_messages_sent_total",
            "Total number of messages sent to broker queues",
            ["queue_name"],
        )
        self._broker_messages_received_total = prometheus_client.Counter(
            "broker_messages_received_total",
            "Total number of messages received from broker queues",
            ["queue_name"],
        )

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
            self._broker_connections_total.labels(status="success").inc()
        except OperationalError:
            self._broker_connections_total.labels(status="failure").inc()
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
            except Exception as e:
                self._logger.warning(f"Error closing broker connection: {e}")

        self._connection = None

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
