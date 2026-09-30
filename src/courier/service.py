"""Service orchestrator: coordinates plugins, broker, and managers."""

from __future__ import annotations

import time
from contextlib import suppress
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

from courier.broker.kombu import (
    PARK_REASON_HEADER,
    MessageBrokerManager,
    declare_fanout_exchange,
    declare_queue,
    publish,
    publish_fanout,
    redeliver_or_park,
)
from courier.broker.kombu import messages as broker_messages
from courier.config import ServiceConfig
from courier.constants import (
    DISPATCHER_QUEUE,
    FILE_FOUND_EXCHANGE,
    dead_letter_queue_for,
    file_found_queue_for,
    job_ready_queue_for,
)
from courier.errors import ConfigurationError, DiscoveryError
from courier.managers.plugin_manager import PluginManager
from courier.managers.prometheus_manager import PrometheusManager
from courier.routing import TargetResolver, build_default_resolver
from courier.tracing import (
    extract_context,
    get_tracer,
    init_tracing,
    inject_trace_headers,
    shutdown_tracing,
)

if TYPE_CHECKING:
    import threading
    from collections.abc import (
        Callable,
        Generator,
        Iterable,
        Iterator,
        Mapping,
        Sequence,
    )

    from courier.interfaces.dispatchers import Dispatcher
    from courier.interfaces.payloads import Payload
    from courier.interfaces.plugin_protocol import ServicePlugin
    from courier.managers.base import ServiceManager
from courier.metrics import (
    BROKER_MESSAGES_DEAD_LETTERED,
    BROKER_MESSAGES_PENDING,
    BROKER_MESSAGES_RECEIVED,
    BROKER_MESSAGES_SENT,
    SERVICE_HEALTH,
    SERVICE_UPTIME,
)
from courier.utils.decorators import log_execution
from courier.utils.logging import get_logger
from courier.utils.signals import SignalHandler

#: Bytes of a parked body echoed into the log line that reports it.
_PARKED_BODY_PREVIEW = 512

#: Characters of a park reason kept in the message header. The full reason is
#: logged; the header only has to identify it, and AMQP headers share a frame
#: with the rest of the message properties, so an unbounded one (a long
#: validation error, say) could make the park itself fail.
_PARK_REASON_HEADER_MAX = 1024


@dataclass(frozen=True)
class PipelineTopology:
    """Plugin names for every step in the service YAML, whatever ``--only`` runs.

    A split deployment runs a job builder and the dispatcher it targets in
    different processes, so neither has the other registered. This carries
    what preflight needs to check their payload/dispatcher compatibility from
    both ends anyway.

    Attributes
    ----------
    dispatcher_plugins : Mapping[str, str]
        Dispatcher identifier to dispatcher plugin name, for every dispatcher.
    builder_payloads : Mapping[str, str]
        Job builder identifier to the plugin name of its nested payload, for
        every builder whose ``payload`` block names one.
    builder_targets : Mapping[str, tuple[str, ...]]
        Job builder identifier to its declared targets, for every builder.
        An empty tuple means the builder relies on implicit routing.
    """

    dispatcher_plugins: Mapping[str, str] = field(default_factory=dict)
    builder_payloads: Mapping[str, str] = field(default_factory=dict)
    builder_targets: Mapping[str, tuple[str, ...]] = field(default_factory=dict)

    def builders_targeting(
        self,
        dispatcher_id: str,
        *,
        allow_implicit_target: bool,
    ) -> list[str]:
        """Return the job builders whose jobs reach *dispatcher_id*.

        Parameters
        ----------
        dispatcher_id : str
            Dispatcher identifier.
        allow_implicit_target : bool
            Whether a builder with no declared targets is auto-wired to the
            sole dispatcher, mirroring preflight's routing rule.

        Returns
        -------
        list[str]
            Builder identifiers, sorted.
        """
        sole = (
            next(iter(self.dispatcher_plugins))
            if allow_implicit_target and len(self.dispatcher_plugins) == 1
            else None
        )
        return sorted(
            builder_id
            for builder_id, declared in self.builder_targets.items()
            if dispatcher_id in declared or (not declared and sole == dispatcher_id)
        )


class Service:
    """Service class with plugin support.

    Coordinates startup, health monitoring, heartbeat loop, and graceful shutdown
    of all service components including plugins. Uses dependency injection for
    manager instances and provides centralized service lifecycle management.

    Parameters
    ----------
    config : ServiceConfig or None, optional
        Service configuration. If None, creates default ServiceConfig instance.

    Attributes
    ----------
    namespace : str
        Service namespace for resource isolation.

    Methods
    -------
    emit(queue, message)
        Publish a message to a message broker queue.
    consume(queue)
        Yield messages from a message broker queue.
    park_message(queue, message, reason)
        Move a message that can never be processed to its dead-letter queue.
    register_plugin(plugin, config)
        Register a plugin with the service.
    start()
        Start service with complete lifecycle management.

    Examples
    --------
    >>> config = ServiceConfig()
    >>> service = Service(config)
    >>> service.config.heartbeat_interval
    30
    >>> len(service._managers)
    3
    """

    def __init__(self, config: ServiceConfig | None = None) -> None:
        """Initialize service with configuration and managers.

        Parameters
        ----------
        config : ServiceConfig or None, optional
            Service configuration. If None, uses default ServiceConfig.
        """
        self._config = config or ServiceConfig()
        self._logger = get_logger("service", self._config.service_id, self._config)
        self._signal_handler = SignalHandler()

        self._prometheus_manager = PrometheusManager(self._config)
        self._broker_manager = MessageBrokerManager(
            self._config,
            stop_event=self._signal_handler.stop_event,
        )
        self._plugin_manager = PluginManager(self._config, self)

        self._managers: list[ServiceManager] = [
            self._prometheus_manager,
            self._broker_manager,
            self._plugin_manager,
        ]
        self.namespace = self._config.namespace
        self._start_time = time.time()

        self._service_uptime_metric = SERVICE_UPTIME
        self._service_health_metric = SERVICE_HEALTH

        self._dispatcher_identifiers: frozenset[str] = frozenset()
        self._builder_identifiers: frozenset[str] = frozenset()
        self._builder_targets: dict[str, tuple[str, ...]] = {}
        self._allow_implicit_target: bool = True
        self._target_resolver: TargetResolver = build_default_resolver(())
        self._topology = PipelineTopology()

        init_tracing(self._config)

    @property
    def target_resolver(self) -> TargetResolver:
        """Return the service's :class:`TargetResolver`.

        Injected into job builders so they map dispatcher identifiers to
        broker queue names consistently across the service and CLI.
        """
        return self._target_resolver

    @property
    def config(self) -> ServiceConfig:
        """Return the service configuration."""
        return self._config

    @log_execution
    def emit(self, queue: str, message: str) -> None:
        """Publish a message to a message broker queue.

        Blocks until the broker confirms the message; see
        :func:`courier.broker.kombu.publish`.

        Parameters
        ----------
        queue : str
            Name of the queue to publish to.
        message : str
            Message content to publish.

        Raises
        ------
        TransientBrokerError
            On retryable publish failures.
        FatalBrokerError
            On non-retryable publish failures.
        """
        # --- Fan-out: FILE_FOUND uses a fanout exchange so every builder
        # --- receives every file notification.
        if queue == FILE_FOUND_EXCHANGE:
            exchange_name = self._broker_manager.get_queue_name(queue)
            with self._broker_manager.get_connection_context() as conn:
                exchange = declare_fanout_exchange(conn, exchange_name)
                tracer = get_tracer(__name__)
                with tracer.start_as_current_span(
                    "broker.publish",
                    attributes={
                        "messaging.system": "amqp",
                        "messaging.destination": exchange_name,
                        "messaging.destination_kind": "fanout",
                        "messaging.message_envelope_size": len(message),
                    },
                ):
                    w3c_headers = inject_trace_headers()
                    publish_fanout(
                        conn,
                        exchange,
                        message,
                        headers=w3c_headers,
                    )
                # Counted per bound queue, matching the label each consumer
                # decrements. Labelling the increment with the exchange put the
                # two halves on different series.
                for bound in self._file_found_queue_names():
                    BROKER_MESSAGES_PENDING.labels(queue_name=bound).inc()
                BROKER_MESSAGES_SENT.labels(queue_name=exchange_name).inc()
            return
        # --- Direct queue path
        queue_name = self._broker_manager.add_queue(
            queue,
            durable=True,
            exclusive=False,
        )
        with self._broker_manager.get_connection_context() as conn:
            self._logger.debug(f"Emitting message to queue '{queue_name}': {message}")
            q = declare_queue(conn, queue_name, durable=True)
            tracer = get_tracer(__name__)
            with tracer.start_as_current_span(
                "broker.publish",
                attributes={
                    "messaging.system": "amqp",
                    "messaging.destination": queue_name,
                    "messaging.message_envelope_size": len(message),
                },
            ):
                w3c_headers = inject_trace_headers()
                publish(conn, q, message, headers=w3c_headers)
            BROKER_MESSAGES_SENT.labels(queue_name=queue_name).inc()

    def _file_found_queue_names(self) -> tuple[str, ...]:
        """Return the namespaced file-found queue name of every job builder.

        Returns
        -------
        tuple[str, ...]
            One name per builder identifier known to this service.
        """
        return tuple(
            self._broker_manager.get_queue_name(file_found_queue_for(ident))
            for ident in sorted(self._builder_identifiers)
        )

    def consume(
        self,
        queue: str,
        stop_event: threading.Event | None = None,
        on_subscribed: Callable[[], None] | None = None,
        *,
        subscriber: str | None = None,
    ) -> Generator[tuple[str, Any], None, None]:
        """Yield messages from a message broker queue.

        Parameters
        ----------
        queue : str
            The name of the queue to consume messages from.
        stop_event : threading.Event or None, optional
            Event that, once set, ends the consume loop after any
            already-buffered messages are delivered.  Plugins pass their own
            per-instance event so a single-plugin restart terminates only that
            consumer.  ``None`` falls back to the service-wide shutdown event
            set by :class:`~courier.utils.signals.SignalHandler`, so a consumer
            that forgets to supply one still exits on SIGTERM/SIGINT rather
            than wedging interpreter shutdown.
        on_subscribed : Callable[[], None] or None, optional
            Invoked once the queue is declared and bound, before the first
            message is read.  :class:`PluginManager` uses it to start consumers
            ahead of producers.  The durable file-found queue is predeclared
            during preflight, so this only affects how promptly the first files
            move.
        subscriber : str or None, optional
            Identifier of the job builder consuming the file-found exchange.
            **Required** on that path: it names the durable queue
            ``FilesFound-<subscriber>``.  Ignored for direct queues.

        Yields
        ------
        tuple[str, Any]
            ``(body, parent_ctx)`` where *body* is the decoded message content
            and *parent_ctx* is the extracted trace context (or None).

        Raises
        ------
        ConfigurationError
            If the file-found exchange is consumed without a *subscriber*.

        Notes
        -----
        A message is acknowledged only after the ``for`` body returns.
        Abandoning the loop while holding one, by ``break`` or by raising,
        leaves it unacknowledged, which counts as a failed attempt: the message
        is requeued behind the backlog, and parked on the dead-letter queue
        after ``broker_max_redeliveries`` attempts.  Setting *stop_event* first
        marks the exit as a shutdown, and the message is returned untouched.
        Breaking may also requeue anything the broker pre-fetched but has not
        yet yielded.  For one-shot consumption, use a separate thread with a
        timeout (see ``concurrent.futures``).

        A missing *subscriber* is reported when ``consume`` is called, not on
        the first ``next()``.
        """
        effective_stop = (
            stop_event if stop_event is not None else self._signal_handler.stop_event
        )
        if queue == FILE_FOUND_EXCHANGE:
            if subscriber is None:
                raise ConfigurationError(
                    "Service.consume(FILE_FOUND_EXCHANGE) requires "
                    "subscriber=<job builder identifier>: it names the durable "
                    "queue FilesFound-<subscriber>. There is no anonymous "
                    "fallback, because a per-connection queue is deleted when "
                    "the consumer disconnects and loses every file published "
                    "while it is away.",
                )
            exchange_name = self._broker_manager.get_queue_name(FILE_FOUND_EXCHANGE)
            queue_name = self._broker_manager.add_file_found_queue(subscriber)

            def declare_bound(conn: Any) -> Any:
                # Redeclared on this connection too. The manager declares a
                # registered queue only on the first connection it opens, so a
                # queue deleted after that is never redeclared. durable,
                # non-exclusive and non-auto-delete are kombu.Queue defaults;
                # respelling them here once let this drift from
                # MessageBrokerManager._file_found_queue_config().
                return declare_queue(
                    conn,
                    queue_name,
                    action="declaring file-found queue",
                    exchange=declare_fanout_exchange(conn, exchange_name),
                )

            return self._consume(
                declare_bound,
                queue_name,
                {
                    "messaging.system": "amqp",
                    "messaging.destination": exchange_name,
                    "messaging.destination_kind": "fanout",
                    "messaging.rabbitmq.destination.queue": queue_name,
                },
                effective_stop,
                on_subscribed,
            )
        direct_name = self._broker_manager.add_queue(
            queue,
            durable=True,
            exclusive=False,
        )
        return self._consume(
            lambda conn: declare_queue(conn, direct_name),
            direct_name,
            {
                "messaging.system": "amqp",
                "messaging.destination": direct_name,
            },
            effective_stop,
            on_subscribed,
        )

    def _consume(
        self,
        declare: Callable[[Any], Any],
        queue_name: str,
        span_attributes: dict[str, str],
        stop_event: threading.Event,
        on_subscribed: Callable[[], None] | None,
    ) -> Generator[tuple[str, Any], None, None]:
        """Open a connection, declare the topology, and relay its messages.

        One generator for both queue families. They differ only in what
        *declare* does and in the span attributes.

        Parameters
        ----------
        declare : Callable[[Any], Any]
            Declares the queue on the open connection and returns it. Runs
            inside the connection context, so a declaration failure is raised
            before *on_subscribed* claims the consumer is attached.
        queue_name : str
            Namespaced queue name, for logging, metric labels and the
            dead-letter name.
        span_attributes : dict[str, str]
            Attributes for the receive span.
        stop_event : threading.Event
            Ends the loop once set.
        on_subscribed : Callable[[], None] or None
            Called once the queue is declared and bound.

        Yields
        ------
        tuple[str, Any]
            ``(body, parent_ctx)`` per message.
        """
        with self._broker_manager.get_connection_context() as conn:
            queue = declare(conn)
            dead_letter = declare_queue(
                conn,
                dead_letter_queue_for(queue_name),
                action="declaring dead-letter queue",
            )
            self._logger.info(
                f"Consuming from {queue_name!r} "
                f"(prefetch={self._config.broker_prefetch_count}, "
                f"max_redeliveries={self._config.broker_max_redeliveries})",
            )
            if on_subscribed is not None:
                on_subscribed()
            yield from self._relay(
                conn,
                queue,
                dead_letter,
                stop_event,
                queue_name,
                span_attributes,
            )

    def _relay(  # noqa: PLR0913, PLR0917 (one consumer's declared topology)
        self,
        conn: Any,
        queue: Any,
        dead_letter: Any,
        stop_event: threading.Event,
        queue_name: str,
        span_attributes: dict[str, str],
    ) -> Generator[tuple[str, Any], None, None]:
        """Yield decoded messages, acknowledging each once the caller returns.

        Parameters
        ----------
        conn : Any
            Open broker connection.
        queue : Any
            Declared queue to consume.
        dead_letter : Any
            Declared queue that parks messages whose attempts are spent.
        stop_event : threading.Event
            Ends the loop once set.
        queue_name : str
            Namespaced queue name, used for logging and metric labels.
        span_attributes : dict[str, str]
            Attributes for the receive span.

        Yields
        ------
        tuple[str, Any]
            ``(body, parent_ctx)`` per message.

        Notes
        -----
        A message the caller could not get past is republished behind the
        current backlog with its attempt count incremented, and parked on the
        dead-letter queue once the count passes ``broker_max_redeliveries``;
        see :func:`courier.broker.kombu.redeliver_or_park`. Rejecting with
        ``requeue=True`` put the message back at the head, and with the shipped
        prefetch of 1 the same message was handed to the same consumer again
        immediately.

        An exception raised by the caller inside the ``for`` body is not thrown
        into this generator. Python closes the generator, so the failure
        arrives here as ``GeneratorExit`` and never reaches the
        ``except Exception`` clause; that clause covers only a failure between
        the ``yield`` and the ``ack``.

        ``GeneratorExit`` is therefore ambiguous: the caller gave up on this
        message, or the consumer is shutting down with the message untried.
        *stop_event* separates them. During shutdown the message is rejected
        unchanged and its attempt count left alone, so a rolling restart does
        not spend a retry on every in-flight message.
        """
        for body, ack, reject, headers in broker_messages(
            conn,
            queue,
            stop_event=stop_event,
            prefetch_count=self._config.broker_prefetch_count,
        ):
            try:
                self._logger.debug(
                    f"Received message from queue '{queue_name}': {body}",
                )
                BROKER_MESSAGES_RECEIVED.labels(queue_name=queue_name).inc()
                parent_ctx = extract_context(headers)
                tracer = get_tracer(__name__)
                with tracer.start_as_current_span(
                    "broker.receive",
                    context=parent_ctx,
                    attributes=span_attributes,
                ):
                    yield body, parent_ctx
                    ack()
            except GeneratorExit:
                if stop_event.is_set():
                    reject()
                else:
                    self._fail_message(
                        conn,
                        queue,
                        dead_letter,
                        body,
                        headers,
                        ack,
                        reject,
                    )
                raise
            except Exception:
                self._fail_message(
                    conn,
                    queue,
                    dead_letter,
                    body,
                    headers,
                    ack,
                    reject,
                )
                raise

    def _fail_message(  # noqa: PLR0913, PLR0917 (one delivery's worth of state)
        self,
        conn: Any,
        queue: Any,
        dead_letter: Any,
        body: str,
        headers: dict[str, str],
        ack: Callable[[], None],
        reject: Callable[[], None],
    ) -> None:
        """Get a message the caller could not handle out of the way of the rest.

        Parameters
        ----------
        conn : Any
            Open broker connection.
        queue : Any
            The queue the message came from.
        dead_letter : Any
            Where the message goes once its attempts are spent.
        body : str
            The message body.
        headers : dict[str, str]
            The delivered message's headers, carrying the attempt count.
        ack : Callable[[], None]
            Acknowledges the original delivery.
        reject : Callable[[], None]
            Returns the original delivery to the queue unchanged.

        Notes
        -----
        Acknowledging is safe only once the republish is confirmed, so the
        republish happens first. If the republish fails the delivery is
        rejected instead, leaving the message on the queue.
        """
        try:
            redeliver_or_park(
                conn,
                queue,
                dead_letter,
                body,
                headers,
                self._config.broker_max_redeliveries,
            )
        except Exception:
            self._logger.exception(
                "Could not requeue or park a failed message from %r; "
                "returning it to the queue unchanged, which leaves it able to "
                "block the messages behind it until the broker recovers",
                getattr(queue, "name", queue),
            )
            with suppress(Exception):
                reject()
            return
        ack()

    def park_message(
        self,
        queue: str,
        message: str,
        reason: str,
        *,
        subscriber: str | None = None,
    ) -> None:
        """Move *message* to the dead-letter queue :meth:`consume` declares for *queue*.

        For a message this consumer can never process however often it is
        redelivered, such as a job no local payload can execute. Retrying
        cannot help and acknowledging it would lose it, so it is parked
        straight away for an operator, with *reason* recorded in the
        :data:`~courier.broker.kombu.PARK_REASON_HEADER` header. The caller
        then returns to the consume loop, which acknowledges the original.

        Parameters
        ----------
        queue : str
            The queue the message was consumed from, named as it was passed
            to :meth:`consume` (without the namespace prefix).
        message : str
            The message body, unchanged.
        reason : str
            Why the message was parked. Logged in full; the header keeps the
            first :data:`_PARK_REASON_HEADER_MAX` characters.
        subscriber : str or None, optional
            Job builder identifier, required when *queue* is the file-found
            exchange, exactly as for :meth:`consume`.

        Raises
        ------
        ConfigurationError
            If *queue* is the file-found exchange and *subscriber* is missing.
        TransientBrokerError
            If the publish fails in a way worth retrying.
        FatalBrokerError
            If the publish fails in a way retrying cannot fix.

        Notes
        -----
        A publish failure propagates on purpose. The caller has not returned
        to the consume loop, so the original delivery is left unacknowledged
        and comes back through the redelivery path instead of being lost.
        """
        queue_name = self._consumed_queue_name(queue, subscriber)
        dead_letter_name = dead_letter_queue_for(queue_name)
        headers = {
            **inject_trace_headers(),
            PARK_REASON_HEADER: reason[:_PARK_REASON_HEADER_MAX],
        }
        with self._broker_manager.get_connection_context() as conn:
            dead_letter = declare_queue(
                conn,
                dead_letter_name,
                action="declaring dead-letter queue",
            )
            publish(conn, dead_letter, message, headers=headers)
        BROKER_MESSAGES_DEAD_LETTERED.labels(queue_name=queue_name).inc()
        self._logger.warning(
            f"Parked a message from {queue_name!r} on {dead_letter_name!r} "
            f"without retrying it: {reason}. It needs an operator. First "
            f"{_PARKED_BODY_PREVIEW} bytes: {message[:_PARKED_BODY_PREVIEW]!r}",
        )

    def _consumed_queue_name(self, queue: str, subscriber: str | None) -> str:
        """Return the namespaced queue :meth:`consume` reads for *queue*.

        Parameters
        ----------
        queue : str
            Queue or exchange name as passed to :meth:`consume`.
        subscriber : str or None
            Job builder identifier for the file-found exchange.

        Returns
        -------
        str
            The namespaced name of the queue messages are read from.

        Raises
        ------
        ConfigurationError
            If *queue* is the file-found exchange and *subscriber* is missing.
        """
        if queue != FILE_FOUND_EXCHANGE:
            return self._broker_manager.get_queue_name(queue)
        if subscriber is None:
            raise ConfigurationError(
                "The file-found exchange has one queue per job builder; pass "
                "subscriber=<job builder identifier> to name it.",
            )
        return self._broker_manager.get_queue_name(file_found_queue_for(subscriber))

    def register_plugin(
        self,
        plugin: type[ServicePlugin],
        config: dict[str, Any],
        identifier: str | None = None,
    ) -> None:
        """Register a plugin with the service.

        Parameters
        ----------
        plugin : type[ServicePlugin]
            Plugin class to register.
        config : dict[str, Any]
            Configuration dictionary for the plugin.
        identifier : str or None, optional
            Per-instance identifier from ``spec.run[*].identifier``.
            Required for dispatchers so they can consume from their own
            per-identifier queue and for job builders that want their
            emit logs to carry the builder's identifier.
        """
        self._plugin_manager.register_plugin(plugin, config, identifier=identifier)

    def configure_routing(
        self,
        dispatcher_identifiers: Iterable[str],
        builder_targets: dict[str, tuple[str, ...]] | None = None,
        allow_implicit_target: bool = True,
        builder_identifiers: Iterable[str] | None = None,
    ) -> None:
        """Wire up the :class:`TargetResolver` and record builder targets.

        Called by the CLI (or the config loader) after parsing the YAML
        and before :meth:`start`.  Exposes the set of known dispatchers
        and the resolver so :meth:`preflight_check` can validate the
        routing graph before any thread starts.

        Parameters
        ----------
        dispatcher_identifiers : Iterable[str]
            Every identifier declared as ``kind: dispatchers`` in the
            service YAML.
        builder_targets : dict[str, tuple[str, ...]] or None, optional
            Map from builder identifier → declared targets.  Used by
            :meth:`preflight_check` to enforce unknown-target /
            duplicate-target / implicit-wire rules.  Filtered by ``--only``:
            it drives routing validation for the builders this process runs.
        allow_implicit_target : bool, optional
            Mirror of ``ServiceSpecModel.allow_implicit_target``.
        builder_identifiers : Iterable[str] or None, optional
            Every identifier declared as ``kind: job_builders`` in the service
            YAML, **regardless of ``--only``**.  Each one gets a durable
            ``FilesFound-<identifier>`` queue predeclared during preflight, so
            a producer in another container never publishes into a fanout with
            nothing bound to it.  Without that, the first deploy of a split
            deployment lost every file.
        """
        self._dispatcher_identifiers = frozenset(dispatcher_identifiers)
        self._builder_targets = builder_targets or {}
        self._builder_identifiers = frozenset(builder_identifiers or ())
        self._allow_implicit_target = allow_implicit_target
        self._target_resolver = build_default_resolver(self._dispatcher_identifiers)

    def configure_topology(self, topology: PipelineTopology) -> None:
        """Record the whole pipeline's plugin names for static checks.

        Called by the CLI with every step in the service YAML, **regardless of
        ``--only``**, before :meth:`start`.  :meth:`preflight_check` uses it to
        check payload/dispatcher compatibility for builders and dispatchers
        that run in other processes.  A harness that never calls this still
        gets the check for everything registered in-process.

        Parameters
        ----------
        topology : PipelineTopology
            Plugin names for every dispatcher and job builder payload.
        """
        self._topology = topology

    def preflight_check(self) -> None:
        """Validate everything the service cannot recover from at runtime.

        Runs before any manager is started.  Failures raise
        :class:`ConfigurationError` (or a :class:`RoutingError` subclass)
        so :meth:`start` never brings a half-configured service up.
        Queue predeclaration happens in :meth:`_predeclare_target_queues`
        after the broker manager is up but before any plugin thread
        starts.

        Raises
        ------
        ConfigurationError
            If any routing invariant is violated.
        """
        self._auto_discover_routing()
        self._validate_dispatch_targets()
        self._bind_builder_payloads()
        self._validate_payload_compatibility()
        self._propagate_builder_targets()
        self._predeclare_target_queues()

    def _auto_discover_routing(self) -> None:
        """Backfill dispatcher identifiers and builder-targets from registered plugins.

        Tests and ad-hoc harnesses register plugins directly via
        :meth:`register_plugin` without calling :meth:`configure_routing`.
        Preflight still needs to know which dispatcher queues to predeclare
        and which builders to wire up, so walk the plugin manager for any
        information :meth:`configure_routing` did not supply.
        """
        if (
            self._dispatcher_identifiers
            and self._builder_targets
            and self._builder_identifiers
        ):
            return
        discovered_dispatchers, discovered_builders = self._discover_plugin_routing()
        if not self._dispatcher_identifiers:
            self._dispatcher_identifiers = frozenset(discovered_dispatchers)
            self._target_resolver = build_default_resolver(
                self._dispatcher_identifiers,
            )
        if not self._builder_targets:
            self._builder_targets = discovered_builders
        if not self._builder_identifiers:
            self._builder_identifiers = frozenset(discovered_builders)
        # A builder named only in the targets map still needs its queue; that
        # is the shape a harness skipping configure_routing produces.
        self._builder_identifiers |= frozenset(self._builder_targets)

    def _registered(self, interface: str) -> Iterator[tuple[str, ServicePlugin]]:
        """Yield ``(registry_key, plugin)`` for every plugin of *interface*."""
        for registry_key, info in self._plugin_manager.get_plugins().items():
            if getattr(info.plugin, "interface", None) == interface:
                yield registry_key, info.plugin

    def _discover_plugin_routing(
        self,
    ) -> tuple[set[str], dict[str, tuple[str, ...]]]:
        """Walk registered plugins for dispatcher and builder routing data.

        Returns
        -------
        tuple[set[str], dict[str, tuple[str, ...]]]
            Discovered dispatcher identifiers, and builder identifiers mapped
            to whatever targets their config already carried.
        """
        discovered_dispatchers: set[str] = set()
        discovered_builders: dict[str, tuple[str, ...]] = {}
        for registry_key, plugin in self._registered("dispatchers"):
            discovered_dispatchers.add(getattr(plugin, "identifier", registry_key))
        for registry_key, plugin in self._registered("job_builders"):
            discovered_builders[registry_key] = tuple(getattr(plugin, "targets", ()))
        return discovered_dispatchers, discovered_builders

    def _propagate_builder_targets(self) -> None:
        """Push preflight-resolved targets back into each builder plugin instance.

        :meth:`_validate_dispatch_targets` resolves implicit auto-wire and
        normalizes the ``builder_id → targets`` map, but the builder
        plugin instances created at :meth:`register_plugin` time still
        hold whatever ``targets`` list was in their config dict (often
        empty).  Copy the resolved tuple onto every matching instance so
        :meth:`JobBuilder.emit` has non-empty fan-out targets.
        """
        for builder_id, plugin in self._registered("job_builders"):
            if builder_id in self._builder_targets:
                plugin.targets = self._builder_targets[builder_id]  # type: ignore[attr-defined]

    def _validate_dispatch_targets(self) -> None:
        """Fail fast on unknown or duplicate dispatch targets.

        Resolves implicit routing (one builder, one dispatcher, no
        ``targets`` declared) to the sole dispatcher when
        ``allow_implicit_target`` is on, logging a WARNING so operators
        never auto-wire silently.
        """
        from courier.errors import (  # noqa: PLC0415
            AmbiguousImplicitTargetError,
            DuplicateTargetError,
            UnknownTargetError,
        )

        resolved: dict[str, tuple[str, ...]] = {}
        for builder_id, declared in self._builder_targets.items():
            if len(declared) != len(set(declared)):
                raise DuplicateTargetError(builder_id, list(declared))
            unknown = set(declared) - self._dispatcher_identifiers
            if unknown:
                raise UnknownTargetError(
                    builder_id,
                    sorted(unknown),
                    sorted(self._dispatcher_identifiers),
                )
            if declared:
                resolved[builder_id] = declared
                continue
            if not self._allow_implicit_target:
                raise AmbiguousImplicitTargetError(
                    builder_id,
                    len(self._dispatcher_identifiers),
                )
            if len(self._dispatcher_identifiers) != 1:
                raise AmbiguousImplicitTargetError(
                    builder_id,
                    len(self._dispatcher_identifiers),
                )
            sole = next(iter(self._dispatcher_identifiers))
            self._logger.warning(
                f"builder {builder_id!r} auto-wired to sole dispatcher "
                f"{sole!r} via allow_implicit_target=true; "
                "set targets explicitly to silence.",
            )
            resolved[builder_id] = (sole,)
        self._builder_targets = resolved
        self._logger.info(f"Resolved routing: {resolved}")

    def _bind_builder_payloads(self) -> None:
        """Wire each registered job builder to its nested payload plugin.

        The ``payload`` block in a builder's config is a nested microservice
        that the plugin manager has already registered under its identifier.
        The builder validated the block when it was constructed (a builder
        cannot be built without one); it needs the live instance to render it
        onto jobs, and refuses to start until it has it.

        Raises
        ------
        ConfigurationError
            If the payload identifier a builder's block names is not
            registered as a payload.
        """
        from courier.interfaces.job_builders import JobBuilder  # noqa: PLC0415
        from courier.interfaces.payloads import Payload  # noqa: PLC0415

        plugins = self._plugin_manager.get_plugins()
        for registry_key, plugin in self._registered("job_builders"):
            if not isinstance(plugin, JobBuilder):
                continue
            payload_id = plugin.payload_identifier
            payload_info = plugins.get(payload_id)
            if payload_info is None or not isinstance(payload_info.plugin, Payload):
                raise ConfigurationError(
                    f"Job builder {registry_key!r} nests payload {payload_id!r}, "
                    "which is not registered as a payload. `courier run` "
                    "registers the plugin a builder's payload block names; code "
                    "that registers plugins itself must register it too.",
                )
            plugin.payload = payload_info.plugin

    def _validate_payload_compatibility(self) -> None:
        """Fail fast when a payload cannot run on a dispatcher it will reach.

        The payload's representation hierarchy must intersect the dispatcher's
        supported representations.  Both ends of every route are checked, so a
        split deployment (``--only``) is covered from whichever process runs:

        * each job builder registered here, against each resolved target:
          the registered dispatcher when the target runs here, otherwise the
          dispatcher class the service YAML names for it;
        * each dispatcher registered here, against every job builder elsewhere
          in the YAML whose jobs reach it: the builder's payload plugin must
          be installed in this process, and compatible.

        Raises
        ------
        ConfigurationError
            If a payload is incompatible with a dispatcher it reaches, or a
            dispatcher here would receive a payload not installed here.
        """
        self._check_local_builder_payloads()
        self._check_remote_builder_payloads()

    def _check_local_builder_payloads(self) -> None:
        """Check each in-process builder's payload against its targets.

        Runs after :meth:`_bind_builder_payloads`, so every job builder here
        has its payload bound.
        """
        from courier.interfaces.job_builders import JobBuilder  # noqa: PLC0415

        for _registry_key, plugin in self._registered("job_builders"):
            if not isinstance(plugin, JobBuilder):
                continue
            payload = plugin.payload
            builder_id = plugin.identifier
            for target in self._builder_targets.get(builder_id, ()):
                dispatcher_cls = self._dispatcher_class(target)
                if dispatcher_cls is not None:
                    _require_compatible(
                        builder_id,
                        type(payload),
                        payload.name,
                        target,
                        dispatcher_cls,
                    )

    def _check_remote_builder_payloads(self) -> None:
        """Check payloads that builders in other processes send to us."""
        from courier.interfaces.dispatchers import Dispatcher  # noqa: PLC0415

        registered = self._plugin_manager.get_plugins()
        for registry_key, plugin in self._registered("dispatchers"):
            if not isinstance(plugin, Dispatcher):
                continue
            dispatcher_id = getattr(plugin, "identifier", registry_key)
            for builder_id in self._topology.builders_targeting(
                dispatcher_id,
                allow_implicit_target=self._allow_implicit_target,
            ):
                payload_name = self._topology.builder_payloads.get(builder_id)
                # A builder running here was checked from its own side.
                if builder_id in registered or payload_name is None:
                    continue
                _require_compatible(
                    builder_id,
                    _installed_payload(builder_id, payload_name, dispatcher_id),
                    payload_name,
                    dispatcher_id,
                    type(plugin),
                )

    def _dispatcher_class(self, target: str) -> type[Dispatcher] | None:
        """Return the dispatcher class that runs *target*, if it can be known.

        Parameters
        ----------
        target : str
            Dispatcher identifier.

        Returns
        -------
        type[Dispatcher] or None
            The registered instance's class when *target* runs here, else the
            class the service YAML names for it. ``None`` when neither is
            available here; the dispatcher's own process checks the route.
        """
        from courier.interfaces.dispatchers import (  # noqa: PLC0415
            Dispatcher,
            dispatchers,
        )

        info = self._plugin_manager.get_plugins().get(target)
        if info is not None:
            plugin = info.plugin
            return type(plugin) if isinstance(plugin, Dispatcher) else None
        plugin_name = self._topology.dispatcher_plugins.get(target)
        if plugin_name is None:
            return None
        try:
            return cast("type[Dispatcher]", dispatchers.get_plugin(plugin_name))
        except DiscoveryError as exc:
            self._logger.warning(
                f"Cannot check payload compatibility with dispatcher {target!r} "
                f"from this process ({exc}); the process that runs it checks "
                "the payloads it receives at startup.",
            )
            return None

    def _predeclare_target_queues(self) -> None:
        """Declare every queue this service or its peers will consume from.

        Runs producer-side, before any plugin thread starts, and declares:

        * one job-ready queue per dispatcher, so a builder can emit before its
          dispatcher exists;
        * the shared dispatcher queue;
        * one durable ``FilesFound-<builder>`` queue per job builder in the
          YAML, including builders that run in other containers. A fanout
          exchange discards anything published while nothing is bound to it, so
          without this a monitor-only container drops every file until a
          builder container has started at least once (issue #44).

        Every registration happens before the connection context opens, because
        that context is what declares the registered queues. The dispatcher
        queue was once registered after it, and so went undeclared during
        preflight.
        """
        for ident in sorted(self._dispatcher_identifiers):
            self._broker_manager.add_queue(
                job_ready_queue_for(ident),
                durable=True,
                exclusive=False,
            )
        self._broker_manager.add_queue(
            DISPATCHER_QUEUE,
            durable=True,
            exclusive=False,
        )
        for ident in sorted(self._builder_identifiers):
            self._broker_manager.add_file_found_queue(ident)

        exchange_name = self._broker_manager.get_queue_name(FILE_FOUND_EXCHANGE)
        self._logger.info(
            f"Predeclaring fanout exchange {exchange_name!r} and "
            f"{len(self._builder_identifiers)} file-found queue(s) for "
            f"{sorted(self._builder_identifiers)}",
        )
        # Opening the context declares everything registered above; the
        # exchange is declared explicitly so it exists even with zero builders.
        with self._broker_manager.get_connection_context() as conn:
            declare_fanout_exchange(conn, exchange_name)

    def _start_managers(self) -> None:
        """Start all service managers in sequence with error handling."""
        for manager in self._managers:
            try:
                manager.start()
            except Exception:
                self._logger.exception(f"Failed to start {manager.__class__.__name__}")
                raise

    def _stop_managers(self) -> None:
        """Stop all managers safely in reverse order."""
        for manager in reversed(self._managers):
            try:
                manager.stop()
            except Exception as e:
                self._logger.warning(
                    f"Error stopping {manager.__class__.__name__}: {e}",
                )

    def _health_check(self) -> bool:
        """Check health status of all service managers."""
        self._logger.debug(
            "Monitor health checks: "
            + ", ".join(
                f"{manager}: {manager.is_healthy()}" for manager in self._managers
            ),
        )
        return all(manager.is_healthy() for manager in self._managers)

    def _run_heartbeat_loop(self) -> None:
        """Execute main heartbeat loop with interruptable sleep intervals."""
        sleep_time = 1.0
        # At least one sleep per cycle: int(0.5 / 1.0) == 0 turned the loop
        # into a busy-wait that pinned a core.
        sleep_iterations = max(1, int(self._config.heartbeat_interval / sleep_time))

        while not self._signal_handler.shutdown_requested:
            for _ in range(sleep_iterations):
                if self._signal_handler.shutdown_requested:
                    break  # type: ignore
                time.sleep(sleep_time)

            if not self._signal_handler.shutdown_requested:
                self._prometheus_manager.send_heartbeat()

                self._service_uptime_metric.set(time.time() - self._start_time)
                self._service_health_metric.set(1 if self._health_check() else 0)

                plugin_status = self._plugin_manager.get_plugin_status()
                if plugin_status:
                    self._logger.debug(f"Plugin status: {plugin_status}")

    @log_execution
    def start(self) -> None:
        """Start service with complete lifecycle management and error handling."""
        self._logger.info(f"Starting Service {self._config.service_id}")

        tracer = get_tracer(__name__)
        with tracer.start_as_current_span(
            "courier.service",
            attributes={
                "courier.service_id": self._config.service_id,
                "courier.service_namespace": self._config.namespace,
            },
        ):
            try:
                self.preflight_check()
                self._start_managers()

                self._logger.info(
                    f"Service {self._config.service_id} started successfully",
                )
                self._run_heartbeat_loop()

            except KeyboardInterrupt:
                self._logger.info("Received keyboard interrupt")
                raise
            except Exception:
                self._logger.exception("Service startup failed")
                raise
            finally:
                if not self._health_check():
                    raise RuntimeError(
                        "Service health check failed after startup",
                    )
                self._cleanup()

    def _cleanup(self) -> None:
        """Perform complete resource cleanup for service shutdown."""
        self._logger.info("Cleaning up resources...")
        shutdown_tracing()
        self._stop_managers()
        self._logger.info(f"Service {self._config.service_id} stopped")


def _installed_payload(
    builder_id: str,
    payload_name: str,
    dispatcher_id: str,
) -> type[Payload]:
    """Return the payload class a remote builder sends, installed here.

    Parameters
    ----------
    builder_id : str
        Job builder identifier, for the error message.
    payload_name : str
        Payload plugin name from the builder's ``payload`` block.
    dispatcher_id : str
        Dispatcher that would receive the payload, for the error message.

    Returns
    -------
    type[Payload]
        The payload plugin class.

    Raises
    ------
    ConfigurationError
        If the payload plugin is not installed in this process or cannot be
        loaded. The dispatcher would otherwise fail every job it receives.
    """
    from courier.interfaces.payloads import payloads  # noqa: PLC0415

    try:
        return cast("type[Payload]", payloads.get_plugin(payload_name))
    except DiscoveryError as exc:
        raise ConfigurationError(
            f"Dispatcher {dispatcher_id!r} receives {payload_name!r} payloads "
            f"from job builder {builder_id!r}, but that payload plugin is not "
            f"usable in this process: {exc} Install it wherever "
            f"{dispatcher_id!r} runs.",
        ) from exc


def _require_compatible(
    builder_id: str,
    payload_cls: type[Payload],
    payload_name: str,
    dispatcher_id: str,
    dispatcher_cls: type[Dispatcher],
) -> None:
    """Raise unless *dispatcher_cls* can execute *payload_cls*.

    Parameters
    ----------
    builder_id : str
        Job builder identifier, for the error message.
    payload_cls : type[Payload]
        Payload class the builder attaches to its jobs.
    payload_name : str
        Payload plugin name, for the error message.
    dispatcher_id : str
        Dispatcher identifier, for the error message.
    dispatcher_cls : type[Dispatcher]
        Class of the dispatcher that receives the jobs.

    Raises
    ------
    ConfigurationError
        If no representation of the payload is one the dispatcher supports.
    """
    if dispatcher_cls.compatible_representation(payload_cls) is None:
        raise ConfigurationError(
            f"Job builder {builder_id!r} payload {payload_name!r} is not "
            f"compatible with dispatcher {dispatcher_id!r} "
            f"(supports {dispatcher_cls.representation_names()}).",
        )


def create_service_with_plugins(
    config: ServiceConfig | None = None,
    plugins: (
        Sequence[tuple[type[ServicePlugin], dict[str, Any], str | None]] | None
    ) = None,
) -> Service:
    """Create new Service instance with optional configuration and plugins.

    Parameters
    ----------
    config : ServiceConfig or None, optional
        Service configuration.
    plugins : sequence of ``(plugin_class, config, identifier)`` or None, optional
        Plugin entries to register. ``identifier`` is the
        ``spec.run[*].identifier`` from the service YAML and is required
        for dispatchers; pass ``None`` when no identifier applies.

    Returns
    -------
    Service
        Configured service instance ready for startup.

    Examples
    --------
    >>> service = create_service_with_plugins()
    >>> isinstance(service, Service)
    True
    """
    service = Service(config)

    if plugins is not None:
        for plugin_class, plugin_config, identifier in plugins:
            service.register_plugin(
                plugin_class,
                plugin_config,
                identifier=identifier,
            )

    return service
