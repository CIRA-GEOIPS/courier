"""Service orchestrator: coordinates plugins, broker, and managers."""

from __future__ import annotations

import time
from contextlib import suppress
from typing import TYPE_CHECKING, Any

from courier.broker.kombu import (
    MessageBrokerManager,
    declare_bound_queue,
    declare_dead_letter_queue,
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
    namespaced_queue_name,
)
from courier.errors import ConfigurationError
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
    from collections.abc import Callable, Generator, Iterable, Sequence

    from courier.interfaces.plugin_protocol import ServicePlugin
    from courier.managers.base import ServiceManager
from courier.metrics import (
    BROKER_MESSAGES_PENDING,
    BROKER_MESSAGES_RECEIVED,
    BROKER_MESSAGES_SENT,
    SERVICE_HEALTH,
    SERVICE_UPTIME,
)
from courier.utils.decorators import log_execution
from courier.utils.logging import get_logger
from courier.utils.signals import SignalHandler


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
    def emit(self, queue: str, message: str, confirm: bool = False) -> None:
        """Publish a message to a message broker queue.

        Parameters
        ----------
        queue : str
            Name of the queue to publish to.
        message : str
            Message content to publish.
        confirm : bool, optional
            When ``True`` and the broker supports it (AMQP), wait for a
            publisher confirm before returning. No-op on memory transport.
            Default ``False``.

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
                        confirm=confirm,
                        headers=w3c_headers,
                    )
                # Counted per bound queue, which is what each consumer
                # decrements. A single exchange-labelled increment made the
                # two halves different series: one climbing forever, the
                # other going negative as a backlog drained.
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
                publish(conn, q, message, confirm=confirm, headers=w3c_headers)
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
            ahead of producers.  Since the file-found queue became durable and
            is predeclared during preflight, this is an ordering nicety that
            keeps the first files moving promptly -- not a guard against loss.
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
        Consume to the end of the loop.  A message is acknowledged only after
        the ``for`` body returns, so abandoning the loop while holding one --
        by ``break``, or by raising -- leaves that message unacknowledged, and
        an unacknowledged message is indistinguishable from one the caller
        failed on.  It is therefore counted as a failed attempt and requeued
        behind the backlog, and is parked on the dead-letter queue if it
        happens ``broker_max_redeliveries`` times.  Setting *stop_event* first
        is what marks the difference: that is a shutdown, and the message is
        returned untouched.  Breaking may also requeue anything the broker
        pre-fetched but has not yet yielded.  For one-shot consumption that
        avoids all of this, use a separate thread with a timeout (see
        ``concurrent.futures``).

        Validation happens when ``consume`` is called rather than on the first
        ``next()``, so a missing *subscriber* is reported at the call site.
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
            return self._consume_file_found(subscriber, effective_stop, on_subscribed)
        return self._consume_direct(queue, effective_stop, on_subscribed)

    def _consume_file_found(
        self,
        subscriber: str,
        stop_event: threading.Event,
        on_subscribed: Callable[[], None] | None,
    ) -> Generator[tuple[str, Any], None, None]:
        """Consume one builder's durable queue bound to the fanout exchange.

        Parameters
        ----------
        subscriber : str
            Job builder identifier naming the queue.
        stop_event : threading.Event
            Ends the loop once set.
        on_subscribed : Callable[[], None] or None
            Called once the queue is declared and bound.

        Yields
        ------
        tuple[str, Any]
            ``(body, parent_ctx)`` per message.
        """
        exchange_name = self._broker_manager.get_queue_name(FILE_FOUND_EXCHANGE)
        queue_name = self._broker_manager.add_file_found_queue(subscriber)
        with self._broker_manager.get_connection_context() as conn:
            exchange = declare_fanout_exchange(conn, exchange_name)
            # Redeclared explicitly on this connection as well: the manager
            # only declares a registered queue on the first connection it
            # opens, so a queue deleted meanwhile would otherwise stay gone
            # for the life of the process.
            q = declare_bound_queue(conn, exchange, queue_name)
            dead_letter = declare_dead_letter_queue(
                conn,
                dead_letter_queue_for(queue_name),
            )
            self._logger.info(
                f"Consuming file-found messages from durable queue "
                f"{queue_name!r} bound to {exchange_name!r} "
                f"(prefetch={self._config.broker_prefetch_count}, "
                f"max_redeliveries={self._config.broker_max_redeliveries})",
            )
            if on_subscribed is not None:
                on_subscribed()
            yield from self._relay(
                conn,
                q,
                dead_letter,
                stop_event,
                queue_name,
                {
                    "messaging.system": "amqp",
                    "messaging.destination": exchange_name,
                    "messaging.destination_kind": "fanout",
                    "messaging.rabbitmq.destination.queue": queue_name,
                },
            )

    def _consume_direct(
        self,
        queue: str,
        stop_event: threading.Event,
        on_subscribed: Callable[[], None] | None,
    ) -> Generator[tuple[str, Any], None, None]:
        """Consume a directly-addressed queue.

        Parameters
        ----------
        queue : str
            Base queue name, namespaced by the broker manager.
        stop_event : threading.Event
            Ends the loop once set.
        on_subscribed : Callable[[], None] or None
            Called once the queue is declared.

        Yields
        ------
        tuple[str, Any]
            ``(body, parent_ctx)`` per message.
        """
        queue_name = self._broker_manager.add_queue(
            queue,
            durable=True,
            exclusive=False,
        )
        self._logger.debug(f"Consuming from queue: {queue_name}")
        with self._broker_manager.get_connection_context() as conn:
            q = declare_queue(conn, queue_name, durable=True)
            dead_letter = declare_dead_letter_queue(
                conn,
                dead_letter_queue_for(queue_name),
            )
            if on_subscribed is not None:
                on_subscribed()
            yield from self._relay(
                conn,
                q,
                dead_letter,
                stop_event,
                queue_name,
                {
                    "messaging.system": "amqp",
                    "messaging.destination": queue_name,
                },
            )

    def _relay(  # noqa: PLR0913, PLR0917 -- one consumer's declared topology
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
        A message the caller could not get past is not rejected back to the
        head of the queue. It is republished behind the current backlog with
        its attempt count incremented, and parked on the dead-letter queue once
        the count passes ``broker_max_redeliveries`` -- see
        :func:`courier.broker.kombu.redeliver_or_park`. Rejecting with
        ``requeue=True``, which is what this did, put the message straight back
        at the head; with the shipped prefetch of 1 the same message was then
        handed to the same consumer again immediately, and nothing behind it
        was ever reached.

        Both failure paths matter, and the one that matters more is the less
        obvious of the two. An exception raised by the caller *inside the*
        ``for`` *body* is not thrown into this generator -- Python abandons it,
        and the close arrives here as ``GeneratorExit`` rather than through the
        ``except Exception`` clause. So a plugin blowing up on one message, the
        case this exists for, never reached that clause at all; only a failure
        between the ``yield`` and the ``ack`` did.

        ``GeneratorExit`` is therefore ambiguous: it means either that the
        caller gave up on this message or that the consumer is shutting down
        with the message untried. *stop_event* separates them. During shutdown
        the message is rejected unchanged and its attempt count left alone,
        because spending a retry on every rolling restart would eventually park
        perfectly good messages.
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

    def _fail_message(  # noqa: PLR0913, PLR0917 -- one delivery's worth of state
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
        Acknowledging is safe only once the republish has been confirmed, so
        the order is republish-then-acknowledge and never the reverse. If the
        republish fails the delivery is rejected instead, which is the
        behaviour this replaced: the message is not lost, and the queue is no
        worse off than it was.
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
            duplicate-target / implicit-wire rules.  Filtered by ``--only``,
            because it drives routing validation for the builders this process
            actually runs.
        allow_implicit_target : bool, optional
            Mirror of ``ServiceSpecModel.allow_implicit_target``.
        builder_identifiers : Iterable[str] or None, optional
            Every identifier declared as ``kind: job_builders`` in the service
            YAML, **regardless of ``--only``**.  Each one gets a durable
            ``FilesFound-<identifier>`` queue predeclared during preflight, so
            a producer in another container never publishes into a fanout with
            nothing bound to it -- which is what made the first deploy of a
            split deployment lose every file.
        """
        self._dispatcher_identifiers = frozenset(dispatcher_identifiers)
        self._builder_targets = builder_targets or {}
        self._builder_identifiers = frozenset(builder_identifiers or ())
        self._allow_implicit_target = allow_implicit_target
        self._target_resolver = build_default_resolver(self._dispatcher_identifiers)

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
        self._validate_queue_name_lengths()
        self._validate_dispatch_targets()
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
        # A builder named only in the targets map still needs its queue: that
        # is the shape every harness that skips configure_routing produces.
        self._builder_identifiers |= frozenset(self._builder_targets)

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
        plugins = self._plugin_manager.get_plugins()
        discovered_dispatchers: set[str] = set()
        discovered_builders: dict[str, tuple[str, ...]] = {}
        for registry_key, info in plugins.items():
            interface = getattr(info.plugin, "interface", None)
            if interface == "dispatchers":
                ident = getattr(info.plugin, "identifier", registry_key)
                discovered_dispatchers.add(ident)
            elif interface == "job_builders":
                existing = getattr(info.plugin, "targets", ())
                discovered_builders[registry_key] = tuple(existing)
        return discovered_dispatchers, discovered_builders

    def _validate_queue_name_lengths(self) -> None:
        """Reject identifiers whose namespaced queue names are too long.

        Checked for both queue families, and against the *namespaced* name,
        because that is what the broker sees. Runs before routing validation
        so an oversized name is reported as a configuration problem rather
        than surfacing later as a broker error.

        Raises
        ------
        InvalidIdentifierError
            If any namespaced queue name exceeds the AMQP limit, or an
            identifier is malformed.
        """
        for ident in sorted(self._dispatcher_identifiers):
            namespaced_queue_name(self.namespace, job_ready_queue_for(ident))
        for ident in sorted(self._builder_identifiers):
            namespaced_queue_name(self.namespace, file_found_queue_for(ident))

    def _propagate_builder_targets(self) -> None:
        """Push preflight-resolved targets back into each builder plugin instance.

        :meth:`_validate_dispatch_targets` resolves implicit auto-wire and
        normalizes the ``builder_id → targets`` map, but the builder
        plugin instances created at :meth:`register_plugin` time still
        hold whatever ``targets`` list was in their config dict (often
        empty).  Copy the resolved tuple onto every matching instance so
        :meth:`JobBuilder.emit` has non-empty fan-out targets.
        """
        plugins = self._plugin_manager.get_plugins()
        for builder_id, targets in self._builder_targets.items():
            info = plugins.get(builder_id)
            if info is None:
                continue
            if getattr(info.plugin, "interface", None) != "job_builders":
                continue
            info.plugin.targets = targets  # type: ignore[attr-defined]

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

    def _predeclare_target_queues(self) -> None:
        """Declare every queue this service or its peers will consume from.

        Runs producer-side, before any plugin thread starts, and declares
        three families:

        * one job-ready queue per dispatcher, so a builder can emit before its
          dispatcher exists;
        * the shared dispatcher queue;
        * one durable ``FilesFound-<builder>`` queue per job builder in the
          YAML -- **including builders that run in other containers**. A fanout
          exchange discards anything published while nothing is bound to it, so
          without this a monitor-only container drops every file until a
          builder container has started at least once (issue #44).

        Every registration happens *before* the connection context opens,
        because that context is what actually declares the registered queues.
        The dispatcher queue used to be registered after it, and so was never
        declared during preflight at all.
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

                if not self._health_check():
                    raise RuntimeError("Service health check failed after startup")  # noqa: TRY301

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
                self._cleanup()

    def _cleanup(self) -> None:
        """Perform complete resource cleanup for service shutdown."""
        self._logger.info("Cleaning up resources...")
        shutdown_tracing()
        self._stop_managers()
        self._logger.info(f"Service {self._config.service_id} stopped")


def create_service_with_plugins(
    config: ServiceConfig | None = None,
    plugins: Sequence[tuple[type[ServicePlugin], dict[str, Any], str | None]]
    | None = None,
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
