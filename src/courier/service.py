"""Service orchestrator: coordinates plugins, broker, and managers."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from courier.broker.kombu import MessageBrokerManager, declare_queue, publish
from courier.broker.kombu import messages as broker_messages
from courier.config import ServiceConfig
from courier.constants import (
    DISPATCHER_QUEUE,
    FILE_FOUND_QUEUE,
    MAX_QUEUE_NAME_LENGTH,
    job_ready_queue_for,
)
from courier.errors import ConfigurationError
from courier.managers.plugin_manager import PluginManager
from courier.managers.prometheus_manager import PrometheusManager
from courier.routing import TargetResolver, build_default_resolver

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable, Sequence

    from courier.interfaces.plugin_protocol import ServicePlugin
    from courier.managers.base import ServiceManager
from courier.metrics import (
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
    10
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
        self._builder_targets: dict[str, tuple[str, ...]] = {}
        self._allow_implicit_target: bool = True
        self._target_resolver: TargetResolver = build_default_resolver(())

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
        queue_name = self._broker_manager.add_queue(
            queue,
            durable=True,
            exclusive=False,
        )
        with self._broker_manager.get_connection_context() as conn:
            self._logger.debug(f"Emitting message to queue '{queue_name}': {message}")
            q = declare_queue(conn, queue_name, durable=True)
            publish(conn, q, message, confirm=confirm)
            BROKER_MESSAGES_SENT.labels(queue_name=queue_name).inc()

    def consume(self, queue: str) -> Generator[str, None, None]:
        """Yield messages from a message broker queue.

        Parameters
        ----------
        queue : str
            The name of the queue to consume messages from.

        Yields
        ------
        str
            The decoded message content from the queue.
        """
        queue_name = self._broker_manager.add_queue(
            queue,
            durable=True,
            exclusive=False,
        )

        self._logger.debug(f"Consuming from queue: {queue_name}")

        with self._broker_manager.get_connection_context() as conn:
            q = declare_queue(conn, queue_name, durable=True)
            for body, ack, reject in broker_messages(conn, q):
                try:
                    self._logger.debug(
                        f"Received message from queue '{queue_name}': {body}",
                    )
                    BROKER_MESSAGES_RECEIVED.labels(
                        queue_name=queue_name,
                    ).inc()
                    yield body
                    ack()
                except GeneratorExit:
                    reject()
                    raise
                except Exception:  # Reject message before propagating any error
                    reject()
                    raise

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
            duplicate-target / implicit-wire rules.
        allow_implicit_target : bool, optional
            Mirror of ``ServiceSpecModel.allow_implicit_target``.
        """
        self._dispatcher_identifiers = frozenset(dispatcher_identifiers)
        self._builder_targets = builder_targets or {}
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
        if self._dispatcher_identifiers and self._builder_targets:
            return
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
        if not self._dispatcher_identifiers:
            self._dispatcher_identifiers = frozenset(discovered_dispatchers)
            self._target_resolver = build_default_resolver(
                self._dispatcher_identifiers,
            )
        if not self._builder_targets:
            self._builder_targets = discovered_builders

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
            info.plugin.targets = targets

    def _validate_dispatch_targets(self) -> None:
        """Fail fast on oversized queue names, unknown / duplicate targets.

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

        for ident in self._dispatcher_identifiers:
            full = f"{self.namespace}-{job_ready_queue_for(ident)}"
            if len(full) > MAX_QUEUE_NAME_LENGTH:
                raise ConfigurationError(
                    f"Namespaced queue {full!r} exceeds "
                    f"{MAX_QUEUE_NAME_LENGTH} chars; shorten the namespace "
                    f"or dispatcher identifier {ident!r}.",
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
        """Declare every per-dispatcher queue plus shared queues.

        Done producer-side so the builder can emit before the dispatcher
        has come up — otherwise AMQP raises on publish to an undeclared
        queue.  A cheap dict insert on memory transport.
        """
        for ident in self._dispatcher_identifiers:
            self._broker_manager.add_queue(
                job_ready_queue_for(ident),
                durable=True,
                exclusive=False,
            )
        self._broker_manager.add_queue(
            FILE_FOUND_QUEUE,
            durable=True,
            exclusive=False,
        )
        self._broker_manager.add_queue(
            DISPATCHER_QUEUE,
            durable=True,
            exclusive=False,
        )

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
        sleep_iterations = int(self._config.heartbeat_interval / sleep_time)

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

        try:
            self.preflight_check()
            self._start_managers()

            if not self._health_check():
                raise RuntimeError("Service health check failed after startup")  # noqa: TRY301

            self._logger.info(f"Service {self._config.service_id} started successfully")
            self._run_heartbeat_loop()

        except KeyboardInterrupt:
            self._logger.info("Received keyboard interrupt")
        except Exception:
            self._logger.exception("Service startup failed")
            raise
        finally:
            self._cleanup()

    def _cleanup(self) -> None:
        """Perform complete resource cleanup for service shutdown."""
        self._logger.info("Cleaning up resources...")
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
