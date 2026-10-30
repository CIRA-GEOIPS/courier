"""Plugin lifecycle management, health monitoring, and auto-restart."""

import sys
import threading
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from courier.config import ServiceConfig
from courier.constants import PluginRunState
from courier.errors import CourierError
from courier.interfaces.plugin_protocol import ServicePlugin
from courier.managers.base import ServiceManager
from courier.metrics import (
    PLUGIN_HEALTH,
    PLUGIN_REGISTRATION_FAILURES,
    PLUGIN_RESTARTS,
    PLUGIN_STATE,
)
from courier.tracing import (
    ATTR_PLUGIN_NAME,
    get_tracer,
)
from courier.utils.config_monitor import ConfigMonitor
from courier.utils.decorators import log_execution
from courier.utils.logging import get_logger


@dataclass
class PluginStateInfo:
    """Information about a plugin instance.

    Parameters
    ----------
    plugin : ServicePlugin
        The plugin instance.
    state : PluginRunState, default=PluginRunState.STOPPED
        Current state of the plugin.
    thread : threading.Thread or None, default=None
        Starter thread for the most recent ``start()``; it exits once the
        plugin is RUNNING or FAILED. The plugin runs its own worker thread.
    abandoned_threads : list[threading.Thread]
        Threads the manager stopped waiting for: a ``start()`` that never
        returned, a stop that outlived its join, or the stop issued to a
        start that was given up on. They cannot be interrupted; they are kept
        so they are at least counted. Finished ones are pruned whenever
        another is added, and status reports count only the live ones.
    starting_since : float or None, default=None
        ``time.monotonic()`` at which the current ``start()`` was called;
        None once ``start()`` has returned, so the health gate that follows
        does not count against ``plugin_start_timeout``.
    last_health_check : float or None, default=None
        ``time.monotonic()`` of the last health check.
    restart_count : int, default=0
        Number of times plugin has been restarted.
    last_restart : datetime or None, default=None
        Wall-clock timestamp of the last restart attempt, for status reports.
    running_since : float or None, default=None
        ``time.monotonic()`` at which the plugin last reached RUNNING; the
        restart-forgiveness window is measured from here. Monotonic so a
        wall-clock adjustment cannot make the plugin look long-healthy, or
        freshly started, when it was not.
    restart_due : float or None, default=None
        ``time.monotonic()`` at which a plugin in ``RESTARTING`` is started
        again. The monitor loop checks it each tick instead of sleeping
        through the delay, so the other plugins stay monitored meanwhile.
    error_message : str or None, default=None
        Last error message if plugin failed.
    ready : threading.Event
        Event set when the plugin reaches RUNNING state.
    """

    plugin: ServicePlugin
    plugin_type: type[ServicePlugin] | None = None
    identifier: str | None = None
    config: dict[str, Any] = field(default_factory=dict)
    state: PluginRunState = PluginRunState.STOPPED
    thread: threading.Thread | None = None
    abandoned_threads: list[threading.Thread] = field(default_factory=list)
    starting_since: float | None = None
    last_health_check: float | None = None
    restart_count: int = 0
    last_restart: datetime | None = None
    running_since: float | None = None
    restart_due: float | None = None
    error_message: str | None = None
    ready: threading.Event = field(default_factory=threading.Event)


class PluginManager(ServiceManager):
    """Manages plugin lifecycle, health monitoring, and auto-restart functionality.

    This manager handles plugins running in separate threads, monitors their
    health, and automatically attempts to restart failed plugins according
    to configured policies.

    Parameters
    ----------
    config : ServiceConfig
        Service configuration containing plugin-related settings.
    parent_service : Any
        Reference to the parent service instance.

    Attributes
    ----------
    _config : ServiceConfig
        Service configuration.
    _plugins : dict[str, PluginStateInfo]
        Registered plugins and their state information.
    _running : bool
        Whether the plugin manager is running.

    Methods
    -------
    register_plugin(plugin, config)
        Register a new plugin with the manager.
    get_plugin_status()
        Get current status of all plugins.
    """

    def __init__(self, config: ServiceConfig, parent_service: Any) -> None:
        """Initialize plugin manager with configuration and parent service.

        Parameters
        ----------
        config : ServiceConfig
            Service configuration.
        parent_service : Any
            Reference to parent service instance.
        """
        self._config = config
        self._logger = get_logger("manager", "PluginManager", config)
        self._plugins: dict[str, PluginStateInfo] = {}
        self._lock = threading.RLock()
        self._state = PluginRunState.STOPPED
        self._monitor_thread: threading.Thread | None = None
        self._service = parent_service

        self._plugin_state_metric = PLUGIN_STATE
        self._plugin_restart_metric = PLUGIN_RESTARTS
        self._plugin_health_metric = PLUGIN_HEALTH
        self._registration_failures_metric = PLUGIN_REGISTRATION_FAILURES

        if self._config.live_reload != "off":
            self._config_monitor = ConfigMonitor(
                self._config.config_path,
                parent_service=self,
            )

        self._tracer = get_tracer(__name__)

    @staticmethod
    def _plugin_identifier(plugin: ServicePlugin) -> str:
        """Return the config identifier or type name for *plugin*."""
        return getattr(plugin, "identifier", plugin.name)

    def register_plugin(
        self,
        plugin: type[ServicePlugin],
        config: dict[str, Any],
        identifier: str | None = None,
    ) -> None:
        """Register a new plugin with the manager.

        Parameters
        ----------
        plugin : type[ServicePlugin]
            Plugin class to register. Will be instantiated with service
            reference and config.
        config : dict[str, Any]
            Configuration to pass to the plugin constructor.
        identifier : str or None, optional
            ``spec.run[*].identifier`` from the service YAML.  Passed to
            the plugin constructor as the ``identifier`` keyword
            argument when supplied — required for dispatchers so they
            can consume from their per-identifier queue.

        Raises
        ------
        ValueError
            If a plugin with the same registry key is already registered.
        """
        with self._lock:
            kwargs: dict[str, Any] = {}
            if identifier is not None:
                # Passed to every plugin kind, not just dispatchers: the
                # identifier is what distinguishes two instances of the same
                # plugin class in metrics labels and log lines. Restricting it
                # to dispatchers left builders and monitors labelled with their
                # class name, making sibling instances indistinguishable.
                kwargs["identifier"] = identifier
                self._logger.debug(f"Registering plugin with identifier: {identifier}")
            plugin_instance = plugin(self._service, config, **kwargs)
            registry_key = identifier or plugin_instance.name
            if registry_key in self._plugins:
                self._registration_failures_metric.labels(
                    plugin_name=plugin_instance.name,
                    plugin_identifier=self._plugin_identifier(plugin_instance),
                    reason="duplicate_key",
                ).inc()
                raise ValueError(f"Plugin {registry_key} already registered")

            plugin_name = plugin_instance.name

            self._plugins[registry_key] = PluginStateInfo(
                plugin=plugin_instance,
                config=config,
                identifier=identifier,
                plugin_type=plugin,
            )

            # Emit initial metric values so the series exists from container
            # start, before the plugin transitions to RUNNING / healthy.
            self._record_state(info, PluginRunState.STARTING)
            self._plugin_health_metric.labels(
                plugin_name=plugin_name,
                plugin_identifier=self._plugin_identifier(plugin_instance),
            ).set(0)

            self._logger.info(
                f"Registered plugin: {registry_key} "
                f"(class={plugin_name} v{plugin_instance.version})",
            )

    def _start_plugin(self, plugin_info: PluginStateInfo) -> None:
        """Start a plugin on a short-lived starter thread.

        The thread calls ``plugin.start()``, waits for the plugin to report
        healthy, marks it RUNNING and exits. Nothing lingers afterwards: the
        plugin's own worker thread is what its ``is_healthy`` watches, so a
        wrapper thread kept alive only to be found alive told the monitor
        nothing.

        Does nothing once the manager has left RUNNING. The state check and
        the thread launch share one hold of the lock, so :meth:`stop` cannot
        slip between them and leave a plugin starting after shutdown.

        Parameters
        ----------
        plugin_info : PluginStateInfo
            Information about the plugin to start.
        """
        with self._lock:
            if self._state != PluginRunState.RUNNING:
                return
            plugin_info.state = PluginRunState.STARTING
            plugin_info.starting_since = time.monotonic()
            plugin_info.last_health_check = None
            plugin_info.running_since = None
            plugin_info.error_message = None
            plugin_info.ready.clear()
            plugin_info.thread = threading.Thread(
                target=self._run_start,
                args=(plugin_info,),
                name=f"Plugin-{plugin_info.plugin.name}-start",
                daemon=True,
            )
            plugin_info.thread.start()

    def _run_start(self, plugin_info: PluginStateInfo) -> None:
        """Body of the starter thread: start, gate on health, mark RUNNING.

        A start the monitor has already given up on (see
        :meth:`_abandon_stuck_start`) that returns late changes nothing: the
        plugin stays FAILED, and this thread just ends.
        """
        name = plugin_info.plugin.name
        try:
            plugin_info.plugin.start()
            with self._lock:
                # Only start() itself is bounded by plugin_start_timeout; the
                # health gate below has its own deadline.
                if self._owns_start_locked(plugin_info):
                    plugin_info.starting_since = None
            self._logger.debug(
                "plugin.start() returned for %s; entering health gate",
                name,
            )
            if not self._wait_until_healthy(plugin_info):
                self._logger.warning(
                    "Plugin %s did not report healthy within deadline; "
                    "proceeding to RUNNING, monitor will catch failures.",
                    name,
                )
            with self._lock:
                if not self._owns_start_locked(plugin_info):
                    self._log_lost_start_locked(plugin_info, "returned")
                    return
                plugin_info.state = PluginRunState.RUNNING
                plugin_info.starting_since = None
                plugin_info.last_health_check = None
                plugin_info.running_since = time.monotonic()
                plugin_info.error_message = None
        except Exception as e:
            with self._lock:
                if not self._owns_start_locked(plugin_info):
                    self._log_lost_start_locked(plugin_info, f"raised {e!r}")
                    return
                plugin_info.state = PluginRunState.FAILED
                plugin_info.starting_since = None
                plugin_info.error_message = (
                    str(e) if isinstance(e, CourierError) else f"Unexpected error: {e}"
                )
            self._logger.exception(f"Plugin {name} failed to start")
            self._record_state(plugin_info)
            return

        self._record_state(plugin_info)
        self._logger.info(f"Plugin started successfully: {name}")
        with self._tracer.start_as_current_span("plugin_lifecycle") as span:
            span.add_event("plugin.started", attributes={ATTR_PLUGIN_NAME: name})
        plugin_info.ready.set()

    @staticmethod
    def _owns_start_locked(plugin_info: PluginStateInfo) -> bool:
        """Return whether the calling starter thread is still the current one."""
        return (
            plugin_info.state == PluginRunState.STARTING
            and plugin_info.thread is threading.current_thread()
        )

    def _log_lost_start_locked(
        self,
        plugin_info: PluginStateInfo,
        outcome: str,
    ) -> None:
        """Log a ``start()`` that finished after its starter lost ownership.

        Only a start the monitor gave up on is worth a warning; one overtaken
        by a stop is ordinary shutdown.
        """
        if plugin_info.state == PluginRunState.FAILED:
            self._logger.warning(
                "Plugin %s start() %s after it was given up on; leaving it FAILED",
                plugin_info.plugin.name,
                outcome,
            )
        else:
            self._logger.debug(
                "Plugin %s start() %s after it was stopped; leaving it %s",
                plugin_info.plugin.name,
                outcome,
                plugin_info.state.name,
            )

    def _wait_until_healthy(self, plugin_info: PluginStateInfo) -> bool:
        """Poll ``is_healthy`` for up to one health-check interval."""
        warn_interval = 5.0
        last_warn = -warn_interval
        deadline = time.monotonic() + self._config.plugin_health_check_interval
        while time.monotonic() < deadline:
            try:
                if plugin_info.plugin.is_healthy():
                    return True
            except Exception:
                now = time.monotonic()
                if now - last_warn > warn_interval:
                    last_warn = now
                    self._logger.warning(
                        "Health check for %s raised an exception; retrying...",
                        plugin_info.plugin.name,
                    )
            time.sleep(0.1)
        return False

    def _record_state(
        self,
        plugin_info: PluginStateInfo,
        state: PluginRunState | None = None,
    ) -> None:
        """Publish *state* (default: the plugin's current one) to the gauge."""
        self._plugin_state_metric.labels(
            plugin_name=plugin_info.plugin.name,
            plugin_identifier=self._plugin_identifier(plugin_info.plugin),
        ).set((state or plugin_info.state).value)

    def _stop_plugin(self, plugin_info: PluginStateInfo) -> None:
        """Stop a plugin gracefully.

        Parameters
        ----------
        plugin_info : PluginStateInfo
            Information about the plugin to stop.
        """
        should_stop = False
        thread_to_join = None

        with self._lock:
            if plugin_info.state in (
                PluginRunState.RUNNING,
                PluginRunState.STARTING,
                PluginRunState.RESTARTING,
            ):
                plugin_info.state = PluginRunState.STOPPING
                should_stop = True
                if plugin_info.thread and plugin_info.thread.is_alive():
                    thread_to_join = plugin_info.thread

        if not should_stop:
            return

        self._call_plugin_stop(plugin_info)
        if thread_to_join:
            thread_to_join.join(timeout=5)

        with self._lock:
            plugin_info.state = PluginRunState.STOPPED
            if thread_to_join is not None and thread_to_join.is_alive():
                abandoned = self._abandon_thread_locked(plugin_info, thread_to_join)
                self._logger.warning(
                    "Thread %s of plugin %s is still running after being told "
                    "to stop; it cannot be interrupted (%d abandoned and "
                    "still running)",
                    thread_to_join.name,
                    plugin_info.plugin.name,
                    abandoned,
                )
            plugin_info.thread = None
            self._record_state(plugin_info)

        self._logger.info(f"Plugin stopped: {plugin_info.plugin.name}")

        with self._tracer.start_as_current_span(
            "plugin_lifecycle",
        ) as span:
            span.add_event(
                "plugin.stopped",
                attributes={
                    ATTR_PLUGIN_NAME: plugin_info.plugin.name,
                },
            )

    def _wipe_plugin(self, plugin_info: PluginStateInfo) -> None:
        """Remove a plugin from the plugin manager safely."""
        if not plugin_info.identifier:
            raise CourierError("No identifier specified for plugin to be wiped.")
        with self._lock:
            self._plugins.pop(plugin_info.identifier, None)

        thread = plugin_info.thread
        self._stop_plugin(plugin_info)
        if thread is not None and thread.is_alive():
            raise CourierError(
                f"Plugin {plugin_info.identifier} failed to stop within the timeout.",
    @staticmethod
    def _abandon_thread_locked(
        plugin_info: PluginStateInfo,
        thread: threading.Thread,
    ) -> int:
        """Record a thread the manager will not wait for. Lock held.

        Threads that have since finished are dropped first, so the list does
        not grow across restarts. Returns how many are still running.
        """
        plugin_info.abandoned_threads = [
            t for t in plugin_info.abandoned_threads if t.is_alive()
        ]
        plugin_info.abandoned_threads.append(thread)
        return len(plugin_info.abandoned_threads)

    @staticmethod
    def _live_abandoned_locked(plugin_info: PluginStateInfo) -> int:
        """Return how many abandoned threads are still running. Lock held."""
        return sum(t.is_alive() for t in plugin_info.abandoned_threads)

    def _call_plugin_stop(self, plugin_info: PluginStateInfo) -> None:
        """Call ``plugin.stop()``, logging rather than raising if it fails."""
        try:
            plugin_info.plugin.stop()
        except CourierError as e:
            self._logger.warning(
                f"Error stopping plugin {plugin_info.plugin.name}: {e}",
            )
        except Exception:
            self._logger.exception(
                f"Unexpected error stopping plugin {plugin_info.plugin.name}",
            )

    def _monitor_plugins(self) -> None:
        """Monitor plugin health, restart failed plugins, and start due restarts.

        Health checks run outside the manager lock: ``is_healthy`` belongs to
        the plugin and may be slow, and holding the lock across it blocked
        registration and shutdown. The restart delay is not slept here
        either; a plugin waiting to restart is started once its
        ``restart_due`` passes, so the other plugins stay monitored.
        """
        while self._state == PluginRunState.RUNNING:
            with self._lock:
                for plugin_name, plugin_info in self._plugins.items():
                    try:
                        now = datetime.now()
                        if plugin_info.last_health_check is not None and (
                            (now - plugin_info.last_health_check).seconds
                            < self._config.plugin_health_check_interval
                        ):
                            continue

                        plugin_info.last_health_check = now

                        if plugin_info.state == PluginRunState.RUNNING:
                            if (
                                not plugin_info.thread
                                or not plugin_info.thread.is_alive()
                            ):
                                self._logger.error(
                                    f"Plugin {plugin_name} thread died",
                                )
                                plugin_info.state = PluginRunState.FAILED
                                failed_plugins.append(plugin_info)
                            else:
                                is_healthy = plugin_info.plugin.is_healthy()
                                self._plugin_health_metric.labels(
                                    plugin_name=plugin_info.plugin.name,
                                    plugin_identifier=self._plugin_identifier(
                                        plugin_info.plugin,
                                    ),
                                ).set(1 if is_healthy else 0)

                                if not is_healthy:
                                    self._logger.warning(
                                        f"Plugin {plugin_name} is unhealthy",
                                    )
                                    plugin_info.state = PluginRunState.FAILED
                                    failed_plugins.append(plugin_info)

                                    with self._tracer.start_as_current_span(
                                        "plugin_lifecycle",
                                    ) as span:
                                        span.add_event(
                                            "plugin.health_check_failed",
                                            attributes={
                                                ATTR_PLUGIN_NAME: (
                                                    plugin_info.plugin.name
                                                ),
                                            },
                                        )

                    except CourierError:
                        self._logger.exception(
                            f"Error monitoring plugin {plugin_name}",
                        )
                    except Exception:
                        self._logger.exception(
                            f"Unexpected error monitoring plugin {plugin_name}",
                        )

            for plugin_info in failed_plugins:
                with self._lock:
                    # if the original reference was popped elsewhere
                    if plugin_info not in self._plugins.values():
                        failed_plugins.remove(plugin_info)
                snapshot = list(self._plugins.items())

            newly_failed = [
                plugin_info
                for plugin_name, plugin_info in snapshot
                if self._check_plugin(plugin_name, plugin_info)
            ]
            for plugin_info in newly_failed:
                self._handle_failed_plugin(plugin_info)

            for plugin_info in self._due_restarts(snapshot):
                self._start_plugin(plugin_info)

            time.sleep(1)

    def _check_plugin(self, plugin_name: str, plugin_info: PluginStateInfo) -> bool:
        """Health-check one plugin if its interval has elapsed.

        Returns
        -------
        bool
            True if the plugin needs handling by :meth:`_handle_failed_plugin`:
            it was found unhealthy by this check, or its start failed and it
            still has restart attempts left.
        """
        now = time.monotonic()
        with self._lock:
            if plugin_info.last_health_check is not None and (
                now - plugin_info.last_health_check
                < self._config.plugin_health_check_interval
            ):
                return False
            plugin_info.last_health_check = now
            if plugin_info.state == PluginRunState.FAILED:
                return self._restarts_left_locked(plugin_info)
            overdue = self._start_overdue_locked(plugin_info, now)
            if plugin_info.state != PluginRunState.RUNNING and not overdue:
                return False
        if overdue:
            self._abandon_stuck_start(plugin_info)
            return False
        return self._assess_running(plugin_name, plugin_info, now)

    def _start_overdue_locked(self, plugin_info: PluginStateInfo, now: float) -> bool:
        """Return whether a plugin's ``start()`` call has outlived its timeout.

        Only the call itself counts: ``starting_since`` is cleared when it
        returns, so a plugin in the health gate is never overdue.
        """
        return (
            plugin_info.state == PluginRunState.STARTING
            and plugin_info.starting_since is not None
            and now - plugin_info.starting_since > self._config.plugin_start_timeout
        )

    def _abandon_stuck_start(self, plugin_info: PluginStateInfo) -> None:
        """Give up on a ``start()`` that has not returned: fail it for good.

        The starter thread is blocked in something the manager cannot
        interrupt, and a restart would only stack a second stuck thread on
        the same dependency. So the plugin is marked FAILED with its restart
        attempts used up; the service's heartbeat then exits the process,
        which is the only recovery. The thread's stack is logged so the
        operator learns what it was waiting on without a debugger.
        """
        name = plugin_info.plugin.name
        timeout = self._config.plugin_start_timeout
        with self._lock:
            if plugin_info.state != PluginRunState.STARTING:
                return
            thread = plugin_info.thread
            stack = self._thread_stack(thread)
            plugin_info.state = PluginRunState.FAILED
            plugin_info.starting_since = None
            plugin_info.restart_count = max(
                plugin_info.restart_count,
                self._config.plugin_max_restart_attempts,
            )
            plugin_info.error_message = f"start() did not return within {timeout}s"
            if thread is not None and thread.is_alive():
                self._abandon_thread_locked(plugin_info, thread)
            plugin_info.thread = None
        self._record_state(plugin_info)
        self._logger.error(
            "Plugin %s start() did not return within %ss; the thread cannot "
            "be interrupted and the service will exit. It is blocked at:\n%s",
            name,
            timeout,
            stack,
        )
        with self._tracer.start_as_current_span("plugin_lifecycle") as span:
            span.add_event(
                "plugin.start_timed_out",
                attributes={ATTR_PLUGIN_NAME: name},
            )
        # Set the plugin's own stop flag in case the blocked call ever
        # returns. stop() may block on the same dependency, so it runs on a
        # thread of its own and the monitor does not wait for it. It is
        # recorded as abandoned straight away: status counts it only while
        # it is still running.
        stopper = threading.Thread(
            target=self._call_plugin_stop,
            args=(plugin_info,),
            name=f"Plugin-{name}-abandon",
            daemon=True,
        )
        with self._lock:
            # Started under the lock so no prune sees it before it is alive.
            self._abandon_thread_locked(plugin_info, stopper)
            stopper.start()

    @staticmethod
    def _thread_stack(thread: threading.Thread | None) -> str:
        """Return the formatted stack of *thread*, or a note if unavailable."""
        if thread is None or thread.ident is None:
            return "<no thread>"
        frame = sys._current_frames().get(thread.ident)
        if frame is None:
            return "<thread has no frame>"
        return "".join(traceback.format_stack(frame))

    def _assess_running(
        self,
        plugin_name: str,
        plugin_info: PluginStateInfo,
        now: float,
    ) -> bool:
        """Probe a RUNNING plugin; True if it was found unhealthy and marked."""
        healthy = self._probe_health(plugin_info)
        if healthy is None:
            # The probe itself broke. That says nothing certain either way:
            # neither fail the plugin nor count the time as healthy.
            return False
        if healthy:
            self._forgive_restarts(plugin_info, now)
            return False
        if not self._mark_failed(plugin_info):
            return False
        self._logger.error(f"Plugin {plugin_name} is unhealthy")
        return True

    def _is_running_locked(self) -> bool:
        """Return whether the manager is RUNNING. Lock held.

        A call rather than an inline comparison so a second check after
        releasing and retaking the lock is not narrowed away by mypy.
        """
        return self._state == PluginRunState.RUNNING

    def _restarts_left_locked(self, plugin_info: PluginStateInfo) -> bool:
        """Return whether the plugin has restart attempts left. Lock held."""
        return plugin_info.restart_count < self._config.plugin_max_restart_attempts

    def _is_exhausted_locked(self, plugin_info: PluginStateInfo) -> bool:
        """Return whether a plugin is FAILED with no restarts left. Lock held."""
        return (
            plugin_info.state == PluginRunState.FAILED
            and not self._restarts_left_locked(plugin_info)
        )

    def _forgive_restarts(self, plugin_info: PluginStateInfo, now: float) -> None:
        """Reset the restart count once a restarted plugin has stayed healthy.

        The window is measured from the moment the plugin reached RUNNING,
        not from when its restart was scheduled, so the delay before the
        restart does not count as time spent healthy. Without forgiveness a
        plugin had ``plugin_max_restart_attempts`` failures for the life of
        the process, however far apart; three transient faults in a month
        ended it.
        """
        with self._lock:
            if (
                plugin_info.restart_count == 0
                or plugin_info.running_since is None
                or now - plugin_info.running_since < self._config.plugin_restart_window
            ):
                return
            plugin_info.restart_count = 0
        self._logger.info(
            f"Plugin {plugin_info.plugin.name} healthy for "
            f"{self._config.plugin_restart_window}s; restart attempts reset",
        )

    def _mark_failed(self, plugin_info: PluginStateInfo) -> bool:
        """Move a still-RUNNING plugin to FAILED; False if stop() got there first."""
        with self._lock:
            if plugin_info.state != PluginRunState.RUNNING:
                return False
            plugin_info.state = PluginRunState.FAILED
            return True

    def _probe_health(self, plugin_info: PluginStateInfo) -> bool | None:
        """Ask the plugin whether it is healthy; None if the probe raised.

        The health gauge records 1 or 0 for a definite answer and is left
        alone when the probe raised.
        """
        try:
            is_healthy = plugin_info.plugin.is_healthy()
        except Exception:
            self._logger.exception(
                f"Error monitoring plugin {plugin_info.plugin.name}",
            )
            return None
        self._plugin_health_metric.labels(
            plugin_name=plugin_info.plugin.name,
            plugin_identifier=self._plugin_identifier(plugin_info.plugin),
        ).set(1 if is_healthy else 0)
        if not is_healthy:
            with self._tracer.start_as_current_span("plugin_lifecycle") as span:
                span.add_event(
                    "plugin.health_check_failed",
                    attributes={ATTR_PLUGIN_NAME: plugin_info.plugin.name},
                )
        return is_healthy

    def _due_restarts(
        self,
        snapshot: list[tuple[str, PluginStateInfo]],
    ) -> list[PluginStateInfo]:
        """Return the plugins whose restart delay has elapsed, clearing it."""
        now = time.monotonic()
        due: list[PluginStateInfo] = []
        with self._lock:
            for _name, plugin_info in snapshot:
                if (
                    plugin_info.state == PluginRunState.RESTARTING
                    and plugin_info.restart_due is not None
                    and now >= plugin_info.restart_due
                ):
                    plugin_info.restart_due = None
                    due.append(plugin_info)
        return due

    def _handle_failed_plugin(self, plugin_info: PluginStateInfo) -> None:
        """Stop a failed plugin and schedule its restart, or give up on it.

        The restart itself happens in :meth:`_monitor_plugins` once
        ``restart_due`` passes. Sleeping through ``plugin_restart_delay``
        here left every other plugin unmonitored for the whole delay.

        Parameters
        ----------
        plugin_info : PluginStateInfo
            Information about the failed plugin.
        """
        plugin_name = plugin_info.plugin.name
        plugin_ident = self._plugin_identifier(plugin_info.plugin)

        with self._lock:
            if not self._is_running_locked():
                return
            can_restart = self._restarts_left_locked(plugin_info)

            if can_restart:
                self._logger.info(
                    f"Attempting to restart plugin {plugin_name} "
                    f"(attempt {plugin_info.restart_count + 1}/"
                    f"{self._config.plugin_max_restart_attempts})",
                )

                with self._tracer.start_as_current_span(
                    "plugin_lifecycle",
                ) as span:
                    span.add_event(
                        "plugin.restarting",
                        attributes={
                            ATTR_PLUGIN_NAME: plugin_name,
                        },
                    )

                plugin_info.state = PluginRunState.RESTARTING
                plugin_info.restart_count += 1
                plugin_info.last_restart = datetime.now()

                self._plugin_restart_metric.labels(
                    plugin_name=plugin_name,
                    plugin_identifier=plugin_ident,
                ).inc()
            else:
                self._logger.error(
                    f"Plugin {plugin_name} failed and cannot be restarted: "
                    f"{self._config.plugin_max_restart_attempts} restart "
                    "attempts used",
                )
                self._record_state(plugin_info)

        if can_restart:
            self._stop_plugin(plugin_info)
            with self._lock:
                # stop() may have run while the plugin was stopping; it is
                # STOPPED now and must stay so.
                if not self._is_running_locked():
                    return
                plugin_info.state = PluginRunState.RESTARTING
                plugin_info.restart_due = (
                    time.monotonic() + self._config.plugin_restart_delay
                )
        else:
            # Given up on, but its worker may still hold work: tell it to
            # stop now. It stays FAILED so exhausted_plugins() reports it,
            # and stop() leaves FAILED plugins alone, so this is the one call.
            self._call_plugin_stop(plugin_info)

    @log_execution
    def start(self) -> None:
        """Start the plugin manager and all registered plugins."""
        with self._lock:
            if self._state == PluginRunState.RUNNING:
                return
            self._state = PluginRunState.RUNNING

            self._monitor_thread = threading.Thread(
                target=self._monitor_plugins,
                name="PluginMonitor",
                daemon=True,
            )
            self._monitor_thread.start()
            if self._config.live_reload != "off":
                self._config_monitor.start()


        # Phase 1: Start every plugin.
        #
        # Start order does not matter here. File-found queues are durable and
        # are declared during preflight, so a file published before its builder
        # attaches waits on the broker.
        with self._lock:
            for plugin_info in self._plugins.values():
                self._start_plugin(plugin_info)

        # Phase 2: Wait for all plugins to signal readiness outside lock.
        deadline = time.time() + self._config.plugin_health_check_interval
        with self._lock:
            plugins = list(self._plugins.values())
        for plugin_info in plugins:
            remaining = max(0.0, deadline - time.time())
            plugin_info.ready.wait(timeout=remaining)

        # Phase 3: Verify at least one plugin started successfully.
        with self._lock:
            running = [info for info in plugins if info.state == PluginRunState.RUNNING]
            failed = [info for info in plugins if info.state == PluginRunState.FAILED]
        if failed and not running:
            names = ", ".join(
                f"{info.plugin.name}={info.error_message or 'unknown'}"
                for info in failed
            )
            raise RuntimeError(f"All plugins failed to start: {names}")
        for info in plugins:
            if not info.ready.is_set() and info.state != PluginRunState.FAILED:
                self._logger.warning(
                    "Plugin %s did not signal readiness within deadline; "
                    "monitor will catch failures.",
                    info.plugin.name,
                )

        # Phase 4: Only now start the monitor. Started earlier, it could
        # schedule a restart for a plugin whose start failed before Phase 3
        # looked, and "all plugins failed" would go unnoticed.
        self._monitor_thread = threading.Thread(
            target=self._monitor_plugins,
            name="PluginMonitor",
            daemon=True,
        )
        self._monitor_thread.start()

    def stop(self) -> None:
        """Stop all plugins and the plugin manager."""
        with self._lock:
            self._state = PluginRunState.STOPPED
            plugins = list(self._plugins.values())

        for plugin_info in plugins:
            self._stop_plugin(plugin_info)

        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=5)
        if self._config.live_reload != "off":
            self._config_monitor.stop()

        self._logger.info("Plugin manager stopped")

    def get_plugins(self) -> dict[str, PluginStateInfo]:
        """Return a snapshot of all registered plugins and their state.

        The returned dict is a shallow copy taken under the internal lock,
        safe for read-only access by external callers such as
        :class:`~courier.service.Service` during preflight routing discovery.

        Returns
        -------
        dict[str, PluginStateInfo]
            Mapping from registry key to plugin state information.
        """
        with self._lock:
            return self._plugins.copy()

    def is_healthy(self) -> bool:
        """Return whether every plugin is running healthily or on its way back.

        Unhealthy if any plugin has failed for good, or any RUNNING plugin
        reports itself unhealthy. Plugins in transit do not count against
        health: starting (a ``start()`` call within ``plugin_start_timeout``,
        or the health gate after it), restarting, stopping, or RUNNING but
        not yet examined by the monitor. That last case covers a plugin
        whose start gate timed out; the monitor decides whether it is failed,
        so the service's own startup check does not abort over a slow plugin.
        A ``start()`` call past its timeout is unhealthy. With no plugins
        registered there is nothing to be unhealthy.

        Plugin health checks run outside the manager lock: they belong to
        the plugin and may block.

        Returns
        -------
        bool
            True if manager is healthy, False otherwise.
        """
        with self._lock:
            if self._state != PluginRunState.RUNNING:
                # If plugins are registered but not started, not healthy.
                # If no plugins are registered, nothing to monitor: healthy.
                return not bool(self._plugins)
            now = time.monotonic()
            if any(
                self._is_exhausted_locked(i) or self._start_overdue_locked(i, now)
                for i in self._plugins.values()
            ):
                return False
            running = [
                info
                for info in self._plugins.values()
                if info.state == PluginRunState.RUNNING
                and info.last_health_check is not None
            ]
        return all(info.plugin.is_healthy() for info in running)

    def exhausted_plugins(self) -> list[str]:
        """Return the plugins that failed and have no restart attempts left.

        Returns
        -------
        list[str]
            Registry keys of exhausted plugins, in registration order.
        """
        with self._lock:
            return [
                name
                for name, info in self._plugins.items()
                if self._is_exhausted_locked(info)
            ]

    def get_plugin_status(self) -> dict[str, dict[str, Any]]:
        """Get current status of all plugins.

        Returns
        -------
        dict[str, dict[str, Any]]
            Dictionary mapping plugin names to their status information.
        """
        with self._lock:
            return {
                name: {
                    "state": info.state.name,
                    "version": info.plugin.version,
                    "restart_count": info.restart_count,
                    "abandoned_threads": self._live_abandoned_locked(info),
                    "last_restart": (
                        info.last_restart.isoformat() if info.last_restart else None
                    ),
                    "error": info.error_message,
                    "metrics": (
                        info.plugin.get_metrics()
                        if info.state == PluginRunState.RUNNING
                        else {}
                    ),
                }
                for name, info in self._plugins.items()
            }
