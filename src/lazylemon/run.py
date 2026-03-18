"""Service entry point for lazylemon."""

from typing import Any

import lazylemon.plugins.modules.dispatchers.serial_bash as serial_bash_dispatcher
from lazylemon import dummy_cli
from lazylemon.interfaces.module_based.service import (
    ServiceConfig,
    create_service_with_plugins,
)
from lazylemon.plugins.modules.data_monitors import file_system_poller_watchdog
from lazylemon.plugins.modules.data_monitors.rabbit_mq_watcher import (
    RabbitMQWatcher,
)
from lazylemon.plugins.modules.job_builders import dummy_job_builder

AVAILABLE_PLUGINS = {
    cls.name.lower(): cls
    for cls in (
        file_system_poller_watchdog.FileSystemPoller,
        dummy_job_builder.DummyJobBuilder,
        serial_bash_dispatcher.SerialBashDispatcher,
        RabbitMQWatcher,
    )
}


def _build_broker_url(broker: Any) -> str:
    """Construct the broker AMQP URL from config."""
    return f"amqp://{broker.username}:{broker.password}@{broker.host}:{broker.port}/"


def _resolve_plugin(plugin: Any) -> tuple[type, dict]:
    """Resolve a plugin config entry to a (class, config) tuple."""
    name = plugin.spec.name.lower()
    if name not in AVAILABLE_PLUGINS:
        raise ValueError(f"Plugin {plugin.spec.name} not found.")
    return (AVAILABLE_PLUGINS[name], plugin.spec.config)


def run_service(config: dict) -> None:
    """Run a dummy service using the dummy-cli module."""
    service_config = ServiceConfig(
        broker_url=_build_broker_url(config.spec.broker),  # type: ignore
        service_namespace=config.spec.service_namespace,  # type: ignore
        service_id=config.name,  # type: ignore
        heartbeat_interval=config.spec.heartbeat_interval,  # type: ignore
    )
    plugins = list(map(_resolve_plugin, config.spec.run))  # type: ignore
    create_service_with_plugins(service_config, plugins).start()


dummy_cli.run_with_config = run_service  # type: ignore
dummy_cli.app()
