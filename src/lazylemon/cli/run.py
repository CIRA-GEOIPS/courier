"""CLI `run` command — loads config and starts the service."""

from pathlib import Path
from typing import Any

import typer

import lazylemon.plugins.modules.dispatchers.serial_bash as serial_bash_dispatcher
from lazylemon.cli.config_loader import load_config
from lazylemon.config import ServiceConfig
from lazylemon.plugins.modules.data_monitors import file_system_poller_watchdog
from lazylemon.plugins.modules.data_monitors.rabbit_mq_watcher import RabbitMQWatcher
from lazylemon.plugins.modules.job_builders import dummy_job_builder
from lazylemon.service import create_service_with_plugins

AVAILABLE_PLUGINS = {
    cls.name.lower(): cls
    for cls in (
        file_system_poller_watchdog.FileSystemPoller,
        dummy_job_builder.DummyJobBuilder,
        serial_bash_dispatcher.SerialBashDispatcher,
        RabbitMQWatcher,
    )
}


def _resolve_plugin(plugin: Any) -> tuple[type, dict]:
    """Resolve a plugin config entry to a (class, config) tuple.

    Parameters
    ----------
    plugin : Any
        Microservice model entry with spec.name and spec.config.

    Returns
    -------
    tuple[type, dict]
        Plugin class and its configuration dict.

    Raises
    ------
    ValueError
        If the plugin name is not registered.
    """
    name = plugin.spec.name.lower()
    if name not in AVAILABLE_PLUGINS:
        raise ValueError(f"Plugin {plugin.spec.name} not found.")
    return (AVAILABLE_PLUGINS[name], plugin.spec.config)


def run_service(config: Any) -> None:
    """Build and start the service from a validated config model.

    Parameters
    ----------
    config : Any
        Validated ServiceConfigModel instance.
    """
    service_config = ServiceConfig(
        broker_url=config.spec.broker.to_url(),
        namespace=config.metadata.namespace or "default",
        service_id=config.metadata.name,
        heartbeat_interval=config.spec.heartbeat_interval,
        broker_max_retries=config.spec.broker.max_retries,
    )
    plugins = list(map(_resolve_plugin, config.spec.run))
    create_service_with_plugins(service_config, plugins).start()


def run(config_file: Path) -> None:
    """Run the service with a config file."""
    if not config_file.exists():
        typer.echo(f"Error: File {config_file} not found")
        raise typer.Exit(1)

    config = load_config(config_file)
    run_service(config)
