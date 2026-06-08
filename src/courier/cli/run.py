"""CLI `run` command — loads config and starts the service."""

from pathlib import Path
from typing import Any

import typer

import courier.plugins.classes.dispatchers.serial_bash as serial_bash_dispatcher
from courier.cli.config_loader import load_config
from courier.config import ServiceConfig
from courier.plugins.classes.data_monitors import file_system_poller_watchdog
from courier.plugins.classes.data_monitors.kafka_consumer import KafkaConsumer
from courier.plugins.classes.data_monitors.rabbit_mq_watcher import RabbitMQWatcher
from courier.plugins.classes.data_monitors.s3_poller import S3Poller
from courier.plugins.classes.data_monitors.sftp_poller import SftpPoller
from courier.plugins.classes.dispatchers.http_dispatcher import HttpDispatcher
from courier.plugins.classes.dispatchers.parallel_bash import ParallelBashDispatcher
from courier.plugins.classes.dispatchers.slurm_dispatcher import SlurmDispatcher
from courier.plugins.classes.job_builders import dummy_job_builder
from courier.plugins.classes.job_builders.file_count_builder import FileCountBuilder
from courier.plugins.classes.job_builders.filter_and_group import (
    FilterAndGroupJobBuilder,
)
from courier.cli.plugins import get_plugins
from courier.plugins.classes.job_builders.metadata_router import MetadataRouterBuilder
from courier.service import create_service_with_plugins

def _collect_builder_targets(config: Any) -> dict[str, tuple[str, ...]]:
    """Flatten declared ``targets`` per builder for preflight validation.

    Returns a mapping from builder identifier to the union of every
    target declared under its config (across routes, for builders like
    ``metadata_router``). An empty tuple means "no target declared" and
    tells preflight to resolve via ``allow_implicit_target``.
    """
    out: dict[str, tuple[str, ...]] = {}
    for entry in config.spec.run:
        if entry.spec.kind != "job_builder":
            continue
        cfg = entry.spec.config or {}
        declared: list[str] = []
        if isinstance(cfg.get("routes"), list):
            for route in cfg["routes"]:
                declared.extend(route.get("targets") or [])
        else:
            declared.extend(cfg.get("targets") or [])
        out[entry.identifier] = tuple(declared)
    return out


def run_service(config: Any, log_level: str | None = None) -> None:
    """Build and start the service from a validated config model.

    Parameters
    ----------
    config : Any
        Validated ServiceConfigModel instance.
    log_level : str or None, optional
        Log level from CLI --log-level flag. If None, uses
        ServiceConfig default (env var COURIER_LOG_LEVEL or 'DEBUG').
    """
    if log_level is not None:
        service_config = ServiceConfig(
            broker_url=config.spec.broker.to_url(),
            namespace=config.metadata.namespace or "default",
            service_id=config.metadata.name,
            heartbeat_interval=config.spec.heartbeat_interval,
            broker_max_retries=config.spec.broker.max_retries,
            log_level=log_level,
        )
    else:
        service_config = ServiceConfig(
            broker_url=config.spec.broker.to_url(),
            namespace=config.metadata.namespace or "default",
            service_id=config.metadata.name,
            heartbeat_interval=config.spec.heartbeat_interval,
            broker_max_retries=config.spec.broker.max_retries,
        )
    plugins = get_plugins(config_file=config, namespace=None)[1]
    service = create_service_with_plugins(service_config, plugins)
    dispatcher_ids = {
        entry.identifier
        for entry in config.spec.run
        if entry.spec.kind == "dispatcher"
    }
    service.configure_routing(
        dispatcher_identifiers=dispatcher_ids,
        builder_targets=_collect_builder_targets(config),
        allow_implicit_target=getattr(
            config.spec,
            "allow_implicit_target",
            True,
        ),
    )
    service.start()


def run(
    ctx: typer.Context,
    config_file: Path,
) -> None:
    """Run the service with a config file."""
    if not config_file.exists():
        typer.echo(f"Error: File {config_file} not found")
        raise typer.Exit(1)

    config = load_config(config_file)
    log_level = ctx.obj.get("log_level") if ctx.obj else None
    run_service(config, log_level=log_level)
