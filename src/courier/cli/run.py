"""CLI `run` command — loads config and starts the service."""

import importlib
import logging
from pathlib import Path
from typing import Any

import typer

from courier.cli.config_loader import load_config
from courier.config import ServiceConfig
from courier.plugins.classes.job_builders.filter_and_group import (
    FilterAndGroupJobBuilder,
)
from courier.service import create_service_with_plugins

_logger = logging.getLogger(__name__)

_MODULE_BASED_INTERFACES = ("data_monitors", "dispatchers", "job_builders")


def _discover_plugins_from_registry() -> dict[str, type]:
    """Build a ``name.lower()`` → plugin-class map from the pluginify registry.

    Returns an empty dict if the registry cannot be read, so callers can
    fall back to the hardcoded list.
    """
    try:
        # Deferred imports to keep the registry lazy and avoid circular issues
        from pluginify.plugin_registry import PluginRegistry  # noqa: PLC0415

        from courier.cli.registry import (  # noqa: PLC0415
            COURIER_NAMESPACE,
            ensure_registry,
        )
    except ImportError:
        _logger.debug("pluginify not available; falling back to hardcoded plugins")
        return {}

    try:
        ensure_registry()
    except Exception:
        _logger.debug(
            "Failed to ensure pluginify registries; falling back to hardcoded plugins",
        )
        return {}

    try:
        registry = PluginRegistry(namespace=COURIER_NAMESPACE)
        class_based = registry.registered_class_based_plugins
    except Exception:
        _logger.debug("Failed to load pluginify registry; falling back")
        return {}

    discovered: dict[str, type] = {}
    for interface_name in _MODULE_BASED_INTERFACES:
        for plugin_name, meta in class_based.get(interface_name, {}).items():
            if meta.get("is_derived_from_module"):
                _logger.debug(
                    "Skipping module-derived plugin %r (not yet supported)",
                    plugin_name,
                )
                continue
            try:
                package: str = meta["package"]
                relpath: str = meta["relpath"]
                module_path = package + "." + relpath.replace("/", ".").removesuffix(
                    ".py",
                )
                module = importlib.import_module(module_path)
                plugin_cls = module.PLUGIN_CLASS
                key = plugin_cls.name.lower()
                discovered[key] = plugin_cls
            except Exception:
                _logger.debug(
                    "Failed to load plugin %r from %r",
                    plugin_name,
                    meta.get("relpath", "?"),
                    exc_info=True,
                )

    _logger.debug(
        "Discovered %d plugins from pluginify registry across %d interfaces",
        len(discovered),
        len(_MODULE_BASED_INTERFACES),
    )
    return discovered


# Hardcoded fallback — used when pluginify registries are unavailable.
# Keep in sync with the actual plugins in courier.plugins.classes.
def _hardcoded_plugins() -> dict[str, type]:
    """Return the hardcoded plugin name→class map."""
    import courier.plugins.classes.data_monitors.file_system_poller_watchdog as _fsp  # noqa: PLC0415
    import courier.plugins.classes.dispatchers.serial_bash as _sbd  # noqa: PLC0415
    from courier.plugins.classes.data_monitors.kafka_consumer import (  # noqa: PLC0415
        KafkaConsumer,
    )
    from courier.plugins.classes.data_monitors.rabbit_mq_watcher import (  # noqa: PLC0415
        RabbitMQWatcher,
    )
    from courier.plugins.classes.data_monitors.s3_poller import (  # noqa: PLC0415
        S3Poller,
    )
    from courier.plugins.classes.data_monitors.sftp_poller import (  # noqa: PLC0415
        SftpPoller,
    )
    from courier.plugins.classes.dispatchers.http_dispatcher import (  # noqa: PLC0415
        HttpDispatcher,
    )
    from courier.plugins.classes.dispatchers.parallel_bash import (  # noqa: PLC0415
        ParallelBashDispatcher,
    )
    from courier.plugins.classes.dispatchers.slurm_dispatcher import (  # noqa: PLC0415
        SlurmDispatcher,
    )
    from courier.plugins.classes.job_builders import (  # noqa: PLC0415
        dummy_job_builder as _djb,
    )
    from courier.plugins.classes.job_builders.file_count_builder import (  # noqa: PLC0415
        FileCountBuilder,
    )
    from courier.plugins.classes.job_builders.metadata_router import (  # noqa: PLC0415
        MetadataRouterBuilder,
    )
    return {
        cls.name.lower(): cls
        for cls in (
            _fsp.FileSystemPoller,
            _djb.DummyJobBuilder,
            _sbd.SerialBashDispatcher,
            RabbitMQWatcher,
            FilterAndGroupJobBuilder,
            S3Poller,
            SftpPoller,
            KafkaConsumer,
            MetadataRouterBuilder,
            FileCountBuilder,
            ParallelBashDispatcher,
            SlurmDispatcher,
            HttpDispatcher,
        )
    }


_AVAILABLE_PLUGINS: dict[str, type] = _discover_plugins_from_registry()
if not _AVAILABLE_PLUGINS:
    _AVAILABLE_PLUGINS = _hardcoded_plugins()
    _logger.info("Using hardcoded plugin list (pluginify registry unavailable)")

# Deprecated alias; retained so existing configs using `filter_pass` keep working.
_AVAILABLE_PLUGINS["filter_pass"] = _AVAILABLE_PLUGINS.get(
    "filterandgroupjobbuilder", FilterAndGroupJobBuilder,
)


def _resolve_plugin(plugin: Any) -> tuple[type, dict, str | None]:
    """Resolve a plugin config entry to a (class, config, identifier) tuple.

    Parameters
    ----------
    plugin : Any
        Microservice model entry with spec.name, spec.config, and identifier.

    Returns
    -------
    tuple[type, dict, str | None]
        Plugin class, configuration dict, and the YAML ``identifier``.

    Raises
    ------
    ValueError
        If the plugin name is not registered.
    """
    name = plugin.spec.name.lower()
    if name not in _AVAILABLE_PLUGINS:
        raise ValueError(f"Plugin {plugin.spec.name} not found.")
    return (_AVAILABLE_PLUGINS[name], plugin.spec.config, plugin.identifier)


def _collect_builder_targets(config: Any) -> dict[str, tuple[str, ...]]:
    """Flatten declared ``targets`` per builder for preflight validation.

    Returns a mapping from builder identifier to the union of every
    target declared under its config (across routes, for builders like
    ``metadata_router``). An empty tuple means "no target declared" and
    tells preflight to resolve via ``allow_implicit_target``.
    """
    out: dict[str, tuple[str, ...]] = {}
    for entry in config.spec.run:
        if entry.spec.kind != "job_builders":
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
    service = create_service_with_plugins(service_config, plugins)
    dispatcher_ids = {
        entry.identifier
        for entry in config.spec.run
        if entry.spec.kind == "dispatchers"
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


def run(config_file: Path) -> None:
    """Run the service with a config file."""
    if not config_file.exists():
        typer.echo(f"Error: File {config_file} not found")
        raise typer.Exit(1)

    config = load_config(config_file)
    run_service(config)
