"""CLI `run` command — loads config and starts the service."""

import importlib
import logging
from pathlib import Path
from typing import Any

import typer

from courier.cli.config_loader import load_config
from courier.config import ServiceConfig
from courier.service import create_service_with_plugins

_logger = logging.getLogger(__name__)

_MODULE_BASED_INTERFACES = ("data_monitors", "dispatchers", "job_builders")

# — lazily populated cache: built on first resolve, never rebuilt —
_plugin_class_cache: dict[str, type] = {}
_cache_initialized = False


def _init_plugin_cache() -> None:
    """Import every class-based plugin from the pluginify registry.

    Called automatically on the first call to :func:`_resolve_plugin`.
    Lazy at the module level — no plugin or registry imports happen
    until the first ``courier run`` resolution.
    """
    global _cache_initialized  # noqa: PLW0603
    if _cache_initialized:
        return

    from pluginify.plugin_registry import PluginRegistry  # noqa: PLC0415

    from courier.cli.registry import (  # noqa: PLC0415
        COURIER_NAMESPACE,
        ensure_registry,
    )

    ensure_registry()
    registry = PluginRegistry(namespace=COURIER_NAMESPACE)
    class_based = registry.registered_class_based_plugins

    for iface in _MODULE_BASED_INTERFACES:
        for _pname, meta in class_based.get(iface, {}).items():
            if meta.get("is_derived_from_module"):
                _logger.debug("Skipping module-derived plugin %r", _pname)
                continue
            try:
                pkg: str = meta["package"]
                rp: str = meta["relpath"]
                modpath = pkg + "." + rp.replace("/", ".").removesuffix(".py")
                module = importlib.import_module(modpath)
                cls = module.PLUGIN_CLASS
                _plugin_class_cache[cls.name.lower()] = cls
            except Exception:
                _logger.debug(
                    "Failed to load plugin %r from %r",
                    _pname,
                    meta.get("relpath", "?"),
                    exc_info=True,
                )

    # Deprecated alias — keep existing configs using ``filter_pass`` working.
    if "filterandgroupjobbuilder" in _plugin_class_cache:
        _plugin_class_cache["filter_pass"] = _plugin_class_cache[
            "filterandgroupjobbuilder"
        ]

    _logger.debug(
        "Loaded %d plugins from registry across %d interfaces",
        len(_plugin_class_cache),
        len(_MODULE_BASED_INTERFACES),
    )
    _cache_initialized = True


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
    _init_plugin_cache()
    name = plugin.spec.name.lower()
    if name not in _plugin_class_cache:
        raise ValueError(f"Plugin {plugin.spec.name} not found.")
    return (_plugin_class_cache[name], plugin.spec.config, plugin.identifier)


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
