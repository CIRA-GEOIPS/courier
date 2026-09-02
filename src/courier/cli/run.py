"""CLI `run` command — loads config and starts the service."""

from __future__ import annotations

import dataclasses
import logging
from pathlib import (
    Path,  # noqa: TC003 — needed at runtime for Typer annotation introspection
)
from typing import TYPE_CHECKING, Annotated, Any

import typer

from courier.cli.feedback import load_config_or_exit
from courier.cli.plugins import PLUGIN_REGISTRIES, RUN_KINDS, normalize_kind
from courier.service import create_service_with_plugins

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from courier.interfaces.plugin_protocol import ServicePlugin


def _collect_builder_targets(config: Any) -> dict[str, tuple[str, ...]]:
    """Flatten declared ``targets`` per builder for preflight validation.

    Returns a mapping from builder identifier to the union of every
    target declared under its config (across routes, for builders like
    ``metadata_router``). An empty tuple means "no target declared" and
    tells preflight to resolve via ``allow_implicit_target``.
    """
    out: dict[str, tuple[str, ...]] = {}
    for entry in config.spec.run:
        if normalize_kind(entry.spec.kind) != "job_builders":
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


def run_service(
    config: Any,
    log_level: str | None = None,
    *,
    only_set: set[str] | None = None,
) -> None:
    """Build and start the service from a validated config model.

    Parameters
    ----------
    config : Any
        Validated ServiceConfigModel instance.

    log_level : str or None, optional
        Log level from CLI --log-level flag. If None, uses
        ServiceConfig default (env var COURIER_LOG_LEVEL or 'DEBUG').

    only_set : set[str] or None, optional
        If set, only run plugins whose identifiers are in this set.
        Keyword-only; passed from the ``--only`` CLI flag.
    """
    # Use the CLI-provided log level if given so the parameter is actually used
    if log_level is not None:
        try:
            lvl = getattr(logging, log_level.upper())
            logging.getLogger().setLevel(lvl)
        except Exception:
            logger.warning(
                "Invalid log level %r; leaving logger level unchanged",
                log_level,
            )

    # since ServiceClass is an immutable object, we replace all necessary attributes
    # from the parent class into the `spec.service_config` overrides
    service_config = dataclasses.replace(
        config.spec.service_config,
        broker_url=config.spec.broker.to_url(),
        namespace=config.metadata.namespace or "default",
        service_id=config.metadata.name,
    )
    # Build plugin registration tuples from the config's run spec.
    plugin_registrations: list[
        tuple[type[ServicePlugin], dict[str, Any], str | None]
    ] = []

    # --only validation
    if only_set is not None:
        all_ids = {e.identifier for e in config.spec.run}
        unknown = only_set - all_ids
        if unknown:
            raise ValueError(
                f"Unknown plugin identifiers: {', '.join(sorted(unknown))}. "
                f"Available: {', '.join(sorted(all_ids))}",
            )
        dmc_ids = {
            e.identifier
            for e in config.spec.run
            if e.spec.kind == "data_monitor_configs"
        }
        dmc_in_only = only_set & dmc_ids
        if dmc_in_only:
            raise ValueError(
                f"'data_monitor_configs' entries cannot be run with --only: "
                f"{', '.join(sorted(dmc_in_only))}. "
                "Use --only with data_monitor,"
                " job_builder, or dispatcher identifiers.",
            )

    for entry in config.spec.run:
        if only_set is not None and entry.identifier not in only_set:
            continue
        kind = normalize_kind(entry.spec.kind)
        # An unrecognised kind used to be skipped silently, which produced a
        # service that started up, reported healthy, and processed nothing.
        if kind not in RUN_KINDS:
            raise ValueError(
                f"{entry.identifier!r}: {entry.spec.kind!r} is not a runnable "
                f"kind. Valid kinds: {', '.join(sorted(RUN_KINDS))}.",
            )
        plugin_class = PLUGIN_REGISTRIES[kind].get_plugin(entry.spec.name)
        plugin_config: dict[str, Any] = (
            entry.spec.config if entry.spec.config is not None else {}
        )
        plugin_registrations.append((plugin_class, plugin_config, entry.identifier))

    service = create_service_with_plugins(
        service_config,
        plugin_registrations,
    )
    dispatcher_ids = {
        e.identifier
        for e in config.spec.run
        if normalize_kind(e.spec.kind) == "dispatchers"
        and (only_set is None or e.identifier in only_set)
    }
    # Union: add any dispatcher targeted by builders in the filtered set
    builder_targets = _collect_builder_targets(config)
    if only_set is not None:
        # Filter builder_targets to only builders in only_set
        builder_targets = {
            bid: targets for bid, targets in builder_targets.items() if bid in only_set
        }
        # Add targets of included builders to dispatcher_ids
        # (queues must be pre-declared on broker even if dispatcher runs elsewhere)
        for targets in builder_targets.values():
            dispatcher_ids.update(targets)
    service.configure_routing(
        dispatcher_identifiers=dispatcher_ids,
        builder_targets=builder_targets,
        allow_implicit_target=getattr(
            config.spec,
            "allow_implicit_target",
            True,
        ),
    )
    service.start()


def run(
    ctx: typer.Context,
    config_file: Annotated[
        Path,
        typer.Argument(
            metavar="CONFIG",
            help="Service YAML describing the pipeline to run.",
        ),
    ],
    only: str | None = typer.Option(
        None,
        "--only",
        help="Comma-separated plugin identifiers to run. "
        "Allows one config to serve multiple containers: "
        "e.g. 'courier run config.yaml --only my-dm' for the data monitor, "
        "'courier run config.yaml --only my-builder,my-dispatcher'"
        " for processing.",
    ),
) -> None:
    """Run the service with a config file."""
    config = load_config_or_exit(config_file)
    log_level = ctx.obj.get("log_level") if ctx.obj else None

    # Parse --only
    if only is None:
        only_set = None
    elif not only.strip():
        logger.debug("empty --only, running all plugins")
        only_set = None
    else:
        parts = [p.strip().lower() for p in only.split(",") if p.strip()]
        only_set = set(parts)  # deduplicate via set

    try:
        run_service(config, log_level=log_level, only_set=only_set)
    except typer.Exit:
        raise
    except Exception as exc:
        logger.exception("Fatal error in run_service")
        raise typer.Exit(code=1) from exc
