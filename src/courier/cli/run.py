"""CLI `run` command — loads config and starts the service."""

from __future__ import annotations

import dataclasses
import logging
import os
import re
from pathlib import (
    Path,  # noqa: TC003 — needed at runtime for Typer annotation introspection
)
from typing import TYPE_CHECKING, Annotated, Any

import typer

from courier.cli.feedback import load_config_or_exit
from courier.cli.plugins import (
    KIND_INFO,
    PLUGIN_REGISTRIES,
    RUN_KINDS,
    normalize_kind,
)
from courier.errors import InvalidPluginConfigError
from courier.interfaces.job_builders import PAYLOAD_KEY, parse_payload_block
from courier.schema.v1alpha1.service_config import MicroserviceModel
from courier.service import PipelineTopology, create_service_with_plugins

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


def _collect_topology(config: Any) -> PipelineTopology:
    """Describe every step in the YAML for preflight's static checks.

    Unlike plugin registration this ignores ``--only``: a container running
    only a builder or only a dispatcher still needs to know what runs at the
    other end of each route to check payload/dispatcher compatibility.

    Parameters
    ----------
    config : Any
        Validated service configuration model.

    Returns
    -------
    PipelineTopology
        Dispatcher plugin names, builder payload plugin names and declared
        builder targets for the whole YAML.
    """
    return PipelineTopology(
        dispatcher_plugins={
            entry.identifier: entry.spec.name
            for entry in config.spec.run
            if normalize_kind(entry.spec.kind) == "dispatchers"
        },
        builder_payloads={
            entry.identifier: name
            for entry in config.spec.run
            if normalize_kind(entry.spec.kind) == "job_builders"
            and (name := _payload_plugin_name(entry)) is not None
        },
        builder_targets=_collect_builder_targets(config),
    )


def _payload_plugin_name(entry: Any) -> str | None:
    """Return the plugin name a builder's ``payload`` block names, if valid.

    A missing or malformed block, or one whose kind is not ``payload``, yields
    ``None``: it is reported as an error by whichever process runs that
    builder, not misreported as a missing payload plugin by every process
    reading the YAML.
    """
    try:
        block = parse_payload_block(entry.identifier, entry.spec.config)
    except InvalidPluginConfigError:
        return None
    return str(block.spec.name)


def _nested_model(raw: Any) -> MicroserviceModel | None:
    """Parse a nested sub-plugin block, or return ``None`` if it is not one.

    Parameters
    ----------
    raw : Any
        The value of a nested config section, such as a builder's ``payload``.

    Returns
    -------
    MicroserviceModel or None
        The parsed sub-plugin, or ``None`` for anything that is not a single
        valid sub-plugin mapping.
    """
    if not isinstance(raw, dict):
        return None
    try:
        return MicroserviceModel.model_validate(raw)
    except (ValueError, TypeError):  # ValidationError subclasses ValueError
        return None


def _check_nested_identifiers(config: Any) -> None:
    """Reject a nested sub-plugin identifier used anywhere else in the YAML.

    Nested sub-plugins share the plugin manager's keyspace and the
    ``plugin_identifier`` metric label with run steps. The schema keeps run
    step identifiers unique; this does the same for nested ones across the
    whole YAML, ignoring ``--only``, so a collision fails in every container
    of a split deployment rather than only in one that hosts both plugins.

    Parameters
    ----------
    config : Any
        Validated service configuration model.

    Raises
    ------
    InvalidPluginConfigError
        If a nested identifier repeats a run step's or another nested
        sub-plugin's.
    """
    owners: dict[str, str | None] = {
        entry.identifier: None for entry in config.spec.run
    }
    for entry in config.spec.run:
        for identifier in _nested_identifiers(entry):
            if identifier in owners:
                owner = owners[identifier]
                taken = (
                    f"the sub-plugin nested under {owner!r}" if owner else "a run step"
                )
                raise InvalidPluginConfigError(
                    f"{entry.identifier!r}: nested sub-plugin {identifier!r} "
                    f"reuses the identifier of {taken}. Every run step and "
                    "nested sub-plugin needs its own identifier.",
                )
            owners[identifier] = entry.identifier


def _nested_identifiers(entry: Any) -> list[str]:
    """Return the identifiers of the valid sub-plugins nested in *entry*.

    A missing or malformed nested section is skipped here; registering the
    step that owns it reports it.

    Parameters
    ----------
    entry : Any
        A ``spec.run`` step.

    Returns
    -------
    list[str]
        One identifier per valid nested section.
    """
    registry = PLUGIN_REGISTRIES.get(normalize_kind(entry.spec.kind))
    keys = registry.nested_values if registry is not None else []
    cfg = entry.spec.config or {}
    nested = (_nested_model(cfg.get(key)) for key in keys)
    return [model.identifier for model in nested if model is not None]


def get_registered_plugin(
    plugin_registrations: list[tuple[type[ServicePlugin], dict[str, Any], str | None]],
    entry: MicroserviceModel,
    *,
    expected_kind: str | None = None,
    parent: str | None = None,
) -> None:
    """Append *entry*'s plugin, and any nested sub-plugins, to the registrations.

    Parameters
    ----------
    plugin_registrations : list[tuple[type[ServicePlugin], dict[str, Any], str | None]]
        Accumulator of ``(plugin_class, config, identifier)`` tuples.
    entry : MicroserviceModel
        The step (or nested sub-plugin) to register.
    expected_kind : str or None, optional
        Registry key a nested sub-plugin must have, e.g. ``"payloads"`` for
        the ``payload`` block under a job builder.  ``None`` for a
        ``spec.run`` step, which must instead be one of :data:`RUN_KINDS`.
    parent : str or None, optional
        Identifier of the plugin that nests *entry*, for error messages.

    Raises
    ------
    ValueError
        If a ``spec.run`` step is not a runnable kind.
    InvalidPluginConfigError
        If a nested sub-plugin has the wrong kind, a required nested section
        is missing or is not a mapping, or an identifier is already taken by
        another plugin in this process.
    """
    kind = _checked_kind(entry, expected_kind, parent)
    if entry.identifier in {registration[2] for registration in plugin_registrations}:
        where = f"the sub-plugin nested under {parent!r}" if parent else "a run step"
        raise InvalidPluginConfigError(
            f"{entry.identifier!r} already identifies another plugin, so "
            f"{where} cannot reuse it. Every run step and nested sub-plugin "
            "needs its own identifier.",
        )

    cfg = entry.spec.config or {}
    registry = PLUGIN_REGISTRIES[kind]
    # Parsed before anything registers, so a bad section fails the step whole.
    nested = [
        (key, _parse_nested_section(kind, entry.identifier, key, cfg))
        for key in registry.nested_values
    ]
    plugin_class = registry.get_plugin(entry.spec.name)

    plugin_registrations.append((plugin_class, cfg, entry.identifier))

    for key, sub_plugin in nested:
        get_registered_plugin(
            plugin_registrations,
            sub_plugin,
            expected_kind=normalize_kind(key),
            parent=entry.identifier,
        )


def _parse_nested_section(
    kind: str,
    parent: str,
    key: str,
    cfg: dict[str, Any],
) -> MicroserviceModel:
    """Parse the required nested section *key* of plugin *parent*.

    A job builder's ``payload`` is parsed by
    :func:`~courier.interfaces.job_builders.parse_payload_block`, the check
    :class:`~courier.interfaces.job_builders.JobBuilder` applies to its own
    config, so ``courier run`` reports a missing or malformed block in the
    same words as constructing the builder does.

    Parameters
    ----------
    kind : str
        Registry key of the plugin whose config holds the section.
    parent : str
        Identifier of the plugin whose config holds the section.
    key : str
        Name of the nested section, e.g. ``"payload"``.
    cfg : dict[str, Any]
        The plugin's config from the YAML.

    Returns
    -------
    MicroserviceModel
        The nested sub-plugin.

    Raises
    ------
    InvalidPluginConfigError
        If the section is missing, is not a mapping, or is not a valid
        sub-plugin mapping.
    """
    if kind == "job_builders" and key == PAYLOAD_KEY:
        return parse_payload_block(parent, cfg)
    raw = cfg.get(key)
    if raw is None:
        raise InvalidPluginConfigError(
            f"{parent!r} is missing required config section {key!r}",
        )
    if not isinstance(raw, dict):
        raise InvalidPluginConfigError(
            f"{parent!r}: required config section {key!r} must be a mapping "
            "describing the sub-plugin",
        )
    try:
        return MicroserviceModel.model_validate(raw)
    except ValueError as exc:  # pydantic's ValidationError subclasses ValueError
        raise InvalidPluginConfigError(
            f"{parent!r}: required config section {key!r} must map one "
            f"identifier to the sub-plugin's kind, name and config: {exc}",
        ) from exc


def _checked_kind(entry: Any, expected_kind: str | None, parent: str | None) -> str:
    """Return *entry*'s registry key, rejecting a kind that cannot go there.

    Parameters
    ----------
    entry : MicroserviceModel
        The step or nested sub-plugin being registered.
    expected_kind : str or None
        Registry key a nested sub-plugin must have; ``None`` for a
        ``spec.run`` step.
    parent : str or None
        Identifier of the nesting plugin, for the error message.

    Returns
    -------
    str
        The normalized registry key.

    Raises
    ------
    ValueError
        If a ``spec.run`` step is not one of :data:`RUN_KINDS`.
    InvalidPluginConfigError
        If a nested sub-plugin's kind is not *expected_kind*.
    """
    kind = normalize_kind(entry.spec.kind)
    if expected_kind is None:
        if kind not in RUN_KINDS:
            raise ValueError(
                f"{entry.identifier!r}: {entry.spec.kind!r} is not a runnable "
                f"kind. Valid kinds: {', '.join(sorted(RUN_KINDS))}.",
            )
        return kind
    if kind != expected_kind:
        singular = KIND_INFO.get(expected_kind, ("", None, ""))[1] or expected_kind
        raise InvalidPluginConfigError(
            f"{parent!r}: sub-plugin {entry.identifier!r} has kind "
            f"{entry.spec.kind!r}, but only kind {singular!r} can be nested "
            f"there.",
        )
    return kind


#: Shape of the identifier ``ServiceConfig`` generates when ``SERVICE_ID`` is
#: unset: ``watcher-service-`` plus eight hex characters from a uuid4.
_GENERATED_SERVICE_ID = re.compile(r"watcher-service-[0-9a-f]{8}")


def _resolve_service_id(config: Any) -> str:
    """Return the identity this process reports in logs and traces.

    Precedence is explicit configuration, then the environment, then the
    config's metadata name. The metadata name once won unconditionally, so
    every replica of one YAML reported the same identity in logs and traces.

    Parameters
    ----------
    config : Any
        Validated service configuration model.

    Returns
    -------
    str
        The resolved service identifier.
    """
    configured = getattr(config.spec.service_config, "service_id", "") or ""
    # The dataclass default is a generated placeholder, so it does not outrank
    # the metadata name. The full shape is matched because a prefix test also
    # discarded a real ``watcher-service-prod`` written in the YAML.
    if configured and not _GENERATED_SERVICE_ID.fullmatch(configured):
        return str(configured)
    from_env = os.environ.get("SERVICE_ID", "")
    if from_env:
        return from_env
    return str(config.metadata.name)


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

    Notes
    -----
    ``--only`` filters which plugins run, and which dispatchers and builder
    targets take part in routing validation. It does not filter the
    job-builder identifiers handed to the service: every container predeclares
    a durable file-found queue for every builder in the YAML, so container
    start order cannot lose files.
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
        service_id=_resolve_service_id(config),
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

    _check_nested_identifiers(config)
    for entry in config.spec.run:
        if only_set is not None and entry.identifier not in only_set:
            continue
        get_registered_plugin(plugin_registrations, entry)
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

    # Every job builder in the YAML, regardless of --only: each needs a durable
    # FilesFound-<builder> queue declared by this container, so a producer never
    # publishes into a fanout exchange with nothing bound to it (issue #44).
    all_builder_targets = _collect_builder_targets(config)
    builder_identifiers = frozenset(all_builder_targets)

    # Union: add any dispatcher targeted by builders in the filtered set
    builder_targets = all_builder_targets
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
        allow_implicit_target=config.spec.allow_implicit_target,
        builder_identifiers=builder_identifiers,
    )
    # Unfiltered too: lets preflight check payload/dispatcher compatibility
    # for routes whose other end runs in a different container.
    service.configure_topology(_collect_topology(config))
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
