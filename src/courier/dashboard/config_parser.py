"""Config-to-dashboard-model parser.

Parses a validated :class:`ServiceConfigModel` (loaded from YAML/JSON via
:func:`courier.cli.config_loader.load_config`) into a structured
:class:`DashboardModel` suitable for dashboard generation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

from courier.cli.config_loader import load_config

if TYPE_CHECKING:
    from courier.schema.v1alpha1.service_config import ServiceConfigModel


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class PluginKind(Enum):
    """Pipeline stage category — mirrors the ``kind`` field in microservice specs."""

    DATA_MONITOR = "data_monitor"
    JOB_BUILDER = "job_builder"
    DISPATCHER = "dispatcher"


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class PluginInfo:
    """Resolved information about a single pipeline stage.

    Built from one entry in the service config ``spec.run`` list.
    """

    identifier: str
    """YAML ``run[*].identifier``, e.g. ``"clavrx-file-watcher"``."""

    kind: PluginKind
    """Mapped plugin kind."""

    plugin_name: str
    """Plugin class name, e.g. ``"rabbit_mq_watcher"``."""

    config: dict[str, Any]
    """Plugin configuration dict (empty dict when absent)."""

    targets: list[str] = field(default_factory=list)
    """Dispatcher identifiers this builder routes jobs to."""

    routes: list[dict[str, Any]] = field(default_factory=list)
    """Raw routing table entries (for ``metadata_router``)."""


@dataclass
class DashboardModel:
    """Complete parsed dashboard model for a courier service configuration."""

    # ---- Metadata ----------------------------------------------------------
    service_name: str
    """From ``metadata.name``."""

    namespace: str
    """From ``metadata.namespace`` — defaults to ``"default"``."""

    description: str
    """From ``metadata.description``."""

    # ---- Plugins -----------------------------------------------------------
    plugins: list[PluginInfo]
    """All ``run`` entries in declaration order."""

    data_monitors: list[PluginInfo]
    """Plugins filtered to :attr:`PluginKind.DATA_MONITOR`."""

    job_builders: list[PluginInfo]
    """Plugins filtered to :attr:`PluginKind.JOB_BUILDER`."""

    dispatchers: list[PluginInfo]
    """Plugins filtered to :attr:`PluginKind.DISPATCHER`."""

    # ---- Routing -----------------------------------------------------------
    routing: dict[str, list[str]]
    """Mapping of builder identifier → list of target dispatcher identifiers."""

    # ---- Capability flags --------------------------------------------------
    has_metadata_router: bool
    """``True`` when any job_builder has ``plugin_name == "metadata_router"``."""

    has_slurm: bool
    """``True`` when any dispatcher has ``plugin_name == "slurm_dispatcher"``."""

    has_http: bool
    """``True`` when any dispatcher has ``plugin_name == "http_dispatcher"``."""

    has_parallel_bash: bool
    """``True`` when any dispatcher has ``plugin_name == "parallel_bash"``."""

    # ---- Sub-section -------------------------------------------------------
    local_identifiers: set[str] | None = None
    """Matching identifiers when a sub-section filter is active.

    ``None`` signals a full pipeline (no filter applied).
    """

    upstream_dependencies: set[str] = field(default_factory=set)
    """Non-local plugins whose output feeds into the sub-section."""

    downstream_dependencies: set[str] = field(default_factory=set)
    """Non-local plugins that consume output from the sub-section."""

    is_sub_section: bool = False
    """``True`` when a sub-section filter was applied."""


# ---------------------------------------------------------------------------
# Span constants (shared with generator modules)
# ---------------------------------------------------------------------------

SPAN_NAMES_BY_KIND: dict[PluginKind, list[str]] = {
    PluginKind.DATA_MONITOR: [
        "data_monitor.process_file",
        "data_monitor.add_metadata",
        "data_monitor.emit_file",
    ],
    PluginKind.JOB_BUILDER: [
        "job_builder.build_job",
        "job_builder.process_job_group",
        "job_builder.emit_job",
        "job_builder.emit_one",
        "metadata_router.route_file",
    ],
    PluginKind.DISPATCHER: [
        "dispatcher.dispatch_job",
        "dispatcher.execute_job",
        "dispatcher.emit_execution_log",
    ],
}
"""Maps each plugin kind to its known OpenTelemetry span name prefixes."""

SPAN_ATTRS: dict[str, str] = {
    "correlation_id": "span.courier.correlation_id",
    "file_path": "span.courier.file.path",
    "file_hostname": "span.courier.file.hostname",
    "file_source": "span.courier.file.source",
    "job_id": "span.courier.job.id",
    "job_name": "span.courier.job.name",
    "job_targets": "span.courier.job.targets",
    "job_file_count": "span.courier.job.file_count",
    "job_group_name": "span.courier.job_group.name",
    "execution_return_code": "span.courier.execution_log.return_code",
    "execution_hostname": "span.courier.execution_log.hostname",
    "target": "span.courier.target",
    "plugin_name": "span.plugin.name",
    "plugin_version": "span.plugin.version",
    "plugin_family": "span.plugin.family",
}
"""Span attribute key constants mirroring :mod:`courier.tracing` conventions."""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _extract_targets(config: dict[str, Any]) -> list[str]:
    """Extract and deduplicate dispatcher target identifiers from a plugin config.

    Handles both direct ``targets`` lists and nested targets inside
    ``routes`` entries (used by ``metadata_router``).
    """
    targets: list[str] = []

    direct = config.get("targets", [])
    if isinstance(direct, list):
        for t in direct:
            if isinstance(t, str) and t not in targets:
                targets.append(t)

    for route in config.get("routes", []):
        if not isinstance(route, dict):
            continue
        route_targets = route.get("targets", [])
        if isinstance(route_targets, list):
            for t in route_targets:
                if isinstance(t, str) and t not in targets:
                    targets.append(t)

    return targets


def _plugin_kind_or_none(raw_kind: str) -> PluginKind | None:
    """Map a YAML ``kind`` onto a :class:`PluginKind`, or ``None`` if not one.

    Accepts both the singular kinds written in configs (``data_monitor``) and
    the plural interface names (``data_monitors``) that
    :func:`courier.cli.plugins.normalize_kind` maps them to, so the dashboard
    understands exactly the set of configs the runtime accepts.
    """
    from courier.cli.plugins import normalize_kind  # noqa: PLC0415

    for candidate in (raw_kind, normalize_kind(raw_kind)):
        try:
            return PluginKind(candidate)
        except ValueError:
            continue
    # Plural interface name -> singular enum value (e.g. "dispatchers").
    singular = raw_kind[:-1] if raw_kind.endswith("s") else raw_kind
    try:
        return PluginKind(singular)
    except ValueError:
        return None


def _sanitise_plugin_config(raw_config: Any) -> dict[str, Any]:
    """Normalise a plugin config value to a plain dict.

    Returns an empty dict when *raw_config* is ``None`` or not a mapping.
    """
    if isinstance(raw_config, dict):
        return raw_config
    return {}


def _build_plugins(config: ServiceConfigModel) -> tuple[
    list[PluginInfo],
    list[PluginInfo],
    list[PluginInfo],
    list[PluginInfo],
]:
    """Build PluginInfo objects from every ``spec.run`` entry.

    Returns ``(all, data_monitors, job_builders, dispatchers)``.
    """
    plugins: list[PluginInfo] = []
    data_monitors: list[PluginInfo] = []
    job_builders: list[PluginInfo] = []
    dispatchers: list[PluginInfo] = []

    for entry in config.spec.run:
        kind = _plugin_kind_or_none(entry.spec.kind)
        if kind is None:
            # Not every run entry is a runnable plugin: ``data_monitor_configs``
            # entries are YAML metadata that ``courier run`` skips. Raising a
            # bare ValueError here made ``courier dashboard`` fail on configs
            # that ``courier validate`` and ``courier run`` both accept.
            continue
        plugin_config = _sanitise_plugin_config(entry.spec.config)
        targets = _extract_targets(plugin_config)

        raw_routes = plugin_config.get("routes")
        routes: list[dict[str, Any]] = (
            [r for r in raw_routes if isinstance(r, dict)]
            if isinstance(raw_routes, list)
            else []
        )

        info = PluginInfo(
            identifier=entry.identifier,
            kind=kind,
            plugin_name=entry.spec.name,
            config=plugin_config,
            targets=targets,
            routes=routes,
        )

        plugins.append(info)

        if kind is PluginKind.DATA_MONITOR:
            data_monitors.append(info)
        elif kind is PluginKind.JOB_BUILDER:
            job_builders.append(info)
        elif kind is PluginKind.DISPATCHER:
            dispatchers.append(info)

    return plugins, data_monitors, job_builders, dispatchers


def _build_routing(job_builders: list[PluginInfo]) -> dict[str, list[str]]:
    """Build mapping of builder identifier → target dispatcher identifiers."""
    return {jb.identifier: list(jb.targets) for jb in job_builders}


def _compute_capability_flags(
    job_builders: list[PluginInfo],
    dispatchers: list[PluginInfo],
) -> tuple[bool, bool, bool, bool]:
    """Compute boolean capability flags from the plugin lists."""
    has_metadata_router = any(
        jb.plugin_name == "metadata_router" for jb in job_builders
    )
    has_slurm = any(d.plugin_name == "slurm_dispatcher" for d in dispatchers)
    has_http = any(d.plugin_name == "http_dispatcher" for d in dispatchers)
    has_parallel_bash = any(d.plugin_name == "parallel_bash" for d in dispatchers)
    return has_metadata_router, has_slurm, has_http, has_parallel_bash


def _resolve_local_identifiers(
    plugins: list[PluginInfo],
    *,
    run_identifiers: set[str] | None,
    run_kinds: set[str] | None,
) -> set[str]:
    """Return the set of plugin identifiers that match at least one filter."""
    local: set[str] = set()
    for p in plugins:
        if (run_identifiers is not None and p.identifier in run_identifiers) or (
            run_kinds is not None and p.kind.value in run_kinds
        ):
            local.add(p.identifier)
    return local


def _compute_upstream_deps(
    local_identifiers: set[str],
    data_monitors: list[PluginInfo],
    job_builders: list[PluginInfo],
    dispatchers: list[PluginInfo],
) -> set[str]:
    """Non-local plugins whose output feeds into the sub-section."""
    if not local_identifiers:
        return set()

    local_jb_ids = {
        jb.identifier
        for jb in job_builders
        if jb.identifier in local_identifiers
    }
    local_disp_ids = {
        d.identifier
        for d in dispatchers
        if d.identifier in local_identifiers
    }

    upstream: set[str] = set()

    # Any local job builder or dispatcher depends on data monitors.
    if local_jb_ids or local_disp_ids:
        for dm in data_monitors:
            if dm.identifier not in local_identifiers:
                upstream.add(dm.identifier)

    # A local dispatcher depends on job builders that target it.
    if local_disp_ids:
        for jb in job_builders:
            if jb.identifier in local_identifiers:
                continue
            if any(t in local_disp_ids for t in jb.targets):
                upstream.add(jb.identifier)

    return upstream


def _compute_downstream_deps(
    local_identifiers: set[str],
    job_builders: list[PluginInfo],
    dispatchers: list[PluginInfo],
) -> set[str]:
    """Non-local plugins that consume output from the sub-section."""
    if not local_identifiers:
        return set()

    local_jb_ids = {
        jb.identifier
        for jb in job_builders
        if jb.identifier in local_identifiers
    }
    local_disp_ids = {
        d.identifier
        for d in dispatchers
        if d.identifier in local_identifiers
    }

    downstream: set[str] = set()

    # Non-local job builders that route to a local dispatcher.
    if local_disp_ids:
        for jb in job_builders:
            if jb.identifier in local_identifiers:
                continue
            if any(t in local_disp_ids for t in jb.targets):
                downstream.add(jb.identifier)

    # Non-local dispatchers targeted by a local job builder.
    if local_jb_ids:
        for jb in job_builders:
            if jb.identifier not in local_identifiers:
                continue
            for target in jb.targets:
                if target not in local_identifiers:
                    downstream.add(target)

    return downstream


# ---------------------------------------------------------------------------
# Main entry-point
# ---------------------------------------------------------------------------


def parse_config(
    config_path: str | Path,
    *,
    run_identifiers: set[str] | None = None,
    run_kinds: set[str] | None = None,
) -> DashboardModel:
    """Parse a courier service config file into a :class:`DashboardModel`.

    Parameters
    ----------
    config_path : str or Path
        Path to a ``.yaml``, ``.yml``, or ``.json`` service config file.
    run_identifiers : set[str] | None
        When provided, restrict the model to these ``identifier`` values
        and compute upstream / downstream dependencies.
    run_kinds : set[str] | None
        When provided, restrict the model to entries whose ``kind``
        matches one of these values (``"data_monitor"``, ``"job_builder"``,
        ``"dispatcher"``).

    Returns
    -------
    DashboardModel
        Fully-parsed dashboard model.
    """
    config_path = Path(config_path)
    config = load_config(config_path)

    # Phase 1 & 2 — build plugin list + routing
    plugins, data_monitors, job_builders, dispatchers = _build_plugins(config)
    routing = _build_routing(job_builders)

    # Phase 3 — capability flags
    has_metadata_router, has_slurm, has_http, has_parallel_bash = (
        _compute_capability_flags(job_builders, dispatchers)
    )

    # Phase 4 — sub-section (only when filters are active)
    if run_identifiers is None and run_kinds is None:
        return DashboardModel(
            service_name=config.metadata.name,
            namespace=(
                config.metadata.namespace if config.metadata.namespace else "default"
            ),
            description=config.metadata.description,
            plugins=plugins,
            data_monitors=data_monitors,
            job_builders=job_builders,
            dispatchers=dispatchers,
            routing=routing,
            has_metadata_router=has_metadata_router,
            has_slurm=has_slurm,
            has_http=has_http,
            has_parallel_bash=has_parallel_bash,
            local_identifiers=None,
            upstream_dependencies=set(),
            downstream_dependencies=set(),
            is_sub_section=False,
        )

    local_identifiers = _resolve_local_identifiers(
        plugins,
        run_identifiers=run_identifiers,
        run_kinds=run_kinds,
    )
    upstream_deps = _compute_upstream_deps(
        local_identifiers, data_monitors, job_builders, dispatchers,
    )
    downstream_deps = _compute_downstream_deps(
        local_identifiers, job_builders, dispatchers,
    )

    return DashboardModel(
        service_name=config.metadata.name,
        namespace=config.metadata.namespace if config.metadata.namespace else "default",
        description=config.metadata.description,
        plugins=plugins,
        data_monitors=data_monitors,
        job_builders=job_builders,
        dispatchers=dispatchers,
        routing=routing,
        has_metadata_router=has_metadata_router,
        has_slurm=has_slurm,
        has_http=has_http,
        has_parallel_bash=has_parallel_bash,
        local_identifiers=local_identifiers,
        upstream_dependencies=upstream_deps,
        downstream_dependencies=downstream_deps,
        is_sub_section=True,
    )
