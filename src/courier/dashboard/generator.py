"""Main dashboard generation orchestrator.

Takes a :class:`DashboardModel`, delegates panel generation to specialist
modules, and assembles the final :class:`grafanalib.core.Dashboard` objects.

Supports three generation modes:

* ``UNIFIED`` — single dashboard containing all panels.
* ``SPLIT_BY_KIND`` — separate dashboard per plugin kind.
* ``SPLIT_BY_PLUGIN`` — separate dashboard per plugin instance.
"""

from __future__ import annotations

import hashlib
import logging
import re
from typing import TYPE_CHECKING

from courier.dashboard.config_parser import DashboardModel, PluginInfo, PluginKind

if TYPE_CHECKING:
    from grafanalib.core import Dashboard

    from courier.dashboard import DashboardGenerationMode


_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def generate_dashboard(  # noqa: PLR0913
    model: DashboardModel,
    *,
    mode: str | DashboardGenerationMode = "UNIFIED",
    only_metrics: bool = False,
    only_traces: bool = False,
    only_logs: bool = False,
    datasource: str = "Prometheus",
    traces_datasource: str = "Tempo",
    loki_datasource: str = "",
    name: str | None = None,
    uid: str | None = None,
    tags: list[str] | None = None,
) -> list[Dashboard]:
    """Generate one or more Grafana dashboard objects from a parsed config model.

    Imports ``grafanalib`` lazily so the module remains importable when the
    optional ``data-courier[viz]`` extra is not installed.  Calling this function
    without ``grafanalib`` raises a descriptive :exc:`ImportError`.

    Parameters
    ----------
    model : DashboardModel
        Parsed pipeline model from :func:`courier.dashboard.config_parser.parse_config`.
    mode : DashboardGenerationMode or str
        How to split the dashboard:

        * ``UNIFIED`` → single dashboard with all panels
        * ``SPLIT_BY_KIND`` → one dashboard per plugin kind
        * ``SPLIT_BY_PLUGIN`` → one dashboard per plugin instance

        Accepts enum members or the raw string names.  Default: ``"UNIFIED"``.
    only_metrics : bool
        If ``True``, only generate Prometheus panels (skip TraceQL and Loki).
    only_traces : bool
        If ``True``, only generate TraceQL panels (skip Prometheus and Loki).
    only_logs : bool
        If ``True``, only generate Loki log panels (skip Prometheus and TraceQL).
    datasource : str
        Prometheus datasource UID or name (default ``"Prometheus"``).
    traces_datasource : str
        Tempo datasource UID or name (default ``"Tempo"``).
    loki_datasource : str
        Loki datasource UID or name.  When empty, Loki panels are not generated.
    name : str | None
        Dashboard title override.  When ``None``, auto-named from the model's
        service name.
    uid : str | None
        Dashboard UID.  When ``None``, auto-generated from the model.
    tags : list[str] | None
        Dashboard tags.  When ``None``, defaults to ``["courier", "generated"]``.

    Returns
    -------
    list[grafanalib.core.Dashboard]
        One or more dashboard objects ready for serialization.
    """
    # ------------------------------------------------------------------
    # Law 1 (Early Exit): validate mutually-exclusive flags.
    # ------------------------------------------------------------------
    if sum((only_metrics, only_traces, only_logs)) > 1:
        raise ValueError(
            "only_metrics, only_traces, and only_logs are mutually exclusive",
        )

    # ------------------------------------------------------------------
    # Law 1 (Early Exit): verify grafanalib is available.
    # ------------------------------------------------------------------
    import importlib.util as _importlib_util  # noqa: PLC0415

    if _importlib_util.find_spec("grafanalib") is None:
        raise ImportError(
            "grafanalib is required for dashboard generation. "
            "Install the optional dependency with:\n"
            "    pip install data-courier[viz]",
        )

    # ------------------------------------------------------------------
    # Normalise the mode parameter (accepts enum members or strings).
    # ------------------------------------------------------------------
    from courier.dashboard import DashboardGenerationMode  # noqa: PLC0415

    if isinstance(mode, str):
        try:
            mode_enum = DashboardGenerationMode[mode.upper()]
        except KeyError:
            valid = [m.name for m in DashboardGenerationMode]
            raise ValueError(
                f"Invalid mode '{mode}'. Choose from: {valid}",
            ) from None
    elif isinstance(mode, DashboardGenerationMode):
        mode_enum = mode
    else:
        raise TypeError(
            f"mode must be DashboardGenerationMode or str, "
            f"got {type(mode).__name__}",
        )

    # ------------------------------------------------------------------
    # Dispatch by mode.
    # ------------------------------------------------------------------
    if mode_enum is DashboardGenerationMode.UNIFIED:
        return [
            _build_single_dashboard(
                model,
                only_metrics=only_metrics,
                only_traces=only_traces,
                only_logs=only_logs,
                datasource=datasource,
                traces_datasource=traces_datasource,
                loki_datasource=loki_datasource,
                name=name,
                uid=uid,
                tags=tags,
            ),
        ]

    if mode_enum is DashboardGenerationMode.SPLIT_BY_KIND:
        return _split_by_kind(
            model,
            only_metrics=only_metrics,
            only_traces=only_traces,
            only_logs=only_logs,
            datasource=datasource,
            traces_datasource=traces_datasource,
            loki_datasource=loki_datasource,
            name=name,
            uid=uid,
            tags=tags,
        )

    if mode_enum is DashboardGenerationMode.SPLIT_BY_PLUGIN:
        return _split_by_plugin(
            model,
            only_metrics=only_metrics,
            only_traces=only_traces,
            only_logs=only_logs,
            datasource=datasource,
            traces_datasource=traces_datasource,
            loki_datasource=loki_datasource,
            name=name,
            uid=uid,
            tags=tags,
        )

    raise ValueError(f"Unknown generation mode: {mode_enum}")


# ---------------------------------------------------------------------------
# Single-dashboard (UNIFIED) assembly
# ---------------------------------------------------------------------------


def _build_single_dashboard(  # noqa: PLR0913
    model: DashboardModel,
    *,
    only_metrics: bool,
    only_traces: bool,
    only_logs: bool,
    datasource: str,
    traces_datasource: str,
    loki_datasource: str,
    name: str | None,
    uid: str | None,
    tags: list[str] | None,
) -> Dashboard:
    """Assemble panels for a single unified dashboard."""
    templates, rows = _assemble_unified_panels(
        model,
        only_metrics=only_metrics,
        only_traces=only_traces,
        only_logs=only_logs,
        datasource=datasource,
        traces_datasource=traces_datasource,
        loki_datasource=loki_datasource,
    )
    suffix = ""
    if only_traces:
        suffix = "Traces"
    elif only_logs:
        suffix = "Logs"

    return _build_dashboard(
        templates,
        rows,
        name=name or _make_dashboard_name(model, suffix=suffix),
        uid=uid or _make_dashboard_uid(model, suffix=suffix),
        tags=tags,
        description=model.description or "",
    )


# ---------------------------------------------------------------------------
# Split-by-kind assembly
# ---------------------------------------------------------------------------


def _split_by_kind(  # noqa: PLR0913
    model: DashboardModel,
    *,
    only_metrics: bool,
    only_traces: bool,
    only_logs: bool,
    datasource: str,
    traces_datasource: str,
    loki_datasource: str,
    name: str | None,
    uid: str | None,
    tags: list[str] | None,
) -> list[Dashboard]:
    """Generate one dashboard per :class:`PluginKind`.

    Each dashboard contains only the panels relevant to plugins of that kind.
    Plugin kinds with zero configured plugins are skipped.
    """
    dashboards: list[Dashboard] = []
    kinds_in_order = [
        PluginKind.DATA_MONITOR,
        PluginKind.JOB_BUILDER,
        PluginKind.DISPATCHER,
    ]

    for kind in kinds_in_order:
        kind_plugins = [p for p in model.plugins if p.kind is kind]
        if not kind_plugins:
            continue

        kind_model = _build_kind_model(model, kind, kind_plugins)
        kind_name = _make_dashboard_name(model, suffix=kind.value)
        kind_uid = _make_dashboard_uid(model, suffix=kind.value)

        templates, rows = _assemble_unified_panels(
            kind_model,
            only_metrics=only_metrics,
            only_traces=only_traces,
            datasource=datasource,
            traces_datasource=traces_datasource,
            only_logs=only_logs,
            loki_datasource=loki_datasource,
        )
        dashboards.append(
            _build_dashboard(
                templates,
                rows,
                name=kind_name if name is None else f"{name} - {kind.value}",
                uid=kind_uid if uid is None else f"{uid}_{kind.value}",
                tags=tags,
                description=f"{model.description or ''} [{kind.value}]".strip(),
            ),
        )

    return dashboards


def _build_kind_model(
    model: DashboardModel,
    kind: PluginKind,
    kind_plugins: list[PluginInfo],
) -> DashboardModel:
    """Create a sub-model scoped to a single :class:`PluginKind`."""
    return DashboardModel(
        service_name=model.service_name,
        namespace=model.namespace,
        description=model.description,
        plugins=kind_plugins,
        data_monitors=[
            p for p in kind_plugins if p.kind is PluginKind.DATA_MONITOR
        ],
        job_builders=[
            p for p in kind_plugins if p.kind is PluginKind.JOB_BUILDER
        ],
        dispatchers=[
            p for p in kind_plugins if p.kind is PluginKind.DISPATCHER
        ],
        routing={},
        has_metadata_router=(
            model.has_metadata_router and kind is PluginKind.JOB_BUILDER
        ),
        has_slurm=model.has_slurm and kind is PluginKind.DISPATCHER,
        has_http=model.has_http and kind is PluginKind.DISPATCHER,
        has_parallel_bash=(
            model.has_parallel_bash and kind is PluginKind.DISPATCHER
        ),
    )


# ---------------------------------------------------------------------------
# Split-by-plugin assembly
# ---------------------------------------------------------------------------


def _split_by_plugin(  # noqa: PLR0913
    model: DashboardModel,
    *,
    only_metrics: bool,
    only_traces: bool,
    only_logs: bool,
    datasource: str,
    traces_datasource: str,
    loki_datasource: str,
    name: str | None,
    uid: str | None,
    tags: list[str] | None,
) -> list[Dashboard]:
    """Generate one dashboard per individual plugin instance."""
    dashboards: list[Dashboard] = []

    for plugin in model.plugins:
        plugin_model = _build_plugin_model(model, plugin)
        plugin_name_str = _make_dashboard_name(
            model, suffix=plugin.identifier,
        )
        plugin_uid_str = _make_dashboard_uid(
            model, suffix=plugin.identifier,
        )

        templates, rows = _assemble_unified_panels(
            plugin_model,
            only_metrics=only_metrics,
            only_traces=only_traces,
            datasource=datasource,
            traces_datasource=traces_datasource,
            only_logs=only_logs,
            loki_datasource=loki_datasource,
        )
        dashboards.append(
            _build_dashboard(
                templates,
                rows,
                name=(
                    plugin_name_str
                    if name is None
                    else f"{name} - {plugin.identifier}"
                ),
                uid=(
                    plugin_uid_str
                    if uid is None
                    else f"{uid}_{_sanitize_uid(plugin.identifier)}"
                ),
                tags=tags,
                description=(
                    f"{model.description or ''}"
                    f" [{plugin.identifier}]".strip()
                ),
            ),
        )

    return dashboards


def _build_plugin_model(
    model: DashboardModel,
    plugin: PluginInfo,
) -> DashboardModel:
    """Create a sub-model scoped to a single plugin instance."""
    is_dm = plugin.kind is PluginKind.DATA_MONITOR
    is_jb = plugin.kind is PluginKind.JOB_BUILDER
    is_dp = plugin.kind is PluginKind.DISPATCHER

    return DashboardModel(
        service_name=model.service_name,
        namespace=model.namespace,
        description=model.description,
        plugins=[plugin],
        data_monitors=[plugin] if is_dm else [],
        job_builders=[plugin] if is_jb else [],
        dispatchers=[plugin] if is_dp else [],
        routing={},
        has_metadata_router=(
            model.has_metadata_router
            and is_jb
            and plugin.plugin_name == "metadata_router"
        ),
        has_slurm=(
            model.has_slurm
            and is_dp
            and plugin.plugin_name == "slurm_dispatcher"
        ),
        has_http=(
            model.has_http
            and is_dp
            and plugin.plugin_name == "http_dispatcher"
        ),
        has_parallel_bash=(
            model.has_parallel_bash
            and is_dp
            and plugin.plugin_name == "parallel_bash"
        ),
    )


# ---------------------------------------------------------------------------
# Panel assembly core
# ---------------------------------------------------------------------------


def _build_trace_templates(model: DashboardModel) -> list:
    """Generate template variables required by TraceQL panels.

    The TraceQL queries reference ``$dm_source``, ``$jb_trace_plugin``,
    and ``$correlation_id``.  These must be present in the dashboard
    templating list for the panels to function.
    """
    from grafanalib.core import Template  # noqa: PLC0415

    templates: list[Template] = []

    templates.append(Template(
        name="dm_source",
        label="Data Monitor Source",
        type="textbox",
        query="",
    ))

    if model.job_builders:
        jb_names = sorted({jb.plugin_name for jb in model.job_builders})
        if jb_names:
            templates.append(Template(
                name="jb_trace_plugin",
                label="Job Builder (Traces)",
                type="custom",
                query=",".join(jb_names),
                multi=True,
                includeAll=True,
                allValue=".*",
            ))

    templates.append(Template(
        name="correlation_id",
        label="Correlation ID",
        type="textbox",
        query="",
    ))

    return templates


def _assemble_unified_panels(
    model: DashboardModel,
    *,
    only_metrics: bool,
    only_traces: bool,
    only_logs: bool,
    datasource: str,
    traces_datasource: str,
    loki_datasource: str,
) -> tuple[list, list]:
    """Assemble all templates and rows for a single dashboard.

    Delegates panel generation to the specialist builder modules and
    returns ``(templates, rows)`` ready for :func:`_build_dashboard`.

    All builder module imports are lazy — grafanalib is only imported
    by those modules and this function is only called after
    :func:`generate_dashboard` has verified it is installed.

    Returns
    -------
    tuple[list, list]
        A 2-tuple of ``(templates, panel_rows)``.
    """
    from grafanalib.core import GridPos, RowPanel  # noqa: PLC0415

    from courier.dashboard.cluster_panels import build_cluster_panels  # noqa: PLC0415
    from courier.dashboard.loki_panels import (  # noqa: PLC0415
        build_loki_panels,
        build_loki_templates,
    )
    from courier.dashboard.prometheus_panels import (  # noqa: PLC0415
        build_prometheus_panels,
        build_prometheus_templates,
    )
    from courier.dashboard.topology import (  # noqa: PLC0415
        build_subsection_header,
        build_topology_panels,
    )
    from courier.dashboard.traceql_panels import build_traceql_panels  # noqa: PLC0415

    panel_rows: list = []

    # -- Templates ----------------------------------------------------------
    templates: list = []
    if not only_traces and not only_logs:
        templates.extend(build_prometheus_templates(model))

    # -- Sub-section header (when applicable) ------------------------------
    header = build_subsection_header(model, datasource=datasource)
    if header is not None:
        panel_rows.append(header)

    # -- Prometheus panels -------------------------------------------------
    if not only_traces and not only_logs:
        prom_panels = build_prometheus_panels(model, datasource=datasource)
        # After Task 2.1 reordering, panels[0] and panels[1] are always
        # Service Overview and Pipeline Summary (always generated).
        # Per-plugin rows are conditionally appended thereafter.
        header_panels = prom_panels[:2]
        body_panels = prom_panels[2:]
        panel_rows.extend(header_panels)

        # Topology panels — inserted between overview/summary and per-plugin detail
        if not only_metrics and not only_traces and not only_logs:
            topo_panels = build_topology_panels(model, datasource=datasource)
            panel_rows.extend(topo_panels)

        panel_rows.extend(body_panels)

    # -- TraceQL panels ----------------------------------------------------
    if not only_metrics and not only_logs:
        # Visual divider between Prometheus metrics and Tempo traces
        panel_rows.append(RowPanel(
            title="Distributed Traces (Tempo)",
            gridPos=GridPos(h=1, w=24, x=0, y=0),
        ))

        traceql_panels = build_traceql_panels(
            model, datasource=traces_datasource,
        )
        panel_rows.extend(traceql_panels)

        trace_templates = _build_trace_templates(model)
        templates.extend(trace_templates)

    # -- Loki (LogQL) panels -----------------------------------------------
    if loki_datasource and not only_metrics and not only_traces:
        panel_rows.append(RowPanel(
            title="Application Logs (Loki)",
            gridPos=GridPos(h=1, w=24, x=0, y=0),
        ))

        loki_panels = build_loki_panels(
            model, datasource=loki_datasource,
        )
        panel_rows.extend(loki_panels)

        loki_templates = build_loki_templates(model)
        templates.extend(loki_templates)

    # Only-logs mode: generate only Loki panels (even without --loki-datasource)
    elif only_logs:
        loki_panels = build_loki_panels(
            model, datasource=loki_datasource or "Loki",
        )
        panel_rows.extend(loki_panels)

        loki_templates = build_loki_templates(model)
        templates.extend(loki_templates)

    # -- Cluster panels (sub-section only) ---------------------------------
    cluster = build_cluster_panels(model, datasource=datasource)
    if cluster:
        panel_rows.extend(cluster)

    return templates, panel_rows


# ---------------------------------------------------------------------------
# Dashboard construction
# ---------------------------------------------------------------------------


def _build_dashboard(  # noqa: PLR0913
    templates: list,
    rows: list,
    *,
    name: str,
    uid: str,
    tags: list[str] | None = None,
    description: str = "",
) -> Dashboard:
    """Build a grafanalib :class:`Dashboard` from templates and rows.

    Parameters
    ----------
    templates : list
        Template variable objects (e.g. :class:`grafanalib.core.Template`).
    rows : list
        Panel objects (Row, RowPanel, Stat, TimeSeries, etc.).
    name : str
        Dashboard title.
    uid : str
        Dashboard unique identifier.
    tags : list[str] | None
        Dashboard tags.  Defaults to ``["courier", "generated"]``.
    description : str
        Dashboard description text.

    Returns
    -------
    Dashboard
        A complete grafanalib dashboard object.
    """
    from grafanalib.core import Dashboard, Templating, Time  # noqa: PLC0415

    return Dashboard(
        title=name,
        uid=uid,
        tags=tags or ["courier", "generated"],
        description=description,
        templating=Templating(list=templates) if templates else Templating(),
        rows=rows,
        time=Time(start="now-30m", end="now"),
        refresh="30s",
    )


# ---------------------------------------------------------------------------
# Naming & UID helpers
# ---------------------------------------------------------------------------

# Characters that are safe in Grafana dashboard UIDs.
# Grafana UIDs are used in URLs, so we keep them simple.
_UID_SAFE_RE = re.compile(r"[^a-z0-9_]")


def _sanitize_uid(raw: str) -> str:
    """Sanitize a string for use as a Grafana dashboard UID.

    Lowercases the input, replaces any character that is not alphanumeric
    or underscore with a single underscore, and strips leading/trailing
    underscores.
    """
    return _UID_SAFE_RE.sub("_", raw.lower()).strip("_")


def _make_dashboard_name(model: DashboardModel, suffix: str = "") -> str:
    """Generate a human-readable dashboard title from the model.

    Uses ``model.service_name`` as the base.  When *suffix* is non-empty,
    appends it after a separator.
    """
    base = model.service_name or "Courier"
    if suffix:
        return f"{base} - {suffix}"
    return base


def _make_dashboard_uid(model: DashboardModel, suffix: str = "") -> str:
    """Generate a stable dashboard UID from the model.

    Grafana keys dashboards by UID: re-importing a dashboard whose UID changed
    creates a *duplicate* rather than updating the existing one, losing its
    starred state, permissions and any links pointing at it. The UID must
    therefore be a pure function of the config, so regenerating from an
    unchanged service yields the same value.

    A random component is used only when there is no service name to derive
    from — and it is derived by hashing the model rather than by ``uuid4`` so
    even that case stays reproducible.
    """
    if model.service_name:
        base = _sanitize_uid(model.service_name)
    else:
        # No name to key on: hash the pipeline shape so the UID is at least
        # stable across runs of the same config.
        shape = "|".join(
            f"{p.kind.value}:{p.identifier}:{p.plugin_name}" for p in model.plugins
        )
        base = f"courier_{hashlib.sha256(shape.encode()).hexdigest()[:8]}"

    if suffix:
        return f"{base}_{_sanitize_uid(suffix)}"
    return base
