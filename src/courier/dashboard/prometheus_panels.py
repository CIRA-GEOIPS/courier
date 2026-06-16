"""Config-aware Prometheus panel generation for Grafana dashboards.

Generates Grafana panel objects (RowPanel, TimeSeries, Stat, GaugePanel,
Table, etc.) using the grafanalib library. Panels are **config-aware**:
only panels relevant to the pipeline's actually-configured plugins are
generated.

Metric provenance — which module emits each Prometheus metric family
--------------------------------------------------------------------
All metrics are defined in :mod:`courier.metrics` and emitted by call-sites
in :mod:`courier.interfaces.module_based` and
:mod:`courier.plugins.classes`.  There is no ``courier.monitoring`` package.

* ``courier_service_*`` — :mod:`courier.metrics`
* ``courier_data_monitor_*`` — :mod:`courier.metrics`
* ``courier_job_builder_*`` — :mod:`courier.metrics`
* ``courier_dispatcher_*`` — :mod:`courier.metrics`
* ``courier_plugin_*`` — :mod:`courier.metrics`
* ``courier_broker_*`` — :mod:`courier.metrics`
* ``courier_dispatcher_slurm_*`` — :mod:`courier.metrics`
* ``courier_dispatcher_http_*`` — :mod:`courier.metrics`

Panel Generation Logic
----------------------
For each plugin kind, panels are ONLY generated if the model has
configured plugins of that kind. Empty rows never appear.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from grafanalib.core import (
    GAUGE_CALC_LAST,
    REFRESH_ON_TIME_RANGE_CHANGE,
    GaugePanel,
    GridPos,
    RowPanel,
    Stat,
    Table,
    Target,
    Template,
    Threshold,
    TimeSeries,
)

if TYPE_CHECKING:
    from courier.dashboard.config_parser import DashboardModel

# ==========================================================================
# Color Palette — Courier-branded, Grafana dark-theme compatible
# ==========================================================================

COLORS = {
    "primary": "#33C7FF",
    "success": "#1EBF6E",
    "warning": "#FFB357",
    "danger": "#FF4B4B",
    "neutral": "#8A9BB5",
    "accent": "#C85EFA",
    "series": [
        "#33C7FF",
        "#1EBF6E",
        "#FFB357",
        "#C85EFA",
        "#FF4B4B",
        "#6ED0FF",
        "#47D487",
        "#FFCF85",
        "#D685FF",
        "#FF7A7A",
    ],
}
"""Color palette for Courier-branded Grafana panels.

Keys map to semantic roles; ``series`` holds ten colours for
multi-series graphs — cycling through them avoids repetition.
"""

# ==========================================================================
# Common Y-Axis Formats
# ==========================================================================

YAXIS_SHORT = "short"
"""Auto-scale with SI prefix (e.g. 1.5k, 2.3M)."""

YAXIS_PERCENT = "percentunit"
"""0.0-1.0 displayed as 0%-100%."""

YAXIS_SECONDS = "s"
"""Duration in seconds."""

# ==========================================================================
# Common Thresholds
# ==========================================================================

THRESHOLD_OK: list[Threshold] = [
    Threshold("green", 0, 0.0),
    Threshold("red", 1, 1.0),
]
"""Binary green/red threshold for health-style metrics."""

THRESHOLD_WARN: list[Threshold] = [
    Threshold("green", 0, 0.0),
    Threshold("yellow", 1, 80.0),
    Threshold("red", 2, 95.0),
]
"""Green/yellow/red threshold for success-rate metrics."""

_THRESH_PENDING_JOBS: list[Threshold] = [
    Threshold("green", 0, 0.0),
    Threshold("amber", 1, 100.0),
    Threshold("red", 2, 500.0),
]

_THRESH_DATA_FRESHNESS: list = [
    Threshold("green", 0, 0.0),
    Threshold("amber", 1, 60.0),
    Threshold("red", 2, 300.0),
]

_THRESH_PIPELINE_HEALTH: list = [
    Threshold("red", 0, 0.0),
    Threshold("amber", 1, 0.80),
    Threshold("green", 2, 0.95),
]

_THRESH_QUEUE_DEPTH: list = [
    Threshold("green", 0, 0.0),
    Threshold("amber", 1, 1.0),
    Threshold("red", 2, 100.0),
]

# ==========================================================================
# Internal constants
# ==========================================================================

_DS = "$datasource"
"""Prometheus datasource template variable reference."""

_PREFIX = "courier"
"""Common metric name prefix for all Courier metrics."""

# ==========================================================================
# Generation state tracker
# ==========================================================================


@dataclass
class _GenState:
    """Mutable state tracked across panel builders during one generation pass.

    Attributes
    ----------
    y : int
        Current vertical cursor position on the 24-column dashboard grid.
    pid : int
        Next available unique panel ID.
    """

    y: int = 0
    pid: int = 1


def _advance(gs: _GenState, height: int) -> int:
    """Return current Y and advance the grid cursor by *height* rows."""
    cur = gs.y
    gs.y += height
    return cur


def _peek_y(gs: _GenState) -> int:
    """Return current y without advancing the cursor."""
    return gs.y


def _next_id(gs: _GenState) -> int:
    """Return next unique panel ID and increment the counter."""
    cur = gs.pid
    gs.pid += 1
    return cur


# ==========================================================================
# PromQL helpers
# ==========================================================================


def _target(
    expr: str,
    legend: str = "",
    *,
    ref: str = "A",
    instant: bool = False,
) -> Target:
    """Create a Prometheus target querying ``$_DS``."""
    return Target(
        expr=expr,
        legendFormat=legend,
        refId=ref,
        instant=instant,
        datasource=_DS,
    )


def _rate(metric: str, labels: str = "", interval: str = "5m") -> str:
    """Wrap a metric in ``rate(...)``.

    Parameters
    ----------
    metric : str
        Full metric name (e.g. ``courier_data_monitor_files_processed_total``).
    labels : str
        Comma-separated label matchers (e.g. ``plugin_name=~"$dp_plugin"``).
    interval : str
        Rate lookback window (default ``"5m"``).
    """
    selector = f"{{{labels}}}" if labels else ""
    return f"rate({metric}{selector}[{interval}])"


def _hq(
    metric: str,
    quantile: float,
    labels: str = "",
    interval: str = "5m",
) -> str:
    """Build a ``histogram_quantile`` query for a histogram metric.

    Parameters
    ----------
    metric : str
        Histogram base name (without ``_bucket`` suffix).
    quantile : float
        Target quantile (0.0-1.0).
    labels : str
        Comma-separated label matchers.
    interval : str
        Rate lookback window.
    """
    selector = f"{{{labels}}}" if labels else ""
    return (
        f"histogram_quantile({quantile},"
        f" rate({metric}_bucket{selector}[{interval}]))"
    )


def _avg_rate(
    metric_sum: str,
    metric_count: str,
    labels: str = "",
    interval: str = "5m",
) -> str:
    """Build a PromQL expression for the average rate of a summary/histogram.

    Computes ``rate(sum[interval]) / rate(count[interval])``.
    """
    sum_sel = f"{{{labels}}}" if labels else ""
    cnt_sel = f"{{{labels}}}" if labels else ""
    return (
        f"rate({metric_sum}{sum_sel}[{interval}])"
        f" /"
        f" rate({metric_count}{cnt_sel}[{interval}])"
    )


def _clamp_div(numerator: str, denominator: str) -> str:
    """Guard against division by zero in PromQL using ``clamp_min``."""
    return f"{numerator} / clamp_min({denominator}, 1)"


# ==========================================================================
# Template builders
# ==========================================================================


def build_prometheus_templates(model: DashboardModel) -> list[Template]:
    """Generate config-aware template variables for the dashboard.

    Templates are populated from the model's configured plugins so the
    Grafana UI only offers values that actually exist in the pipeline.
    All plugin filters use the plugin **type-name** domain
    (``plugin_name`` / ``self.name`` ClassVar) — the same domain that
    the runtime metrics are labelled with — so that ``{…=~"$filter"}``
    selectors actually match emitted data.

    Parameters
    ----------
    model : DashboardModel
        Parsed dashboard configuration model.

    Returns
    -------
    list[Template]
        Template variable objects for the dashboard's templating list.
    """
    templates: list[Template] = []

    # -- Datasource (always) ------------------------------------------------
    templates.append(
        Template(
            name="datasource",
            label="Data Source",
            query="prometheus",
            type="datasource",
        ),
    )

    # -- Plugin filter (always — populated from all plugins) ----------------
    plugin_names = [p.plugin_name for p in model.plugins]
    if plugin_names:
        templates.append(
            _custom_template(
                name="plugin_filter",
                label="Plugin Filter",
                options=plugin_names,
            ),
        )

    # -- Data Monitor plugins -----------------------------------------------
    if model.data_monitors:
        dm_ids = [dm.plugin_name for dm in model.data_monitors]
        templates.append(
            _custom_template(
                name="dm_plugin",
                label="Data Monitor",
                options=dm_ids,
            ),
        )

    # -- Metadata Router targets --------------------------------------------
    if model.has_metadata_router:
        route_ids = _collect_route_targets(model)
        if route_ids:
            templates.append(
                _custom_template(
                    name="route_target",
                    label="Route Target",
                    options=route_ids,
                ),
            )

    # -- Job Builder plugins ------------------------------------------------
    if model.job_builders:
        jb_names = [jb.plugin_name for jb in model.job_builders]
        templates.append(
            _custom_template(
                name="jb_plugin",
                label="Job Builder",
                options=jb_names,
            ),
        )

    # -- Dispatcher plugins -------------------------------------------------
    if model.dispatchers:
        dp_names = [dp.plugin_name for dp in model.dispatchers]
        templates.append(
            _custom_template(
                name="dp_plugin",
                label="Dispatcher",
                options=dp_names,
            ),
        )

    return templates


def _custom_template(
    name: str,
    label: str,
    options: list[str],
) -> Template:
    """Build a multi-select custom template with static options."""
    return Template(
        name=name,
        label=label,
        type="custom",
        query=",".join(options),
        multi=True,
        includeAll=True,
        allValue=".*",
        refresh=REFRESH_ON_TIME_RANGE_CHANGE,
    )


def _collect_route_targets(model: DashboardModel) -> list[str]:
    """Gather unique route target identifiers from job builder plugin configs."""
    targets: list[str] = []
    for jb in model.job_builders:
        for route in jb.routes:
            route_targets = route.get("targets", [])
            if isinstance(route_targets, list):
                for t in route_targets:
                    if isinstance(t, str) and t not in targets:
                        targets.append(t)
    return targets


# ==========================================================================
# Panel factories — individual panel constructors
# ==========================================================================


# ==========================================================================
# 1. Service Overview Row (always generated)
# ==========================================================================


def _service_overview_panels(
    _model: DashboardModel, gs: _GenState,
) -> RowPanel:
    """Generate the Service Overview section with four KPI stat panels.

    Returns a RowPanel containing uptime, active plugin count,
    files processed rate, and pending jobs — the key health signals
    an operator needs at a glance.
    """
    y_row = _advance(gs, 0)  # RowPanel header at this Y (0)
    gs.y = y_row + 1          # stats start one row below the header

    panels: list[Stat] = []

    # Uptime in hours
    panels.append(
        _stat_panel_h(
            gs,
            title="Service Uptime (hours)",
            expr=f"{_PREFIX}_service_uptime_seconds / 3600",
            x=0,
            description=(
                "Shows how long the Courier service has been running continuously. "
                "Why it matters: a recent restart may indicate a crash or deployment. "
                "When dropping to zero: check service logs for crash or OOM events."
            ),
        ),
    )

    # Active plugins — count plugins in RUNNING state (enum value 3)
    panels.append(
        _stat_panel_h(
            gs,
            title="Active Plugins",
            expr=(
                f"count({_PREFIX}_plugin_state"
                f'{{plugin_name=~"$plugin_filter"}} == 3)'
            ),
            x=6,
            thresholds=[
                Threshold("red", 0, 0.0),
                Threshold("green", 1, 0.01),
            ],
            description=(
                "Shows the number of configured plugins currently in RUNNING state. "
                "Why it matters: plugins in any other state are not processing data. "
                "When red (zero): check the plugin manager logs for startup failures."
            ),
        ),
    )

    # Files processed rate (>0 means data is flowing)
    panels.append(
        _stat_panel_h(
            gs,
            title="Files Processed /s",
            expr=(
                f"sum({_rate(f'{_PREFIX}_data_monitor_files_processed_total')})"
            ),
            x=12,
            thresholds=[
                Threshold("red", 0, 0.0),
                Threshold("green", 1, 0.01),
            ],
            description=(
                "Shows the aggregate file processing rate across data monitors. "
                "Why it matters: zero rate means no data flowing. "
                "When red: verify monitors are running and sources accessible."
            ),
        ),
    )

    # Pending jobs (max across builders) with amber/red escalation
    panels.append(
        _stat_panel_h(
            gs,
            title="Pending Jobs",
            expr=(
                f"max({_PREFIX}_job_builder_active_groups"
                f'{{job_builder_name=~"$jb_plugin"}})'
            ),
            x=18,
            thresholds=_THRESH_PENDING_JOBS,
            description=(
                "Shows the maximum pending job count across all job builders. "
                "Why it matters: growing pending jobs indicate downstream saturation. "
                "When amber (>100): monitor dispatcher throughput. "
                "When red (>500): scale dispatchers or check broker connectivity."
            ),
        ),
    )

    # Pipeline Health gauge — fraction of plugins in RUNNING state
    gs.y += 4  # advance to new sub-row
    panels.append(
        _stat_panel_h(
            gs,
            title="Pipeline Health",
            expr=(
                f"clamp_max("
                f"count({_PREFIX}_plugin_state"
                f'{{plugin_name=~"$plugin_filter"}} == 3)'
                f" / clamp_min(count({_PREFIX}_plugin_state"
                f'{{plugin_name=~"$plugin_filter"}}), 1), 1)'
            ),
            x=0,
            w=12,
            thresholds=_THRESH_PIPELINE_HEALTH,
        ),
    )

    # gs.y += 4 removed — now handled by the two sub-rows above

    return RowPanel(
        id=_next_id(gs),
        title="Service Overview",
        gridPos=GridPos(h=1, w=24, x=0, y=y_row),
        panels=panels,
    )


def _stat_panel_h(  # noqa: PLR0913
    gs: _GenState,
    title: str,
    expr: str,
    *,
    x: int,
    w: int = 6,
    thresholds: list[Threshold] | None = None,
    description: str | None = None,
) -> Stat:
    """Create a Stat panel at a specific X offset on the current Y row."""
    return Stat(
        id=_next_id(gs),
        title=title,
        dataSource=_DS,
        targets=[_target(expr)],
        reduceCalc=GAUGE_CALC_LAST,
        format=YAXIS_SHORT,
        thresholds=thresholds,
        description=description,
        gridPos=GridPos(h=4, w=w, x=x, y=gs.y),
    )


# ==========================================================================
# 2. Data Monitor Row (ONLY if model.data_monitors)
# ==========================================================================


def _data_monitor_row(model: DashboardModel, gs: _GenState) -> RowPanel | None:
    """Generate the Data Monitor row with per-plugin rate and error panels."""
    if not model.data_monitors:
        return None

    dm_plugin_names = {dm.plugin_name for dm in model.data_monitors}
    _poll_monitors = {"s3_poller", "sftp_poller", "cron_glob"}
    has_poll_monitors = bool(dm_plugin_names & _poll_monitors)

    y_row = _advance(gs, 1)
    lbl = 'monitor_name=~"$dm_plugin"'

    panels: list = []

    # --- Sub-row 1: files processed, proc. time, errors ---
    py1 = _advance(gs, 8)

    panels.append(
        _timeseries(
            gs,
            title="Files Processed",
            description=(
                "Shows the file processing rate per selected data monitor plugin. "
                "Why it matters: flatlined rate may indicate a stalled data monitor "
                "or inaccessible source. When dropping to zero: check the data "
                "monitor plugin logs."
            ),
            targets=[
                _target(
                    _rate(
                        f"{_PREFIX}_data_monitor_files_processed_total", lbl,
                    ),
                    "{{monitor_name}}",
                ),
            ],
            unit="ops",
            y=py1,
            w=8,
            x=0,
        ),
    )

    if has_poll_monitors:
        panels.append(
            _timeseries(
                gs,
                title="Processing Time (avg)",
                description=(
                    "Average scan duration for poll-based data monitors "
                    "(s3_poller, sftp_poller, cron_glob). "
                    "Event-driven monitors do not emit this metric."
                ),
                targets=[
                    _target(
                        _avg_rate(
                            f"{_PREFIX}_data_monitor_scan_duration_seconds_sum",
                            f"{_PREFIX}_data_monitor_scan_duration_seconds_count",
                            lbl,
                        ),
                        "{{monitor_name}}",
                    ),
                ],
                unit=YAXIS_SECONDS,
                y=py1,
                w=8,
                x=8,
            ),
        )

    panels.append(
        _timeseries(
            gs,
            title="Errors",
            description=(
                "Shows the file processing failure rate per selected data monitor "
                "plugin. Why it matters: any non-zero error rate indicates file "
                "processing failures. When errors appear: inspect the data "
                "monitor's error logs for the failing file paths."
            ),
            targets=[
                _target(
                    _rate(
                        f"{_PREFIX}_data_monitor_files_processed_total",
                        lbl + ', status="failure"',
                    ),
                    "{{monitor_name}}",
                ),
            ],
            stacking={"mode": "normal", "group": "A"},
            fill_opacity=30,
            unit="ops",
            y=py1,
            w=8 if has_poll_monitors else 12,
            x=16 if has_poll_monitors else 8,
        ),
    )

    # --- Sub-row 2: data freshness ----------------------------------------
    py_fresh = _advance(gs, 4)
    panels.append(
        Stat(
            id=_next_id(gs),
            title="Data Freshness",
            dataSource=_DS,
            targets=[
                _target(
                    f"time() - {_PREFIX}_data_monitor_last_processed_timestamp_seconds"
                    f'{{plugin_name=~"$dm_plugin"}}',
                ),
            ],
            reduceCalc=GAUGE_CALC_LAST,
            format=YAXIS_SECONDS,
            thresholds=_THRESH_DATA_FRESHNESS,
            gridPos=GridPos(h=4, w=24, x=0, y=py_fresh),
        ),
    )

    # --- Sub-row 3: files by status (bar gauge) ----------------------
    py2 = _advance(gs, 8)

    panels.append(
        _timeseries(
            gs,
            title="Files by Status",
            targets=[
                _target(
                    f"sum({_rate(f'{_PREFIX}_data_monitor_files_processed_total')})"
                    " by (status)",
                    "{{status}}",
                ),
            ],
            unit="ops",
            y=py2,
            w=12,
            x=0,
        ),
    )

    return RowPanel(
        id=_next_id(gs),
        title="Data Monitors",
        gridPos=GridPos(h=1, w=24, x=0, y=y_row),
        panels=panels,
    )


# ==========================================================================
# 3. Metadata Router Row (ONLY if model.has_metadata_router)
# ==========================================================================


def _metadata_router_row(model: DashboardModel, gs: _GenState) -> RowPanel | None:
    """Generate Metadata Router panels — files routed and route distribution."""
    if not model.has_metadata_router:
        return None

    y_row = _advance(gs, 1)
    lbl = 'route_name=~"$route_target"'
    py = _advance(gs, 8)

    panels: list = []

    panels.append(
        _timeseries(
            gs,
            title="Router Files Routed",
            targets=[
                _target(
                    _rate(
                        f"{_PREFIX}_job_builder_route_matches_total", lbl,
                    ),
                    "{{route_name}} — {{target}}",
                ),
            ],
            unit="ops",
            y=py,
            w=12,
            x=0,
        ),
    )

    panels.append(
        _timeseries(
            gs,
            title="Route Distribution",
            targets=[
                _target(
                    f"sum({_rate(f'{_PREFIX}_job_builder_route_matches_total')})"
                    " by (route_name)",
                    "{{route_name}}",
                ),
            ],
            unit="ops",
            y=py,
            w=12,
            x=12,
        ),
    )

    return RowPanel(
        id=_next_id(gs),
        title="Metadata Router",
        gridPos=GridPos(h=1, w=24, x=0, y=y_row),
        panels=panels,
    )


# ==========================================================================
# 4. Job Builder Row (ONLY if model.job_builders)
# ==========================================================================


def _job_builder_row(model: DashboardModel, gs: _GenState) -> RowPanel | None:
    """Generate Job Builder panels — jobs built, pending groups, timing."""
    if not model.job_builders:
        return None

    y_row = _advance(gs, 1)
    lbl = 'job_builder_name=~"$jb_plugin"'

    panels: list = []

    # --- Sub-row 1: jobs built, pending groups, processing time ------------------
    py1 = _advance(gs, 8)

    panels.append(
        _timeseries(
            gs,
            title="Jobs Built",
            targets=[
                _target(
                    _rate(f"{_PREFIX}_job_builder_jobs_built_total", lbl),
                    "{{job_builder_name}} — {{status}}",
                ),
            ],
            unit="ops",
            y=py1,
            w=8,
            x=0,
        ),
    )

    panels.append(
        GaugePanel(
            id=_next_id(gs),
            title="Active Groups",
            description=(
                "Number of active file groups currently accumulating in the job "
                "builder. Why it matters: growing active groups indicate files "
                "are arriving faster than groups are being completed. "
                "When amber: check file arrival patterns and group timeout settings."
            ),
            dataSource=_DS,
            targets=[
                _target(
                    f"{_PREFIX}_job_builder_active_groups{{{lbl}}}",
                    "{{job_builder_name}}",
                ),
            ],
            gridPos=GridPos(h=8, w=4, x=8, y=py1),
        ),
    )

    panels.append(
        _timeseries(
            gs,
            title="Processing Time (avg)",
            targets=[
                _target(
                    _avg_rate(
                        f"{_PREFIX}_job_builder_file_processing_duration_seconds_sum",
                        f"{_PREFIX}_job_builder_file_processing_duration_seconds_count",
                        lbl,
                    ),
                    "{{job_builder_name}}",
                ),
            ],
            unit=YAXIS_SECONDS,
            y=py1,
            w=12,
            x=12,
        ),
    )

    return RowPanel(
        id=_next_id(gs),
        title="Job Builders",
        gridPos=GridPos(h=1, w=24, x=0, y=y_row),
        panels=panels,
    )


# ==========================================================================
# 5. Dispatcher Row (ONLY if model.dispatchers)
# ==========================================================================


def _dispatcher_row(model: DashboardModel, gs: _GenState) -> RowPanel | None:
    """Generate Dispatcher panels (jobs, timing, success, status codes)."""
    if not model.dispatchers:
        return None

    y_row = _advance(gs, 1)
    lbl = 'dispatcher_name=~"$dp_plugin"'

    panels: list = []

    # --- Sub-row 1: jobs processed, execution time, success rate -----------
    py1 = _advance(gs, 8)

    panels.append(
        _timeseries(
            gs,
            title="Jobs Processed",
            targets=[
                _target(
                    _rate(f"{_PREFIX}_dispatcher_jobs_processed_total", lbl),
                    "{{dispatcher_name}} — {{status}}",
                ),
            ],
            unit="ops",
            y=py1,
            w=8,
            x=0,
        ),
    )

    panels.append(
        _timeseries(
            gs,
            title="Execution Time (avg)",
            targets=[
                _target(
                    _avg_rate(
                        f"{_PREFIX}_dispatcher_job_execution_duration_seconds_sum",
                        f"{_PREFIX}_dispatcher_job_execution_duration_seconds_count",
                        lbl,
                    ),
                    "{{dispatcher_name}}",
                ),
            ],
            unit=YAXIS_SECONDS,
            y=py1,
            w=8,
            x=8,
        ),
    )

    success_rate = _rate(
        f"{_PREFIX}_dispatcher_jobs_processed_total", lbl + ', status="success"',
    )
    executed_rate = _rate(
        f"{_PREFIX}_dispatcher_jobs_processed_total", lbl,
    )

    panels.append(
        GaugePanel(
            id=_next_id(gs),
            title="Success Rate",
            dataSource=_DS,
            targets=[
                _target(
                    _clamp_div(
                        f"sum({success_rate})",
                        f"sum({executed_rate})",
                    ),
                ),
            ],
            format=YAXIS_PERCENT,
            min=0,
            max=100,
            thresholds=THRESHOLD_WARN,
            gridPos=GridPos(h=8, w=4, x=16, y=py1),
        ),
    )

    # --- Sub-row 2: status codes table ------------------------------------
    py2 = _advance(gs, 8)

    panels.append(
        Table(
            id=_next_id(gs),
            title="Status Codes",
            dataSource=_DS,
            targets=[
                _target(
                    f"sum by (status)"
                    f" ({_rate(f'{_PREFIX}_dispatcher_jobs_processed_total', lbl)})",
                    "{{status}}",
                ),
            ],
            gridPos=GridPos(h=8, w=24, x=0, y=py2),
        ),
    )

    return RowPanel(
        id=_next_id(gs),
        title="Dispatchers",
        gridPos=GridPos(h=1, w=24, x=0, y=y_row),
        panels=panels,
    )


# ==========================================================================
# 6. SLURM Row (ONLY if model.has_slurm)
# ==========================================================================


def _slurm_row(model: DashboardModel, gs: _GenState) -> RowPanel | None:
    """Generate SLURM-specific panels — jobs submitted, queue depth."""
    if not model.has_slurm:
        return None

    y_row = _advance(gs, 1)
    lbl = 'dispatcher_name=~"$dp_plugin"'
    py = _advance(gs, 8)

    panels: list = []

    panels.append(
        _timeseries(
            gs,
            title="SLURM Jobs Submitted",
            targets=[
                _target(
                    _rate(f"{_PREFIX}_dispatcher_slurm_submissions_total", lbl),
                    "{{dispatcher_name}} — {{status}}",
                ),
            ],
            unit="ops",
            y=py,
            w=12,
            x=0,
        ),
    )

    panels.append(
        _timeseries(
            gs,
            title="SLURM Jobs Pending",
            targets=[
                _target(
                    f"{_PREFIX}_dispatcher_slurm_jobs_pending{{{lbl}}}",
                    "{{dispatcher_name}}",
                ),
            ],
            unit=YAXIS_SHORT,
            y=py,
            w=12,
            x=12,
        ),
    )

    return RowPanel(
        id=_next_id(gs),
        title="SLURM Dispatcher",
        collapsed=True,
        gridPos=GridPos(h=1, w=24, x=0, y=y_row),
        panels=panels,
    )


# ==========================================================================
# 7. HTTP Row (ONLY if model.has_http)
# ==========================================================================


def _http_row(model: DashboardModel, gs: _GenState) -> RowPanel | None:
    """Generate HTTP-specific panels — requests, latency, status codes."""
    if not model.has_http:
        return None

    y_row = _advance(gs, 1)
    lbl = 'dispatcher_name=~"$dp_plugin"'
    py = _advance(gs, 8)

    panels: list = []

    panels.append(
        _timeseries(
            gs,
            title="HTTP Responses",
            targets=[
                _target(
                    _rate(f"{_PREFIX}_dispatcher_http_response_codes_total", lbl),
                    "{{dispatcher_name}} — {{status_code}}",
                ),
            ],
            unit="ops",
            y=py,
            w=8,
            x=0,
        ),
    )

    panels.append(
        _timeseries(
            gs,
            title="HTTP Latency (avg)",
            targets=[
                _target(
                    _avg_rate(
                        f"{_PREFIX}_dispatcher_http_request_duration_seconds_sum",
                        f"{_PREFIX}_dispatcher_http_request_duration_seconds_count",
                        lbl,
                    ),
                    "{{dispatcher_name}}",
                ),
            ],
            unit=YAXIS_SECONDS,
            y=py,
            w=8,
            x=8,
        ),
    )

    http_rate = _rate(
        f"{_PREFIX}_dispatcher_http_response_codes_total", lbl,
    )
    panels.append(
        _timeseries(
            gs,
            title="HTTP Status Codes",
            targets=[
                _target(
                    f"sum by (status_code) ({http_rate})",
                    "{{status_code}}",
                ),
            ],
            stacking={"mode": "normal", "group": "A"},
            fill_opacity=30,
            unit="ops",
            y=py,
            w=8,
            x=16,
        ),
    )

    return RowPanel(
        id=_next_id(gs),
        title="HTTP Dispatcher",
        collapsed=True,
        gridPos=GridPos(h=1, w=24, x=0, y=y_row),
        panels=panels,
    )


# ==========================================================================
# 8. Plugin Manager Row (always generated)
# ==========================================================================


def _plugin_manager_row(_model: DashboardModel, gs: _GenState) -> RowPanel:
    """Generate Plugin Manager panels — state table, restarts, health."""
    y_row = _advance(gs, 1)
    lbl = 'plugin_name=~"$plugin_filter"'
    py = _advance(gs, 8)

    panels: list = []

    panels.append(
        Table(
            id=_next_id(gs),
            title="Plugin States",
            dataSource=_DS,
            targets=[
                _target(
                    f"{_PREFIX}_plugin_state{{{lbl}}}",
                    instant=True,
                ),
            ],
            gridPos=GridPos(h=8, w=10, x=0, y=py),
        ),
    )

    panels.append(
        _timeseries(
            gs,
            title="Plugin Restarts",
            targets=[
                _target(
                    _rate(f"{_PREFIX}_plugin_restarts_total", lbl),
                    "{{plugin_name}}",
                ),
            ],
            unit="ops",
            y=py,
            w=7,
            x=10,
        ),
    )

    panels.append(
        _timeseries(
            gs,
            title="Plugin Health Status",
            targets=[
                _target(
                    f"{_PREFIX}_plugin_health{{{lbl}}}",
                    "{{plugin_name}}",
                ),
            ],
            unit=YAXIS_SHORT,
            y=py,
            w=7,
            x=17,
        ),
    )

    return RowPanel(
        id=_next_id(gs),
        title="Plugin Manager",
        gridPos=GridPos(h=1, w=24, x=0, y=y_row),
        panels=panels,
    )


# ==========================================================================
# 9. Broker Row (always generated)
# ==========================================================================


def _broker_row(_model: DashboardModel, gs: _GenState) -> RowPanel:
    """Generate Broker panels — messages sent/received, errors, connection."""
    y_row = _advance(gs, 1)
    py = _advance(gs, 8)

    panels: list = []

    panels.append(
        _timeseries(
            gs,
            title="Broker Messages Sent",
            targets=[
                _target(
                    _rate(f"{_PREFIX}_broker_messages_sent_total"),
                    "{{queue_name}}",
                ),
            ],
            unit="ops",
            y=py,
            w=6,
            x=0,
        ),
    )

    panels.append(
        _timeseries(
            gs,
            title="Broker Messages Received",
            targets=[
                _target(
                    _rate(f"{_PREFIX}_broker_messages_received_total"),
                    "{{queue_name}}",
                ),
            ],
            unit="ops",
            y=py,
            w=6,
            x=6,
        ),
    )

    panels.append(
        _timeseries(
            gs,
            title="Emit Failures",
            description=(
                "Aggregate rate of job-emit failures across all job builders. "
                "Why it matters: non-zero means dispatched jobs are being "
                "dropped before they reach the broker. "
                "When errors appear: check job builder logs and broker connectivity."
            ),
            targets=[
                _target(
                    _rate(f"{_PREFIX}_job_builder_emit_failures_total"),
                    "{{job_builder_name}} — {{target}} — {{reason}}",
                ),
            ],
            unit="ops",
            y=py,
            w=6,
            x=12,
        ),
    )

    panels.append(
        _timeseries(
            gs,
            title="Broker Connection Status",
            targets=[
                _target(
                    f"{_PREFIX}_broker_connected",
                    "",
                ),
            ],
            unit=YAXIS_SHORT,
            y=py,
            w=6,
            x=18,
        ),
    )

    return RowPanel(
        id=_next_id(gs),
        title="Broker",
        gridPos=GridPos(h=1, w=24, x=0, y=y_row),
        panels=panels,
    )


# ==========================================================================
# 10. Pipeline Summary Row (always generated)
# ==========================================================================


def _pipeline_summary_row(_model: DashboardModel, gs: _GenState) -> RowPanel:
    """Generate end-to-end pipeline summary — timeline overlay, throughput, errors."""
    y_row = _advance(gs, 1)
    py = _advance(gs, 8)

    panels: list = []

    panels.append(
        _timeseries(
            gs,
            title="End-to-End Processing Timeline",
            description=(
                "Overlay of files detected, jobs built, and jobs dispatched "
                "rates to visualise the processing funnel."
            ),
            targets=[
                _target(
                    f"sum({_rate(f'{_PREFIX}_data_monitor_files_processed_total')})",
                    "Files Detected",
                ),
                _target(
                    f"sum({_rate(f'{_PREFIX}_job_builder_jobs_built_total')})",
                    "Jobs Built",
                    ref="B",
                ),
                _target(
                    f"sum({_rate(f'{_PREFIX}_dispatcher_jobs_processed_total')})",
                    "Jobs Processed",
                    ref="C",
                ),
            ],
            unit="ops",
            fill_opacity=10,
            y=py,
            w=12,
            x=0,
        ),
    )

    panels.append(
        Table(
            id=_next_id(gs),
            title="Throughput Summary",
            dataSource=_DS,
            targets=[
                _target(
                    f"sum({_rate(f'{_PREFIX}_data_monitor_files_processed_total')})",
                    "Files/s",
                    ref="A",
                ),
                _target(
                    f"sum({_rate(f'{_PREFIX}_job_builder_jobs_built_total')})",
                    "Jobs/s",
                    ref="B",
                ),
                _target(
                    f"sum({_rate(f'{_PREFIX}_dispatcher_jobs_processed_total')})",
                    "Processed/s",
                    ref="C",
                ),
            ],
            gridPos=GridPos(h=8, w=6, x=12, y=py),
        ),
    )

    dm_err = _rate(
        f"{_PREFIX}_data_monitor_files_processed_total",
        'status="failure"',
    )
    emit_err = _rate(f"{_PREFIX}_job_builder_emit_failures_total")
    dp_err = _rate(
        f"{_PREFIX}_dispatcher_jobs_processed_total",
        'status="failure"',
    )

    panels.append(
        _timeseries(
            gs,
            title="Error Summary (all stages)",
            targets=[
                _target(
                    (
                        f"sum({dm_err})"
                        f" or sum({emit_err})"
                        f" or sum({dp_err})"
                    ),
                    "Total Errors",
                ),
            ],
            unit="ops",
            y=py,
            w=6,
            x=18,
        ),
    )

    return RowPanel(
        id=_next_id(gs),
        title="Pipeline Summary",
        gridPos=GridPos(h=1, w=24, x=0, y=y_row),
        panels=panels,
    )


# ==========================================================================
# Reusable panel constructor helpers
# ==========================================================================


def _timeseries(  # noqa: PLR0913
    gs: _GenState,
    title: str,
    targets: list[Target],
    *,
    unit: str = YAXIS_SHORT,
    y: int,
    w: int = 8,
    x: int = 0,
    stacking: dict | None = None,
    fill_opacity: int = 0,
    description: str = "",
) -> TimeSeries:
    """Create a TimeSeries panel with consistent defaults."""
    kwargs: dict = {}
    if stacking is not None:
        kwargs["stacking"] = stacking
    if fill_opacity:
        kwargs["fillOpacity"] = fill_opacity
    if description:
        kwargs["description"] = description
    return TimeSeries(
        id=_next_id(gs),
        title=title,
        dataSource=_DS,
        targets=targets,
        unit=unit,
        gridPos=GridPos(h=8, w=w, x=x, y=y),
        **kwargs,
    )


# ==========================================================================
# Main entry-point
# ==========================================================================


def build_prometheus_panels(
    model: DashboardModel,
    *,
    datasource: str = "Prometheus",  # noqa: ARG001 — reserved for future use
) -> list:
    """Generate config-aware Prometheus panels for a Courier pipeline model.

    Only panels relevant to the actually-configured plugins are emitted.
    Rows whose plugin kind has zero entries in *model* are skipped entirely.

    Parameters
    ----------
    model : DashboardModel
        Parsed dashboard configuration model from
        :func:`courier.dashboard.config_parser.parse_config`.
    datasource : str
        Name of the Prometheus datasource in Grafana. Used only for
        the datasource template variable; all panel targets reference
        ``$datasource``.

    Returns
    -------
    list
        Flat list of grafanalib panel objects (Stat, TimeSeries, GaugePanel,
        Table, RowPanel). Suitable for passing as the *panels* argument to
        :class:`grafanalib.core.Dashboard`.

        Template variables are NOT included — use
        :func:`build_prometheus_templates` to obtain them.
    """
    gs = _GenState()
    panels: list = []

    # 1. Service Overview — always generated
    so_row = _service_overview_panels(model, gs)
    if so_row is not None:
        panels.append(so_row)

    # 2. Pipeline Summary — always generated
    panels.append(_pipeline_summary_row(model, gs))

    # 3. Data Monitor — only if configured
    dm_row = _data_monitor_row(model, gs)
    if dm_row is not None:
        panels.append(dm_row)

    # 4. Metadata Router — only if has_metadata_router
    mr_row = _metadata_router_row(model, gs)
    if mr_row is not None:
        panels.append(mr_row)

    # 5. Job Builder — only if configured
    jb_row = _job_builder_row(model, gs)
    if jb_row is not None:
        panels.append(jb_row)

    # 6. Dispatcher — only if configured
    dp_row = _dispatcher_row(model, gs)
    if dp_row is not None:
        panels.append(dp_row)

    # 7. SLURM — only if has_slurm
    slurm_row = _slurm_row(model, gs)
    if slurm_row is not None:
        panels.append(slurm_row)

    # 8. HTTP — only if has_http
    http_row = _http_row(model, gs)
    if http_row is not None:
        panels.append(http_row)

    # 9. Plugin Manager — always generated
    panels.append(_plugin_manager_row(model, gs))

    # 10. Broker — always generated
    panels.append(_broker_row(model, gs))

    return panels
