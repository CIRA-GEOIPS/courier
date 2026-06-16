"""LogQL query panel generation for Grafana Loki dashboards.

Generates Grafana panel objects (Row containers with nested TimeSeries, Table,
and Logs panels) that query the Loki datasource using LogQL.  Panels provide
log-level visibility, error inspection, and per-service log streams for
Courier pipelines.

Every query is expressed as an inline dict target (not a grafanalib Loki
wrapper class), since grafanalib's Loki integration varies by version.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from grafanalib.core import GridPos, Row, Table

if TYPE_CHECKING:
    from courier.dashboard.config_parser import DashboardModel


# ---------------------------------------------------------------------------
# Helper -- inline Loki datasource target dict
# ---------------------------------------------------------------------------


def _target(
    expr: str,
    *,
    ref_id: str = "A",
    datasource_uid: str,
    legend: str = "",
) -> dict:
    """Return a grafanalib-compatible inline target dict for a LogQL query.

    Parameters
    ----------
    expr : str
        The LogQL query string, e.g. ``"{service_name=\"mypipeline\"}"``.
    ref_id : str
        Grafana target reference identifier (``"A"``, ``"B"``, ...).
    datasource_uid : str
        The ``uid`` of the Loki datasource (e.g. ``"Loki"``).
    legend : str
        Legend format string for the series.

    Returns
    -------
    dict
        Inline target dict suitable for passing inside a ``TimeSeries`` or
        ``Table`` panel's ``targets`` list.
    """
    return {
        "refId": ref_id,
        "datasource": {"type": "loki", "uid": datasource_uid},
        "expr": expr,
        "legendFormat": legend or ref_id,
    }


# ---------------------------------------------------------------------------
# Row-level constants -- shared across callers
# ---------------------------------------------------------------------------

_PANEL_W = 24

_LOG_LEVELS_H = 8
_LOG_RATE_H = 8
_ERROR_LOG_H = 12
_RECENT_LOGS_H = 10
_PER_PLUGIN_H = 8


# ---------------------------------------------------------------------------
# 1. Log overview (always generated)
# ---------------------------------------------------------------------------


def _build_log_overview_row(datasource: str, y: int) -> tuple[list, int]:
    """Log Overview -- log level distribution + log rate over time."""
    service_filter = '{service_name="$service_name"}'
    level_filter = (
        f"{service_filter} | logfmt"
        ' | level != ""'
    )

    panels = [
        Table(
            title="Log Level Distribution (last 1h)",
            gridPos=GridPos(h=_LOG_LEVELS_H, w=12, x=0, y=y),
            targets=[
                _target(
                    f"sum by(level) (count_over_time({level_filter} [$__range]))",
                    datasource_uid=datasource,
                    legend="",
                ),
            ],
        ),
        Table(
            title="Log Rate (lines/min)",
            gridPos=GridPos(h=_LOG_LEVELS_H, w=12, x=12, y=y),
            targets=[
                _target(
                    f"sum by(plugin) (rate({service_filter} | logfmt [$__auto]))",
                    datasource_uid=datasource,
                    legend="{{plugin}}",
                ),
            ],
        ),
    ]
    y += max(p.gridPos.h for p in panels)
    row = Row(
        title="Log Overview",
        panels=panels,
    )
    return [row], y


# ---------------------------------------------------------------------------
# 2. Error log inspection (always generated)
# ---------------------------------------------------------------------------


def _build_error_log_row(datasource: str, y: int) -> tuple[list, int]:
    """Error logs -- filtered to ERROR level."""
    error_filter = (
        '{service_name="$service_name"}'
        " | logfmt"
        ' | level = "ERROR"'
    )

    panels = [
        Table(
            title="Error Logs by Plugin (last 1h)",
            gridPos=GridPos(h=_LOG_LEVELS_H, w=12, x=0, y=y),
            targets=[
                _target(
                    f"sum by(plugin) (count_over_time({error_filter} [$__range]))",
                    datasource_uid=datasource,
                    legend="{{plugin}}",
                ),
            ],
        ),
        Table(
            title="Error Log Details",
            gridPos=GridPos(h=_ERROR_LOG_H, w=12, x=12, y=y),
            targets=[
                _target(
                    error_filter + ' | line_format "{{.message}}"',
                    datasource_uid=datasource,
                    legend="{{plugin}}",
                ),
            ],
        ),
    ]
    y += max(p.gridPos.h for p in panels)
    row = Row(
        title="Error Logs",
        panels=panels,
    )
    return [row], y


# ---------------------------------------------------------------------------
# 3. Data Monitor logs
# ---------------------------------------------------------------------------


def _build_data_monitor_log_row(datasource: str, y: int) -> tuple[list, int]:
    """Data Monitor log stream."""
    dm_filter = (
        '{service_name="$service_name", plugin="$dm_log_plugin"}'
        " | logfmt"
    )
    panels = [
        Table(
            title="Data Monitor: Log Rate by Level",
            gridPos=GridPos(h=_LOG_LEVELS_H, w=12, x=0, y=y),
            targets=[
                _target(
                    f"sum by(level) (rate({dm_filter} | level != \"\" [$__auto]))",
                    datasource_uid=datasource,
                    legend="{{level}}",
                ),
            ],
        ),
        Table(
            title="Data Monitor: Recent Logs",
            gridPos=GridPos(h=_PER_PLUGIN_H, w=12, x=12, y=y),
            targets=[
                _target(
                    dm_filter + ' | line_format "{{.message}}"',
                    datasource_uid=datasource,
                    legend="",
                ),
            ],
        ),
    ]
    y += max(p.gridPos.h for p in panels)
    row = Row(
        title="Data Monitor Logs",
        panels=panels,
    )
    return [row], y


# ---------------------------------------------------------------------------
# 4. Job Builder logs
# ---------------------------------------------------------------------------


def _build_job_builder_log_row(datasource: str, y: int) -> tuple[list, int]:
    """Job Builder log stream."""
    jb_filter = (
        '{service_name="$service_name", plugin="$jb_log_plugin"}'
        " | logfmt"
    )
    panels = [
        Table(
            title="Job Builder: Log Rate by Level",
            gridPos=GridPos(h=_LOG_LEVELS_H, w=12, x=0, y=y),
            targets=[
                _target(
                    f"sum by(level) (rate({jb_filter} | level != \"\" [$__auto]))",
                    datasource_uid=datasource,
                    legend="{{level}}",
                ),
            ],
        ),
        Table(
            title="Job Builder: Recent Logs",
            gridPos=GridPos(h=_PER_PLUGIN_H, w=12, x=12, y=y),
            targets=[
                _target(
                    jb_filter + ' | line_format "{{.message}}"',
                    datasource_uid=datasource,
                    legend="",
                ),
            ],
        ),
    ]
    y += max(p.gridPos.h for p in panels)
    row = Row(
        title="Job Builder Logs",
        panels=panels,
    )
    return [row], y


# ---------------------------------------------------------------------------
# 5. Dispatcher logs
# ---------------------------------------------------------------------------


def _build_dispatcher_log_row(datasource: str, y: int) -> tuple[list, int]:
    """Dispatcher log stream."""
    dp_filter = (
        '{service_name="$service_name", plugin="$dp_log_plugin"}'
        " | logfmt"
    )
    panels = [
        Table(
            title="Dispatcher: Log Rate by Level",
            gridPos=GridPos(h=_LOG_LEVELS_H, w=12, x=0, y=y),
            targets=[
                _target(
                    f"sum by(level) (rate({dp_filter} | level != \"\" [$__auto]))",
                    datasource_uid=datasource,
                    legend="{{level}}",
                ),
            ],
        ),
        Table(
            title="Dispatcher: Recent Logs",
            gridPos=GridPos(h=_PER_PLUGIN_H, w=12, x=12, y=y),
            targets=[
                _target(
                    dp_filter + ' | line_format "{{.message}}"',
                    datasource_uid=datasource,
                    legend="",
                ),
            ],
        ),
    ]
    y += max(p.gridPos.h for p in panels)
    row = Row(
        title="Dispatcher Logs",
        panels=panels,
    )
    return [row], y


# ---------------------------------------------------------------------------
# 6. Log search (always generated)
# ---------------------------------------------------------------------------


def _build_log_search_row(datasource: str, y: int) -> tuple[list, int]:
    """Full log search -- grep-style query with template variable."""
    search_filter = (
        '{service_name="$service_name"}'
        " | logfmt"
        " |~ `$log_search`"
    )
    panels = [
        Table(
            title="Log Search Results",
            gridPos=GridPos(h=_RECENT_LOGS_H, w=24, x=0, y=y),
            targets=[
                _target(
                    search_filter + ' | line_format "{{.message}}"',
                    datasource_uid=datasource,
                    legend="{{plugin}}",
                ),
            ],
        ),
    ]
    y += _RECENT_LOGS_H
    row = Row(
        title="Log Search",
        panels=panels,
    )
    return [row], y


# ---------------------------------------------------------------------------
# Template variables
# ---------------------------------------------------------------------------


def build_loki_templates(model: DashboardModel) -> list:
    """Build grafanalib Template objects for Loki dashboard variables.

    Parameters
    ----------
    model : DashboardModel
        Parsed dashboard configuration model.

    Returns
    -------
    list
        Grafana Template objects for the Loki datasource dashboard.
    """
    from grafanalib.core import (  # noqa: PLC0415
        Template,
    )

    templates: list = []

    service = model.service_name or "data-inventory-ingest"
    templates.append(
        Template(
            name="service_name",
            label="Service",
            type="custom",
            query=service,
            default=service,
            allValue=".*",
            includeAll=False,
            multi=False,
        )
    )

    dm_names = [p.plugin_name for p in model.data_monitors]
    if dm_names:
        templates.append(
            Template(
                name="dm_log_plugin",
                label="DM Plugin",
                type="custom",
                query=",".join(sorted(set(dm_names))),
                default=dm_names[0],
                options=[],
                allValue=".*",
                includeAll=True,
                multi=False,
            )
        )

    jb_names = [p.plugin_name for p in model.job_builders]
    if jb_names:
        templates.append(
            Template(
                name="jb_log_plugin",
                label="JB Plugin",
                type="custom",
                query=",".join(sorted(set(jb_names))),
                default=jb_names[0],
                options=[],
                allValue=".*",
                includeAll=True,
                multi=False,
            )
        )

    dp_names = [p.plugin_name for p in model.dispatchers]
    if dp_names:
        templates.append(
            Template(
                name="dp_log_plugin",
                label="DP Plugin",
                type="custom",
                query=",".join(sorted(set(dp_names))),
                default=dp_names[0],
                options=[],
                allValue=".*",
                includeAll=True,
                multi=False,
            )
        )

    templates.append(
        Template(
            name="log_search",
            label="Log Search",
            type="textbox",
            query="",
            allValue=".*",
            includeAll=False,
            multi=False,
        )
    )

    return templates


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def build_loki_panels(
    model: DashboardModel,
    *,
    datasource: str = "Loki",
) -> list:
    """Generate config-aware LogQL panels for a Loki datasource.

    Returns a list of grafanalib ``Row`` panel objects, each containing
    ``Table`` panels that query Loki with LogQL.  Panels cover log levels,
    errors, per-plugin log streams, and free-text search.

    Panel rows are only emitted when the model contains relevant plugins.

    Parameters
    ----------
    model : DashboardModel
        Parsed dashboard model built from a courier service config.
    datasource : str
        Grafana datasource name or UID for Loki (default ``"Loki"``).

    Returns
    -------
    list
        Ordered list of grafanalib panel objects (``Row`` instances).
    """
    rows: list = []
    y = 0

    # 1. Log Overview -- always
    ov_rows, y = _build_log_overview_row(datasource, y)
    rows.extend(ov_rows)

    # 2. Error Logs -- always
    err_rows, y = _build_error_log_row(datasource, y)
    rows.extend(err_rows)

    # 3. Data Monitor logs -- only if configured
    if model.data_monitors:
        dm_rows, y = _build_data_monitor_log_row(datasource, y)
        rows.extend(dm_rows)

    # 4. Job Builder logs -- only if configured
    if model.job_builders:
        jb_rows, y = _build_job_builder_log_row(datasource, y)
        rows.extend(jb_rows)

    # 5. Dispatcher logs -- only if configured
    if model.dispatchers:
        dp_rows, y = _build_dispatcher_log_row(datasource, y)
        rows.extend(dp_rows)

    # 6. Log Search -- always
    search_rows, y = _build_log_search_row(datasource, y)
    rows.extend(search_rows)

    return rows
