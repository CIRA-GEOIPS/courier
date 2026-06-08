#!/usr/bin/env python3
"""Generate a Grafana dashboard JSON for Courier monitoring.

Produces a comprehensive dashboard covering all Prometheus metrics
exposed by the Courier service. The output is valid Grafana
dashboard JSON that can be imported via the UI or placed in a
provisioning directory.

Usage
-----
    python examples/grafana/courier_dashboard.py > dashboard.json

Then import ``dashboard.json`` into Grafana (Dashboards -> Import)
or copy it into your Grafana provisioning directory.

Requirements
------------
    pip install grafanalib          # or: pip install -e .[grafana]
"""

from __future__ import annotations

import json
import sys

from grafanalib._gen import DashboardEncoder
from grafanalib.core import (
    GAUGE_CALC_LAST,
    GRAPH_TOOLTIP_MODE_SHARED_CROSSHAIR,
    PERCENT_FORMAT,
    REFRESH_ON_TIME_RANGE_CHANGE,
    SECONDS_FORMAT,
    Dashboard,
    GaugePanel,
    GridPos,
    RowPanel,
    Stat,
    StatValueMappingItem,
    StatValueMappings,
    Table,
    Target,
    Template,
    Templating,
    Threshold,
    Time,
    TimeSeries,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DS = "$datasource"
_PREFIX = "courier"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _target(
    expr: str,
    legend: str = "",
    *,
    ref: str = "A",
    instant: bool = False,
) -> Target:
    """Create a Prometheus target."""
    return Target(
        expr=expr,
        legendFormat=legend,
        refId=ref,
        instant=instant,
        datasource=_DS,
    )


def _rate(metric: str, labels: str = "", interval: str = "5m") -> str:
    """Wrap a metric in ``rate(...)``."""
    selector = f"{{{labels}}}" if labels else ""
    return f"rate({metric}{selector}[{interval}])"


def _hq(
    metric: str,
    quantile: float,
    labels: str = "",
    interval: str = "5m",
) -> str:
    """Build a ``histogram_quantile`` query."""
    selector = f"{{{labels}}}" if labels else ""
    return (
        f"histogram_quantile({quantile}, rate({metric}_bucket{selector}[{interval}]))"
    )


# ---------------------------------------------------------------------------
# Template variables
# ---------------------------------------------------------------------------


def _templating() -> Templating:
    ds = Template(
        name="datasource",
        label="Data Source",
        query="prometheus",
        type="datasource",
    )
    monitor = Template(
        name="monitor_name",
        label="Data Monitor",
        query=(
            f"label_values({_PREFIX}_data_monitor_files_processed_total, monitor_name)"
        ),
        dataSource=_DS,
        includeAll=True,
        multi=True,
        refresh=REFRESH_ON_TIME_RANGE_CHANGE,
    )
    builder = Template(
        name="job_builder_name",
        label="Job Builder",
        query=(
            f"label_values({_PREFIX}_job_builder_files_received_total"
            ", job_builder_name)"
        ),
        dataSource=_DS,
        includeAll=True,
        multi=True,
        refresh=REFRESH_ON_TIME_RANGE_CHANGE,
    )
    dispatcher = Template(
        name="dispatcher_name",
        label="Dispatcher",
        query=(
            f"label_values({_PREFIX}_dispatcher_jobs_processed_total, dispatcher_name)"
        ),
        dataSource=_DS,
        includeAll=True,
        multi=True,
        refresh=REFRESH_ON_TIME_RANGE_CHANGE,
    )
    plugin = Template(
        name="plugin_name",
        label="Plugin",
        query=f"label_values({_PREFIX}_plugin_state, plugin_name)",
        dataSource=_DS,
        includeAll=True,
        multi=True,
        refresh=REFRESH_ON_TIME_RANGE_CHANGE,
    )
    sync_builder = Template(
        name="builder_name",
        label="Sync Builder",
        query=f"label_values({_PREFIX}_state_sync_pushes_total, builder_name)",
        dataSource=_DS,
        includeAll=True,
        multi=True,
        refresh=REFRESH_ON_TIME_RANGE_CHANGE,
    )
    dispatcher_identifier = Template(
        name="dispatcher_identifier",
        label="Dispatcher Identifier",
        query=(
            f"label_values({_PREFIX}_dispatcher_jobs_consumed_total,"
            " dispatcher_identifier)"
        ),
        dataSource=_DS,
        includeAll=True,
        multi=True,
        refresh=REFRESH_ON_TIME_RANGE_CHANGE,
    )
    queue = Template(
        name="queue",
        label="Queue",
        query=(
            f"label_values("
            f"{_PREFIX}_rabbitmq_last_file_emitted_timestamp_seconds, queue)"
        ),
        dataSource=_DS,
        includeAll=True,
        multi=True,
        refresh=REFRESH_ON_TIME_RANGE_CHANGE,
    )
    error_type = Template(
        name="error_type",
        label="Error Type",
        query=(f"label_values({_PREFIX}_data_monitor_poll_errors_total, error_type)"),
        dataSource=_DS,
        includeAll=True,
        multi=True,
        refresh=REFRESH_ON_TIME_RANGE_CHANGE,
    )
    topic = Template(
        name="topic",
        label="Topic",
        query=(f"label_values({_PREFIX}_data_monitor_consumer_lag, topic)"),
        dataSource=_DS,
        includeAll=True,
        multi=True,
        refresh=REFRESH_ON_TIME_RANGE_CHANGE,
    )
    route_name = Template(
        name="route_name",
        label="Route Name",
        query=(f"label_values({_PREFIX}_job_builder_route_matches_total, route_name)"),
        dataSource=_DS,
        includeAll=True,
        multi=True,
        refresh=REFRESH_ON_TIME_RANGE_CHANGE,
    )
    status_code = Template(
        name="status_code",
        label="HTTP Status Code",
        query=(
            f"label_values({_PREFIX}_dispatcher_http_response_codes_total, status_code)"
        ),
        dataSource=_DS,
        includeAll=True,
        multi=True,
        refresh=REFRESH_ON_TIME_RANGE_CHANGE,
    )
    return Templating(
        list=[
            ds,
            monitor,
            builder,
            dispatcher,
            plugin,
            sync_builder,
            dispatcher_identifier,
            queue,
            error_type,
            topic,
            route_name,
            status_code,
        ],
    )


# ---------------------------------------------------------------------------
# Row builders
# ---------------------------------------------------------------------------

# Vertical cursor — tracks the current Y position on the dashboard grid.
_y = 0


def _advance(height: int) -> int:
    """Return current Y and advance the cursor by *height* rows."""
    global _y  # noqa: PLW0603
    cur = _y
    _y += height
    return cur


def _service_overview() -> list[Stat]:
    """Row 1 — four stat panels across the top."""
    y = _advance(4)

    health = Stat(
        title="Service Health",
        dataSource=_DS,
        targets=[_target(f"{_PREFIX}_service_health")],
        reduceCalc=GAUGE_CALC_LAST,
        thresholds=[
            Threshold("red", 0, 0.0),
            Threshold("green", 1, 1.0),
        ],
        mappings=[
            StatValueMappings(
                StatValueMappingItem("Healthy", "1", "green"),
                StatValueMappingItem("Unhealthy", "0", "red"),
            ),
        ],
        gridPos=GridPos(4, 6, 0, y),
    )

    uptime = Stat(
        title="Service Uptime",
        dataSource=_DS,
        targets=[_target(f"{_PREFIX}_service_uptime_seconds")],
        format=SECONDS_FORMAT,
        reduceCalc=GAUGE_CALC_LAST,
        gridPos=GridPos(4, 6, 6, y),
    )

    heartbeat = Stat(
        title="Last Heartbeat Age",
        dataSource=_DS,
        targets=[
            _target(
                f"time() - {_PREFIX}_service_heartbeat_timestamp_seconds",
            ),
        ],
        format=SECONDS_FORMAT,
        reduceCalc=GAUGE_CALC_LAST,
        thresholds=[
            Threshold("green", 0, 0.0),
            Threshold("yellow", 1, 30.0),
            Threshold("red", 2, 60.0),
        ],
        gridPos=GridPos(4, 6, 12, y),
    )

    files_total = Stat(
        title="Total Files Processed",
        dataSource=_DS,
        targets=[
            _target(
                f"sum({_PREFIX}_data_monitor_files_processed_total)",
            ),
        ],
        reduceCalc=GAUGE_CALC_LAST,
        gridPos=GridPos(4, 6, 18, y),
    )

    return [health, uptime, heartbeat, files_total]


def _data_monitor_row() -> RowPanel:
    """Row 2 — Data Monitor panels."""
    y = _advance(1)  # row header
    py = _advance(8)
    lbl = 'monitor_name=~"$monitor_name"'

    rate_panel = TimeSeries(
        title="File Processing Rate",
        dataSource=_DS,
        targets=[
            _target(
                _rate(f"{_PREFIX}_data_monitor_files_processed_total", lbl),
                "{{monitor_name}} — {{status}}",
            ),
        ],
        unit="ops",
        gridPos=GridPos(8, 8, 0, py),
    )

    by_status = TimeSeries(
        title="Files by Status",
        dataSource=_DS,
        targets=[
            _target(
                f"increase({_PREFIX}_data_monitor_files_processed_total"
                f"{{{lbl}}}[$__range])",
                "{{monitor_name}} — {{status}}",
            ),
        ],
        stacking={"mode": "normal", "group": "A"},
        fillOpacity=30,
        gridPos=GridPos(8, 8, 8, py),
    )

    scan_age = TimeSeries(
        title="Last Scan Age",
        dataSource=_DS,
        targets=[
            _target(
                f"time() - {_PREFIX}_data_monitor_last_scan_timestamp_seconds{{{lbl}}}",
                "{{monitor_name}}",
            ),
        ],
        unit=SECONDS_FORMAT,
        gridPos=GridPos(8, 8, 16, py),
    )

    # Second sub-row
    py2 = _advance(8)

    scan_dur = TimeSeries(
        title="Scan Duration",
        dataSource=_DS,
        targets=[
            _target(
                _hq(
                    f"{_PREFIX}_data_monitor_scan_duration_seconds",
                    0.50,
                    lbl,
                ),
                "p50 — {{monitor_name}}",
            ),
            _target(
                _hq(
                    f"{_PREFIX}_data_monitor_scan_duration_seconds",
                    0.95,
                    lbl,
                ),
                "p95 — {{monitor_name}}",
                ref="B",
            ),
        ],
        unit=SECONDS_FORMAT,
        gridPos=GridPos(8, 24, 0, py2),
    )

    # Third sub-row — poll errors and connection status
    py3 = _advance(8)
    lbl_err = 'monitor_name=~"$monitor_name", error_type=~"$error_type"'

    poll_errors = TimeSeries(
        title="Poll Errors by Type",
        dataSource=_DS,
        targets=[
            _target(
                _rate(f"{_PREFIX}_data_monitor_poll_errors_total", lbl_err),
                "{{monitor_name}} — {{error_type}}",
            ),
        ],
        stacking={"mode": "normal", "group": "A"},
        fillOpacity=30,
        gridPos=GridPos(8, 12, 0, py3),
    )

    connection_status = Stat(
        title="Connection Status",
        dataSource=_DS,
        targets=[
            _target(
                f"{_PREFIX}_data_monitor_connection_status"
                f'{{monitor_name=~"$monitor_name"}}',
                "{{monitor_name}}",
            ),
        ],
        reduceCalc=GAUGE_CALC_LAST,
        thresholds=[
            Threshold("red", 0, 0.0),
            Threshold("green", 1, 1.0),
        ],
        mappings=[
            StatValueMappings(
                StatValueMappingItem("Connected", "1", "green"),
                StatValueMappingItem("Disconnected", "0", "red"),
            ),
        ],
        gridPos=GridPos(8, 6, 12, py3),
    )

    # Fourth sub-row — consumer lag and RabbitMQ file timestamp
    py4 = _advance(8)
    lbl_lag = 'monitor_name=~"$monitor_name", topic=~"$topic"'
    lbl_queue = 'queue=~"$queue"'

    consumer_lag = TimeSeries(
        title="Consumer Lag",
        dataSource=_DS,
        targets=[
            _target(
                f"{_PREFIX}_data_monitor_consumer_lag{{{lbl_lag}}}",
                "{{monitor_name}} — {{topic}}",
            ),
        ],
        gridPos=GridPos(8, 12, 0, py4),
    )

    rabbitmq_timestamp = TimeSeries(
        title="Last Emitted File Age",
        description="Age of the most recent file emitted to RabbitMQ.",
        dataSource=_DS,
        targets=[
            _target(
                f"time() - "
                f"{_PREFIX}_rabbitmq_last_file_emitted_timestamp_seconds"
                f"{{{lbl_queue}}}",
                "{{queue}}",
            ),
        ],
        unit=SECONDS_FORMAT,
        gridPos=GridPos(8, 12, 12, py4),
    )

    return RowPanel(
        title="Data Monitors",
        gridPos=GridPos(1, 24, 0, y),
        panels=[
            rate_panel,
            by_status,
            scan_age,
            scan_dur,
            poll_errors,
            connection_status,
            consumer_lag,
            rabbitmq_timestamp,
        ],
    )


def _job_builder_row() -> RowPanel:
    """Row 3 — Job Builder panels."""
    y = _advance(1)
    py = _advance(8)
    lbl = 'job_builder_name=~"$job_builder_name"'

    files_recv = TimeSeries(
        title="Files Received Rate",
        dataSource=_DS,
        targets=[
            _target(
                _rate(f"{_PREFIX}_job_builder_files_received_total", lbl),
                "{{job_builder_name}}",
            ),
        ],
        unit="ops",
        gridPos=GridPos(8, 8, 0, py),
    )

    jobs_built = TimeSeries(
        title="Jobs Built Rate",
        dataSource=_DS,
        targets=[
            _target(
                _rate(f"{_PREFIX}_job_builder_jobs_built_total", lbl),
                "{{job_builder_name}} — {{status}}",
            ),
        ],
        unit="ops",
        gridPos=GridPos(8, 8, 8, py),
    )

    active_groups = TimeSeries(
        title="Active Groups",
        dataSource=_DS,
        targets=[
            _target(
                f"{_PREFIX}_job_builder_active_groups{{{lbl}}}",
                "{{job_builder_name}}",
            ),
        ],
        gridPos=GridPos(8, 8, 16, py),
    )

    # Second sub-row
    py2 = _advance(8)

    discarded = TimeSeries(
        title="Jobs Discarded Rate",
        dataSource=_DS,
        targets=[
            _target(
                _rate(f"{_PREFIX}_job_builder_jobs_discarded_total", lbl),
                "{{job_builder_name}}",
            ),
        ],
        unit="ops",
        gridPos=GridPos(8, 12, 0, py2),
    )

    duration = TimeSeries(
        title="File Processing Duration",
        dataSource=_DS,
        targets=[
            _target(
                _hq(
                    f"{_PREFIX}_job_builder_file_processing_duration_seconds",
                    0.50,
                    lbl,
                ),
                "p50 — {{job_builder_name}}",
            ),
            _target(
                _hq(
                    f"{_PREFIX}_job_builder_file_processing_duration_seconds",
                    0.95,
                    lbl,
                ),
                "p95 — {{job_builder_name}}",
                ref="B",
            ),
            _target(
                _hq(
                    f"{_PREFIX}_job_builder_file_processing_duration_seconds",
                    0.99,
                    lbl,
                ),
                "p99 — {{job_builder_name}}",
                ref="C",
            ),
        ],
        unit=SECONDS_FORMAT,
        gridPos=GridPos(8, 12, 12, py2),
    )

    # Third sub-row
    py3 = _advance(8)

    files_per_job = TimeSeries(
        title="Files per Job",
        dataSource=_DS,
        targets=[
            _target(
                _hq(
                    f"{_PREFIX}_job_builder_files_per_job",
                    0.50,
                    lbl,
                ),
                "p50 — {{job_builder_name}}",
            ),
            _target(
                _hq(
                    f"{_PREFIX}_job_builder_files_per_job",
                    0.95,
                    lbl,
                ),
                "p95 — {{job_builder_name}}",
                ref="B",
            ),
        ],
        gridPos=GridPos(8, 24, 0, py3),
    )

    # Fourth sub-row — timeout emissions
    py4 = _advance(8)

    timeout_emissions = TimeSeries(
        title="Timeout Emissions",
        description="Rate of jobs discarded due to builder timeouts.",
        dataSource=_DS,
        targets=[
            _target(
                _rate(f"{_PREFIX}_job_builder_timeout_emissions_total", lbl),
                "{{job_builder_name}}",
            ),
        ],
        unit="ops",
        gridPos=GridPos(8, 24, 0, py4),
    )

    return RowPanel(
        title="Job Builders",
        gridPos=GridPos(1, 24, 0, y),
        panels=[
            files_recv,
            jobs_built,
            active_groups,
            discarded,
            duration,
            files_per_job,
            timeout_emissions,
        ],
    )


def _dispatcher_row() -> RowPanel:
    """Row 4 — Dispatcher panels."""
    y = _advance(1)
    py = _advance(8)
    lbl = 'dispatcher_name=~"$dispatcher_name"'
    m_proc = f"{_PREFIX}_dispatcher_jobs_processed_total"

    jobs_rate = TimeSeries(
        title="Jobs Processed Rate",
        dataSource=_DS,
        targets=[
            _target(_rate(m_proc, lbl), "{{dispatcher_name}} — {{status}}"),
        ],
        unit="ops",
        gridPos=GridPos(8, 8, 0, py),
    )

    success_ratio = GaugePanel(
        title="Job Success Ratio",
        dataSource=_DS,
        targets=[
            _target(
                f"sum({_rate(m_proc, f'{lbl}, status="success"')})"
                f" / sum({_rate(m_proc, lbl)})",
            ),
        ],
        format=PERCENT_FORMAT,
        min=0,
        max=100,
        thresholds=[
            Threshold("red", 0, 0.0),
            Threshold("yellow", 1, 80.0),
            Threshold("green", 2, 95.0),
        ],
        gridPos=GridPos(8, 4, 8, py),
    )

    active = TimeSeries(
        title="Active Jobs",
        dataSource=_DS,
        targets=[
            _target(
                f"{_PREFIX}_dispatcher_active_jobs{{{lbl}}}",
                "{{dispatcher_name}}",
            ),
        ],
        gridPos=GridPos(8, 12, 12, py),
    )

    # Second sub-row
    py2 = _advance(8)

    exec_dur = TimeSeries(
        title="Execution Duration",
        dataSource=_DS,
        targets=[
            _target(
                _hq(
                    f"{_PREFIX}_dispatcher_job_execution_duration_seconds",
                    0.50,
                    lbl,
                ),
                "p50 — {{dispatcher_name}}",
            ),
            _target(
                _hq(
                    f"{_PREFIX}_dispatcher_job_execution_duration_seconds",
                    0.95,
                    lbl,
                ),
                "p95 — {{dispatcher_name}}",
                ref="B",
            ),
            _target(
                _hq(
                    f"{_PREFIX}_dispatcher_job_execution_duration_seconds",
                    0.99,
                    lbl,
                ),
                "p99 — {{dispatcher_name}}",
                ref="C",
            ),
        ],
        unit=SECONDS_FORMAT,
        gridPos=GridPos(8, 12, 0, py2),
    )

    logs_emitted = TimeSeries(
        title="Execution Logs Emitted",
        dataSource=_DS,
        targets=[
            _target(
                _rate(
                    f"{_PREFIX}_dispatcher_execution_logs_emitted_total",
                    lbl,
                ),
                "{{dispatcher_name}}",
            ),
        ],
        unit="ops",
        gridPos=GridPos(8, 12, 12, py2),
    )

    # Third sub-row
    py3 = _advance(8)

    queue_wait = TimeSeries(
        title="Queue Wait Duration",
        description="Time jobs spent waiting in queue before dispatch.",
        dataSource=_DS,
        targets=[
            _target(
                _hq(
                    f"{_PREFIX}_dispatcher_queue_wait_duration_seconds",
                    0.50,
                    lbl,
                ),
                "p50 — {{dispatcher_name}}",
            ),
            _target(
                _hq(
                    f"{_PREFIX}_dispatcher_queue_wait_duration_seconds",
                    0.95,
                    lbl,
                ),
                "p95 — {{dispatcher_name}}",
                ref="B",
            ),
        ],
        unit=SECONDS_FORMAT,
        gridPos=GridPos(8, 24, 0, py3),
    )

    # Fourth sub-row — parallel workers active
    py4 = _advance(8)

    parallel_workers = TimeSeries(
        title="Parallel Workers Active",
        description="Number of parallel dispatch workers currently processing.",
        dataSource=_DS,
        targets=[
            _target(
                f"{_PREFIX}_dispatcher_parallel_workers_active{{{lbl}}}",
                "{{dispatcher_name}}",
            ),
        ],
        gridPos=GridPos(8, 24, 0, py4),
    )

    return RowPanel(
        title="Dispatchers",
        gridPos=GridPos(1, 24, 0, y),
        panels=[
            jobs_rate,
            success_ratio,
            active,
            exec_dur,
            logs_emitted,
            queue_wait,
            parallel_workers,
        ],
    )


def _plugin_manager_row() -> RowPanel:
    """Row 5 — Plugin manager panels."""
    y = _advance(1)
    py = _advance(8)
    lbl = 'plugin_name=~"$plugin_name"'

    health_stat = Stat(
        title="Plugin Health",
        dataSource=_DS,
        targets=[
            _target(
                f"{_PREFIX}_plugin_health{{{lbl}}}",
                "{{plugin_name}}",
            ),
        ],
        reduceCalc=GAUGE_CALC_LAST,
        thresholds=[
            Threshold("red", 0, 0.0),
            Threshold("green", 1, 1.0),
        ],
        mappings=[
            StatValueMappings(
                StatValueMappingItem("Healthy", "1", "green"),
                StatValueMappingItem("Unhealthy", "0", "red"),
            ),
        ],
        gridPos=GridPos(8, 6, 0, py),
    )

    state_table = Table(
        title="Plugin Status",
        dataSource=_DS,
        targets=[
            _target(
                f"{_PREFIX}_plugin_state{{{lbl}}}",
                instant=True,
            ),
            _target(
                f"{_PREFIX}_plugin_health{{{lbl}}}",
                ref="B",
                instant=True,
            ),
        ],
        gridPos=GridPos(8, 10, 6, py),
    )

    restarts = TimeSeries(
        title="Plugin Restarts",
        dataSource=_DS,
        targets=[
            _target(
                _rate(f"{_PREFIX}_plugin_restarts_total", lbl),
                "{{plugin_name}}",
            ),
        ],
        unit="ops",
        gridPos=GridPos(8, 8, 16, py),
    )

    return RowPanel(
        title="Plugin Manager",
        gridPos=GridPos(1, 24, 0, y),
        panels=[health_stat, state_table, restarts],
    )


def _broker_row() -> RowPanel:
    """Broker connectivity and message throughput."""
    y = _advance(1)
    py = _advance(8)

    connected = Stat(
        title="Broker Connected",
        dataSource=_DS,
        targets=[_target(f"{_PREFIX}_broker_connected")],
        reduceCalc=GAUGE_CALC_LAST,
        thresholds=[
            Threshold("red", 0, 0.0),
            Threshold("green", 1, 1.0),
        ],
        mappings=[
            StatValueMappings(
                StatValueMappingItem("Connected", "1", "green"),
                StatValueMappingItem("Disconnected", "0", "red"),
            ),
        ],
        gridPos=GridPos(8, 4, 0, py),
    )

    connections = TimeSeries(
        title="Connection Attempts",
        dataSource=_DS,
        targets=[
            _target(
                _rate(f"{_PREFIX}_broker_connections_total"),
                "{{status}}",
            ),
        ],
        unit="ops",
        gridPos=GridPos(8, 6, 4, py),
    )

    msgs_sent = TimeSeries(
        title="Messages Sent",
        dataSource=_DS,
        targets=[
            _target(
                _rate(f"{_PREFIX}_broker_messages_sent_total"),
                "{{queue_name}}",
            ),
        ],
        unit="ops",
        gridPos=GridPos(8, 7, 10, py),
    )

    msgs_recv = TimeSeries(
        title="Messages Received",
        dataSource=_DS,
        targets=[
            _target(
                _rate(f"{_PREFIX}_broker_messages_received_total"),
                "{{queue_name}}",
            ),
        ],
        unit="ops",
        gridPos=GridPos(8, 7, 17, py),
    )

    return RowPanel(
        title="Broker",
        gridPos=GridPos(1, 24, 0, y),
        panels=[connected, connections, msgs_sent, msgs_recv],
    )


def _state_sync_row() -> RowPanel:
    """HA state sync panels (collapsed by default)."""
    y = _advance(1)
    py = _advance(8)
    lbl = 'builder_name=~"$builder_name"'

    pushes = TimeSeries(
        title="State Pushes",
        dataSource=_DS,
        targets=[
            _target(
                _rate(f"{_PREFIX}_state_sync_pushes_total", lbl),
                "{{builder_name}} — {{event}}",
            ),
        ],
        unit="ops",
        gridPos=GridPos(8, 6, 0, py),
    )

    applies = TimeSeries(
        title="State Applies",
        dataSource=_DS,
        targets=[
            _target(
                _rate(f"{_PREFIX}_state_sync_applies_total", lbl),
                "{{builder_name}}",
            ),
        ],
        unit="ops",
        gridPos=GridPos(8, 6, 6, py),
    )

    claims = TimeSeries(
        title="Emit Claims",
        dataSource=_DS,
        targets=[
            _target(
                _rate(f"{_PREFIX}_state_sync_emit_claims_total", lbl),
                "{{builder_name}} — {{result}}",
            ),
        ],
        unit="ops",
        gridPos=GridPos(8, 6, 12, py),
    )

    errors = TimeSeries(
        title="Sync Errors",
        dataSource=_DS,
        targets=[
            _target(
                _rate(f"{_PREFIX}_state_sync_errors_total", lbl),
                "{{builder_name}} — {{operation}}",
            ),
        ],
        unit="ops",
        gridPos=GridPos(8, 6, 18, py),
    )

    return RowPanel(
        title="State Sync / HA",
        collapsed=True,
        gridPos=GridPos(1, 24, 0, y),
        panels=[pushes, applies, claims, errors],
    )


def _routing_row() -> RowPanel:
    """Row — per-target routing throughput, failures, and dispatch latency."""
    y = _advance(1)
    py = _advance(8)
    lbl_d = 'dispatcher_identifier=~"$dispatcher_identifier"'

    emitted = TimeSeries(
        title="Jobs Emitted by Target",
        description=(
            "Stacked rate of jobs a builder published to each dispatcher "
            "identifier — validates fan-out distribution."
        ),
        dataSource=_DS,
        targets=[
            _target(
                _rate(f"{_PREFIX}_job_builder_jobs_emitted_total"),
                "{{job_builder_name}} → {{target}}",
            ),
        ],
        stacking={"mode": "normal", "group": "A"},
        fillOpacity=30,
        unit="ops",
        gridPos=GridPos(8, 8, 0, py),
    )

    emit_failures = TimeSeries(
        title="Emit Failures by Reason",
        description=(
            "Non-zero fatal failures indicate jobs lost before reaching a "
            "dispatcher; route an alert on fatal > 0."
        ),
        dataSource=_DS,
        targets=[
            _target(
                _rate(f"{_PREFIX}_job_builder_emit_failures_total"),
                "{{job_builder_name}} → {{target}} — {{reason}}",
            ),
        ],
        unit="ops",
        gridPos=GridPos(8, 8, 8, py),
    )

    consumed = TimeSeries(
        title="Jobs Consumed by Dispatcher",
        description=(
            "Zero on a dispatcher that should be receiving traffic is a "
            "smoke-test for misrouting."
        ),
        dataSource=_DS,
        targets=[
            _target(
                _rate(
                    f"{_PREFIX}_dispatcher_jobs_consumed_total",
                    lbl_d,
                ),
                "{{dispatcher_identifier}}",
            ),
        ],
        unit="ops",
        gridPos=GridPos(8, 8, 16, py),
    )

    py2 = _advance(8)

    latency = TimeSeries(
        title="Dispatch Latency (emit → consume)",
        dataSource=_DS,
        targets=[
            _target(
                _hq(
                    f"{_PREFIX}_dispatcher_dispatch_latency_seconds",
                    0.50,
                    lbl_d,
                ),
                "p50 — {{dispatcher_identifier}}",
            ),
            _target(
                _hq(
                    f"{_PREFIX}_dispatcher_dispatch_latency_seconds",
                    0.95,
                    lbl_d,
                ),
                "p95 — {{dispatcher_identifier}}",
                ref="B",
            ),
            _target(
                _hq(
                    f"{_PREFIX}_dispatcher_dispatch_latency_seconds",
                    0.99,
                    lbl_d,
                ),
                "p99 — {{dispatcher_identifier}}",
                ref="C",
            ),
        ],
        unit=SECONDS_FORMAT,
        gridPos=GridPos(8, 8, 0, py2),
    )

    depth = TimeSeries(
        title="Queue Depth per Dispatcher",
        dataSource=_DS,
        targets=[
            _target(
                f"{_PREFIX}_dispatcher_queue_depth{{{lbl_d}}}",
                "{{dispatcher_identifier}}",
            ),
        ],
        gridPos=GridPos(8, 8, 8, py2),
    )

    dedupe_skips = TimeSeries(
        title="Dedupe Skips",
        description=(
            "Rate of duplicate dispatch jobs skipped by the deduplication filter."
        ),
        dataSource=_DS,
        targets=[
            _target(
                _rate(f"{_PREFIX}_dispatcher_dedupe_skips_total", lbl_d),
                "{{dispatcher_identifier}}",
            ),
        ],
        unit="ops",
        gridPos=GridPos(8, 8, 16, py2),
    )

    return RowPanel(
        title="Routing",
        gridPos=GridPos(1, 24, 0, y),
        panels=[emitted, emit_failures, consumed, latency, depth, dedupe_skips],
    )


def _metadata_router_row() -> RowPanel:
    """Row — Job Builder metadata routing (collapsed by default)."""
    y = _advance(1)
    py = _advance(8)
    lbl = 'job_builder_name=~"$job_builder_name"'
    lbl_mr = 'job_builder_name=~"$job_builder_name", route_name=~"$route_name"'

    route_matches = TimeSeries(
        title="Route Matches",
        dataSource=_DS,
        targets=[
            _target(
                _rate(f"{_PREFIX}_job_builder_route_matches_total", lbl_mr),
                "{{job_builder_name}} — {{route_name}}",
            ),
        ],
        stacking={"mode": "normal", "group": "A"},
        fillOpacity=30,
        unit="ops",
        gridPos=GridPos(8, 12, 0, py),
    )

    unmatched_files = TimeSeries(
        title="Unmatched Files",
        dataSource=_DS,
        targets=[
            _target(
                _rate(f"{_PREFIX}_job_builder_unmatched_files_total", lbl),
                "{{job_builder_name}}",
            ),
        ],
        unit="ops",
        gridPos=GridPos(8, 12, 12, py),
    )

    return RowPanel(
        title="Job Builder — Metadata Router",
        collapsed=True,
        gridPos=GridPos(1, 24, 0, y),
        panels=[route_matches, unmatched_files],
    )


def _slurm_row() -> RowPanel:
    """Row — Dispatcher SLURM metrics (collapsed by default)."""
    y = _advance(1)
    py = _advance(8)
    lbl = 'dispatcher_name=~"$dispatcher_name"'

    slurm_pending = TimeSeries(
        title="SLURM Jobs Pending",
        dataSource=_DS,
        targets=[
            _target(
                f"{_PREFIX}_dispatcher_slurm_jobs_pending{{{lbl}}}",
                "{{dispatcher_name}}",
            ),
        ],
        gridPos=GridPos(8, 12, 0, py),
    )

    slurm_submissions = TimeSeries(
        title="SLURM Submissions",
        dataSource=_DS,
        targets=[
            _target(
                _rate(f"{_PREFIX}_dispatcher_slurm_submissions_total", lbl),
                "{{dispatcher_name}} — {{status}}",
            ),
        ],
        stacking={"mode": "normal", "group": "A"},
        fillOpacity=30,
        unit="ops",
        gridPos=GridPos(8, 12, 12, py),
    )

    return RowPanel(
        title="Dispatcher — SLURM",
        collapsed=True,
        gridPos=GridPos(1, 24, 0, y),
        panels=[slurm_pending, slurm_submissions],
    )


def _http_row() -> RowPanel:
    """Row — Dispatcher HTTP metrics (collapsed by default)."""
    y = _advance(1)
    py = _advance(8)
    lbl = 'dispatcher_name=~"$dispatcher_name"'
    lbl_status = 'dispatcher_name=~"$dispatcher_name", status_code=~"$status_code"'

    http_responses = TimeSeries(
        title="HTTP Response Codes",
        dataSource=_DS,
        targets=[
            _target(
                _rate(
                    f"{_PREFIX}_dispatcher_http_response_codes_total",
                    lbl_status,
                ),
                "{{dispatcher_name}} — {{status_code}}",
            ),
        ],
        stacking={"mode": "normal", "group": "A"},
        fillOpacity=30,
        unit="ops",
        gridPos=GridPos(8, 12, 0, py),
    )

    http_duration = TimeSeries(
        title="HTTP Request Duration",
        dataSource=_DS,
        targets=[
            _target(
                _hq(
                    f"{_PREFIX}_dispatcher_http_request_duration_seconds",
                    0.50,
                    lbl,
                ),
                "p50 — {{dispatcher_name}}",
            ),
            _target(
                _hq(
                    f"{_PREFIX}_dispatcher_http_request_duration_seconds",
                    0.90,
                    lbl,
                ),
                "p90 — {{dispatcher_name}}",
                ref="B",
            ),
            _target(
                _hq(
                    f"{_PREFIX}_dispatcher_http_request_duration_seconds",
                    0.95,
                    lbl,
                ),
                "p95 — {{dispatcher_name}}",
                ref="C",
            ),
        ],
        unit=SECONDS_FORMAT,
        gridPos=GridPos(8, 12, 12, py),
    )

    return RowPanel(
        title="Dispatcher — HTTP",
        collapsed=True,
        gridPos=GridPos(1, 24, 0, y),
        panels=[http_responses, http_duration],
    )


def _pipeline_summary() -> RowPanel:
    """Row 7 — end-to-end pipeline throughput overlay."""
    y = _advance(1)
    py = _advance(8)

    throughput = TimeSeries(
        title="Pipeline Throughput (end-to-end)",
        description=(
            "Overlay of files detected, jobs built, and jobs dispatched "
            "rates to visualize the processing funnel."
        ),
        dataSource=_DS,
        targets=[
            _target(
                "sum("
                + _rate(
                    f"{_PREFIX}_data_monitor_files_processed_total",
                    'status="success"',
                )
                + ")",
                "Files Detected",
            ),
            _target(
                "sum("
                + _rate(
                    f"{_PREFIX}_job_builder_jobs_built_total",
                    'status="ready"',
                )
                + ")",
                "Jobs Built",
                ref="B",
            ),
            _target(
                "sum("
                + _rate(
                    f"{_PREFIX}_dispatcher_jobs_processed_total",
                    'status="success"',
                )
                + ")",
                "Jobs Dispatched",
                ref="C",
            ),
        ],
        unit="ops",
        fillOpacity=10,
        gridPos=GridPos(8, 24, 0, py),
    )

    return RowPanel(
        title="Pipeline Summary",
        gridPos=GridPos(1, 24, 0, y),
        panels=[throughput],
    )


# ---------------------------------------------------------------------------
# Dashboard assembly
# ---------------------------------------------------------------------------


def build_dashboard() -> Dashboard:
    """Build the complete Courier Grafana dashboard."""
    panels: list[object] = []

    # Row 1: stat panels (no wrapping row — top-level KPIs)
    panels.extend(_service_overview())

    # Rows 2-7: collapsible row panels
    panels.append(_data_monitor_row())
    panels.append(_metadata_router_row())
    panels.append(_job_builder_row())
    panels.append(_dispatcher_row())
    panels.append(_slurm_row())
    panels.append(_http_row())
    panels.append(_plugin_manager_row())
    panels.append(_broker_row())
    panels.append(_state_sync_row())
    panels.append(_routing_row())
    panels.append(_pipeline_summary())

    return Dashboard(
        title="Courier — Overview",
        description="Comprehensive monitoring dashboard for the Courier service.",
        uid="courier-overview",
        tags=["courier", "satellite", "monitoring"],
        timezone="browser",
        graphTooltip=GRAPH_TOOLTIP_MODE_SHARED_CROSSHAIR,
        refresh="30s",
        time=Time("now-1h", "now"),
        templating=_templating(),
        panels=panels,
    )


def main() -> None:
    """Generate dashboard JSON and print to stdout."""
    dashboard = build_dashboard()
    json.dump(
        dashboard.to_json_data(),
        sys.stdout,
        indent=2,
        sort_keys=True,
        cls=DashboardEncoder,
    )
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
