"""Centralized Prometheus metric definitions for lazylemon.

All metric objects are defined here as module-level singletons.
Plugins and managers import these objects rather than creating their own,
ensuring a consistent naming convention across the entire service.

Naming convention: ``lazylemon_{plugin_type}_{description}_{unit}``

Plugin/instance names are always labels (e.g. ``monitor_name``, ``dispatcher_name``),
never embedded in the metric name itself.
"""

from typing import Any

from prometheus_client import Counter, Gauge, Histogram
from prometheus_client.metrics import MetricWrapperBase

# ---------------------------------------------------------------------------
# Data monitor metrics
# ---------------------------------------------------------------------------

DATA_MONITOR_FILES_PROCESSED: Counter = Counter(
    "lazylemon_data_monitor_files_processed_total",
    "Total number of files processed by a data monitor plugin",
    ["monitor_name", "status"],
)

DATA_MONITOR_LAST_SCAN_TIMESTAMP: Gauge = Gauge(
    "lazylemon_data_monitor_last_scan_timestamp_seconds",
    "Unix timestamp of the last completed directory scan for a data monitor plugin",
    ["monitor_name"],
)

# ---------------------------------------------------------------------------
# Job builder metrics
# ---------------------------------------------------------------------------

JOB_BUILDER_FILES_RECEIVED: Counter = Counter(
    "lazylemon_job_builder_files_received_total",
    "Total number of files received by a job builder plugin",
    ["job_builder_name"],
)

JOB_BUILDER_JOBS_BUILT: Counter = Counter(
    "lazylemon_job_builder_jobs_built_total",
    "Total number of jobs built by a job builder plugin",
    ["job_builder_name", "status"],
)

JOB_BUILDER_ACTIVE_GROUPS: Gauge = Gauge(
    "lazylemon_job_builder_active_groups",
    "Number of currently active job groups for a job builder plugin",
    ["job_builder_name"],
)

JOB_BUILDER_JOBS_DISCARDED: Counter = Counter(
    "lazylemon_job_builder_jobs_discarded_total",
    "Total number of old jobs discarded by a job builder plugin",
    ["job_builder_name"],
)

JOB_BUILDER_FILE_PROCESSING_DURATION: Histogram = Histogram(
    "lazylemon_job_builder_file_processing_duration_seconds",
    "File processing duration in seconds for a job builder plugin",
    ["job_builder_name"],
    buckets=(0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0),
)

# ---------------------------------------------------------------------------
# Dispatcher metrics
# ---------------------------------------------------------------------------

DISPATCHER_JOBS_PROCESSED: Counter = Counter(
    "lazylemon_dispatcher_jobs_processed_total",
    "Total number of jobs processed by a dispatcher plugin",
    ["dispatcher_name", "status"],
)

DISPATCHER_JOB_EXECUTION_DURATION: Histogram = Histogram(
    "lazylemon_dispatcher_job_execution_duration_seconds",
    "Job execution duration in seconds for a dispatcher plugin",
    ["dispatcher_name"],
    buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0),
)

DISPATCHER_ACTIVE_JOBS: Gauge = Gauge(
    "lazylemon_dispatcher_active_jobs",
    "Number of currently active jobs for a dispatcher plugin",
    ["dispatcher_name"],
)

DISPATCHER_EXECUTION_LOGS_EMITTED: Counter = Counter(
    "lazylemon_dispatcher_execution_logs_emitted_total",
    "Total number of execution logs emitted by a dispatcher plugin",
    ["dispatcher_name"],
)

# ---------------------------------------------------------------------------
# Plugin manager metrics
# ---------------------------------------------------------------------------

PLUGIN_STATE: Gauge = Gauge(
    "lazylemon_plugin_state",
    "Current run state of a plugin (maps to PluginRunState enum values)",
    ["plugin_name"],
)

PLUGIN_RESTARTS: Counter = Counter(
    "lazylemon_plugin_restarts_total",
    "Total number of times a plugin has been restarted",
    ["plugin_name"],
)

PLUGIN_HEALTH: Gauge = Gauge(
    "lazylemon_plugin_health",
    "Health status of a plugin (1 = healthy, 0 = unhealthy)",
    ["plugin_name"],
)

# ---------------------------------------------------------------------------
# Service-level metrics
# ---------------------------------------------------------------------------

SERVICE_UPTIME: Gauge = Gauge(
    "lazylemon_service_uptime_seconds",
    "Service uptime in seconds",
)

SERVICE_HEALTH: Gauge = Gauge(
    "lazylemon_service_health",
    "Overall service health status (1 = healthy, 0 = unhealthy)",
)

APP_HEARTBEAT: Gauge = Gauge(
    "lazylemon_service_heartbeat_timestamp_seconds",
    "Last reported service heartbeat timestamp",
)

# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# State sync metrics
# ---------------------------------------------------------------------------

STATE_SYNC_PUSHES: Counter = Counter(
    "lazylemon_state_sync_pushes_total",
    "Total number of job state pushes sent to the shared Redis instance",
    ["builder_name", "event"],
)

STATE_SYNC_APPLIES: Counter = Counter(
    "lazylemon_state_sync_applies_total",
    "Total number of remote job state updates applied from Redis",
    ["builder_name"],
)

STATE_SYNC_EMIT_CLAIMS: Counter = Counter(
    "lazylemon_state_sync_emit_claims_total",
    "Total number of emit claim attempts via Redis SETNX",
    ["builder_name", "result"],
)

# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


def collect_labeled(
    metric: MetricWrapperBase,
    label_key: str,
    label_value: str,
) -> dict[str, dict[str, Any]]:
    """Extract samples from a shared metric filtered to a specific label value.

    Iterates the metric's collected samples and returns only those whose
    ``label_key`` equals ``label_value``.  Histogram ``_bucket`` and
    ``_created`` samples are excluded; only the primary sample name is kept.

    Parameters
    ----------
    metric : MetricWrapperBase
        A prometheus_client metric object (Counter, Gauge, Histogram, …).
    label_key : str
        The label dimension to filter on (e.g. ``"monitor_name"``).
    label_value : str
        The label value to keep (e.g. the plugin's ``name`` attribute).

    Returns
    -------
    dict[str, dict[str, Any]]
        Mapping of ``"<metric_name>_<label_dict>"`` to
        ``{"value": float, "labels": dict[str, str]}``.

    Examples
    --------
    >>> from lazylemon.metrics import DATA_MONITOR_FILES_PROCESSED, collect_labeled
    >>> DATA_MONITOR_FILES_PROCESSED.labels(
    ...     monitor_name="my_monitor", status="success"
    ... ).inc()
    >>> result = collect_labeled(
    ...     DATA_MONITOR_FILES_PROCESSED, "monitor_name", "my_monitor"
    ... )
    >>> list(result.keys())  # doctest: +SKIP
    ["lazylemon_data_monitor_files_processed_total_..."]
    """
    result: dict[str, dict[str, Any]] = {}
    base_name: str = metric._name
    for family in metric.collect():
        for sample in family.samples:
            if sample.name == base_name and sample.labels.get(label_key) == label_value:
                key = f"{base_name}_{sample.labels}"
                result[key] = {"value": sample.value, "labels": sample.labels}
    return result
