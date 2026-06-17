"""Centralized Prometheus metric definitions for courier.

All metric objects are defined here as module-level singletons.
Plugins and managers import these objects rather than creating their own,
ensuring a consistent naming convention across the entire service.

Naming convention: ``courier_{plugin_type}_{description}_{unit}``

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
    "courier_data_monitor_files_processed_total",
    "Total number of files processed by a data monitor plugin",
    ["monitor_name", "monitor_identifier", "status"],
)

DATA_MONITOR_LAST_SCAN_TIMESTAMP: Gauge = Gauge(
    "courier_data_monitor_last_scan_timestamp_seconds",
    "Unix timestamp of the last completed directory scan for a data monitor plugin",
    ["monitor_name", "monitor_identifier"],
)

DATA_MONITOR_SCAN_DURATION: Histogram = Histogram(
    "courier_data_monitor_scan_duration_seconds",
    "Duration of a single directory scan cycle for a data monitor plugin",
    ["monitor_name", "monitor_identifier"],
    buckets=(0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0),
)

RABBITMQ_LAST_FILE_EMITTED_TIMESTAMP: Gauge = Gauge(
    "courier_rabbitmq_last_file_emitted_timestamp_seconds",
    "Unix timestamp when the last file was processed by the RabbitMQ watcher",
    ["queue", "monitor_identifier"],
)

DATA_MONITOR_POLL_ERRORS: Counter = Counter(
    "courier_data_monitor_poll_errors_total",
    "Total number of polling errors encountered by a data monitor plugin",
    ["monitor_name", "monitor_identifier", "error_type"],
)

DATA_MONITOR_CONNECTION_STATUS: Gauge = Gauge(
    "courier_data_monitor_connection_status",
    "Connection status of a remote data monitor (1 = connected, 0 = disconnected)",
    ["monitor_name", "monitor_identifier"],
)

DATA_MONITOR_CONSUMER_LAG: Gauge = Gauge(
    "courier_data_monitor_consumer_lag",
    "Estimated consumer lag (messages behind latest offset) for a queue monitor",
    ["monitor_name", "monitor_identifier", "topic"],
)

DATA_MONITOR_LAST_PROCESSED_TIMESTAMP: Gauge = Gauge(
    "courier_data_monitor_last_processed_timestamp_seconds",
    "Unix timestamp of the last file processed and emitted by a data monitor plugin",
    ["plugin_name", "monitor_identifier"],
)

# ---------------------------------------------------------------------------
# Job builder metrics
# ---------------------------------------------------------------------------

JOB_BUILDER_FILES_RECEIVED: Counter = Counter(
    "courier_job_builder_files_received_total",
    "Total number of files received by a job builder plugin",
    ["job_builder_name", "job_builder_identifier"],
)

JOB_BUILDER_JOBS_BUILT: Counter = Counter(
    "courier_job_builder_jobs_built_total",
    "Total number of jobs built by a job builder plugin",
    ["job_builder_name", "job_builder_identifier", "status"],
)

JOB_BUILDER_ACTIVE_GROUPS: Gauge = Gauge(
    "courier_job_builder_active_groups",
    "Number of currently active job groups for a job builder plugin",
    ["job_builder_name", "job_builder_identifier"],
)

JOB_BUILDER_JOBS_DISCARDED: Counter = Counter(
    "courier_job_builder_jobs_discarded_total",
    "Total number of old jobs discarded by a job builder plugin",
    ["job_builder_name", "job_builder_identifier"],
)

JOB_BUILDER_FILE_PROCESSING_DURATION: Histogram = Histogram(
    "courier_job_builder_file_processing_duration_seconds",
    "File processing duration in seconds for a job builder plugin",
    ["job_builder_name", "job_builder_identifier"],
    buckets=(0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0),
)

JOB_BUILDER_FILES_PER_JOB: Histogram = Histogram(
    "courier_job_builder_files_per_job",
    "Number of files accumulated in a job before it was emitted",
    ["job_builder_name", "job_builder_identifier"],
    buckets=(1, 2, 5, 10, 20, 50, 100, 200),
)

JOB_BUILDER_TIMEOUT_EMISSIONS: Counter = Counter(
    "courier_job_builder_timeout_emissions_total",
    "Total number of jobs emitted due to window timeout (not file count)",
    ["job_builder_name", "job_builder_identifier"],
)

JOB_BUILDER_ROUTE_MATCHES: Counter = Counter(
    "courier_job_builder_route_matches_total",
    "Files matched to each route in a metadata_router job builder",
    ["job_builder_name", "job_builder_identifier", "route_name"],
)

JOB_BUILDER_UNMATCHED_FILES: Counter = Counter(
    "courier_job_builder_unmatched_files_total",
    "Files that matched no route in a metadata_router job builder",
    ["job_builder_name", "job_builder_identifier"],
)

JOB_BUILDER_JOBS_EMITTED: Counter = Counter(
    "courier_job_builder_jobs_emitted_total",
    "Total number of (job, target) publish successes from a job builder",
    ["job_builder_name", "job_builder_identifier", "target"],
)

JOB_BUILDER_EMIT_FAILURES: Counter = Counter(
    "courier_job_builder_emit_failures_total",
    "Total number of (job, target) publish failures from a job builder",
    ["job_builder_name", "job_builder_identifier", "target", "reason"],
)


# ---------------------------------------------------------------------------
# Dispatcher metrics
# ---------------------------------------------------------------------------

DISPATCHER_JOBS_PROCESSED: Counter = Counter(
    "courier_dispatcher_jobs_processed_total",
    "Total number of jobs processed by a dispatcher plugin",
    ["dispatcher_name", "dispatcher_identifier", "status"],
)

DISPATCHER_JOB_EXECUTION_DURATION: Histogram = Histogram(
    "courier_dispatcher_job_execution_duration_seconds",
    "Job execution duration in seconds for a dispatcher plugin",
    ["dispatcher_name", "dispatcher_identifier"],
    buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0),
)

DISPATCHER_ACTIVE_JOBS: Gauge = Gauge(
    "courier_dispatcher_active_jobs",
    "Number of currently active jobs for a dispatcher plugin",
    ["dispatcher_name", "dispatcher_identifier"],
)

DISPATCHER_EXECUTION_LOGS_EMITTED: Counter = Counter(
    "courier_dispatcher_execution_logs_emitted_total",
    "Total number of execution logs emitted by a dispatcher plugin",
    ["dispatcher_name", "dispatcher_identifier"],
)

DISPATCHER_QUEUE_WAIT_DURATION: Histogram = Histogram(
    "courier_dispatcher_queue_wait_duration_seconds",
    "Time a job spent waiting in the queue before dispatch started",
    ["dispatcher_name", "dispatcher_identifier"],
    buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0),
)

DISPATCHER_PARALLEL_WORKERS_ACTIVE: Gauge = Gauge(
    "courier_dispatcher_parallel_workers_active",
    "Number of script execution threads currently active in a parallel dispatcher",
    ["dispatcher_name", "dispatcher_identifier"],
)

DISPATCHER_SLURM_JOBS_PENDING: Gauge = Gauge(
    "courier_dispatcher_slurm_jobs_pending",
    "Number of submitted SLURM jobs currently in PENDING or RUNNING state",
    ["dispatcher_name", "dispatcher_identifier"],
)

DISPATCHER_SLURM_SUBMISSIONS: Counter = Counter(
    "courier_dispatcher_slurm_submissions_total",
    "Total number of sbatch submissions made by a SLURM dispatcher",
    ["dispatcher_name", "dispatcher_identifier", "status"],
)

DISPATCHER_HTTP_RESPONSE_CODES: Counter = Counter(
    "courier_dispatcher_http_response_codes_total",
    "Total number of HTTP requests processed by an HTTP dispatcher",
    ["dispatcher_name", "dispatcher_identifier", "status_code"],
)

DISPATCHER_HTTP_REQUEST_DURATION: Histogram = Histogram(
    "courier_dispatcher_http_request_duration_seconds",
    "HTTP request latency in seconds for an HTTP dispatcher",
    ["dispatcher_name", "dispatcher_identifier"],
    buckets=(0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0),
)

DISPATCHER_JOBS_CONSUMED: Counter = Counter(
    "courier_dispatcher_jobs_consumed_total",
    "Total number of jobs consumed from a dispatcher's per-identifier queue",
    ["dispatcher_identifier"],
)

DISPATCHER_DISPATCH_LATENCY_SECONDS: Histogram = Histogram(
    "courier_dispatcher_dispatch_latency_seconds",
    "End-to-end routing latency: time from builder emit to dispatcher consume",
    ["dispatcher_identifier"],
    buckets=(0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0),
)

DISPATCHER_QUEUE_DEPTH: Gauge = Gauge(
    "courier_dispatcher_queue_depth",
    "Current depth of a dispatcher's per-identifier queue (poll-based; "
    "memory transport reports 0 with a documented caveat)",
    ["dispatcher_identifier"],
)

DISPATCHER_DEDUPE_SKIPS: Counter = Counter(
    "courier_dispatcher_dedupe_skips_total",
    "Jobs dropped by a dispatcher's consumer-side dedupe (duplicate job id)",
    ["dispatcher_identifier"],
)

# ---------------------------------------------------------------------------
# Custom gauge — populated by deployment scripts via COURIER_METRIC: stdout
# ---------------------------------------------------------------------------

COURIER_CUSTOM_GAUGE: Gauge = Gauge(
    "courier_custom_gauge",
    "Labels-specific custom gauge populated by deployment scripts via the "
    "``COURIER_METRIC: <name> <value>`` stdout protocol.  Set by serial_bash "
    "and parallel_bash dispatchers after every job execution.",
    ["dispatcher_identifier", "metric_name"],
)

# ---------------------------------------------------------------------------
# Plugin manager metrics
# ---------------------------------------------------------------------------

PLUGIN_STATE: Gauge = Gauge(
    "courier_plugin_state",
    "Current run state of a plugin (maps to PluginRunState enum values)",
    ["plugin_name", "plugin_identifier"],
)

PLUGIN_RESTARTS: Counter = Counter(
    "courier_plugin_restarts_total",
    "Total number of times a plugin has been restarted",
    ["plugin_name", "plugin_identifier"],
)

PLUGIN_HEALTH: Gauge = Gauge(
    "courier_plugin_health",
    "Health status of a plugin (1 = healthy, 0 = unhealthy)",
    ["plugin_name", "plugin_identifier"],
)

PLUGIN_REGISTRATION_FAILURES: Counter = Counter(
    "courier_plugin_registration_failures_total",
    "Total number of plugin registration failures",
    ["plugin_name", "plugin_identifier", "reason"],
)

# ---------------------------------------------------------------------------
# Service-level metrics
# ---------------------------------------------------------------------------

SERVICE_UPTIME: Gauge = Gauge(
    "courier_service_uptime_seconds",
    "Service uptime in seconds",
)

SERVICE_HEALTH: Gauge = Gauge(
    "courier_service_health",
    "Overall service health status (1 = healthy, 0 = unhealthy)",
)

APP_HEARTBEAT: Gauge = Gauge(
    "courier_service_heartbeat_timestamp_seconds",
    "Last reported service heartbeat timestamp",
)

# ---------------------------------------------------------------------------
# Broker metrics
# ---------------------------------------------------------------------------

BROKER_CONNECTIONS: Counter = Counter(
    "courier_broker_connections_total",
    "Total number of broker connection attempts",
    ["status"],
)

BROKER_MESSAGES_SENT: Counter = Counter(
    "courier_broker_messages_sent_total",
    "Total number of messages published to broker queues",
    ["queue_name"],
)

BROKER_MESSAGES_RECEIVED: Counter = Counter(
    "courier_broker_messages_received_total",
    "Total number of messages consumed from broker queues",
    ["queue_name"],
)

BROKER_CONNECTED: Gauge = Gauge(
    "courier_broker_connected",
    "Whether the broker connection is active (1 = connected, 0 = disconnected)",
)

BROKER_MESSAGES_PENDING: Gauge = Gauge(
    "courier_broker_messages_pending",
    "Best-effort count of messages published but not yet consumed. "
    "Resets to 0 on restart; may drift due to requeues.",
    ["queue_name"],
)

# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# State sync metrics
# ---------------------------------------------------------------------------

STATE_SYNC_PUSHES: Counter = Counter(
    "courier_state_sync_pushes_total",
    "Total number of job state pushes sent to the shared Redis instance",
    ["builder_name", "event"],
)

STATE_SYNC_APPLIES: Counter = Counter(
    "courier_state_sync_applies_total",
    "Total number of remote job state updates applied from Redis",
    ["builder_name"],
)

STATE_SYNC_EMIT_CLAIMS: Counter = Counter(
    "courier_state_sync_emit_claims_total",
    "Total number of emit claim attempts via Redis SETNX",
    ["builder_name", "result"],
)

STATE_SYNC_ERRORS: Counter = Counter(
    "courier_state_sync_errors_total",
    "Total number of Redis operation errors during state synchronization",
    ["builder_name", "operation"],
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
    >>> from courier.metrics import DATA_MONITOR_FILES_PROCESSED, collect_labeled
    >>> DATA_MONITOR_FILES_PROCESSED.labels(
    ...     monitor_name="my_monitor", status="success"
    ... ).inc()
    >>> result = collect_labeled(
    ...     DATA_MONITOR_FILES_PROCESSED, "monitor_name", "my_monitor"
    ... )
    >>> list(result.keys())  # doctest: +SKIP
    ["courier_data_monitor_files_processed_total_..."]
    """
    result: dict[str, dict[str, Any]] = {}
    base_name: str = metric._name
    for family in metric.collect():
        for sample in family.samples:
            if sample.name == base_name and sample.labels.get(label_key) == label_value:
                key = f"{base_name}_{sample.labels}"
                result[key] = {"value": sample.value, "labels": sample.labels}
    return result
