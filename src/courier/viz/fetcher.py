"""Prometheus metrics fetcher for the Courier Viz TUI.

Fetches the /metrics endpoint from a running courier instance,
parses the Prometheus text format, and populates typed models.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import prometheus_client.parser as prom_parser

from courier.viz.models import (
    BrokerMetrics,
    DataMonitorInfo,
    DataMonitorMetrics,
    DispatcherInfo,
    DispatcherMetrics,
    JobBuilderInfo,
    JobBuilderMetrics,
    MetricsSnapshot,
    PipelineSummary,
    PluginInfo,
    PluginMetrics,
    RoutingInfo,
    RoutingMetrics,
    ServiceMetrics,
    StateSyncMetrics,
)

if TYPE_CHECKING:
    import httpx

# ---------------------------------------------------------------------------
# Histogram helpers — compute averages from _sum / _count samples
# ---------------------------------------------------------------------------


def _matching_samples(
    metrics: dict[str, dict[frozenset[tuple[str, str]], float]],
    name: str,
    labels: dict[str, str],
) -> list[float]:
    """Return every sample of *name* whose labels are a superset of *labels*.

    Courier's metrics all carry at least two label dimensions (a plugin name
    *and* an instance identifier, often plus ``status``), but callers here
    select on one or two of them. Comparing the label set for exact equality —
    which is what a ``frozenset`` lookup does — therefore matched nothing, and
    the TUI rendered zeros for most fields while reporting every processed
    file as a failure. Subset matching is what these call sites always meant.
    """
    if name not in metrics:
        return []
    wanted = set(labels.items())
    return [
        value
        for label_set, value in metrics[name].items()
        if wanted <= set(label_set)
    ]


def _histogram_avg(
    metrics: dict[str, dict[frozenset[tuple[str, str]], float]],
    base_name: str,
    labels: dict[str, str],
) -> float:
    """Return average value for a histogram metric, or 0.0 if no data."""
    sum_val = sum(_matching_samples(metrics, f"{base_name}_sum", labels))
    count_val = sum(_matching_samples(metrics, f"{base_name}_count", labels))
    if count_val <= 0:
        return 0.0
    return sum_val / count_val


# ---------------------------------------------------------------------------
# MetricsFetcher
# ---------------------------------------------------------------------------


class MetricsFetcher:
    """Fetches and parses Prometheus metrics from a courier service.

    Parameters
    ----------
    client : httpx.AsyncClient
        Injectable HTTP client (Law 3: Atomic Predictability).
    host : str
        Courier hostname or IP.
    port : int
        Prometheus metrics port.
    """

    _HTTP_OK: int = 200

    def __init__(
        self,
        client: httpx.AsyncClient,
        host: str = "localhost",
        port: int = 8000,
    ) -> None:
        self._client = client
        self._host = host
        self._port = port

    @property
    def url(self) -> str:
        """Full metrics endpoint URL."""
        return f"http://{self._host}:{self._port}/metrics"

    async def fetch(self) -> MetricsSnapshot:
        """Fetch and parse metrics, returning a complete snapshot."""
        response = await self._client.get(self.url)

        # Law 1: Early Exit — validate response immediately
        if response.status_code != self._HTTP_OK:
            raise ConnectionError(
                f"Metrics endpoint returned {response.status_code} "
                f"from {self.url}. Is courier running?",
            )

        text = response.text
        if not text.strip():
            raise ValueError(
                f"Empty response from {self.url}. "
                "The metrics endpoint returned no data.",
            )

        families = list(prom_parser.text_string_to_metric_families(text))

        # Build lookup: metric_name -> {frozenset(label_items) -> value}
        metrics: dict[str, dict[frozenset[tuple[str, str]], float]] = {}
        for family in families:
            for sample in family.samples:
                name = sample.name
                label_key = frozenset(sample.labels.items())
                if name not in metrics:
                    metrics[name] = {}
                metrics[name][label_key] = sample.value

        return self._build_snapshot(metrics)

    # ------------------------------------------------------------------
    # Metric lookup helpers
    # ------------------------------------------------------------------

    def _get(
        self,
        metrics: dict[str, dict[frozenset[tuple[str, str]], float]],
        name: str,
        labels: dict[str, str] | None = None,
    ) -> float:
        """Sum the samples of *name* matching *labels*.

        *labels* is treated as a filter, not as the complete label set: every
        metric here has more dimensions than callers select on, so requiring
        an exact match returned 0.0 for essentially everything. Matching
        samples are summed, which is the right aggregation for the counters
        and gauges this is used with (e.g. one series per ``status``).

        Returns 0.0 when the metric is absent — courier may have just started
        and not yet emitted it.
        """
        if name not in metrics:
            return 0.0
        if labels is None:
            return sum(metrics[name].values())
        return sum(_matching_samples(metrics, name, labels))

    def _sum_all(
        self,
        metrics: dict[str, dict[frozenset[tuple[str, str]], float]],
        name: str,
    ) -> float:
        """Sum all samples for a metric, regardless of labels."""
        if name not in metrics:
            return 0.0
        return sum(metrics[name].values())

    def _unique_label_values(
        self,
        metrics: dict[str, dict[frozenset[tuple[str, str]], float]],
        name: str,
        label_key: str,
    ) -> set[str]:
        """Return all unique values for a label dimension on a metric."""
        if name not in metrics:
            return set()
        values: set[str] = set()
        for label_set in metrics[name]:
            label_dict = dict(label_set)
            if label_key in label_dict:
                values.add(label_dict[label_key])
        return values

    # ------------------------------------------------------------------
    # Snapshot builder — one method per category
    # ------------------------------------------------------------------

    def _build_snapshot(
        self,
        m: dict[str, dict[frozenset[tuple[str, str]], float]],
    ) -> MetricsSnapshot:
        """Populate all metric models from the raw metric dict."""
        now = time.time()

        return MetricsSnapshot(
            service=self._build_service(m, now),
            data_monitors=self._build_data_monitors(m, now),
            job_builders=self._build_job_builders(m),
            dispatchers=self._build_dispatchers(m),
            plugins=self._build_plugins(m),
            broker=self._build_broker(m),
            routing=self._build_routing(m),
            state_sync=self._build_state_sync(m),
            pipeline_summary=self._build_pipeline_summary(m),
        )

    # ------------------------------------------------------------------
    # Service
    # ------------------------------------------------------------------

    def _build_service(
        self,
        m: dict[str, dict[frozenset[tuple[str, str]], float]],
        now: float,
    ) -> ServiceMetrics:
        """Extract service-level health and uptime metrics."""
        health = self._get(m, "courier_service_health")
        uptime = self._get(m, "courier_service_uptime_seconds")
        heartbeat_ts = self._get(m, "courier_service_heartbeat_timestamp_seconds")

        heartbeat_age = 0.0
        if heartbeat_ts > 0:
            heartbeat_age = max(0.0, now - heartbeat_ts)

        total_files = self._sum_all(m, "courier_data_monitor_files_processed_total")

        return ServiceMetrics(
            health=health,
            uptime_seconds=uptime,
            heartbeat_age_seconds=heartbeat_age,
            total_files_processed=total_files,
        )

    # ------------------------------------------------------------------
    # Data monitors
    # ------------------------------------------------------------------

    def _build_data_monitors(
        self,
        m: dict[str, dict[frozenset[tuple[str, str]], float]],
        now: float,
    ) -> DataMonitorMetrics:
        """Extract per-monitor data monitor metrics."""
        monitor_names = self._unique_label_values(
            m,
            "courier_data_monitor_files_processed_total",
            "monitor_name",
        )
        monitors: list[DataMonitorInfo] = []

        for name in sorted(monitor_names):
            labels = {"monitor_name": name}

            # Files processed total (all statuses) — inline summation because
            # _get requires exact frozenset match and this metric has a
            # second label dimension (status).
            files_processed = 0.0
            if "courier_data_monitor_files_processed_total" in m:
                for label_set, value in m[
                    "courier_data_monitor_files_processed_total"
                ].items():
                    if dict(label_set).get("monitor_name") == name:
                        files_processed += value

            # Success count
            success = self._get(
                m,
                "courier_data_monitor_files_processed_total",
                {"monitor_name": name, "status": "success"},
            )

            # Failure count = total - success (covers error, timeout, etc.)
            failure = max(0.0, files_processed - success)

            # Last scan age
            last_scan_ts = self._get(
                m,
                "courier_data_monitor_last_scan_timestamp_seconds",
                labels,
            )
            last_scan_age = 0.0
            if last_scan_ts > 0:
                last_scan_age = max(0.0, now - last_scan_ts)

            # Scan duration average (from histogram)
            avg_scan = _histogram_avg(
                m,
                "courier_data_monitor_scan_duration_seconds",
                labels,
            )

            monitors.append(
                DataMonitorInfo(
                    name=name,
                    files_processed=files_processed,
                    success_count=success,
                    failure_count=failure,
                    last_scan_age_seconds=last_scan_age,
                    scan_duration_p50=avg_scan,
                    scan_duration_p95=avg_scan,
                ),
            )

        return DataMonitorMetrics(monitors=monitors)

    # ------------------------------------------------------------------
    # Job builders
    # ------------------------------------------------------------------

    def _build_job_builders(
        self,
        m: dict[str, dict[frozenset[tuple[str, str]], float]],
    ) -> JobBuilderMetrics:
        """Extract per-builder job builder metrics."""
        builder_names = self._unique_label_values(
            m,
            "courier_job_builder_jobs_built_total",
            "job_builder_name",
        )
        builders: list[JobBuilderInfo] = []

        for name in sorted(builder_names):
            labels = {"job_builder_name": name}

            # Files received rate
            files_received = self._get(
                m,
                "courier_job_builder_files_received_total",
                labels,
            )

            # Jobs built (status="ready")
            ready_jobs = self._get(
                m,
                "courier_job_builder_jobs_built_total",
                {"job_builder_name": name, "status": "ready"},
            )

            # Success rate: emitted / (emitted + failures)
            emitted = 0.0
            if "courier_job_builder_jobs_emitted_total" in m:
                for label_set, value in m[
                    "courier_job_builder_jobs_emitted_total"
                ].items():
                    if dict(label_set).get("job_builder_name") == name:
                        emitted += value
            failures = 0.0
            if "courier_job_builder_emit_failures_total" in m:
                for label_set, value in m[
                    "courier_job_builder_emit_failures_total"
                ].items():
                    if dict(label_set).get("job_builder_name") == name:
                        failures += value
            total_emits = emitted + failures
            success_ratio = emitted / total_emits if total_emits > 0 else 0.0

            # Active groups
            active_groups = self._get(
                m,
                "courier_job_builder_active_groups",
                labels,
            )

            # Jobs discarded
            discarded = self._get(
                m,
                "courier_job_builder_jobs_discarded_total",
                labels,
            )

            # Processing duration average
            proc_avg = _histogram_avg(
                m,
                "courier_job_builder_file_processing_duration_seconds",
                labels,
            )

            # Files per job average
            fpj_avg = _histogram_avg(
                m,
                "courier_job_builder_files_per_job",
                labels,
            )

            builders.append(
                JobBuilderInfo(
                    name=name,
                    files_received_rate=files_received,
                    jobs_built_rate=ready_jobs,
                    success_rate=success_ratio,
                    active_groups=active_groups,
                    jobs_discarded_rate=discarded,
                    processing_duration_p50=proc_avg,
                    processing_duration_p95=proc_avg,
                    processing_duration_p99=proc_avg,
                    files_per_job_p50=fpj_avg,
                    files_per_job_p95=fpj_avg,
                ),
            )

        return JobBuilderMetrics(builders=builders)

    # ------------------------------------------------------------------
    # Dispatchers
    # ------------------------------------------------------------------

    def _build_dispatchers(
        self,
        m: dict[str, dict[frozenset[tuple[str, str]], float]],
    ) -> DispatcherMetrics:
        """Extract per-dispatcher metrics."""
        disp_names = self._unique_label_values(
            m,
            "courier_dispatcher_jobs_processed_total",
            "dispatcher_name",
        )
        dispatchers: list[DispatcherInfo] = []

        for name in sorted(disp_names):
            labels = {"dispatcher_name": name}

            # Jobs processed and success ratio — inline summation because
            # _get requires exact frozenset match and this metric has a
            # second label dimension (status).
            total_processed = 0.0
            success = 0.0
            if "courier_dispatcher_jobs_processed_total" in m:
                for label_set, value in m[
                    "courier_dispatcher_jobs_processed_total"
                ].items():
                    label_dict = dict(label_set)
                    if label_dict.get("dispatcher_name") == name:
                        total_processed += value
                        if label_dict.get("status") == "success":
                            success += value
            success_ratio = success / total_processed if total_processed > 0 else 0.0

            # Active jobs
            active_jobs = self._get(m, "courier_dispatcher_active_jobs", labels)

            # Execution duration average
            exec_avg = _histogram_avg(
                m,
                "courier_dispatcher_job_execution_duration_seconds",
                labels,
            )

            # Logs emitted rate
            logs_emitted = self._get(
                m,
                "courier_dispatcher_execution_logs_emitted_total",
                labels,
            )

            # Queue wait average
            queue_avg = _histogram_avg(
                m,
                "courier_dispatcher_queue_wait_duration_seconds",
                labels,
            )

            dispatchers.append(
                DispatcherInfo(
                    name=name,
                    jobs_processed_rate=total_processed,
                    success_ratio=success_ratio,
                    active_jobs=active_jobs,
                    execution_duration_p50=exec_avg,
                    execution_duration_p95=exec_avg,
                    execution_duration_p99=exec_avg,
                    logs_emitted_rate=logs_emitted,
                    queue_wait_p50=queue_avg,
                    queue_wait_p95=queue_avg,
                ),
            )

        return DispatcherMetrics(dispatchers=dispatchers)

    # ------------------------------------------------------------------
    # Plugins
    # ------------------------------------------------------------------

    def _build_plugins(
        self,
        m: dict[str, dict[frozenset[tuple[str, str]], float]],
    ) -> PluginMetrics:
        """Extract per-plugin metrics."""
        plugin_names = self._unique_label_values(
            m,
            "courier_plugin_state",
            "plugin_name",
        )
        plugins: list[PluginInfo] = []

        for name in sorted(plugin_names):
            labels = {"plugin_name": name}

            state = self._get(m, "courier_plugin_state", labels)
            health = self._get(m, "courier_plugin_health", labels)
            restarts = self._get(m, "courier_plugin_restarts_total", labels)

            plugins.append(
                PluginInfo(
                    name=name,
                    state=state,
                    health=health,
                    restart_rate=restarts,
                ),
            )

        return PluginMetrics(plugins=plugins)

    # ------------------------------------------------------------------
    # Broker
    # ------------------------------------------------------------------

    def _build_broker(
        self,
        m: dict[str, dict[frozenset[tuple[str, str]], float]],
    ) -> BrokerMetrics:
        """Extract broker connectivity and throughput metrics."""
        connected = self._get(m, "courier_broker_connected")
        attempts = self._sum_all(m, "courier_broker_connections_total")
        sent = self._sum_all(m, "courier_broker_messages_sent_total")
        received = self._sum_all(m, "courier_broker_messages_received_total")

        return BrokerMetrics(
            connected=connected,
            connection_attempts_rate=attempts,
            messages_sent_rate=sent,
            messages_received_rate=received,
        )

    # ------------------------------------------------------------------
    # Routing
    # ------------------------------------------------------------------

    def _build_routing(
        self,
        m: dict[str, dict[frozenset[tuple[str, str]], float]],
    ) -> RoutingMetrics:
        """Extract per-target routing metrics."""
        identifiers = self._unique_label_values(
            m,
            "courier_dispatcher_jobs_consumed_total",
            "dispatcher_identifier",
        )
        routes: list[RoutingInfo] = []

        for identifier in sorted(identifiers):
            labels = {"dispatcher_identifier": identifier}

            jobs_consumed = self._get(
                m,
                "courier_dispatcher_jobs_consumed_total",
                labels,
            )
            latency_avg = _histogram_avg(
                m,
                "courier_dispatcher_dispatch_latency_seconds",
                labels,
            )
            queue_depth = self._get(
                m,
                "courier_dispatcher_queue_depth",
                labels,
            )

            routes.append(
                RoutingInfo(
                    dispatcher_identifier=identifier,
                    jobs_consumed_rate=jobs_consumed,
                    dispatch_latency_p50=latency_avg,
                    dispatch_latency_p95=latency_avg,
                    dispatch_latency_p99=latency_avg,
                    queue_depth=queue_depth,
                ),
            )

        # Emit failures — sum all job builder emit failures
        emit_failures = self._sum_all(
            m,
            "courier_job_builder_emit_failures_total",
        )

        return RoutingMetrics(routes=routes, emit_failures_rate=emit_failures)

    # ------------------------------------------------------------------
    # State sync
    # ------------------------------------------------------------------

    def _build_state_sync(
        self,
        m: dict[str, dict[frozenset[tuple[str, str]], float]],
    ) -> StateSyncMetrics:
        """Extract HA state sync metrics."""
        pushes = self._sum_all(m, "courier_state_sync_pushes_total")
        applies = self._sum_all(m, "courier_state_sync_applies_total")
        claims = self._sum_all(m, "courier_state_sync_emit_claims_total")
        errors = self._sum_all(m, "courier_state_sync_errors_total")

        return StateSyncMetrics(
            pushes_rate=pushes,
            applies_rate=applies,
            emit_claims_rate=claims,
            errors_rate=errors,
        )

    # ------------------------------------------------------------------
    # Pipeline summary
    # ------------------------------------------------------------------

    def _build_pipeline_summary(
        self,
        m: dict[str, dict[frozenset[tuple[str, str]], float]],
    ) -> PipelineSummary:
        """Extract end-to-end pipeline throughput metrics."""
        # Files detected — sum all data monitor files processed
        files_detected = self._sum_all(
            m,
            "courier_data_monitor_files_processed_total",
        )

        # Jobs built — sum jobs built with status "ready"
        jobs_built = 0.0
        if "courier_job_builder_jobs_built_total" in m:
            for label_set, value in m["courier_job_builder_jobs_built_total"].items():
                label_dict = dict(label_set)
                if label_dict.get("status") == "ready":
                    jobs_built += value

        # Jobs dispatched — sum all dispatcher jobs processed
        jobs_dispatched = self._sum_all(
            m,
            "courier_dispatcher_jobs_processed_total",
        )

        return PipelineSummary(
            files_detected_rate=files_detected,
            jobs_built_rate=jobs_built,
            jobs_dispatched_rate=jobs_dispatched,
        )
