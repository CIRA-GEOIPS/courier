"""Typed metric models for the Courier Viz visualizer.

Each dataclass represents one category of Prometheus metrics
exposed by the courier service. The fetcher populates these
from parsed /metrics output.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ServiceMetrics:
    """Service-level health and uptime metrics."""

    health: float = 0.0  # 1=healthy, 0=unhealthy
    uptime_seconds: float = 0.0
    heartbeat_age_seconds: float = 0.0
    total_files_processed: float = 0.0


@dataclass
class DataMonitorInfo:
    """Per-monitor metric values."""

    name: str = ""
    files_processed: float = 0.0
    success_count: float = 0.0
    failure_count: float = 0.0
    last_scan_age_seconds: float = 0.0
    scan_duration_p50: float = 0.0
    scan_duration_p95: float = 0.0


@dataclass
class DataMonitorMetrics:
    """Aggregated data monitor metrics."""

    monitors: list[DataMonitorInfo] = field(default_factory=list)


@dataclass
class JobBuilderInfo:
    """Per-builder metric values."""

    name: str = ""
    files_received_rate: float = 0.0
    jobs_built_rate: float = 0.0
    success_rate: float = 0.0
    active_groups: float = 0.0
    jobs_discarded_rate: float = 0.0
    processing_duration_p50: float = 0.0
    processing_duration_p95: float = 0.0
    processing_duration_p99: float = 0.0
    files_per_job_p50: float = 0.0
    files_per_job_p95: float = 0.0


@dataclass
class JobBuilderMetrics:
    """Aggregated job builder metrics."""

    builders: list[JobBuilderInfo] = field(default_factory=list)


@dataclass
class DispatcherInfo:
    """Per-dispatcher metric values."""

    name: str = ""
    jobs_processed_rate: float = 0.0
    success_ratio: float = 0.0
    active_jobs: float = 0.0
    execution_duration_p50: float = 0.0
    execution_duration_p95: float = 0.0
    execution_duration_p99: float = 0.0
    logs_emitted_rate: float = 0.0
    queue_wait_p50: float = 0.0
    queue_wait_p95: float = 0.0


@dataclass
class DispatcherMetrics:
    """Aggregated dispatcher metrics."""

    dispatchers: list[DispatcherInfo] = field(default_factory=list)


@dataclass
class PluginInfo:
    """Per-plugin metric values."""

    name: str = ""
    state: float = 0.0  # PluginRunState enum value
    health: float = 0.0  # 1=healthy, 0=unhealthy
    restart_rate: float = 0.0


@dataclass
class PluginMetrics:
    """Aggregated plugin manager metrics."""

    plugins: list[PluginInfo] = field(default_factory=list)


@dataclass
class BrokerMetrics:
    """Broker connectivity and message throughput."""

    connected: float = 0.0  # 1=connected, 0=disconnected
    connection_attempts_rate: float = 0.0
    messages_sent_rate: float = 0.0
    messages_received_rate: float = 0.0


@dataclass
class RoutingInfo:
    """Per-target routing metrics."""

    dispatcher_identifier: str = ""
    jobs_consumed_rate: float = 0.0
    dispatch_latency_p50: float = 0.0
    dispatch_latency_p95: float = 0.0
    dispatch_latency_p99: float = 0.0
    queue_depth: float = 0.0


@dataclass
class RoutingMetrics:
    """Aggregated routing metrics."""

    routes: list[RoutingInfo] = field(default_factory=list)
    emit_failures_rate: float = 0.0


@dataclass
class StateSyncMetrics:
    """HA state sync metrics."""

    pushes_rate: float = 0.0
    applies_rate: float = 0.0
    emit_claims_rate: float = 0.0
    errors_rate: float = 0.0


@dataclass
class PipelineSummary:
    """End-to-end pipeline throughput."""

    files_detected_rate: float = 0.0
    jobs_built_rate: float = 0.0
    jobs_dispatched_rate: float = 0.0


@dataclass
class MetricsSnapshot:
    """Complete snapshot of all courier metrics at a point in time."""

    service: ServiceMetrics = field(default_factory=ServiceMetrics)
    data_monitors: DataMonitorMetrics = field(default_factory=DataMonitorMetrics)
    job_builders: JobBuilderMetrics = field(default_factory=JobBuilderMetrics)
    dispatchers: DispatcherMetrics = field(default_factory=DispatcherMetrics)
    plugins: PluginMetrics = field(default_factory=PluginMetrics)
    broker: BrokerMetrics = field(default_factory=BrokerMetrics)
    routing: RoutingMetrics = field(default_factory=RoutingMetrics)
    state_sync: StateSyncMetrics = field(default_factory=StateSyncMetrics)
    pipeline_summary: PipelineSummary = field(default_factory=PipelineSummary)
