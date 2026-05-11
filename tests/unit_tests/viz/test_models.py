"""Unit tests for viz models."""

from __future__ import annotations

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


class TestModels:
    """Tests for dataclass instantiation and default values."""

    def test_service_metrics_defaults(self):
        sm = ServiceMetrics()
        assert sm.health == 0.0
        assert sm.uptime_seconds == 0.0

    def test_metrics_snapshot_defaults(self):
        snap = MetricsSnapshot()
        assert snap.service.health == 0.0
        assert snap.data_monitors.monitors == []
        assert snap.job_builders.builders == []
        assert snap.dispatchers.dispatchers == []

    def test_metrics_snapshot_partial_population(self):
        """Test that a partially populated snapshot works."""
        snap = MetricsSnapshot(
            service=ServiceMetrics(health=1.0, uptime_seconds=3600.0),
            broker=BrokerMetrics(connected=1.0),
        )
        assert snap.service.health == 1.0
        assert snap.broker.connected == 1.0
        assert snap.plugins.plugins == []

    def test_data_monitor_info(self):
        info = DataMonitorInfo(
            name="test-monitor",
            files_processed=100.0,
            success_count=95.0,
            failure_count=5.0,
        )
        assert info.name == "test-monitor"
        assert info.success_count == 95.0

    def test_all_info_dataclasses_instantiate(self):
        """Ensure all info dataclasses can be instantiated with defaults."""
        DataMonitorInfo()
        JobBuilderInfo()
        DispatcherInfo()
        PluginInfo()
        RoutingInfo()
