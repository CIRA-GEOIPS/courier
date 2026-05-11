"""Unit tests for MetricsFetcher."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from courier.viz.fetcher import MetricsFetcher
from courier.viz.models import MetricsSnapshot


class TestMetricsFetcher:
    """Tests for the Prometheus metrics fetcher and parser."""

    @pytest.fixture
    def mock_client(self):
        client = AsyncMock()
        client.get = AsyncMock()
        return client

    @pytest.fixture
    def fetcher(self, mock_client):
        return MetricsFetcher(mock_client, host="localhost", port=8000)

    def test_url_property(self, fetcher):
        assert fetcher.url == "http://localhost:8000/metrics"

    def test_fetch_returns_snapshot_on_valid_response(self, fetcher, mock_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = (
            "# HELP courier_service_health Overall service health\n"
            "# TYPE courier_service_health gauge\n"
            "courier_service_health 1.0\n"
            "# HELP courier_service_uptime_seconds Service uptime\n"
            "# TYPE courier_service_uptime_seconds gauge\n"
            "courier_service_uptime_seconds 3600.0\n"
        )
        mock_client.get.return_value = mock_response

        result = asyncio.run(fetcher.fetch())

        assert isinstance(result, MetricsSnapshot)
        assert result.service.health == 1.0
        assert result.service.uptime_seconds == 3600.0

    def test_fetch_raises_on_non_200(self, fetcher, mock_client):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_client.get.return_value = mock_response

        with pytest.raises(ConnectionError, match="500"):
            asyncio.run(fetcher.fetch())

    def test_fetch_raises_on_empty_body(self, fetcher, mock_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = ""
        mock_client.get.return_value = mock_response

        with pytest.raises(ValueError, match="Empty response"):
            asyncio.run(fetcher.fetch())

    def test_fetch_with_full_metrics(self, fetcher, mock_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = (
            "# HELP courier_service_health Overall service health\n"
            "# TYPE courier_service_health gauge\n"
            "courier_service_health 1.0\n"
            "# HELP courier_service_uptime_seconds Service uptime\n"
            "# TYPE courier_service_uptime_seconds gauge\n"
            "courier_service_uptime_seconds 7200.0\n"
            "# HELP courier_service_heartbeat_timestamp_seconds Heartbeat\n"
            "# TYPE courier_service_heartbeat_timestamp_seconds gauge\n"
            "courier_service_heartbeat_timestamp_seconds 1715000000.0\n"
            "# HELP courier_data_monitor_files_processed_total Files\n"
            "# TYPE courier_data_monitor_files_processed_total counter\n"
            "courier_data_monitor_files_processed_total{monitor_name=\"m1\",status=\"success\"} 42.0\n"
            "courier_data_monitor_files_processed_total{monitor_name=\"m1\",status=\"error\"} 3.0\n"
            "courier_data_monitor_files_processed_total{monitor_name=\"m2\",status=\"success\"} 15.0\n"
            "# HELP courier_broker_connected Broker connected\n"
            "# TYPE courier_broker_connected gauge\n"
            "courier_broker_connected 1.0\n"
            "# HELP courier_job_builder_files_received_total Files received\n"
            "# TYPE courier_job_builder_files_received_total counter\n"
            "courier_job_builder_files_received_total{job_builder_name=\"b1\"} 100.0\n"
            "# HELP courier_job_builder_jobs_built_total Jobs built\n"
            "# TYPE courier_job_builder_jobs_built_total counter\n"
            "courier_job_builder_jobs_built_total{job_builder_name=\"b1\",status=\"ready\"} 100.0\n"
            "# HELP courier_job_builder_jobs_emitted_total Jobs emitted\n"
            "# TYPE courier_job_builder_jobs_emitted_total counter\n"
            "courier_job_builder_jobs_emitted_total{job_builder_name=\"b1\",target=\"d1\"} 80.0\n"
            "# HELP courier_job_builder_emit_failures_total Emit failures\n"
            "# TYPE courier_job_builder_emit_failures_total counter\n"
            "courier_job_builder_emit_failures_total{job_builder_name=\"b1\",target=\"d1\",reason=\"timeout\"} 20.0\n"
            "# HELP courier_dispatcher_jobs_processed_total Dispatcher jobs\n"
            "# TYPE courier_dispatcher_jobs_processed_total counter\n"
            "courier_dispatcher_jobs_processed_total{dispatcher_name=\"d1\",status=\"success\"} 50.0\n"
            "courier_dispatcher_jobs_processed_total{dispatcher_name=\"d1\",status=\"error\"} 2.0\n"
            "# HELP courier_plugin_state Plugin state\n"
            "# TYPE courier_plugin_state gauge\n"
            "courier_plugin_state{plugin_name=\"p1\"} 2.0\n"
            "courier_plugin_health{plugin_name=\"p1\"} 1.0\n"
        )
        mock_client.get.return_value = mock_response

        result = asyncio.run(fetcher.fetch())

        assert result.service.health == 1.0
        assert result.service.uptime_seconds == 7200.0
        assert result.broker.connected == 1.0
        assert len(result.data_monitors.monitors) == 2
        assert len(result.plugins.plugins) == 1
        assert result.plugins.plugins[0].name == "p1"
        assert result.plugins.plugins[0].health == 1.0
        assert len(result.job_builders.builders) == 1
        builder = result.job_builders.builders[0]
        assert builder.name == "b1"
        assert builder.success_rate == 0.8  # 80/(80+20)

        # Data monitor assertions
        m1 = result.data_monitors.monitors[0]
        assert m1.name == "m1"
        assert m1.files_processed == 45.0  # 42 + 3
        assert m1.success_count == 42.0
        assert m1.failure_count == 3.0     # 45 - 42
        m2 = result.data_monitors.monitors[1]
        assert m2.name == "m2"
        assert m2.files_processed == 15.0
        assert m2.success_count == 15.0
        assert m2.failure_count == 0.0

        # Dispatcher assertions
        assert len(result.dispatchers.dispatchers) == 1
        d1 = result.dispatchers.dispatchers[0]
        assert d1.name == "d1"
        assert d1.jobs_processed_rate == 52.0  # 50 + 2
        assert abs(d1.success_ratio - 0.9615) < 0.01  # 50/52
