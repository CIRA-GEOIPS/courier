"""Tests that all plugin-scoped metrics carry the required identifier labels (ISSUE 8)."""

from __future__ import annotations

import pytest

from courier import metrics


# ── data monitor family ────────────────────────────────────────────────────


_DATA_MONITOR_COUNTERS = [
    metrics.DATA_MONITOR_FILES_PROCESSED,
    metrics.DATA_MONITOR_POLL_ERRORS,
]

_DATA_MONITOR_GAUGES = [
    metrics.DATA_MONITOR_CONNECTION_STATUS,
    metrics.DATA_MONITOR_CONSUMER_LAG,
    metrics.DATA_MONITOR_LAST_SCAN_TIMESTAMP,
]

_DATA_MONITOR_HISTOGRAMS = [
    metrics.DATA_MONITOR_SCAN_DURATION,
]


@pytest.mark.parametrize("m", _DATA_MONITOR_COUNTERS)
def test_data_monitor_counter_has_identifier_label(
    m: metrics.Counter,
) -> None:
    assert "monitor_identifier" in m._labelnames
    assert "monitor_name" in m._labelnames


@pytest.mark.parametrize("m", _DATA_MONITOR_GAUGES)
def test_data_monitor_gauge_has_identifier_label(
    m: metrics.Gauge,
) -> None:
    assert "monitor_identifier" in m._labelnames
    assert "monitor_name" in m._labelnames


@pytest.mark.parametrize("m", _DATA_MONITOR_HISTOGRAMS)
def test_data_monitor_histogram_has_identifier_label(
    m: metrics.Histogram,
) -> None:
    assert "monitor_identifier" in m._labelnames
    assert "monitor_name" in m._labelnames


# ── dispatcher family ──────────────────────────────────────────────────────


def test_dispatcher_jobs_processed_has_identifier_label() -> None:
    assert "dispatcher_identifier" in metrics.DISPATCHER_JOBS_PROCESSED._labelnames


def test_dispatcher_job_execution_duration_has_identifier_label() -> None:
    assert "dispatcher_identifier" in metrics.DISPATCHER_JOB_EXECUTION_DURATION._labelnames


def test_dispatcher_active_jobs_has_identifier_label() -> None:
    assert "dispatcher_identifier" in metrics.DISPATCHER_ACTIVE_JOBS._labelnames


def test_dispatcher_execution_logs_emitted_has_identifier_label() -> None:
    assert "dispatcher_identifier" in metrics.DISPATCHER_EXECUTION_LOGS_EMITTED._labelnames


def test_dispatcher_queue_wait_duration_has_identifier_label() -> None:
    assert "dispatcher_identifier" in metrics.DISPATCHER_QUEUE_WAIT_DURATION._labelnames


def test_dispatcher_jobs_consumed_has_identifier_label() -> None:
    assert "dispatcher_identifier" in metrics.DISPATCHER_JOBS_CONSUMED._labelnames


def test_dispatcher_dispatch_latency_has_identifier_label() -> None:
    assert "dispatcher_identifier" in metrics.DISPATCHER_DISPATCH_LATENCY_SECONDS._labelnames


def test_dispatcher_dedupe_skips_has_identifier_label() -> None:
    assert "dispatcher_identifier" in metrics.DISPATCHER_DEDUPE_SKIPS._labelnames


def test_dispatcher_queue_depth_has_identifier_label() -> None:
    assert "dispatcher_identifier" in metrics.DISPATCHER_QUEUE_DEPTH._labelnames


# ── job builder family ─────────────────────────────────────────────────────


def test_job_builder_jobs_emitted_has_identifier_label() -> None:
    assert "job_builder_identifier" in metrics.JOB_BUILDER_JOBS_EMITTED._labelnames


def test_job_builder_emit_failures_has_identifier_label() -> None:
    assert "job_builder_identifier" in metrics.JOB_BUILDER_EMIT_FAILURES._labelnames


def test_job_builder_timeout_emissions_has_identifier_label() -> None:
    assert "job_builder_identifier" in metrics.JOB_BUILDER_TIMEOUT_EMISSIONS._labelnames


def test_job_builder_files_received_has_identifier_label() -> None:
    assert "job_builder_identifier" in metrics.JOB_BUILDER_FILES_RECEIVED._labelnames


# ── plugin family ──────────────────────────────────────────────────────────


def test_plugin_state_has_identifier_label() -> None:
    assert "plugin_identifier" in metrics.PLUGIN_STATE._labelnames


def test_plugin_health_has_identifier_label() -> None:
    assert "plugin_identifier" in metrics.PLUGIN_HEALTH._labelnames


def test_plugin_registration_failures_has_identifier_label() -> None:
    assert "plugin_identifier" in metrics.PLUGIN_REGISTRATION_FAILURES._labelnames


def test_plugin_restarts_has_identifier_label() -> None:
    assert "plugin_identifier" in metrics.PLUGIN_RESTARTS._labelnames


def test_data_monitor_last_processed_timestamp_has_identifier_label() -> None:
    """LAST_PROCESSED_TIMESTAMP uses plugin_name + monitor_identifier (historic)."""
    assert "monitor_identifier" in metrics.DATA_MONITOR_LAST_PROCESSED_TIMESTAMP._labelnames
    assert "plugin_name" in metrics.DATA_MONITOR_LAST_PROCESSED_TIMESTAMP._labelnames


# ── rabbitmq-specific ──────────────────────────────────────────────────────


def test_rabbitmq_last_file_emitted_has_identifier_label() -> None:
    assert "monitor_identifier" in metrics.RABBITMQ_LAST_FILE_EMITTED_TIMESTAMP._labelnames
