"""Unit tests for the filter_and_group job builder."""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from courier.constants import PluginRunState
from courier.plugins.classes.job_builders.filter_and_group import (
    FilterAndGroupConfig,
    FilterAndGroupJobBuilder,
    FilterAndGroupJobGroup,
    make_job_class,
)


def _make_config(**overrides: Any) -> dict[str, Any]:
    cfg: dict[str, Any] = {}
    cfg.update(overrides)
    return cfg


# ─── Config Validation ──────────────────────────────────────────────────────


class TestFilterAndGroupConfig:
    def test_defaults(self) -> None:
        cfg = FilterAndGroupConfig.model_validate({})
        assert cfg.files_per_job == 5
        assert cfg.min_files == 1

    def test_min_files_le_files_per_job(self) -> None:
        with pytest.raises(ValidationError, match="min_files"):
            FilterAndGroupConfig.model_validate(
                _make_config(files_per_job=2, min_files=5),
            )

    def test_timeout_positive(self) -> None:
        with pytest.raises(ValidationError):
            FilterAndGroupConfig.model_validate(
                _make_config(window_timeout_seconds=0),
            )


# ─── make_job_class ─────────────────────────────────────────────────────────


class TestJobClass:
    def test_ready_at_files_per_job(self, make_frozen_file) -> None:
        cfg = FilterAndGroupConfig.model_validate({"files_per_job": 2})
        JobCls = make_job_class(cfg)
        job = JobCls(name="g", identifier="j", config={})
        assert job.ready() is False
        job.add_file(make_frozen_file(source="a"))
        job.add_file(make_frozen_file(source="b"))
        assert job.ready() is True

    def test_timeout_dropout_emits(self, make_frozen_file) -> None:
        cfg = FilterAndGroupConfig.model_validate(
            {"files_per_job": 10, "min_files": 1, "window_timeout_seconds": 0.01},
        )
        JobCls = make_job_class(cfg)
        job = JobCls(name="g", identifier="j", config={})
        job.add_file(make_frozen_file())
        time.sleep(0.05)
        assert job.ready() is True

    def test_add_file_rejects_filter_miss(self, make_frozen_file) -> None:
        cfg = FilterAndGroupConfig.model_validate({"filters": {"source": "goes16"}})
        JobCls = make_job_class(cfg)
        job = JobCls(name="g", identifier="j", config={})
        job.add_file(make_frozen_file(source="other"))
        assert len(job.files) == 0


# ─── FilterAndGroupJobGroup ─────────────────────────────────────────────────


class TestJobGroup:
    def test_time_grouping_buckets(self, make_frozen_file) -> None:
        cfg = FilterAndGroupConfig.model_validate(
            {"time_grouping": {"hours": 1, "start": "2026-01-01 00:00:00"}},
        )
        group = FilterAndGroupJobGroup(cfg)
        f1 = make_frozen_file(timestamp=datetime(2026, 1, 1, 0, 30))
        f2 = make_frozen_file(timestamp=datetime(2026, 1, 1, 1, 30))
        assert group.get_job_ids_from_file(f1) != group.get_job_ids_from_file(f2)

    def test_time_grouping_requires_timestamp(self, make_frozen_file) -> None:
        cfg = FilterAndGroupConfig.model_validate(
            {"time_grouping": {"hours": 1}},
        )
        group = FilterAndGroupJobGroup(cfg)
        assert group.get_job_ids_from_file(make_frozen_file(timestamp=None)) == []


# ─── Builder lifecycle ──────────────────────────────────────────────────────


class TestBuilder:
    def test_initializes_without_reaper(self, mock_service: MagicMock) -> None:
        builder = FilterAndGroupJobBuilder(mock_service, {})
        assert builder._reaper_thread is None

    def test_module_init_short_circuits(self) -> None:
        builder = FilterAndGroupJobBuilder(None, None)
        assert not hasattr(builder, "validated_config")

    def test_reap_group_emits_and_removes(
        self, mock_service: MagicMock, make_frozen_file, mocker
    ) -> None:
        builder = FilterAndGroupJobBuilder(
            mock_service, {"files_per_job": 1},
        )
        group = builder.job_groups[0]
        JobCls = group.job
        job = JobCls(name=group.name, identifier="jid", config={})
        job.add_file(make_frozen_file())
        group.jobs["jid"] = job

        emit = mocker.patch.object(builder, "emit")
        builder._reap_group(group)
        emit.assert_called_once()
        assert "jid" not in group.jobs

    def test_is_healthy_without_running_state(self, mock_service: MagicMock) -> None:
        builder = FilterAndGroupJobBuilder(mock_service, {})
        assert builder.is_healthy() is False

    def test_is_healthy_running_no_reaper(
        self, mock_service: MagicMock, mocker
    ) -> None:
        builder = FilterAndGroupJobBuilder(mock_service, {})
        mocker.patch.object(builder, "_state", PluginRunState.RUNNING)
        assert builder.is_healthy() is True

    def test_reap_group_bumps_overflow_counter(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        mocker,
    ) -> None:
        """_reap_group bumps _overflow_counters for popped job's base ID."""
        builder = FilterAndGroupJobBuilder(
            mock_service,
            {
                "files_per_job": 1,
                "min_files": 1,
                "window_timeout_seconds": 0.01,
            },
        )
        group = builder.job_groups[0]
        JobCls = group.job
        job = JobCls(name=group.name, identifier="jid_overflow_3", config={})
        job.add_file(make_frozen_file())
        group.jobs["jid_overflow_3"] = job

        emit = mocker.patch.object(builder, "emit")
        builder._reap_group(group)
        emit.assert_called_once()
        assert "jid_overflow_3" not in group.jobs
        assert group._overflow_counters["jid"] == 1

    def test_pop_ready_jobs_bumps_overflow_counter(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        mocker,
    ) -> None:
        """_pop_ready_jobs bumps _overflow_counters for popped job's base ID."""
        builder = FilterAndGroupJobBuilder(
            mock_service, {"files_per_job": 1},
        )
        group = builder.job_groups[0]
        JobCls = group.job
        job = JobCls(name=group.name, identifier="bucket_42", config={})
        job.add_file(make_frozen_file())
        group.jobs["bucket_42"] = job

        mocker.patch.object(builder, "emit")
        builder._pop_ready_jobs(group, [job])
        assert "bucket_42" not in group.jobs
        assert group._overflow_counters["bucket_42"] == 1
