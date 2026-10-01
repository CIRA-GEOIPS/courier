"""Unit tests for the dummy_job_builder plugin."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from courier.interfaces.job_builders import JobBuilder
from courier.plugins.job_builders.dummy_job_builder import (
    DummyJob,
    DummyJobBuilder,
    DummyJobGroup,
)
from tests._helpers import with_payload

# ─── DummyJob ───────────────────────────────────────────────────────────────


class TestDummyJob:
    def test_ready_always_true(self) -> None:
        job = DummyJob(name="x", identifier="j", config={})
        assert job.ready() is True

    def test_add_file_caps_at_one(self, make_frozen_file) -> None:
        job = DummyJob(name="x", identifier="j", config={})
        job.add_file(make_frozen_file())
        assert len(job.files) == 1
        # Second add is ignored
        job.add_file(make_frozen_file(source="other"))
        assert len(job.files) == 1


# ─── DummyJobGroup ──────────────────────────────────────────────────────────


class TestDummyJobGroup:
    def test_file_is_relevant_always_true(self, make_frozen_file) -> None:
        group = DummyJobGroup({})
        assert group.file_is_relevant(make_frozen_file()) is True


# ─── DummyJobBuilder ────────────────────────────────────────────────────────


class TestDummyJobBuilder:
    def test_initializes(self, mock_service: MagicMock) -> None:
        builder = DummyJobBuilder(mock_service, with_payload())
        assert len(builder.job_groups) == 1
        assert isinstance(builder.job_groups[0], DummyJobGroup)

    def test_healthy(self, mock_service: MagicMock) -> None:
        builder = DummyJobBuilder(mock_service, with_payload())
        assert builder.is_healthy() is True

    def test_process_job_group_removes_ready_job(
        self, mock_service: MagicMock, make_frozen_file
    ) -> None:
        """Ready jobs are removed from the group after emission."""
        builder = DummyJobBuilder(mock_service, with_payload())
        group = builder.job_groups[0]

        from pathlib import Path

        file1 = make_frozen_file(file=Path("/tmp/a.nc"))
        builder._process_job_group(group, file1)
        assert len(group.jobs) == 0, "Ready job should be removed after emission"

    def test_n_files_produce_n_jobs_not_n_squared(
        self, mock_service: MagicMock, make_frozen_file
    ) -> None:
        """N files should produce exactly N ready jobs, not O(N^2)."""
        builder = DummyJobBuilder(mock_service, with_payload())
        group = builder.job_groups[0]

        from pathlib import Path

        n = 5
        for i in range(n):
            file_i = make_frozen_file(file=Path(f"/tmp/file_{i}.nc"))
            builder._process_job_group(group, file_i)
            # After each file, the group should be empty because
            # the ready job is popped immediately after emission.
            assert (
                len(group.jobs) == 0
            ), f"After file {i}, group should be empty but has {len(group.jobs)} jobs"

    def test_group_config_leaves_out_builder_only_keys(
        self,
        mock_service: MagicMock,
    ) -> None:
        """Jobs carry the group config, so builder-only blocks stay out of it.

        The payload block travels once, as ``job.payload``; ``state_sync``
        holds Redis credentials.
        """
        config = with_payload({"targets": ["dp-1"], "state_sync": {}, "extra": 1})
        with patch.object(JobBuilder, "_init_sync", return_value=MagicMock()):
            builder = DummyJobBuilder(mock_service, config)

        assert builder.job_groups[0].config == {"targets": ["dp-1"], "extra": 1}
