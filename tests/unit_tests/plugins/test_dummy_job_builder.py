"""Unit tests for the dummy_job_builder plugin."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from courier.interfaces.job_builders import JobBuilder
from courier.plugins.job_builders.dummy_job_builder import (
    DummyJob,
    DummyJobBuilder,
    DummyJobGroup,
)
from courier.types.job import Job
from tests._helpers import bind_payload, with_payload


def _builder(service: MagicMock, **config: object) -> DummyJobBuilder:
    """Build a DummyJobBuilder with a payload block, and bind the payload."""
    builder = DummyJobBuilder(service, with_payload(dict(config)))
    bind_payload(builder)
    return builder


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
        builder = _builder(mock_service)
        assert len(builder.job_groups) == 1
        assert isinstance(builder.job_groups[0], DummyJobGroup)

    def test_healthy(self, mock_service: MagicMock) -> None:
        builder = _builder(mock_service)
        assert builder.is_healthy() is True

    def test_process_job_group_removes_ready_job(
        self, mock_service: MagicMock, make_frozen_file
    ) -> None:
        """Ready jobs are removed from the group after emission."""
        builder = _builder(mock_service)
        group = builder.job_groups[0]

        file1 = make_frozen_file(file=Path("/tmp/a.nc"))
        builder._process_job_group(group, file1)
        assert len(group.jobs) == 0, "Ready job should be removed after emission"

    def test_n_files_produce_n_jobs_not_n_squared(
        self, mock_service: MagicMock, make_frozen_file
    ) -> None:
        """N files should produce exactly N ready jobs, not O(N^2)."""
        builder = _builder(mock_service)
        group = builder.job_groups[0]

        n = 5
        for i in range(n):
            file_i = make_frozen_file(file=Path(f"/tmp/file_{i}.nc"))
            builder._process_job_group(group, file_i)
            # After each file, the group should be empty because
            # the ready job is popped immediately after emission.
            assert len(group.jobs) == 0, (
                f"After file {i}, group should be empty but has {len(group.jobs)} jobs"
            )

    def test_job_config_leaves_out_the_payload_block(
        self,
        mock_service: MagicMock,
        make_frozen_file,
    ) -> None:
        """The block is the builder's; the job carries it once, as its payload.

        Copying the raw config into the group config put the payload's whole
        block, template included, into every job's ``config`` as well.
        """
        builder = _builder(mock_service, targets=["dp-1"], extra="kept")
        builder._process_job_group(
            builder.job_groups[0],
            make_frozen_file(file=Path("/tmp/a.nc")),
        )

        assert builder.job_groups[0].config == {"targets": ["dp-1"], "extra": "kept"}
        message = mock_service.emit.call_args.kwargs["message"]
        published = Job.from_string(message)
        assert "payload" not in published.config
        assert published.payload is not None
        assert published.payload.identifier == builder.payload_identifier

    def test_job_config_leaves_out_the_state_sync_block(
        self,
        mock_service: MagicMock,
        make_frozen_file,
    ) -> None:
        """``state_sync`` holds Redis credentials; no job may carry them."""
        state_sync = {"host": "redis", "password": "redis-password-not-for-the-wire"}
        # No Redis here: construct as a synced builder, then emit unsynced.
        with patch.object(JobBuilder, "_init_sync", return_value=MagicMock()):
            builder = _builder(
                mock_service,
                targets=["dp-1"],
                state_sync=state_sync,
                extra="kept",
            )
        builder._sync = None
        builder._process_job_group(
            builder.job_groups[0],
            make_frozen_file(file=Path("/tmp/a.nc")),
        )

        assert builder.config["state_sync"] == state_sync
        assert builder.job_groups[0].config == {"targets": ["dp-1"], "extra": "kept"}
        message = mock_service.emit.call_args.kwargs["message"]
        assert "redis-password-not-for-the-wire" not in message
        assert "state_sync" not in Job.from_string(message).config
