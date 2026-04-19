"""Unit tests for the dummy_job_builder plugin."""

from __future__ import annotations

from unittest.mock import MagicMock

from courier.plugins.classes.job_builders.dummy_job_builder import (
    DummyJob,
    DummyJobBuilder,
    DummyJobGroup,
)


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
        builder = DummyJobBuilder(mock_service, {})
        assert len(builder.job_groups) == 1
        assert isinstance(builder.job_groups[0], DummyJobGroup)

    def test_healthy(self, mock_service: MagicMock) -> None:
        builder = DummyJobBuilder(mock_service, {})
        assert builder.is_healthy() is True

    def test_module_init_short_circuits(self) -> None:
        builder = DummyJobBuilder(None, None)
        assert not hasattr(builder, "job_groups")
