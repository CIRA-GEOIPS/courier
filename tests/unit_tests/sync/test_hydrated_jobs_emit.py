"""A job already complete in shared state must be emitted at startup.

``JobBuilderStateSync.start`` hydrates the Redis hash into the local groups,
but the hydration path never fires the merge callback -- that only runs on a
live ``job_updated`` message. So the one startup scan for ready jobs never
ran, and a job already complete in the hash just sat there.

It gets into the hash that way whenever a replica dies between
``push_job_update`` and ``push_job_deletion``. With no ``window_timeout_seconds``
there is no reaper either, so nothing looked at that group again until an
unrelated file arrived for it -- or until ``job.timeout`` discarded the job.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from courier.interfaces.job_builders import JobBuilder
from courier.types.file import File, FrozenFile
from courier.types.job import Job, JobGroup


class _AlwaysReadyJob(Job):
    def ready(self) -> bool:
        return bool(self.files)


class _StubGroup(JobGroup):
    def __init__(self) -> None:
        super().__init__("grp", {})
        self.job = _AlwaysReadyJob

    def file_is_relevant(self, file: File | FrozenFile) -> bool:
        return True

    def get_job_ids_from_file(self, file: File | FrozenFile) -> list[str]:
        return ["bucket"]


@pytest.fixture
def service() -> MagicMock:
    svc = MagicMock()
    svc.config = MagicMock(log_level="DEBUG", loki_enabled=False, namespace="ns")
    svc.target_resolver.resolve.side_effect = lambda ident: f"JobReady-{ident}"
    return svc


def test_a_job_complete_in_the_hash_is_emitted_on_start(
    service: MagicMock,
) -> None:
    """Otherwise it waits for an unrelated file, or for job.timeout."""
    builder = JobBuilder(service, {"targets": ["dp-1"]}, identifier="jb-1")
    group = _StubGroup()
    builder.job_groups = [group]

    sync = MagicMock()
    sync.try_claim_emit.return_value = True

    def _hydrate(job_groups: list[JobGroup], _locks: dict) -> None:
        """Stand in for load_remote_state: fills the group, fires nothing."""
        stranded = _AlwaysReadyJob("n", "stranded", {})
        stranded.add_file(FrozenFile(file=Path("/data/a.nc"), hostname="h"))
        job_groups[0].jobs["stranded"] = stranded

    sync.start.side_effect = _hydrate
    builder._sync = sync

    builder.start()
    try:
        assert service.emit.call_count == 1
        published = Job.from_string(service.emit.call_args.kwargs["message"])
        assert len(published.files) == 1
        assert group.jobs == {}
        sync.push_job_deletion.assert_any_call("grp", "stranded")
    finally:
        builder.stop()
