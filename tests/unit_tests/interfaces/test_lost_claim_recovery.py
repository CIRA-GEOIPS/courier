"""A job no replica published must not take its files down with it.

``_claim_ready_jobs`` pops a ready job out of its group under the lock and
closes the bucket, so from that moment the only copy of those files is the
popped ``Job``. If every target is then skipped because a peer already holds
the emit claim, nothing published it and nothing put it back -- and on the
file path the replica has already HSET-then-HDEL'd the job in Redis, so no
copy survives in shared state either. It surfaced only as an INFO line,
indistinguishable from a correct dedup.

Reachable precisely in the deployment the durable-queue work enables: N
replicas of one accumulating builder sharing a queue with ``state_sync`` on,
where two replicas legitimately hold different file subsets under the same
bucket id until a merge lands.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from courier.interfaces.job_builders import JobBuilder
from courier.types.file import File, FrozenFile
from courier.types.job import Job, JobGroup


class _PairJob(Job):
    """Ready once it holds two files; rejects a third."""

    def ready(self) -> bool:
        return len(self.files) >= 2

    def add_file(self, file: File | FrozenFile) -> bool:
        if len(self.files) >= 2:
            return False
        return super().add_file(file)


class _PairGroup(JobGroup):
    """Every file is relevant and lands in one bucket."""

    def __init__(self) -> None:
        super().__init__("grp", {})
        self.job = _PairJob

    def file_is_relevant(self, file: File | FrozenFile) -> bool:
        return True

    def get_job_ids_from_file(self, file: File | FrozenFile) -> list[str]:
        return ["bucket"]


def _file(name: str) -> FrozenFile:
    return FrozenFile(file=Path(f"/data/{name}.nc"), hostname="h")


@pytest.fixture
def service() -> MagicMock:
    svc = MagicMock()
    svc.config = MagicMock(log_level="DEBUG", loki_enabled=False, namespace="ns")
    svc.target_resolver.resolve.side_effect = lambda ident: f"JobReady-{ident}"
    return svc


def _builder(service: MagicMock, *, peer_owns_claim: bool) -> JobBuilder:
    builder = JobBuilder(service, {"targets": ["dp-1"]}, identifier="jb-1")
    builder.job_groups = [_PairGroup()]
    sync = MagicMock()
    sync.try_claim_emit.return_value = not peer_owns_claim
    builder._sync = sync
    return builder


def _all_files(group: JobGroup) -> set[FrozenFile]:
    return {f for job in group.jobs.values() for f in job.files}


class TestWhollyClaimedJob:
    """Every target skipped means nothing published it."""

    def test_files_go_back_into_the_group(self, service: MagicMock) -> None:
        """The whole point: the files are still there to be emitted later."""
        builder = _builder(service, peer_owns_claim=True)
        group = builder.job_groups[0]
        sent = {_file("a"), _file("b")}

        for file in sorted(sent, key=str):
            builder._process_job_group(group, file)

        service.emit.assert_not_called()
        assert _all_files(group) == sent

    def test_the_returned_files_get_a_fresh_identifier(
        self,
        service: MagicMock,
    ) -> None:
        """A recycled id would be dropped by the dispatcher's dedupe LRU."""
        builder = _builder(service, peer_owns_claim=True)
        group = builder.job_groups[0]

        builder._process_job_group(group, _file("a"))
        first_id = next(iter(group.jobs))
        builder._process_job_group(group, _file("b"))

        assert _all_files(group) == {_file("a"), _file("b")}
        assert first_id not in group.jobs

    def test_it_is_logged_loudly_enough_to_act_on(
        self,
        service: MagicMock,
    ) -> None:
        """At INFO this was indistinguishable from a correct dedup."""
        builder = _builder(service, peer_owns_claim=True)
        builder._logger = MagicMock()

        assert builder.emit(Job("n", "job-1", {}, files=[_file("a")])) is False

        builder._logger.warning.assert_called_once()
        assert "claimed by a peer" in builder._logger.warning.call_args[0][0]

    def test_a_job_that_did_publish_is_not_put_back(
        self,
        service: MagicMock,
    ) -> None:
        """Recovery must not resurrect a job that actually went out."""
        builder = _builder(service, peer_owns_claim=False)
        group = builder.job_groups[0]

        builder._process_job_group(group, _file("a"))
        builder._process_job_group(group, _file("b"))

        assert service.emit.call_count == 1
        assert group.jobs == {}

    def test_a_partially_claimed_job_is_not_put_back(
        self,
        service: MagicMock,
    ) -> None:
        """One target reached is a publish; only *no* target is a loss."""
        builder = _builder(service, peer_owns_claim=False)
        builder._sync.try_claim_emit.side_effect = [False, True]

        assert builder.emit(Job("n", "job-1", {}), ["dp-a", "dp-b"]) is True
        assert service.emit.call_count == 1
