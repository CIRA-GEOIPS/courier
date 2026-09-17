"""Two replicas of one job builder converge on a complete job.

Convergence is the property that makes runtime scaling of a builder safe.
Replicas of one identifier share a queue, so each receives a different subset
of the files belonging to one job. If their views of that job cannot be
reconciled, the pipeline emits short jobs and reports no error.
"""

from __future__ import annotations

import threading
from pathlib import Path

from courier.sync.job_builder_state_sync import JobBuilderStateSync
from courier.types.file import FrozenFile
from courier.types.job import Job, JobGroup
from tests._helpers import poll_until


class _CountingJob(Job):
    """Ready once it holds two files."""

    def ready(self) -> bool:
        """Return whether the job has both of its files."""
        return len(self.files) >= 2


class _Group(JobGroup):
    """One bucket, every file relevant."""

    def __init__(self) -> None:
        super().__init__("grp", {})
        self.job = _CountingJob

    def file_is_relevant(self, file: object) -> bool:
        """Accept every file."""
        del file
        return True

    def get_job_ids_from_file(self, file: object) -> list[str]:
        """Route every file to one bucket."""
        del file
        return ["bucket"]


def _sync(config, namespace: str, identifier: str = "jb") -> JobBuilderStateSync:
    """Return a connected state sync for one replica."""
    sync = JobBuilderStateSync(
        config=config,
        namespace=namespace,
        builder_name=identifier,
    )
    sync.connect()
    return sync


def _decode(value: bytes | str) -> str:
    """Return a Redis hash value as text."""
    return value.decode("utf-8") if isinstance(value, bytes) else value


def _file(name: str) -> FrozenFile:
    """Return a file with a stable, value-comparable identity."""
    return FrozenFile(file=Path(f"/data/{name}.nc"), hostname="h")


def test_two_replicas_converge_on_one_complete_job(redis_config, namespace) -> None:
    """Files delivered to different replicas end up in one job.

    Reverted check: make the write a plain overwrite instead of a server-side
    union. The second replica's write then erases the first replica's file
    from the stored record and the assertion below fails.

    The assertion is on the stored record. A replica rebuilds the full set
    locally from its peer's notification, so a check of local state passes
    even when the shared record, which is all that survives a restart, holds
    half the job.
    """
    group_a, group_b = _Group(), _Group()
    sync_a = _sync(redis_config, namespace)
    sync_b = _sync(redis_config, namespace)
    locks_a = {group_a.name: threading.Lock()}
    locks_b = {group_b.name: threading.Lock()}

    merged_b = threading.Event()
    sync_b.set_merge_callback(lambda _group: merged_b.set())

    sync_a.start([group_a], locks_a)
    sync_b.start([group_b], locks_b)
    try:
        # Replica A receives one file for the bucket.
        job_a = group_a.add_file(_file("one"))
        assert job_a is not None or group_a.jobs
        for job_id, job in group_a.jobs.items():
            sync_a.push_job_update(group_a.name, job_id, job)

        # Replica B receives the other file, having never seen the first.
        group_b.add_file(_file("two"))
        for job_id, job in group_b.jobs.items():
            sync_b.push_job_update(group_b.name, job_id, job)

        # Read the shared record, not one replica's local view.
        def shared_record_has_both() -> bool:
            client = sync_a._require_client()  # noqa: SLF001
            stored = client.hgetall(sync_a._hash_key(group_a.name))  # noqa: SLF001
            return any(
                len(_CountingJob.from_string(_decode(value)).files) == 2
                for value in stored.values()
            )

        assert poll_until(shared_record_has_both, timeout=15), (
            "the shared record did not converge: one replica's files were "
            "overwritten by the other's"
        )

        sync_b.load_remote_state()
        assert poll_until(
            lambda: any(job.ready() for job in group_b.jobs.values()),
            timeout=15,
        )
    finally:
        sync_a.stop()
        sync_b.stop()


def test_only_one_replica_may_emit_a_job(redis_config, namespace) -> None:
    """The shared claim admits one emitter.

    Convergence lets both replicas see the same finished job, so only this
    claim keeps the job from being dispatched twice.
    """
    sync_a = _sync(redis_config, namespace)
    sync_b = _sync(redis_config, namespace)
    try:
        verdicts = sorted(
            [
                sync_a.try_claim_emit("job-1::target", ttl=60),
                sync_b.try_claim_emit("job-1::target", ttl=60),
            ],
        )
        assert verdicts == [False, True]
    finally:
        sync_a.stop()
        sync_b.stop()


def test_restored_state_is_not_overwritten_by_the_next_file(
    redis_config,
    namespace,
) -> None:
    """A restarted replica keeps what it restored.

    Restored jobs were placed in the group but never registered as their
    bucket's open job, so the next file for that bucket minted the same
    identifier again and replaced the restored job, discarding every file it
    carried. This loses data on a plain restart, with no scaling involved.

    Reverted check: remove the ``adopt_job`` call from the merge path. The
    restored file is dropped and the job ends up holding one file.
    """
    group = _Group()
    sync = _sync(redis_config, namespace)
    sync.start([group], {group.name: threading.Lock()})
    try:
        group.add_file(_file("first"))
        for job_id, job in group.jobs.items():
            sync.push_job_update(group.name, job_id, job)
    finally:
        sync.stop()

    # A fresh process for the same builder identifier.
    restarted = _Group()
    sync2 = _sync(redis_config, namespace)
    sync2.start([restarted], {restarted.name: threading.Lock()})
    try:
        sync2.load_remote_state()
        assert restarted.jobs, "nothing was restored"

        restarted.add_file(_file("second"))

        assert any(len(job.files) == 2 for job in restarted.jobs.values()), (
            "the restored file was discarded when the next one arrived: "
            f"{[(jid, len(job.files)) for jid, job in restarted.jobs.items()]}"
        )
    finally:
        sync2.stop()
