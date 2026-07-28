"""Unit tests and property-based round-trip tests for Job and JobGroup."""

import json
import time
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from courier.types.file import File, FrozenFile
from courier.types.job import _OVERFLOW_SEPARATOR, Job, JobGroup


# ─── Helpers ─────────────────────────────────────────────────────────────────


def _jobs_equal(a: Job, b: Job) -> bool:
    """Compare two Jobs field-by-field (Job has no __eq__)."""
    return (
        a.name == b.name
        and a.identifier == b.identifier
        and a.config == b.config
        and a.files == b.files
        and a.last_modified == b.last_modified
        and a.timeout == b.timeout
    )


# ─── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def simple_job() -> Job:
    """Minimal Job with no files."""
    return Job(name="test_job", identifier="job-001", config={"key": "value"}, last_modified=1000.0)


@pytest.fixture
def job_with_files() -> Job:
    """Job containing two FrozenFile instances."""
    return Job(
        name="test_job",
        identifier="job-002",
        config=None,
        files={
            FrozenFile(file=Path("/data/file_a.nc"), hostname="host1", source="goes16"),
            FrozenFile(file=Path("/data/file_b.nc"), hostname="host1", source="goes16"),
        },
        last_modified=2000.0,
        timeout=3600.0,
    )


# ─── Construction ─────────────────────────────────────────────────────────────


def test_defaults(simple_job: Job) -> None:
    """Constructed fields are accessible."""
    assert simple_job.name == "test_job"
    assert simple_job.identifier == "job-001"
    assert simple_job.config == {"key": "value"}
    assert isinstance(simple_job.files, set)
    assert len(simple_job.files) == 0


def test_last_modified_defaults_to_now() -> None:
    """last_modified defaults to current time when not provided."""
    before = time.time()
    job = Job(name="j", identifier="i", config=None)
    after = time.time()
    assert before <= job.last_modified <= after


def test_add_file_updates_last_modified(simple_job: Job) -> None:
    """add_file() adds the file and updates last_modified."""
    before = time.time()
    f = FrozenFile(file=Path("/x.nc"))
    simple_job.add_file(f)
    after = time.time()

    assert f in simple_job.files
    assert before <= simple_job.last_modified <= after


def test_is_old_false_for_new_job(simple_job: Job) -> None:
    """A freshly created job is not old."""
    job = Job(name="j", identifier="i", config=None, timeout=3600.0)
    assert not job.is_old()


def test_is_old_true_for_expired_job() -> None:
    """A job older than its timeout is old."""
    old_ts = time.time() - 7200
    job = Job(name="j", identifier="i", config=None, last_modified=old_ts, timeout=3600.0)
    assert job.is_old()


# ─── Serialization ────────────────────────────────────────────────────────────


def test_str_produces_valid_json(simple_job: Job) -> None:
    """__str__ produces parseable JSON with expected keys."""
    data = json.loads(str(simple_job))
    assert data["name"] == "test_job"
    assert data["identifier"] == "job-001"
    assert data["last_modified"] == 1000.0


def test_round_trip_empty_files(simple_job: Job) -> None:
    """Job with no files round-trips correctly."""
    restored = Job.from_string(str(simple_job))
    assert _jobs_equal(simple_job, restored)


def test_round_trip_with_files(job_with_files: Job) -> None:
    """Job with FrozenFile instances round-trips correctly."""
    restored = Job.from_string(str(job_with_files))
    assert _jobs_equal(job_with_files, restored)
    assert all(isinstance(f, FrozenFile) for f in restored.files)


def test_round_trip_null_config() -> None:
    """Job with None config round-trips correctly."""
    job = Job(name="n", identifier="i", config=None, last_modified=5.0)
    assert _jobs_equal(job, Job.from_string(str(job)))


# ─── Property-based round-trip ────────────────────────────────────────────────

_identifiers = st.from_regex(r"[a-zA-Z0-9\-_]{1,40}", fullmatch=True)
_json_scalars = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-1000, max_value=1000),
    st.text(max_size=50),
)
# Timestamps are generated in every form a monitor can produce -- naive (from
# a filename regex), aware UTC (from S3 LastModified), and aware non-UTC. They
# were previously pinned to None here "because test_file.py covers it", but
# test_file.py had no property test over timestamps either, and the gap between
# the two files is exactly where a timezone bug hid: naive values were read as
# host-local, so the same instant produced different time-grouping buckets.
_timestamps = st.one_of(
    st.none(),
    st.datetimes(
        min_value=datetime(2000, 1, 1),
        max_value=datetime(2100, 1, 1),
    ),
    st.datetimes(
        min_value=datetime(2000, 1, 1),
        max_value=datetime(2100, 1, 1),
        timezones=st.sampled_from(
            [UTC, timezone(timedelta(hours=-6)), timezone(timedelta(hours=5, minutes=30))],
        ),
    ),
)

_frozen_files = st.builds(
    FrozenFile,
    file=st.one_of(
        st.none(),
        st.builds(Path, st.from_regex(r"/[a-zA-Z0-9/\-_\.]{1,60}", fullmatch=True)),
    ),
    hostname=st.one_of(st.none(), st.from_regex(r"[a-zA-Z0-9\-]{1,30}", fullmatch=True)),
    source=st.one_of(st.none(), st.from_regex(r"[a-z0-9]{2,10}", fullmatch=True)),
    timestamp=_timestamps,
)


@given(
    name=_identifiers,
    identifier=_identifiers,
    config=st.one_of(st.none(), st.dictionaries(_identifiers, _json_scalars, max_size=5)),
    files=st.frozensets(_frozen_files, max_size=4),
    last_modified=st.floats(min_value=0.0, max_value=1e12, allow_nan=False, allow_infinity=False),
    timeout=st.floats(min_value=1.0, max_value=1e9, allow_nan=False, allow_infinity=False),
)
@settings(max_examples=50)
def test_hypothesis_round_trip(
    name: str,
    identifier: str,
    config: dict | None,
    files: frozenset,
    last_modified: float,
    timeout: float,
) -> None:
    """Property: Job.from_string(str(job)) matches all fields of the original."""
    job = Job(
        name=name,
        identifier=identifier,
        config=config,
        files=set(files),
        last_modified=last_modified,
        timeout=timeout,
    )
    restored = Job.from_string(str(job))
    assert _jobs_equal(job, restored)


# ─── JobGroup ─────────────────────────────────────────────────────────────────


def test_job_group_ready_jobs_empty() -> None:
    """New JobGroup has no ready jobs."""
    group = JobGroup(job_name="g", config=None)
    assert group.ready_jobs() == []


def test_job_group_file_not_relevant_by_default() -> None:
    """Base JobGroup raises NotImplementedError for file_is_relevant."""
    group = JobGroup(job_name="g", config=None)
    f = FrozenFile(file=Path("/x.nc"))
    with pytest.raises(NotImplementedError):
        group.file_is_relevant(f)
    with pytest.raises(NotImplementedError):
        group.add_file(f)
    assert len(group.jobs) == 0


# ─── Overflow Counter ───────────────────────────────────────────────────────


class _FixedBucketGroup(JobGroup):
    """Minimal group: every file is relevant and maps to one fixed bucket."""

    def file_is_relevant(self, _file: File | FrozenFile) -> bool:
        return True

    def get_job_ids_from_file(self, _file: File | FrozenFile) -> list[str]:
        return ["proto"]


class _CapOneGroup(_FixedBucketGroup):
    """Fixed-bucket group whose jobs accept exactly one file."""

    def __init__(self, job_name: str = "g", config: object = None) -> None:
        super().__init__(job_name, config)

        class _CapOneJob(Job):
            def ready(self) -> bool:
                return False

            def add_file(self, file: File | FrozenFile) -> bool:
                if len(self.files) >= 1:
                    return False
                return super().add_file(file)

        self.job = _CapOneJob


class TestJobIdSequencing:
    """Job IDs must never be reused within a group's lifetime.

    The dispatcher's dedupe LRU drops a job whose identifier it has already
    seen, so a recycled ID means a silently skipped job. Sequence numbers are
    therefore issued at creation time and only ever increase.
    """

    def test_first_job_keeps_the_bucket_id(self) -> None:
        """The common case stays readable: no suffix until one is needed."""
        group = _FixedBucketGroup(job_name="g", config=None)
        assert group.add_file(FrozenFile(file=Path("/data/a.nc"))) is True
        assert "proto" in group.jobs

    def test_files_accumulate_into_the_open_job(self) -> None:
        """Files for one bucket join the same job while it still accepts them."""
        group = _FixedBucketGroup(job_name="g", config=None)
        group.add_file(FrozenFile(file=Path("/data/a.nc")))
        group.add_file(FrozenFile(file=Path("/data/b.nc")))
        assert list(group.jobs) == ["proto"]
        assert len(group.jobs["proto"].files) == 2

    def test_rejected_file_opens_a_successor_job(self) -> None:
        """A full job is retired and its files kept; the file is not dropped."""
        group = _CapOneGroup()
        group.add_file(FrozenFile(file=Path("/data/a.nc")))
        group.add_file(FrozenFile(file=Path("/data/b.nc")))

        assert sorted(group.jobs) == ["proto", "proto_overflow_1"]
        retained = {str(f.file) for job in group.jobs.values() for f in job.files}
        assert retained == {"/data/a.nc", "/data/b.nc"}

    def test_ids_are_never_reused_across_emit_cycles(self) -> None:
        """Emitting and refilling a bucket must not recycle an identifier."""
        group = _FixedBucketGroup(job_name="g", config=None)
        issued: list[str] = []

        for i in range(5):
            group.add_file(FrozenFile(file=Path(f"/data/{i}.nc")))
            for job_id in list(group.jobs):
                issued.append(job_id)
                del group.jobs[job_id]
                group._record_job_emitted(job_id)

        assert len(issued) == len(set(issued)), f"recycled IDs: {issued}"

    def test_removal_without_record_does_not_clobber_a_live_job(self) -> None:
        """The timeout-discard path drops jobs without _record_job_emitted.

        Regression guard: the successor ID used to be derived from a counter
        that had not advanced, so the next file overwrote a job that was still
        accumulating files.
        """
        group = _CapOneGroup()
        group.add_file(FrozenFile(file=Path("/data/a.nc")))
        group.add_file(FrozenFile(file=Path("/data/b.nc")))
        del group.jobs["proto"]  # timeout discard: no _record_job_emitted

        group.add_file(FrozenFile(file=Path("/data/c.nc")))

        retained = {str(f.file) for job in group.jobs.values() for f in job.files}
        assert "/data/b.nc" in retained
        assert retained == {"/data/b.nc", "/data/c.nc"}

    def test_a_file_reaches_every_bucket_it_maps_to(self) -> None:
        """``get_job_ids_from_file`` may return several buckets; all must fill.

        Found by mutation testing: turning the loop's ``continue`` into a
        ``break`` survived the whole suite, because nothing exercised a file
        belonging to more than one bucket.
        """

        class _MultiBucketGroup(JobGroup):
            def file_is_relevant(self, _file: File | FrozenFile) -> bool:
                return True

            def get_job_ids_from_file(
                self, _file: File | FrozenFile,
            ) -> list[str]:
                return ["alpha", "beta", "gamma"]

        group = _MultiBucketGroup(job_name="g", config=None)
        group.add_file(FrozenFile(file=Path("/data/a.nc")))

        assert sorted(group.jobs) == ["alpha", "beta", "gamma"]
        for job in group.jobs.values():
            assert len(job.files) == 1

    def test_stale_open_job_pointer_is_replaced_not_dereferenced(self) -> None:
        """A pointer to a job that has been deleted must open a fresh one.

        Found by mutation testing: relaxing the ``and`` in ``_open_job_for``
        to ``or`` survived, meaning nothing covered a bucket whose open job
        had been removed without clearing the pointer — which is exactly what
        the timeout-discard path does.
        """
        group = _FixedBucketGroup(job_name="g", config=None)
        group.add_file(FrozenFile(file=Path("/data/a.nc")))
        assert group._open_job_ids["proto"] == "proto"

        # Timeout discard removes the job but leaves the pointer behind.
        del group.jobs["proto"]

        group.add_file(FrozenFile(file=Path("/data/b.nc")))

        assert len(group.jobs) == 1
        (job,) = group.jobs.values()
        assert {str(f.file) for f in job.files} == {"/data/b.nc"}

    def test_record_job_emitted_closes_only_the_matching_job(self) -> None:
        """Retiring a stale ID must not close the bucket's current job."""
        group = _FixedBucketGroup(job_name="g", config=None)
        group.add_file(FrozenFile(file=Path("/data/a.nc")))
        assert group._open_job_ids["proto"] == "proto"

        group._record_job_emitted("proto_overflow_9")  # not the open job
        assert group._open_job_ids["proto"] == "proto"

        group._record_job_emitted("proto")
        assert "proto" not in group._open_job_ids

    def test_overflow_separator_constant_exists(self) -> None:
        """_OVERFLOW_SEPARATOR is a module-level constant with expected value."""
        assert _OVERFLOW_SEPARATOR == "_overflow_"
