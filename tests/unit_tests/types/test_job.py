"""Unit tests and property-based round-trip tests for Job and JobGroup."""

import json
import time
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
_frozen_files = st.builds(
    FrozenFile,
    file=st.one_of(
        st.none(),
        st.builds(Path, st.from_regex(r"/[a-zA-Z0-9/\-_\.]{1,60}", fullmatch=True)),
    ),
    hostname=st.one_of(st.none(), st.from_regex(r"[a-zA-Z0-9\-]{1,30}", fullmatch=True)),
    source=st.one_of(st.none(), st.from_regex(r"[a-z0-9]{2,10}", fullmatch=True)),
    timestamp=st.none(),  # datetime serialisation tested in test_file.py
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


class TestOverflowCounter:
    """Unit tests for _overflow_counters and _record_job_emitted."""

    def test_record_job_emitted_plain_id(self) -> None:
        """_record_job_emitted bumps counter for a plain (non-suffixed) job ID."""
        group = JobGroup(job_name="g", config=None)
        group._record_job_emitted("bucket_0")
        assert group._overflow_counters["bucket_0"] == 1

    def test_record_job_emitted_suffixed_id(self) -> None:
        """_record_job_emitted strips _overflow_N suffix, bumps base counter."""
        group = JobGroup(job_name="g", config=None)
        group._record_job_emitted("bucket_0_overflow_5")
        assert group._overflow_counters["bucket_0"] == 1
        assert "bucket_0_overflow_5" not in group._overflow_counters

    def test_record_job_emitted_monotonic(self) -> None:
        """Multiple calls produce monotonically increasing counter."""
        group = JobGroup(job_name="g", config=None)
        group._record_job_emitted("bucket_0")
        group._record_job_emitted("bucket_0")
        group._record_job_emitted("bucket_0_overflow_3")
        assert group._overflow_counters["bucket_0"] == 3

    def test_add_file_else_branch_uses_suffixed_id_when_counter_gt_0(
        self,
    ) -> None:
        """add_file else-branch creates a suffixed job ID when counter > 0.

        Uses a minimal JobGroup subclass that makes every file relevant
        and maps it to a fixed bucket ID.
        """

        class _TestGroup(JobGroup):
            def file_is_relevant(self, _file: File | FrozenFile) -> bool:
                return True

            def get_job_ids_from_file(
                self, _file: File | FrozenFile,
            ) -> list[str]:
                return ["proto"]

        group = _TestGroup(job_name="g", config=None)
        # Simulate a previous job for 'proto' having been emitted
        group._overflow_counters["proto"] = 3

        f = FrozenFile(file=Path("/data/a.nc"))
        added = group.add_file(f)
        assert added is True
        assert "proto_overflow_3" in group.jobs
        assert "proto" not in group.jobs

    def test_add_file_else_branch_plain_id_when_counter_0(self) -> None:
        """add_file else-branch uses plain job ID when counter == 0."""

        class _TestGroup(JobGroup):
            def file_is_relevant(self, _file: File | FrozenFile) -> bool:
                return True

            def get_job_ids_from_file(
                self, _file: File | FrozenFile,
            ) -> list[str]:
                return ["proto"]

        group = _TestGroup(job_name="g", config=None)
        assert group._overflow_counters.get("proto", 0) == 0

        f = FrozenFile(file=Path("/data/a.nc"))
        added = group.add_file(f)
        assert added is True
        assert "proto" in group.jobs

    def test_overflow_separator_constant_exists(self) -> None:
        """_OVERFLOW_SEPARATOR is a module-level constant with expected value."""
        assert _OVERFLOW_SEPARATOR == "_overflow_"
