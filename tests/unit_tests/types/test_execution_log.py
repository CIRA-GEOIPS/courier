"""Unit tests and property-based round-trip tests for ExecutionLog."""

import json

import pytest
from hypothesis import given
from hypothesis import strategies as st

from courier.types.execution_log import ExecutionLog


# ─── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def minimal_log() -> ExecutionLog:
    """ExecutionLog with all fields None."""
    return ExecutionLog()


@pytest.fixture
def full_log() -> ExecutionLog:
    """Fully populated ExecutionLog."""
    return ExecutionLog(
        return_code=0,
        stdout="all good",
        stderr="",
        hostname="worker-1",
    )


# ─── Basic construction ───────────────────────────────────────────────────────


def test_defaults_are_none(minimal_log: ExecutionLog) -> None:
    """All fields default to None."""
    assert minimal_log.return_code is None
    assert minimal_log.stdout is None
    assert minimal_log.stderr is None
    assert minimal_log.hostname is None


def test_full_construction(full_log: ExecutionLog) -> None:
    """Fields are stored as provided."""
    assert full_log.return_code == 0
    assert full_log.stdout == "all good"
    assert full_log.stderr == ""
    assert full_log.hostname == "worker-1"


def test_frozen(full_log: ExecutionLog) -> None:
    """ExecutionLog is immutable (frozen=True)."""
    with pytest.raises(Exception):
        full_log.return_code = 1  # type: ignore[misc]


# ─── Serialization ────────────────────────────────────────────────────────────


def test_str_produces_valid_json(full_log: ExecutionLog) -> None:
    """__str__ produces parseable JSON."""
    data = json.loads(str(full_log))
    assert data["return_code"] == 0
    assert data["stdout"] == "all good"
    assert data["stderr"] == ""
    assert data["hostname"] == "worker-1"


def test_round_trip_full(full_log: ExecutionLog) -> None:
    """from_string(str(log)) == log for fully populated instance."""
    assert ExecutionLog.from_string(str(full_log)) == full_log


def test_round_trip_minimal(minimal_log: ExecutionLog) -> None:
    """from_string(str(log)) == log for minimal (all-None) instance."""
    assert ExecutionLog.from_string(str(minimal_log)) == minimal_log


def test_round_trip_failure() -> None:
    """Non-zero return code survives the round-trip."""
    log = ExecutionLog(return_code=1, stderr="script failed", hostname="node-42")
    assert ExecutionLog.from_string(str(log)) == log


# ─── Property-based round-trip ────────────────────────────────────────────────

_text = st.one_of(st.none(), st.text(max_size=200))
_return_code = st.one_of(st.none(), st.integers(min_value=-255, max_value=255))


@given(
    return_code=_return_code,
    stdout=_text,
    stderr=_text,
    hostname=st.one_of(st.none(), st.from_regex(r"[a-zA-Z0-9\-\.]{1,63}", fullmatch=True)),
)
def test_hypothesis_round_trip(
    return_code: int | None,
    stdout: str | None,
    stderr: str | None,
    hostname: str | None,
) -> None:
    """Property: ExecutionLog.from_string(str(log)) == log for all valid inputs."""
    log = ExecutionLog(
        return_code=return_code,
        stdout=stdout,
        stderr=stderr,
        hostname=hostname,
    )
    assert ExecutionLog.from_string(str(log)) == log
