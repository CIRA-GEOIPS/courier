"""Contract: ``Job == Job.from_string(str(Job))`` for routing-specific fields.

Focuses on ``correlation_id``, ``emit_time``, and ``targets`` — the
three fields this ADR adds to :class:`Job`.
"""

from __future__ import annotations

import json
import re

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from courier.types.job import Job

pytestmark = pytest.mark.contract


_IDENT_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,62}")


@given(
    correlation_id=st.uuids().map(str),
    emit_time=st.one_of(st.none(), st.floats(min_value=0, max_value=1e10)),
    targets=st.lists(st.from_regex(_IDENT_RE, fullmatch=True), max_size=5),
)
@settings(max_examples=200)
def test_job_roundtrip_preserves_routing_fields(
    correlation_id: str,
    emit_time: float | None,
    targets: list[str],
) -> None:
    job = Job(
        name="test",
        identifier="job-001",
        config={"k": "v"},
        correlation_id=correlation_id,
        emit_time=emit_time,
        targets=tuple(targets),
    )
    payload = str(job)
    # Serializes to valid JSON.
    data = json.loads(payload)
    assert data["correlation_id"] == correlation_id
    assert data["emit_time"] == emit_time
    assert data["targets"] == list(targets)

    restored = Job.from_string(payload)
    assert restored.correlation_id == correlation_id
    assert restored.emit_time == emit_time
    assert restored.targets == tuple(targets)


def test_default_job_has_generated_correlation_id() -> None:
    job = Job(name="x", identifier="y", config={})
    assert job.correlation_id
    assert job.targets == ()
    assert job.emit_time is None


def test_roundtrip_handles_missing_optional_fields() -> None:
    minimal = json.dumps(
        {
            "name": "x",
            "identifier": "y",
            "config": {},
            "files": [],
            "last_modified": 0.0,
            "timeout": 60.0,
        },
    )
    restored = Job.from_string(minimal)
    assert restored.targets == ()
    assert restored.emit_time is None
    assert restored.correlation_id
