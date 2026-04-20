"""Tests for dispatcher identifier validation."""

from __future__ import annotations

import re

import pytest
from hypothesis import given
from hypothesis import strategies as st

from courier.constants import (
    MAX_QUEUE_NAME_LENGTH,
    job_ready_queue_for,
    validate_dispatcher_identifier,
)
from courier.errors import InvalidIdentifierError


@pytest.mark.parametrize(
    "ident",
    ["a", "runner-gpu", "R", "0", "runner_1", "runner.v2", "abc-123_def.xyz"],
)
def test_valid_identifiers(ident: str) -> None:
    validate_dispatcher_identifier(ident)


@pytest.mark.parametrize(
    "ident",
    ["", "-a", ".a", "_a", "a b", "a/b", "a:b", "a!", "a@b", "a" * 64, "a" + "b" * 63],
)
def test_invalid_identifiers(ident: str) -> None:
    with pytest.raises(InvalidIdentifierError):
        validate_dispatcher_identifier(ident)


def test_nonstring_identifier_rejected() -> None:
    with pytest.raises(InvalidIdentifierError):
        validate_dispatcher_identifier(42)  # type: ignore[arg-type]


def test_job_ready_queue_for_round_trip() -> None:
    assert job_ready_queue_for("runner-gpu") == "JobReady-runner-gpu"


def test_job_ready_queue_respects_length_limit() -> None:
    ident = "a" * 63
    queue = job_ready_queue_for(ident)
    assert len(queue) <= MAX_QUEUE_NAME_LENGTH


@given(
    st.from_regex(re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,62}"), fullmatch=True),
)
def test_identifiers_matching_regex_are_always_valid(ident: str) -> None:
    validate_dispatcher_identifier(ident)
    assert job_ready_queue_for(ident).startswith("JobReady-")
