"""Tests for the IdentityTargetResolver."""

from __future__ import annotations

import pytest

from courier.errors import InvalidIdentifierError
from courier.routing import IdentityTargetResolver, build_default_resolver


def test_resolve_maps_identifier_to_job_ready_queue() -> None:
    resolver = build_default_resolver(["runner-a", "runner-b"])
    assert resolver.resolve("runner-a") == "JobReady-runner-a"
    assert resolver.resolve("runner-b") == "JobReady-runner-b"


def test_unknown_identifier_rejected() -> None:
    resolver = build_default_resolver(["only"])
    with pytest.raises(InvalidIdentifierError):
        resolver.resolve("other")


def test_malformed_identifier_rejected_at_construction() -> None:
    with pytest.raises(InvalidIdentifierError):
        IdentityTargetResolver(["bad identifier"])


def test_known_identifiers_is_frozen() -> None:
    resolver = build_default_resolver(["x", "y"])
    assert resolver.known_identifiers() == frozenset({"x", "y"})


def test_duplicate_identifiers_deduplicated() -> None:
    resolver = build_default_resolver(["x", "x", "y"])
    assert resolver.known_identifiers() == frozenset({"x", "y"})
