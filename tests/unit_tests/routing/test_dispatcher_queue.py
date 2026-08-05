"""Tests for per-identifier dispatcher queue wiring + dedupe LRU."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from courier.constants import FILE_FOUND_EXCHANGE
from courier.interfaces.dispatchers import Dispatcher, _DEDUPE_LRU_SIZE
from courier.types.file import File


def _make_dispatcher(mock_service: MagicMock, identifier: str = "runner-a") -> Dispatcher:
    """Construct a Dispatcher via its base class with an identifier."""
    return Dispatcher(mock_service, {}, identifier=identifier)


def test_missing_identifier_raises() -> None:
    """The base ``__init__`` refuses to construct without an identifier."""
    svc = MagicMock()
    svc.config = svc
    with pytest.raises(ValueError, match="requires an identifier"):
        Dispatcher(svc, {}, identifier=None)


def test_incoming_queue_is_per_identifier(mock_service: MagicMock) -> None:
    """The dispatcher consumes from its own ``JobReady-<id>`` queue."""
    disp = _make_dispatcher(mock_service, "runner-a")
    assert disp.incoming_queue == "JobReady-runner-a"


def test_dedupe_lru_hit_returns_true(mock_service: MagicMock) -> None:
    """Second occurrence of the same id hits the dedupe LRU."""
    disp = _make_dispatcher(mock_service)
    assert disp._recently_seen("job-1") is False
    assert disp._recently_seen("job-1") is True


def test_dedupe_lru_evicts_oldest(mock_service: MagicMock) -> None:
    """LRU stays bounded at _DEDUPE_LRU_SIZE entries."""
    disp = _make_dispatcher(mock_service)
    for i in range(_DEDUPE_LRU_SIZE + 5):
        disp._recently_seen(f"job-{i}")
    assert len(disp._seen_jobs) == _DEDUPE_LRU_SIZE
    # The earliest ids should have been evicted.
    assert "job-0" not in disp._seen_jobs
    assert f"job-{_DEDUPE_LRU_SIZE + 4}" in disp._seen_jobs


def test_dedupe_move_to_end_extends_residency(mock_service: MagicMock) -> None:
    """Recently-seen entries are refreshed on hit so they survive eviction."""
    disp = _make_dispatcher(mock_service)
    disp._recently_seen("keeper")
    for i in range(_DEDUPE_LRU_SIZE):
        disp._recently_seen(f"other-{i}")
        disp._recently_seen("keeper")  # refresh
    # "keeper" should still be present despite _DEDUPE_LRU_SIZE new ids.
    assert "keeper" in disp._seen_jobs


def test_emit_file_publishes_to_file_found_exchange(mock_service: MagicMock) -> None:
    """``emit_file()`` emits a File to FILE_FOUND_EXCHANGE."""
    disp = _make_dispatcher(mock_service)
    file = File(file=Path("/tmp/test.txt"), hostname="test")
    disp.emit_file(file)
    mock_service.emit.assert_called_once()
    call_args, call_kwargs = mock_service.emit.call_args
    assert call_kwargs["queue"] == FILE_FOUND_EXCHANGE
    roundtripped = File.from_string(call_kwargs["message"])
    assert roundtripped.file == Path("/tmp/test.txt")
    assert roundtripped.hostname == "test"
