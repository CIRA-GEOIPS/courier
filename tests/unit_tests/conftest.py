"""Fixtures shared by the payload and dispatcher unit tests."""

from __future__ import annotations

import tempfile
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def service() -> MagicMock:
    """Service stub for payload and dispatcher tests, with no broker."""
    svc = MagicMock()
    svc.config = MagicMock(log_level="DEBUG", loki_enabled=False, namespace="ns")
    svc._broker_manager._connection = None
    return svc


@pytest.fixture
def private_tmpdir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point ``TMPDIR`` (and ``tempfile``'s cached copy) at an empty directory."""
    directory = tmp_path / "tmpdir"
    directory.mkdir()
    monkeypatch.setenv("TMPDIR", str(directory))
    monkeypatch.setattr(tempfile, "tempdir", None)
    return directory
