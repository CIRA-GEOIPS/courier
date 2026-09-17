"""The environment-dependent test tiers stay out of the default run.

Each tier (containers, a real broker, a real Redis) needs something the default
run does not have, so each is marked and deselected. The marking is done from a
``pytest_collection_modifyitems`` hook: pytest honours ``pytestmark`` only in a
test module or class body and silently ignores it in a conftest.

Both the marking and the deselection are asserted, because either one alone
fails open. A marker that is applied but not deselected runs the tier by
default; a marker that is deselected but never applied is a no-op.
"""

from __future__ import annotations

import importlib
import tomllib
from pathlib import Path
from typing import Any

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]

#: Marker name -> the conftest module that applies it.
_TIERS = {
    "docker": "tests.docker.conftest",
    "rabbitmq": "tests.rabbitmq.conftest",
    "redis": "tests.redis.conftest",
}


class _FakeItem:
    """Minimal stand-in for a collected item, recording what was applied."""

    def __init__(self, path: Path) -> None:
        self.fspath = path
        self.own_markers: list[Any] = []

    def add_marker(self, marker: Any) -> None:
        """Record *marker* the way pytest would attach it to the item."""
        self.own_markers.append(marker)


def _marker_names(item: _FakeItem) -> set[str]:
    """Return the names of every marker applied to *item*."""
    return {getattr(marker, "name", "") for marker in item.own_markers}


@pytest.fixture(scope="module")
def addopts() -> str:
    """Return the default command-line options from the project config."""
    config = tomllib.loads((_REPO_ROOT / "pyproject.toml").read_text())
    return str(config["tool"]["pytest"]["ini_options"]["addopts"])


@pytest.mark.parametrize("marker", sorted(_TIERS))
def test_the_tier_is_deselected_by_default(marker: str, addopts: str) -> None:
    """A plain ``pytest`` run must not select the tier."""
    assert f"not {marker}" in addopts


@pytest.mark.parametrize("marker", sorted(_TIERS))
def test_the_tier_is_a_registered_marker(marker: str, addopts: str) -> None:
    """Registered, so strict-marker runs and ``--markers`` both know it."""
    del addopts
    config = tomllib.loads((_REPO_ROOT / "pyproject.toml").read_text())
    registered = config["tool"]["pytest"]["ini_options"]["markers"]
    assert any(entry.split(":", 1)[0] == marker for entry in registered)


@pytest.mark.parametrize(("marker", "module_name"), sorted(_TIERS.items()))
def test_the_collection_hook_marks_its_own_tier(
    marker: str,
    module_name: str,
) -> None:
    """The hook applies the marker to a test file inside its package.

    Reverted check: replace the hook with a module-level ``pytestmark`` in the
    conftest. Nothing is applied and this fails.
    """
    module = importlib.import_module(module_name)
    tier_dir = Path(module.__file__ or "").parent
    item = _FakeItem(tier_dir / "test_something.py")

    module.pytest_collection_modifyitems(config=None, items=[item])

    assert marker in _marker_names(item)


@pytest.mark.parametrize(("marker", "module_name"), sorted(_TIERS.items()))
def test_the_collection_hook_leaves_other_tests_alone(
    marker: str,
    module_name: str,
) -> None:
    """The hook leaves items outside its own tier directory unmarked.

    A conftest hook sees every collected item, so each hook filters by path.
    Without the filter a tier marks the whole suite and a default run selects
    nothing.
    """
    module = importlib.import_module(module_name)
    item = _FakeItem(_REPO_ROOT / "tests" / "unit_tests" / "test_elsewhere.py")

    module.pytest_collection_modifyitems(config=None, items=[item])

    assert marker not in _marker_names(item)
