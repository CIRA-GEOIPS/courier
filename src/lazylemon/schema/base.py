"""Shared base model and helpers for lazylemon schema validation."""

from __future__ import annotations

from collections import Counter
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from collections.abc import Iterable

__all__ = ["FrozenModel"]


def _ensure_non_empty(value: str | None, *, field_name: str | None) -> str:
    """Guarantee that a string value is non-empty after trimming whitespace."""
    if not isinstance(value, str):
        raise TypeError(f"Field '{field_name}' must be a string.")
    stripped = value.strip()
    if not stripped:
        raise ValueError(f"Field '{field_name}' must be a non-empty string.")
    return stripped


def _find_duplicate_values(values: Iterable[str]) -> set[str]:
    """Return the set of duplicated values within an iterable."""
    counts = Counter(values)
    return {value for value, count in counts.items() if count > 1}


class FrozenModel(BaseModel):
    """Base model enforcing immutability and strict field handling."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        populate_by_name=True,
        str_strip_whitespace=True,
    )
