"""Shared base model and helpers for courier schema validation."""

from __future__ import annotations

import re
from collections import Counter
from typing import TYPE_CHECKING, Final

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from collections.abc import Iterable

__all__ = ["FrozenModel"]

_DNS_SUBDOMAIN_RE: Final = re.compile(
    r"^[a-z0-9]([a-z0-9\-]{0,61}[a-z0-9])?$",
)

_APIVERSION_RE: Final = re.compile(
    r"^[a-z0-9.-]+/v[0-9]+(alpha[0-9]+|beta[0-9]+)?$",
)


def _ensure_non_empty(value: str | None, *, field_name: str | None) -> str:
    """Guarantee that a string value is non-empty after trimming whitespace."""
    if not isinstance(value, str):
        raise TypeError(f"Field '{field_name}' must be a string.")
    stripped = value.strip()
    if not stripped:
        raise ValueError(f"Field '{field_name}' must be a non-empty string.")
    return stripped


def _ensure_dns_name(value: str, *, field_name: str | None) -> str:
    """Validate that a value is a Kubernetes-compatible DNS subdomain name.

    Parameters
    ----------
    value : str
        The name to validate.
    field_name : str | None
        Field name for error messages.

    Returns
    -------
    str
        The validated (stripped) name.

    Raises
    ------
    ValueError
        If the name does not match ``[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?``.
    """
    value = _ensure_non_empty(value, field_name=field_name)
    if not _DNS_SUBDOMAIN_RE.match(value):
        raise ValueError(
            f"Field '{field_name}' must be a lowercase DNS subdomain name "
            "(letters, digits, hyphens; max 63 chars; no leading/trailing hyphens).",
        )
    return value


def _ensure_api_version(value: str, *, field_name: str | None) -> str:
    """Validate that apiVersion follows the ``<group>/v<N>[alphaN|betaN]`` format.

    Parameters
    ----------
    value : str
        The apiVersion string to validate.
    field_name : str | None
        Field name for error messages.

    Returns
    -------
    str
        The validated (stripped) apiVersion.
    """
    value = _ensure_non_empty(value, field_name=field_name)
    if not _APIVERSION_RE.match(value):
        raise ValueError(
            f"Field '{field_name}' value '{value}' must follow "
            "'<group>/v<N>[alphaN|betaN]' format, e.g. 'runcourier.dev/v1alpha1'.",
        )
    return value


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
