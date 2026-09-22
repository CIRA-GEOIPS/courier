"""Functional programming utilities."""

import hashlib
import re
from collections.abc import Callable, Iterable
from typing import TypeVar

T = TypeVar("T")
R = TypeVar("R")

_UNSAFE_FILENAME_CHARS = re.compile(r"[^A-Za-z0-9._-]+")
_SLUG_MAX_LEN = 64


def slugify_for_filename(value: str, max_length: int = _SLUG_MAX_LEN) -> str:
    """Convert *value* into a single safe path segment.

    Job identifiers are operator- and template-controlled: the default
    ``JobGroup.get_job_ids_from_file`` returns ``str(file.file)``, an absolute
    path. Interpolating one straight into a log or script filename produces a
    path with embedded separators (so the write fails on a directory that does
    not exist) and lets ``..`` escape the configured output directory
    altogether.

    A truncated hash is appended whenever the input had to be altered, so two
    identifiers that differ only in stripped characters cannot collide.

    Parameters
    ----------
    value : str
        Arbitrary identifier text.
    max_length : int
        Maximum length of the slug body, before the hash suffix.

    Returns
    -------
    str
        A non-empty string containing only ``[A-Za-z0-9._-]`` and no path
        separators, leading dots, or parent-directory references.
    """
    cleaned = _UNSAFE_FILENAME_CHARS.sub("-", value).strip("-.")
    truncated = cleaned[:max_length]
    if truncated == value:
        return truncated
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:8]
    return f"{truncated}-{digest}" if truncated else digest


def filter_map(  # noqa: UP047
    predicate: Callable[[T], bool],
    transform: Callable[[T], R],
    items: Iterable[T],
) -> list[R]:
    """Filter and map in a single operation.

    Filters items using a predicate function and transforms matching
    items using a transform function in a single pass.

    Parameters
    ----------
    predicate : Callable[[T], bool]
        Function to test each item. Only items returning True are transformed.
    transform : Callable[[T], R]
        Function to transform filtered items.
    items : Iterable[T]
        Items to filter and transform.

    Returns
    -------
    list[R]
        List of transformed items that passed the predicate test.

    Examples
    --------
    >>> filter_map(lambda x: x % 2 == 0, lambda x: x * 2, [1, 2, 3, 4])
    [4, 8]
    """
    return [transform(item) for item in items if predicate(item)]
