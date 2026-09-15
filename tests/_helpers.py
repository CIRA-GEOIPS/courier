"""Shared waiting helpers for tests that observe asynchronous behaviour.

These were copied verbatim into three integration modules before the container
tier needed them a fourth time.  Prefer them over a fixed sleep: a sleep long
enough to be reliable on a loaded CI box wastes time on every other run, and a
sleep short enough to be quick is flaky.
"""

from __future__ import annotations

import time
from collections.abc import Callable

__all__ = ["poll_until", "stays_false"]


def poll_until(
    predicate: Callable[[], bool],
    timeout: float = 30.0,
    interval: float = 0.2,
) -> bool:
    """Block until *predicate* is true or *timeout* elapses.

    Parameters
    ----------
    predicate : Callable[[], bool]
        Condition re-evaluated every *interval* seconds.
    timeout : float, optional
        Seconds to wait before giving up.  Default 30.
    interval : float, optional
        Seconds between evaluations.  Default 0.2.

    Returns
    -------
    bool
        ``True`` when the condition was met within *timeout*.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def stays_false(
    predicate: Callable[[], bool],
    window: float = 3.0,
    interval: float = 0.2,
) -> bool:
    """Return ``True`` if *predicate* stays false for the whole *window*.

    For asserting a negative -- that something does *not* happen -- where a
    settle period genuinely is required.  Bails out on the first violation so
    a real failure is reported immediately rather than after the full window.

    Parameters
    ----------
    predicate : Callable[[], bool]
        Condition that must remain false.
    window : float, optional
        Seconds to keep checking.  Default 3.
    interval : float, optional
        Seconds between evaluations.  Default 0.2.

    Returns
    -------
    bool
        ``True`` if *predicate* never became true during *window*.
    """
    deadline = time.monotonic() + window
    while time.monotonic() < deadline:
        if predicate():
            return False
        time.sleep(interval)
    return True
