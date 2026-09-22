"""Shared waiting helpers for tests that observe asynchronous behaviour.

Prefer them to a fixed sleep. A sleep long enough to be reliable on a loaded CI
box wastes time on every run, and a shorter one is flaky.
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

    For asserting that something does not happen, where a settle period is
    required.  Returns as soon as *predicate* becomes true, so a failure is
    reported without waiting out the window.

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
