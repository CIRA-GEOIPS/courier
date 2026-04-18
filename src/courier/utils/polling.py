"""Polling helpers shared by interval-based data monitor plugins."""

import threading


def interruptible_sleep(seconds: float, stop_event: threading.Event) -> bool:
    """Sleep for *seconds* while remaining responsive to a stop event.

    Parameters
    ----------
    seconds : float
        Total duration to wait, in seconds. Non-positive values return
        immediately (checking the event once).
    stop_event : threading.Event
        Event polled every second. When set, the sleep aborts early.

    Returns
    -------
    bool
        ``True`` if the stop event fired during the sleep (caller should
        exit), ``False`` if the full duration elapsed normally.
    """
    if seconds <= 0:
        return stop_event.is_set()
    remaining = seconds
    while remaining > 0:
        if stop_event.wait(timeout=min(1.0, remaining)):
            return True
        remaining -= 1.0
    return False
