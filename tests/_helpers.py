"""Shared waiting helpers for tests that observe asynchronous behaviour.

Prefer them to a fixed sleep. A sleep long enough to be reliable on a loaded CI
box wastes time on every run, and a shorter one is flaky.

:func:`payload_block` and :func:`with_payload` build the ``payload`` block every
job builder's config needs; the builder constructs its payload from it.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

__all__ = [
    "DEFAULT_PAYLOAD_ID",
    "DEFAULT_SCRIPT",
    "payload_block",
    "poll_until",
    "stays_false",
    "with_payload",
]

#: Identifier :func:`payload_block` gives the payload unless told otherwise.
DEFAULT_PAYLOAD_ID = "test-payload"
#: Script :func:`payload_block` gives the payload unless told otherwise.
DEFAULT_SCRIPT = "echo {{ files | length }}"


def payload_block(
    identifier: str = DEFAULT_PAYLOAD_ID,
    script: str = DEFAULT_SCRIPT,
    *,
    name: str = "bash_payload",
    kind: str = "payload",
    settings: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a valid nested ``payload`` block for a job builder's config.

    Parameters
    ----------
    identifier : str, optional
        The payload's identifier.
    script : str, optional
        The payload's inline script template.  Ignored when *settings* is
        given.
    name : str, optional
        Payload plugin name.  Default ``bash_payload``.
    kind : str, optional
        The block's ``kind``.  Default ``payload``.
    settings : Mapping[str, Any] or None, optional
        The payload plugin's whole ``config``, in place of
        ``{script: script}``: a template ``file``, or settings of a plugin
        other than ``bash_payload``.  An empty mapping is kept as given.

    Returns
    -------
    dict[str, Any]
        ``{identifier: {kind, name, config}}``, the singleton form a service
        YAML uses.
    """
    config = {"script": script} if settings is None else dict(settings)
    return {identifier: {"kind": kind, "name": name, "config": config}}


def with_payload(
    config: Mapping[str, Any] | None = None,
    identifier: str = DEFAULT_PAYLOAD_ID,
    script: str = DEFAULT_SCRIPT,
    *,
    name: str = "bash_payload",
    settings: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a copy of a builder *config* with a :func:`payload_block` added.

    The other arguments are :func:`payload_block`'s.
    """
    block = payload_block(identifier, script, name=name, settings=settings)
    return {**(config or {}), "payload": block}


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
