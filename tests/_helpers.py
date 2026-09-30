"""Shared test helpers.

Waiting helpers for tests that observe asynchronous behaviour: prefer them to
a fixed sleep. A sleep long enough to be reliable on a loaded CI box wastes
time on every run, and a shorter one is flaky.

Payload helpers for tests that build a job builder: every job builder needs a
``payload`` block in its config, and a bound payload before it can start or
emit a job.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from collections.abc import Callable

    from courier.interfaces.job_builders import JobBuilder
    from courier.interfaces.payloads import Payload

__all__ = [
    "DEFAULT_PAYLOAD_ID",
    "DEFAULT_SCRIPT",
    "bind_payload",
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
) -> dict[str, Any]:
    """Return a valid nested ``payload`` block for a job builder's config.

    Parameters
    ----------
    identifier : str, optional
        The payload's identifier.
    script : str, optional
        The payload's inline script template.
    name : str, optional
        Payload plugin name.  Default ``bash_payload``.
    kind : str, optional
        The block's ``kind``.  Default ``payload``.

    Returns
    -------
    dict[str, Any]
        ``{identifier: {kind, name, config: {script}}}``, the singleton form a
        service YAML uses.
    """
    return {
        identifier: {"kind": kind, "name": name, "config": {"script": script}},
    }


def with_payload(
    config: dict[str, Any] | None = None,
    identifier: str = DEFAULT_PAYLOAD_ID,
    script: str = DEFAULT_SCRIPT,
) -> dict[str, Any]:
    """Return a copy of a builder *config* with a :func:`payload_block` added.

    Parameters
    ----------
    config : dict[str, Any] or None, optional
        The rest of the builder's config.
    identifier : str, optional
        The payload's identifier.
    script : str, optional
        The payload's inline script template.

    Returns
    -------
    dict[str, Any]
        *config* plus ``payload``.
    """
    return {**(config or {}), "payload": payload_block(identifier, script)}


def bind_payload(builder: JobBuilder, service: Any | None = None) -> Payload:
    """Construct the payload *builder*'s block describes and bind it.

    What service preflight does for a registered builder, for a test that
    drives a builder directly.

    Parameters
    ----------
    builder : JobBuilder
        The builder to bind.
    service : Any or None, optional
        Service the payload is constructed against (it reads only
        ``service.config``).  Defaults to the builder's own service.

    Returns
    -------
    Payload
        The bound payload, a real instance of the plugin the block names.
    """
    from courier.interfaces.payloads import payloads  # noqa: PLC0415

    block = builder.payload_block
    payload_cls = cast("type[Payload]", payloads.get_plugin(block.spec.name))
    payload = payload_cls(
        service if service is not None else builder.parent_service,
        block.spec.config,
        identifier=block.identifier,
    )
    builder.payload = payload
    return payload


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
