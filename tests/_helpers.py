"""Shared waiting helpers for tests that observe asynchronous behaviour.

Prefer them to a fixed sleep. A sleep long enough to be reliable on a loaded CI
box wastes time on every run, and a shorter one is flaky.

:func:`payload_block` and :func:`with_payload` build the ``payload`` block every
job builder's config needs; the builder constructs its payload from it.  The
remaining helpers carry a payload to a dispatcher and observe what it does.
"""

from __future__ import annotations

import contextlib
import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from courier.plugins.dispatchers.local_dispatcher import LocalDispatcher
from courier.plugins.payloads.bash_payload import BashPayload
from courier.types.file import File
from courier.types.job import Job

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping

    from courier.interfaces.dispatchers import Dispatcher
    from courier.interfaces.payloads import Payload
    from courier.types.execution_log import ExecutionLog

__all__ = [
    "DEFAULT_PAYLOAD_ID",
    "DEFAULT_SCRIPT",
    "IN_TREE_BUILDER_SETTINGS",
    "captured_records",
    "consume",
    "file_job",
    "payload_block",
    "poll_until",
    "run_locally",
    "stays_false",
    "wire_job",
    "with_payload",
]

#: Identifier :func:`payload_block` gives the payload unless told otherwise.
DEFAULT_PAYLOAD_ID = "test-payload"
#: Script :func:`payload_block` gives the payload unless told otherwise.
DEFAULT_SCRIPT = "echo {{ files | length }}"
#: Every in-tree job builder, with the settings besides ``targets`` and
#: ``payload`` that it needs to emit one job per file.
IN_TREE_BUILDER_SETTINGS: dict[str, dict[str, Any]] = {
    "DummyJobBuilder": {},
    "file_count_builder": {"files_per_job": 1},
    "filter_and_group": {"files_per_job": 1},
    "metadata_router": {"routes": [{"name": "all", "files_per_job": 1}]},
}


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


def file_job(path: Path | str = "/d/a.nc", identifier: str = "job-1") -> Job:
    """Return a job holding the one file *path*."""
    return Job("n", identifier, {}, files=[File(file=Path(path)).freeze()])


def wire_job(
    service: Any,
    payload_config: dict[str, Any],
    *,
    payload_cls: type[Payload] = BashPayload,
    payload_identifier: str = "p1",
    job: Job | None = None,
) -> Job:
    """Return *job* (default :func:`file_job`) carrying a payload, off the wire."""
    job = job if job is not None else file_job()
    payload = payload_cls(service, payload_config, payload_identifier)
    job.payload = payload.to_job_spec(job)
    return Job.from_string(str(job))


def run_locally(
    service: Any,
    payload_config: dict[str, Any],
    dispatcher_config: dict[str, Any] | None = None,
    *,
    dispatcher_cls: type[LocalDispatcher] = LocalDispatcher,
    **wire: Any,
) -> list[ExecutionLog]:
    """Run a payload through a real ``LocalDispatcher``; see :func:`wire_job`."""
    dispatcher = dispatcher_cls(service, dispatcher_config or {}, identifier="ld")
    return dispatcher.get_execution_log(wire_job(service, payload_config, **wire))


def consume(dispatcher: Dispatcher, *jobs: Job) -> None:
    """Feed *jobs* through *dispatcher*'s consume loop, then let it stop."""

    def _consume(*_args: object, **_kwargs: object) -> Iterator[tuple[str, None]]:
        for job in jobs:
            yield str(job), None
        dispatcher._stop_event.set()  # noqa: SLF001

    dispatcher.parent_service.consume.side_effect = _consume
    dispatcher.handle_incoming_jobs()


@contextlib.contextmanager
def captured_records(logger_name: str) -> Iterator[list[logging.LogRecord]]:
    """Collect the records *logger_name* emits, whatever its current setup.

    Courier loggers do not propagate, so ``caplog`` cannot see them, and their
    level depends on whichever config configured them first.
    """
    records: list[logging.LogRecord] = []

    class _Collector(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    logger = logging.getLogger(logger_name)
    handler = _Collector(level=logging.DEBUG)
    previous_level, previous_disabled = logger.level, logger.disabled
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.disabled = False
    try:
        yield records
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
        logger.disabled = previous_disabled


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
