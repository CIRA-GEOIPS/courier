"""Implementation of the local_dispatcher dispatcher class."""

import re
from typing import ClassVar

from courier.interfaces.dispatchers import (
    Dispatcher,
    DispatcherConfig,
    ExecutionPayload,
)
from courier.interfaces.payloads import DispatcherGroupConfig, Payload
from courier.metrics import COURIER_CUSTOM_GAUGE
from courier.plugins.payloads.bash_payload import BashPayload
from courier.plugins.payloads.python_payload import PythonPayload
from courier.plugins.payloads.shell_payload import ShellPayload
from courier.types.execution_log import ExecutionLog
from courier.types.job import Job
from courier.utils.logging import get_logger

#: Module logger for the stdout-metric conduit, which is a module-level
#: function with no access to a plugin instance logger.
_logger = get_logger("module", "local_dispatcher", None)

#: Prefix of a ``COURIER_METRIC: <name> <value>`` stdout line.
_COURIER_METRIC_PREFIX = "COURIER_METRIC:"

#: Regex that extracts ``metric_name`` and ``value`` from a
#: ``COURIER_METRIC: <name> <value>`` stdout line. The value pattern is
#: deliberately permissive; :func:`_parse_courier_metric` validates it with
#: ``float()``.
_COURIER_METRIC_RE = re.compile(
    r"^COURIER_METRIC:\s+(?P<metric_name>\S+)\s+(?P<value>-?[\d.e+-]+)",
)

#: Characters of a malformed metric line echoed into the warning.
_METRIC_LINE_PREVIEW = 200


def _parse_courier_metric(line: str) -> tuple[str, float] | None:
    """Return the name and value of a ``COURIER_METRIC:`` *line*, if it has them."""
    match = _COURIER_METRIC_RE.match(line)
    if not match:
        return None
    try:
        return match.group("metric_name"), float(match.group("value"))
    except ValueError:
        # The value pattern admits strings float() rejects ("1.2.3", "5--").
        return None


def _ingest_courier_metrics(
    stdout: str,
    dispatcher_identifier: str,
) -> None:
    """Scan *stdout* for ``COURIER_METRIC:`` lines and update Prometheus.

    This is a general-purpose conduit: deployment bash scripts emit custom
    Prometheus gauge values by printing::

        COURIER_METRIC: <metric_name> <numeric_value>

    The dispatcher recognises the prefix after every job execution and pushes
    the value into ``courier_custom_gauge`` with labels
    ``dispatcher_identifier`` and ``metric_name``.  A line with the prefix
    but no name and numeric value is skipped with a WARNING, never raised:
    the payload has already run, and one bad metric line must not fail a job
    that succeeded and drop its execution log.
    """
    for line in stdout.splitlines():
        text = line.strip()
        if not text.startswith(_COURIER_METRIC_PREFIX):
            continue
        parsed = _parse_courier_metric(text)
        if parsed is None:
            _logger.warning(
                "Ignoring malformed COURIER_METRIC line %r: expected "
                "'COURIER_METRIC: <metric_name> <numeric_value>'",
                text[:_METRIC_LINE_PREVIEW],
            )
            continue
        metric_name, value = parsed
        COURIER_CUSTOM_GAUGE.labels(
            dispatcher_identifier=dispatcher_identifier,
            metric_name=metric_name,
        ).set(value)


# config class for courier init discovery
class LocalDispatcherConfig(DispatcherConfig):  # noqa: D101
    pass


class LocalDispatcher(Dispatcher):
    """Dispatcher that executes payloads on the local host.

    Execution itself is the base :class:`~courier.interfaces.dispatchers.Dispatcher`
    generic path; this class declares which payload representations a local
    machine can run and ingests the ``COURIER_METRIC:`` stdout conduit.
    """

    interface: ClassVar[str] = "dispatchers"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "local_dispatcher"
    version: ClassVar[str] = "-1"

    representations: ClassVar[list[type[Payload]]] = [
        ShellPayload,
        BashPayload,
        PythonPayload,
    ]
    config_class: ClassVar[type[DispatcherGroupConfig]] = LocalDispatcherConfig

    def _execute_job(
        self,
        job: Job,
        payload: Payload,
        env: ExecutionPayload,
    ) -> list[ExecutionLog]:
        logs = super()._execute_job(job, payload, env)
        for log in logs:
            if log.stdout:
                _ingest_courier_metrics(log.stdout, self.identifier)
        return logs
