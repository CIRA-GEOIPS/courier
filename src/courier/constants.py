"""Typed constants, enums, and identifier helpers for courier."""

from __future__ import annotations

import re
from enum import Enum, StrEnum, auto

from courier.errors import InvalidIdentifierError


class QueueName(StrEnum):
    """Queue names used for inter-plugin messaging.

    Per-dispatcher job-ready queues are built via
    :func:`job_ready_queue_for` and are not members of this enum.
    """

    FILE_FOUND = "FilesFoundQueue"
    DISPATCHER = "DispatcherQueue"


FILE_FOUND_QUEUE: str = QueueName.FILE_FOUND
DISPATCHER_QUEUE: str = QueueName.DISPATCHER

#: Prefix for per-dispatcher job-ready queues. Full queue names are
#: ``JobReady-<dispatcher_identifier>``; :class:`MessageBrokerManager`
#: namespaces them further with ``<namespace>-``.
JOB_READY_PREFIX = "JobReady"

# RabbitMQ queue-name limit (AMQP 0-9-1 spec).
MAX_QUEUE_NAME_LENGTH = 255

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$")


def validate_dispatcher_identifier(identifier: str) -> None:
    """Reject identifiers that would produce unsafe or oversized queue names.

    Rules — mirror Kubernetes DNS-label conventions so operator muscle
    memory carries over:

    * must be 1-63 characters long;
    * must start with an alphanumeric character;
    * must contain only ``[A-Za-z0-9._-]``;
    * may not be empty.

    Parameters
    ----------
    identifier : str
        Dispatcher identifier from the YAML ``spec.run[*].identifier`` field.

    Raises
    ------
    InvalidIdentifierError
        If *identifier* violates any rule.
    """
    if not isinstance(identifier, str):
        raise InvalidIdentifierError(repr(identifier), "must be a string")
    if not identifier:
        raise InvalidIdentifierError(identifier, "must not be empty")
    if not _IDENTIFIER_RE.fullmatch(identifier):
        raise InvalidIdentifierError(
            identifier,
            f"must match {_IDENTIFIER_RE.pattern!r}",
        )


def job_ready_queue_for(dispatcher_identifier: str) -> str:
    """Return the job-ready queue name a dispatcher consumes from.

    The returned string is the *base* queue name without any service
    namespace — :class:`MessageBrokerManager.get_queue_name` is
    responsible for prefixing it with the service namespace.

    Parameters
    ----------
    dispatcher_identifier : str
        The dispatcher's ``spec.run[*].identifier`` value.

    Returns
    -------
    str
        ``JobReady-<dispatcher_identifier>``.

    Raises
    ------
    InvalidIdentifierError
        If *dispatcher_identifier* fails :func:`validate_dispatcher_identifier`.
    """
    validate_dispatcher_identifier(dispatcher_identifier)
    return f"{JOB_READY_PREFIX}-{dispatcher_identifier}"


class PluginRunState(Enum):
    """Enumeration of possible plugin states.

    Attributes
    ----------
    STOPPED : int
        Plugin is not running.
    STARTING : int
        Plugin is in the process of starting.
    RUNNING : int
        Plugin is running normally.
    STOPPING : int
        Plugin is in the process of stopping.
    FAILED : int
        Plugin has failed.
    RESTARTING : int
        Plugin is being restarted after failure.
    """

    STOPPED = auto()
    STARTING = auto()
    RUNNING = auto()
    STOPPING = auto()
    FAILED = auto()
    RESTARTING = auto()
