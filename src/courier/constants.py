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

    FILE_FOUND = "FilesFoundExchange"
    DISPATCHER = "DispatcherQueue"


FILE_FOUND_EXCHANGE: str = QueueName.FILE_FOUND
DISPATCHER_QUEUE: str = QueueName.DISPATCHER

#: Prefix for per-dispatcher job-ready queues. Full queue names are
#: ``JobReady-<dispatcher_identifier>``; :class:`MessageBrokerManager`
#: namespaces them further with ``<namespace>-``.
JOB_READY_PREFIX = "JobReady"

#: Prefix for per-job-builder file-found queues. Full queue names are
#: ``FilesFound-<builder_identifier>``, namespaced further by
#: :class:`MessageBrokerManager` to ``<namespace>-FilesFound-<identifier>``.
#:
#: Every replica of one builder identifier shares this queue, so they are
#: competing consumers rather than each receiving a copy. The name deliberately
#: omits ``Exchange`` so it can never collide with the exclusive
#: ``<namespace>-FilesFoundExchange-fanout-<uuid>`` queues used before the
#: queue was made durable.
FILE_FOUND_QUEUE_PREFIX = "FilesFound"

#: Suffix appended to a namespaced queue name to build its dead-letter queue.
#: Messages a consumer could not get past are parked there rather than being
#: requeued forever (see :func:`dead_letter_queue_for`).
DEAD_LETTER_SUFFIX = "DeadLetter"

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
        Dispatcher or job-builder identifier from the YAML
        ``spec.run[*].identifier`` field. Both become queue names, so both
        are held to the same rules.

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


def file_found_queue_for(builder_identifier: str) -> str:
    """Return the durable file-found queue name a job builder consumes from.

    The returned string is the *base* queue name without any service
    namespace — :meth:`MessageBrokerManager.get_queue_name` prefixes it.

    Parameters
    ----------
    builder_identifier : str
        The job builder's ``spec.run[*].identifier`` value.

    Returns
    -------
    str
        ``FilesFound-<builder_identifier>``.

    Raises
    ------
    InvalidIdentifierError
        If *builder_identifier* fails :func:`validate_dispatcher_identifier`.
    """
    validate_dispatcher_identifier(builder_identifier)
    return f"{FILE_FOUND_QUEUE_PREFIX}-{builder_identifier}"


def dead_letter_queue_for(queue_name: str) -> str:
    """Return the dead-letter queue name that parks *queue_name*'s poison.

    Courier dead-letters by publishing, not by setting ``x-dead-letter-exchange``
    on the source queue: that argument cannot be added to a durable queue an
    existing deployment already declared without the broker answering 406 on
    every subsequent declare. A separate queue is a name no deployment has
    declared yet, so it can be introduced without a migration.

    Parameters
    ----------
    queue_name : str
        A queue name that is *already namespaced*, unlike the base names
        returned by :func:`job_ready_queue_for` and
        :func:`file_found_queue_for`. The dead-letter queue shares its
        source's namespace by construction rather than being namespaced again.

    Returns
    -------
    str
        ``<queue_name>-DeadLetter``.

    Raises
    ------
    InvalidIdentifierError
        If the result exceeds :data:`MAX_QUEUE_NAME_LENGTH`. Raised when the
        consumer subscribes rather than when a message is parked, so a name
        that is too long is a startup failure and never a poison message with
        nowhere to go.

    Notes
    -----
    A builder or dispatcher identifier ending in ``-DeadLetter`` would collide
    with another's dead-letter queue. Identifiers permit ``-``, so this is
    possible; forbidding it would invalidate configs that are legal today, and
    the name is unlikely enough to be left as a documented sharp edge.
    """
    full = f"{queue_name}-{DEAD_LETTER_SUFFIX}"
    if len(full) > MAX_QUEUE_NAME_LENGTH:
        raise InvalidIdentifierError(
            full,
            f"dead-letter queue name exceeds {MAX_QUEUE_NAME_LENGTH} "
            "characters; shorten the namespace or the identifier",
        )
    return full


def namespaced_queue_name(namespace: str, base_name: str) -> str:
    """Return ``<namespace>-<base_name>``, rejecting oversized results.

    The broker enforces the limit on the *namespaced* name, so checking the
    base name alone lets an over-long namespace through to a broker error at
    publish time. Both the runtime and ``courier queues`` build names through
    this helper so they cannot disagree about what is too long.

    Parameters
    ----------
    namespace : str
        Service namespace.
    base_name : str
        Queue name without the namespace prefix.

    Returns
    -------
    str
        The namespaced queue name.

    Raises
    ------
    InvalidIdentifierError
        If the namespaced name exceeds :data:`MAX_QUEUE_NAME_LENGTH`.
    """
    full = f"{namespace}-{base_name}"
    if len(full) > MAX_QUEUE_NAME_LENGTH:
        raise InvalidIdentifierError(
            full,
            f"namespaced queue name exceeds {MAX_QUEUE_NAME_LENGTH} characters; "
            "shorten the namespace or the identifier",
        )
    return full


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
