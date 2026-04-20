"""Target-to-queue resolution.

Even without a second concrete implementation, keeping the indirection
explicit avoids re-threading every builder later if operators need to
rewrite identifiers to physical queues (multi-cluster routing, shadow
traffic, blue/green).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from courier.constants import (
    MAX_QUEUE_NAME_LENGTH,
    job_ready_queue_for,
    validate_dispatcher_identifier,
)
from courier.errors import InvalidIdentifierError

if TYPE_CHECKING:
    from collections.abc import Iterable


class TargetResolver(Protocol):
    """Map dispatcher identifiers to broker queue names.

    Implementations: :class:`IdentityTargetResolver` (built-in, used in
    every current deployment). A second concrete implementation will be
    added when multi-cluster or shadow-traffic routing lands.
    """

    def known_identifiers(self) -> frozenset[str]:
        """Return the identifiers this resolver can route to."""

    def resolve(self, identifier: str) -> str:
        """Return the base queue name for *identifier*.

        Parameters
        ----------
        identifier : str
            A dispatcher identifier declared in the service config.

        Returns
        -------
        str
            The broker queue name (without namespace prefix).

        Raises
        ------
        InvalidIdentifierError
            If *identifier* is not known to this resolver.
        """


class IdentityTargetResolver:
    """Default resolver: ``identifier`` → ``JobReady-<identifier>``.

    Thread-safe: constructed once at :class:`courier.service.Service`
    startup from an immutable frozenset and never mutated.
    """

    def __init__(self, identifiers: Iterable[str]) -> None:
        """Validate each identifier and freeze the known set.

        Parameters
        ----------
        identifiers : Iterable[str]
            Dispatcher identifiers from the validated service config.

        Raises
        ------
        InvalidIdentifierError
            If any identifier is malformed or produces an oversized
            queue name once namespace-prefixed with the worst-case
            ``<namespace>-<queue>`` envelope.
        """
        seen: set[str] = set()
        for ident in identifiers:
            validate_dispatcher_identifier(ident)
            queue = job_ready_queue_for(ident)
            # Allow 64 chars of namespace padding ("<ns>-") for preflight.
            # Service performs the exact namespace-aware check separately.
            if len(queue) > MAX_QUEUE_NAME_LENGTH:
                raise InvalidIdentifierError(
                    ident,
                    f"queue name {queue!r} exceeds {MAX_QUEUE_NAME_LENGTH} chars",
                )
            seen.add(ident)
        self._identifiers: frozenset[str] = frozenset(seen)

    def known_identifiers(self) -> frozenset[str]:
        """Return the identifiers this resolver can route to."""
        return self._identifiers

    def resolve(self, identifier: str) -> str:
        """Return ``JobReady-<identifier>`` for a known identifier.

        Raises
        ------
        InvalidIdentifierError
            If *identifier* is not in :meth:`known_identifiers`.
        """
        if identifier not in self._identifiers:
            raise InvalidIdentifierError(
                identifier,
                f"not registered; known={sorted(self._identifiers)}",
            )
        return job_ready_queue_for(identifier)


def build_default_resolver(identifiers: Iterable[str]) -> TargetResolver:
    """Return the default identity resolver populated with *identifiers*.

    Parameters
    ----------
    identifiers : Iterable[str]
        Dispatcher identifiers from the validated service config.
    """
    return IdentityTargetResolver(identifiers)
