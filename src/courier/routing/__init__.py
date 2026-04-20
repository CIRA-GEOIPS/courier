"""Dispatcher target resolution.

A :class:`TargetResolver` maps operator-facing dispatcher identifiers
(the ``spec.run[*].identifier`` values) to the physical broker queue
names jobs are published to. The default :class:`IdentityTargetResolver`
returns ``job_ready_queue_for(identifier)``; swapping in a different
implementation lets future deployments alias identifiers, fan out to
multiple physical clusters, or shadow traffic without touching any
builder or dispatcher code.
"""

from courier.routing.resolver import (
    IdentityTargetResolver,
    TargetResolver,
    build_default_resolver,
)

__all__ = [
    "IdentityTargetResolver",
    "TargetResolver",
    "build_default_resolver",
]
