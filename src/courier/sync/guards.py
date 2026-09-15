"""Whether a job builder can safely run as more than one replica.

Replicas of one builder identifier are competing consumers of a single durable
queue, so each receives a different subset of the files belonging to a job. A
builder that accumulates files into a job therefore needs shared state to
reassemble them. Without it every job is split across replicas and emitted
short, which looks exactly like message loss.

The decision is a pure function of three observations so it can be tested
exhaustively without a broker, a Redis, or a plugin instance.
"""

from __future__ import annotations

from enum import Enum

__all__ = ["ReplicationVerdict", "replication_verdict"]


class ReplicationVerdict(Enum):
    """Outcome of the replication-safety check.

    Attributes
    ----------
    OK : str
        Safe to run.
    UNDETECTABLE : str
        Unsafe if replicated, but no peer was observed. The consumer count is
        only a snapshot, so this is re-checked rather than trusted.
    REFUSE : str
        Unsafe and a peer was observed. Starting would split jobs.
    """

    OK = "ok"
    UNDETECTABLE = "undetectable"
    REFUSE = "refuse"


def replication_verdict(
    *,
    accumulates: bool,
    shared_state: bool,
    observed_consumers: int,
) -> ReplicationVerdict:
    """Decide whether this builder may start.

    Parameters
    ----------
    accumulates : bool
        Whether any of the builder's groups gathers more than one file into a
        job. Derived from the group configuration rather than declared, so it
        cannot drift from what the builder actually does.
    shared_state : bool
        Whether state sync is configured, so replicas can reassemble a job.
    observed_consumers : int
        Consumers already attached to this builder's queue, excluding this
        one. Only ever a snapshot: a peer that has started but not yet bound
        is invisible.

    Returns
    -------
    ReplicationVerdict
        ``REFUSE`` only when a peer is actually observed, because refusing on
        suspicion would break the ordinary single-replica deployment.
    """
    if shared_state or not accumulates:
        return ReplicationVerdict.OK
    if observed_consumers > 0:
        return ReplicationVerdict.REFUSE
    return ReplicationVerdict.UNDETECTABLE
