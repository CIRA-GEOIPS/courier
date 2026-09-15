"""Whether a job builder may run as more than one replica.

Replicas of one builder identifier are competing consumers of a single queue,
so each receives a different subset of a job's files. A builder that gathers
files into a job therefore needs shared state to reassemble them; without it
every job is emitted short, which is indistinguishable from losing files.

The decision is a pure function, so every combination is covered here without a
broker, a Redis, or a plugin instance.
"""

from __future__ import annotations

import pytest

from courier.sync.guards import ReplicationVerdict, replication_verdict


@pytest.mark.parametrize("peers", [0, 1, 5])
@pytest.mark.parametrize("accumulates", [False, True])
def test_shared_state_is_always_safe(accumulates: bool, peers: int) -> None:
    """With shared state the replicas can reassemble a job however they split."""
    verdict = replication_verdict(
        accumulates=accumulates,
        shared_state=True,
        observed_consumers=peers,
    )

    assert verdict is ReplicationVerdict.OK


@pytest.mark.parametrize("peers", [0, 1, 5])
def test_a_per_file_builder_is_always_safe(peers: int) -> None:
    """A builder emitting one job per file has nothing to reassemble."""
    verdict = replication_verdict(
        accumulates=False,
        shared_state=False,
        observed_consumers=peers,
    )

    assert verdict is ReplicationVerdict.OK


def test_an_accumulating_builder_with_a_peer_is_refused() -> None:
    """A second replica of an accumulating builder is refused outright.

    Refused rather than warned about: this configuration splits every job, and
    a warning in a log nobody reads is how that reaches production.
    """
    verdict = replication_verdict(
        accumulates=True,
        shared_state=False,
        observed_consumers=1,
    )

    assert verdict is ReplicationVerdict.REFUSE


def test_an_accumulating_builder_alone_is_allowed_but_flagged() -> None:
    """One replica is a legitimate deployment, so it starts.

    The consumer count is only a snapshot: a peer that has started but not yet
    bound is invisible, which is why this is reported as undetectable rather
    than as safe.
    """
    verdict = replication_verdict(
        accumulates=True,
        shared_state=False,
        observed_consumers=0,
    )

    assert verdict is ReplicationVerdict.UNDETECTABLE
