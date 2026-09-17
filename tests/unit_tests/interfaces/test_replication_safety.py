"""Replicating an accumulating builder without shared state is refused.

Replicas of one identifier are competing consumers of a single durable queue,
so each sees a different subset of a job's files. Without shared state to
reassemble them every job is emitted short -- indistinguishable from loss.

What is worth testing here is not the three-way decision, which is three lines,
but the two things around it that can actually be wrong: whether the error is
raised at all, and whether the peer count survives a service that does not
expose a broker manager.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from courier.errors import UnsafeReplicationError
from courier.interfaces.job_builders import JobBuilder
from courier.types.job import JobGroup


def _builder(service: MagicMock, *, files_per_job: int) -> JobBuilder:
    builder = JobBuilder(service, {"targets": ["dp-1"]}, identifier="jb-1")
    group = JobGroup("grp", MagicMock(files_per_job=files_per_job))
    builder.job_groups = [group]
    return builder


@pytest.fixture
def service() -> MagicMock:
    svc = MagicMock()
    svc.config = MagicMock(log_level="DEBUG", loki_enabled=False, namespace="ns")
    return svc


def _with_peers(service: MagicMock, peers: int) -> None:
    service._broker_manager.consumer_count.return_value = peers
    service._broker_manager.get_queue_name.side_effect = lambda n: f"ns-{n}"


def test_an_observed_peer_refuses_startup(service: MagicMock) -> None:
    """The one case that must hard-fail rather than quietly split jobs."""
    _with_peers(service, 1)
    builder = _builder(service, files_per_job=5)

    with pytest.raises(UnsafeReplicationError) as caught:
        builder._check_replication_safety()

    assert caught.value.identifier == "jb-1"
    assert caught.value.peers == 1
    assert "state_sync" in str(caught.value)


def test_no_peer_warns_but_starts(service: MagicMock) -> None:
    """A peer is only a snapshot; refusing on suspicion breaks one replica."""
    _with_peers(service, 0)
    builder = _builder(service, files_per_job=5)
    builder._logger = MagicMock()

    builder._check_replication_safety()

    builder._logger.warning.assert_called_once()


def test_shared_state_skips_the_broker_round_trip(service: MagicMock) -> None:
    """With state sync the answer is known, so do not ask the broker."""
    _with_peers(service, 3)
    builder = _builder(service, files_per_job=5)
    builder._sync = MagicMock()

    builder._check_replication_safety()

    service._broker_manager.consumer_count.assert_not_called()


def test_a_one_file_builder_skips_the_broker_round_trip(
    service: MagicMock,
) -> None:
    """One job per file means replicas cannot split anything."""
    _with_peers(service, 3)
    builder = _builder(service, files_per_job=1)

    builder._check_replication_safety()

    service._broker_manager.consumer_count.assert_not_called()


def test_a_service_without_a_broker_manager_counts_no_peers(
    service: MagicMock,
) -> None:
    """Unit harnesses build a JobBuilder with no broker behind it."""
    builder = _builder(service, files_per_job=5)
    del service._broker_manager
    builder._logger = MagicMock()

    builder._check_replication_safety()

    builder._logger.warning.assert_called_once()
