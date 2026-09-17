"""A reaped job must leave the shared hash as well as the local group.

Both timeout reapers used to hand-roll ``_claim_ready_jobs`` inline and omit
``_push_deletions``, so with state sync a reaped job's Redis field outlived it
by ``job.timeout`` -- 24 hours by default. ``JobGroup.adopt_job`` then made
that leak load-bearing: on a restart inside the window the stale field is
rehydrated and adopted as the bucket's *open* job, the next file is appended
under an identifier whose emit claim is still live, and the lost-claim path
discards both.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from courier.plugins.job_builders.filter_and_group import FilterAndGroupJobBuilder
from courier.plugins.job_builders.metadata_router import MetadataRouterBuilder
from courier.types.file import FrozenFile


@pytest.fixture
def service() -> MagicMock:
    svc = MagicMock()
    svc.config = MagicMock(log_level="DEBUG", loki_enabled=False, namespace="ns")
    svc.target_resolver.resolve.side_effect = lambda ident: f"JobReady-{ident}"
    return svc


def _file(name: str) -> FrozenFile:
    return FrozenFile(file=Path(f"/data/{name}.nc"), hostname="h")


_FILTER_CONFIG = {
    "targets": ["dp-1"],
    "files_per_job": 2,
    "min_files": 1,
    "window_timeout_seconds": 1,
}

_ROUTER_CONFIG = {
    "targets": ["dp-1"],
    "routes": [
        {
            "name": "grp",
            "match": {"file_patterns": [r".*\.nc"]},
            "targets": ["dp-1"],
            "files_per_job": 2,
            "window_timeout_seconds": 1,
        },
    ],
}


@pytest.mark.parametrize(
    ("builder_cls", "config"),
    [
        (FilterAndGroupJobBuilder, _FILTER_CONFIG),
        (MetadataRouterBuilder, _ROUTER_CONFIG),
    ],
    ids=["filter_and_group", "metadata_router"],
)
def test_reaping_an_incomplete_job_deletes_its_shared_field(
    service: MagicMock,
    builder_cls: type,
    config: dict,
) -> None:
    """The reaper is an emit path, so it owes the same deletion as the rest."""
    builder = builder_cls(service, config, identifier="jb-1")
    sync = MagicMock()
    sync.try_claim_emit.return_value = True
    builder._sync = sync
    group = builder.job_groups[0]

    builder._process_job_group(group, _file("only-one"))
    assert group.jobs, "one file of two should leave an open job behind"
    job_id = next(iter(group.jobs))
    # Backdate the job past its window so ready() takes the timeout path,
    # which is the whole point of the reaper.
    for job in group.jobs.values():
        job.last_modified -= 10

    builder._reap_group(group)

    service.emit.assert_called_once()
    sync.push_job_deletion.assert_any_call(group.name, job_id)
    assert group.jobs == {}
