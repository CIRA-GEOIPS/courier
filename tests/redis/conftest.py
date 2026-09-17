"""Fixtures for the live-Redis tier.

Set ``COURIER_TEST_REDIS_URL`` to run these, e.g.::

    docker run -d --name courier-redis -p 6379:6379 redis:7-alpine
    export COURIER_TEST_REDIS_URL=redis://localhost:6379/0
    python -m pytest -m redis --no-cov

The marker is applied from a collection hook, as in the broker tier. pytest
ignores ``pytestmark`` in a conftest, which would leave the tier unmarked and
selected by the default run.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from urllib.parse import urlparse

import pytest

REDIS_URL_ENV = "COURIER_TEST_REDIS_URL"


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    """Mark every test here ``redis`` and skip it without a server.

    Parameters
    ----------
    config : pytest.Config
        Active configuration.  Unused.
    items : list[pytest.Item]
        Collected items, mutated in place.
    """
    del config
    here = str(__file__.rsplit("/", 1)[0])
    skip = pytest.mark.skipif(
        not os.environ.get(REDIS_URL_ENV),
        reason=f"set {REDIS_URL_ENV} to run the live-Redis tier",
    )
    for item in items:
        if str(item.fspath).startswith(here):
            item.add_marker(pytest.mark.redis)
            item.add_marker(skip)


@pytest.fixture
def redis_config(request: pytest.FixtureRequest):
    """Return a state-sync configuration pointed at the live server."""
    from courier.schema.v1alpha1.sync_config import RedisStateSyncConfig

    url = os.environ.get(REDIS_URL_ENV)
    if not url:
        pytest.skip(f"set {REDIS_URL_ENV} to run the live-Redis tier")
    parsed = urlparse(url)
    del request
    return RedisStateSyncConfig(
        host=parsed.hostname or "localhost",
        port=parsed.port or 6379,
        db=int((parsed.path or "/0").lstrip("/") or 0),
        channel_prefix=f"ct-{uuid.uuid4().hex[:8]}",
    )


@pytest.fixture
def namespace() -> Iterator[str]:
    """Return a namespace unique to one test."""
    yield f"rd-{uuid.uuid4().hex[:8]}"
