"""Fixtures for the real-broker tier.

Set ``COURIER_TEST_AMQP_URL`` to run these, e.g.::

    docker compose -f tests/docker-compose.rabbitmq-testing.yaml up -d
    export COURIER_TEST_AMQP_URL='amqp://admin:admin_test@localhost:5672//'
    python -m pytest -m rabbitmq --no-cov

The marker is applied from :func:`pytest_collection_modifyitems` rather than a
module-level ``pytestmark`` here: pytest only honours ``pytestmark`` in a test
module or class body, so one in a conftest is silently ignored -- which would
leave this whole tier unmarked, selected by the default run, and failing
against a broker that is not there.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator

import kombu
import pytest

from courier.config import ServiceConfig
from courier.constants import dead_letter_queue_for

AMQP_URL_ENV = "COURIER_TEST_AMQP_URL"


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    """Mark every test in this package ``rabbitmq`` and skip without a broker.

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
        not os.environ.get(AMQP_URL_ENV),
        reason=f"set {AMQP_URL_ENV} to run the real-broker tier",
    )
    for item in items:
        if str(item.fspath).startswith(here):
            item.add_marker(pytest.mark.rabbitmq)
            item.add_marker(skip)


@pytest.fixture
def amqp_url() -> str:
    """Return the broker URL under test."""
    url = os.environ.get(AMQP_URL_ENV)
    if not url:
        pytest.skip(f"set {AMQP_URL_ENV} to run the real-broker tier")
    return url


@pytest.fixture
def namespace() -> str:
    """Return a namespace unique to one test, so runs cannot collide."""
    return f"rmq-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def amqp_config(amqp_url: str, namespace: str) -> ServiceConfig:
    """Return a service configuration pointed at the real broker."""
    return ServiceConfig(
        broker_url=amqp_url,
        prometheus_port=0,
        namespace=namespace,
        tracing_enabled=False,
        broker_max_retries=1,
    )


@pytest.fixture
def raw_conn(amqp_url: str, namespace: str) -> Iterator[kombu.Connection]:
    """Yield a bare connection and delete everything this test declared."""
    conn = kombu.Connection(amqp_url)
    conn.ensure_connection(max_retries=3)
    try:
        yield conn
    finally:
        with conn.channel() as channel:
            for suffix in ("FilesFound-jb", "JobReady-dp", "DispatcherQueue"):
                for name in (
                    f"{namespace}-{suffix}",
                    dead_letter_queue_for(f"{namespace}-{suffix}"),
                ):
                    try:
                        channel.queue_delete(name)
                    except Exception:  # noqa: BLE001 -- teardown is best effort
                        pass
            try:
                channel.exchange_delete(f"{namespace}-FilesFoundExchange")
            except Exception:  # noqa: BLE001 -- teardown is best effort
                pass
        conn.release()


def queue_depth(conn: kombu.Connection, name: str) -> int:
    """Return how many messages are ready on *name*.

    Parameters
    ----------
    conn : kombu.Connection
        An open connection.
    name : str
        Fully namespaced queue name.

    Returns
    -------
    int
        Ready message count.
    """
    with conn.channel() as channel:
        return kombu.Queue(name, channel=channel).queue_declare(passive=True)[1]
