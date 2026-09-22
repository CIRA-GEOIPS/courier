"""Fixtures for the container tier.

Every test here drives a real container built from the repository Dockerfile.
The tier skips, with an actionable reason, when docker is unavailable or no
image was supplied.  Setting ``COURIER_TEST_DOCKER_REQUIRED`` turns each skip
into a failure, so a CI job cannot go green by collecting nothing.

:func:`pytest_collection_modifyitems` applies the ``docker`` marker.  pytest
honours ``pytestmark`` only in a test module or class body and ignores it in a
conftest without complaint, which would leave the tier unmarked and selected by
the default run.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import uuid
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from tests.docker._pipeline import Pipeline

#: Image under test, e.g. ``courier:dev``.  Built by the docker workflow, or
#: locally with ``docker build -t courier:dev .``.
IMAGE_ENV = "COURIER_TEST_IMAGE"
#: When set, every skip in this tier becomes a failure.  CI sets it.
REQUIRED_ENV = "COURIER_TEST_DOCKER_REQUIRED"

#: Broker image the pipeline tests run against.
RABBITMQ_IMAGE = "rabbitmq:4.1-management-alpine"
BROKER_USER = "admin"
BROKER_PASSWORD = "admin_test"  # noqa: S105 -- test broker, not a secret


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    """Mark every test in this package as ``docker``.

    Parameters
    ----------
    config : pytest.Config
        Active pytest configuration.  Unused.
    items : list[pytest.Item]
        Collected items, mutated in place.
    """
    del config
    here = str(__file__.rsplit("/", 1)[0])
    for item in items:
        if str(item.fspath).startswith(here):
            item.add_marker(pytest.mark.docker)


def _unavailable(reason: str) -> None:
    """Skip, or fail when the tier is declared required.

    Parameters
    ----------
    reason : str
        Operator-actionable explanation.

    Raises
    ------
    Failed
        When ``COURIER_TEST_DOCKER_REQUIRED`` is set.
    """
    if os.environ.get(REQUIRED_ENV):
        pytest.fail(f"{reason} (and {REQUIRED_ENV} is set)")
    pytest.skip(reason)


def run(args: Sequence[str], timeout: float = 120.0) -> subprocess.CompletedProcess:
    """Run a command and return the completed process without raising.

    Parameters
    ----------
    args : Sequence[str]
        Argument vector.
    timeout : float, optional
        Seconds before the command is killed.  Default 120.

    Returns
    -------
    subprocess.CompletedProcess
        Completed process with captured text output.
    """
    return subprocess.run(  # noqa: S603 -- fixed argv, no shell
        list(args),
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


@pytest.fixture(scope="session")
def docker_image() -> str:
    """Return the image under test, skipping the tier when unusable.

    Returns
    -------
    str
        Image reference such as ``courier:dev``.
    """
    if shutil.which("docker") is None:
        _unavailable("docker is not on PATH; install docker to run this tier")
    image = os.environ.get(IMAGE_ENV)
    if not image:
        _unavailable(
            f"{IMAGE_ENV} is unset; build one with "
            f"'docker build -t courier:dev .' and export {IMAGE_ENV}=courier:dev",
        )
    if run(["docker", "info"]).returncode != 0:
        _unavailable("the docker daemon is not reachable; start docker")
    if run(["docker", "image", "inspect", image]).returncode != 0:
        _unavailable(f"image {image!r} is not present locally; build or pull it")
    return image


@pytest.fixture(scope="session")
def broker_image(docker_image: str) -> str:
    """Pull the broker image once, before any test needs it.

    Pulling inside a test races its own readiness deadline: a first pull of a
    few hundred megabytes can outlast the wait, and the failure then reads as a
    broker that never started.

    Returns
    -------
    str
        The broker image reference.
    """
    del docker_image  # ordering only: proves docker is usable first
    pulled = run(["docker", "pull", RABBITMQ_IMAGE], timeout=900.0)
    if pulled.returncode != 0:
        _unavailable(f"could not pull {RABBITMQ_IMAGE}: {pulled.stderr.strip()}")
    return RABBITMQ_IMAGE


@pytest.fixture
def docker_network() -> Iterator[str]:
    """Create a user-defined network and remove it afterwards.

    The containers publish no host ports and reach the broker by container
    DNS, so this tier cannot collide with a broker already bound to 5672.

    Yields
    ------
    str
        Network name.
    """
    name = f"courier-test-{uuid.uuid4().hex[:8]}"
    created = run(["docker", "network", "create", name])
    if created.returncode != 0:
        _unavailable(f"could not create a docker network: {created.stderr.strip()}")
    try:
        yield name
    finally:
        run(["docker", "network", "rm", name])


@pytest.fixture
def docker_volume() -> Iterator[str]:
    """Create a named volume for pipeline data and remove it afterwards.

    The volume is named because inotify over a host bind mount is unreliable on
    Docker Desktop, and the filesystem monitor is inotify only.

    Yields
    ------
    str
        Volume name.
    """
    name = f"courier-data-{uuid.uuid4().hex[:8]}"
    created = run(["docker", "volume", "create", name])
    if created.returncode != 0:
        _unavailable(f"could not create a docker volume: {created.stderr.strip()}")
    try:
        yield name
    finally:
        run(["docker", "volume", "rm", "-f", name])


@pytest.fixture
def pipeline(
    docker_image: str,
    docker_network: str,
    docker_volume: str,
    tmp_path: Path,
    broker_image: str,
) -> Iterator[Pipeline]:
    """Provide a pipeline helper and tear its containers down.

    The import is deferred: ``tests.docker._pipeline`` imports this module for
    the broker credentials and ``run``.

    Yields
    ------
    Pipeline
        Helper with the broker already running.
    """
    from tests.docker._pipeline import Pipeline

    helper = Pipeline(
        docker_image, docker_network, docker_volume, tmp_path, broker_image,
    )
    try:
        helper.start_broker()
        yield helper
    finally:
        helper.cleanup()


def container_logs(container: str) -> str:
    """Return a container's combined output, for failure messages.

    Parameters
    ----------
    container : str
        Container name or id.

    Returns
    -------
    str
        Captured stdout and stderr.
    """
    result = run(["docker", "logs", container])
    return f"{result.stdout}\n{result.stderr}"


def image_labels(image: str) -> dict[str, str]:
    """Return the image's OCI labels.

    Parameters
    ----------
    image : str
        Image reference.

    Returns
    -------
    dict[str, str]
        Label mapping, empty when the image declares none.
    """
    result = run(["docker", "image", "inspect", "-f", "{{json .Config.Labels}}", image])
    if result.returncode != 0:
        return {}
    return json.loads(result.stdout.strip() or "null") or {}
