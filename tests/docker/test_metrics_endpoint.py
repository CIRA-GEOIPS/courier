"""The shipped container is scrapable from off the container, unconfigured.

Live metrics are read elsewhere in this repository, so "the exporter serves
bytes at all" is not what is missing. ``tests/docker/test_scaling_mid_flight``
scrapes a running container's endpoint through ``docker exec`` and
``127.0.0.1`` (:meth:`Pipeline.scrape_metrics`) and hangs real counting
assertions off the result; the unit tiers read values straight out of the
in-process ``REGISTRY``. What none of them can see is the **bind address**. A
listener on ``127.0.0.1`` answers a loopback scrape from inside its own
container exactly as a listener on ``0.0.0.0`` does, while being invisible to
every Prometheus in the world -- and ``courier dashboard --live``
(``src/courier/dashboard/live_detector.py``) fetches ``/metrics`` over the
network, so it is a real consumer of precisely that property.

The scrape here is therefore issued from a THIRD container on the same
user-defined network, addressing the courier container by its DNS name. No
host port is published; this tier deliberately publishes none so it cannot
collide with anything.

The port default is the other half. Every other container test that reads a
metric sets ``prometheus_port`` in its YAML; this one passes no
``extra_service_config`` at all, so what answers is the shipped default and
observability is proved to need no enabling.

Everything read from the scrape is a traffic-driven counter, taken after a
file has provably crossed the whole pipeline. ``courier_service_uptime_
seconds``, ``courier_service_health``, ``courier_broker_connected`` and
``courier_plugin_state`` are all populated by the heartbeat loop with nothing
flowing at all, so a test built on those would pass against a pipeline that
never moved a byte.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests._helpers import poll_until
from tests.docker._pipeline import build_config, sample_value
from tests.docker.conftest import container_logs

if TYPE_CHECKING:
    from tests.docker._pipeline import Pipeline

# pytest-timeout applies this PER TEST rather than to the module, and the gates
# below legitimately sum past the 300s default on a cold daemon: broker
# start-up 180s, two queue gates at 120s each, the endpoint gate at 120s,
# ``seed_until``'s 150s warm-up and a 120s poll for the dispatcher's counter. A
# budget under that sum reports a bare timeout instead of whichever gate gave
# up, and the gate's message is the one that says what broke.
pytestmark = pytest.mark.timeout(900)

#: The port the shipped image listens on when neither the configuration nor the
#: environment says anything.  ``ServiceConfig.prometheus_port`` defaults to
#: ``int(os.environ.get("PROMETHEUS_PORT", "8000"))``, so this constant is only
#: the effective default while the image sets no ``PROMETHEUS_PORT`` -- it
#: currently sets none, and adding one would have to be reflected here. It is
#: mirrored from ``src/courier/config.py`` rather than read back out of
#: ``ServiceConfig()`` on purpose: reading it from the code under test would
#: make the test assert only that the default equals itself.
DEFAULT_PROMETHEUS_PORT = 8000

#: Identifiers declared by :func:`build_config`, and the plugin names behind
#: them.  Both halves appear in metric labels.
MONITOR_ID = "watch-files"
MONITOR_NAME = "file_system_poller_watchdog"
BUILDER_ID = "create-jobs"
DISPATCHER_ID = "process-files"
DISPATCHER_NAME = "serial_bash"


def _files_seen(exposition: str) -> float | None:
    """Return the monitor's success counter, or ``None`` when never recorded.

    Parameters
    ----------
    exposition : str
        A single scrape's exposition text.

    Returns
    -------
    float or None
        Files the filesystem monitor has emitted successfully.
    """
    return sample_value(
        exposition,
        "courier_data_monitor_files_processed_total",
        {
            "monitor_name": MONITOR_NAME,
            "monitor_identifier": MONITOR_ID,
            "status": "success",
        },
    )


def _jobs_dispatched(exposition: str) -> float | None:
    """Return the dispatcher's success counter, or ``None`` when never recorded.

    Parameters
    ----------
    exposition : str
        A single scrape's exposition text.

    Returns
    -------
    float or None
        Jobs the bash dispatcher has run to completion.
    """
    return sample_value(
        exposition,
        "courier_dispatcher_jobs_processed_total",
        {
            "dispatcher_name": DISPATCHER_NAME,
            "dispatcher_identifier": DISPATCHER_ID,
            "status": "success",
        },
    )


def test_a_running_container_serves_courier_metrics_to_a_scraper_on_the_network(
    pipeline: Pipeline,
) -> None:
    """The shipped image is scrapable from off the container, unconfigured.

    No ``extra_service_config`` is passed: the point is that observability
    needs no enabling, so the configuration must not mention the port and the
    assertion is against the shipped default. Every number read comes from one
    scrape, taken after a file has provably reached ``/data/out``.

    Reverted check: pass ``addr="127.0.0.1"`` to the
    ``prometheus_client.start_http_server`` call in ``PrometheusManager.start``
    (``src/courier/managers/prometheus_manager.py``). Nothing else in the
    repository moves -- the endpoint still answers
    :meth:`Pipeline.scrape_metrics`, the ``docker exec`` + loopback scrape
    ``tests/docker/test_scaling_mid_flight`` uses, exactly as before -- and
    this test fails at ``await_metrics_endpoint``, because its request comes
    from a different container over the network. That revert is the module's
    reason to exist.

    A blunter revert, deleting that ``start_http_server`` call outright, also
    fails here, but this test is not what catches it: ``test_scaling_mid_
    flight`` then scrapes an empty body too, and fails first, at its
    per-replica receipt assertion ("replica A served no files-received
    sample").

    The queue name pinned on the received counter is this pipeline's own
    namespaced per-builder queue, which makes that assertion the issue-44
    reading as well: a builder consuming from a server-named exclusive queue
    would record its receipts under that name instead, and the ``await_queue``
    gate below cannot see the difference, because the durable queue is
    predeclared during preflight whether or not anything ever binds to it.
    """
    config = build_config(pipeline.namespace, pipeline.broker)
    container = pipeline.start_courier("metrics", config)

    files_found = f"{pipeline.namespace}-FilesFound-{BUILDER_ID}"

    # Broker topology first, and by exact name. A config whose broker block is
    # wrong falls back to the in-memory transport in silence, and every metric
    # below would still be exported by that service -- so this gate, which can
    # only pass over real AMQP, is what makes the scrape mean anything.
    pipeline.await_queue(f"{pipeline.namespace}-JobReady-{DISPATCHER_ID}")
    pipeline.await_queue(files_found)

    pipeline.await_metrics_endpoint(container, DEFAULT_PROMETHEUS_PORT)

    assert pipeline.seed_until("scraped"), (
        f"no file crossed the pipeline, so there is no traffic to measure:\n"
        f"{container_logs(container)}"
    )

    # The dispatcher's counter is incremented after its script returns, so the
    # output file seed_until waited for can exist a moment before the counter
    # moves. Poll the endpoint rather than sleeping, and KEEP the body that
    # satisfied the poll: taking a fresh, un-retried scrape here would let one
    # transient docker-exec failure read as a product defect, and every number
    # below has to describe the same instant anyway.
    proven: list[str] = []

    def dispatched() -> bool:
        body = pipeline.scrape_over_network(container, DEFAULT_PROMETHEUS_PORT)
        value = _jobs_dispatched(body)
        if value is None or value < 1:
            return False
        proven.append(body)
        return True

    assert poll_until(dispatched, timeout=120.0, interval=2.0), (
        "a file reached /data/out but the dispatcher never counted a "
        f"successful job:\n{container_logs(container)}"
    )
    exposition = proven[-1]

    # The dispatcher's own counter is not re-asserted: the poll above already
    # read it as >= 1 out of this exact body.
    seen = _files_seen(exposition)
    # Presence is the whole assertion. A labelled counter is materialised by
    # its first .inc(), and this one is only ever touched that way, so a series
    # that exists cannot hold zero -- a separate ">= 1" would restate this.
    assert seen is not None, (
        "the monitor's file counter is absent from the scrape, so the endpoint "
        f"is not describing this service's work:\n{exposition}"
    )

    received = sample_value(
        exposition,
        "courier_broker_messages_received_total",
        {"queue_name": files_found},
    )
    assert received is not None, (
        f"nothing was recorded as consumed from {files_found!r}; the builder "
        f"read from somewhere else:\n{exposition}"
    )
