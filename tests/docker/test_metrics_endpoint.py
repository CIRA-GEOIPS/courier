"""The shipped container is scrapable from another container, unconfigured.

Other tiers already read live metrics. ``tests/docker/test_scaling_mid_flight``
scrapes a running container's endpoint through ``docker exec`` and
``127.0.0.1`` (:meth:`Pipeline.scrape_metrics`) and hangs counting assertions
off the result; the unit tiers read values out of the in-process ``REGISTRY``.
Neither sees the bind address. A listener on ``127.0.0.1`` answers a loopback
scrape from inside its own container the same way a listener on ``0.0.0.0``
does, while being unreachable from any Prometheus. ``courier dashboard --live``
(``src/courier/dashboard/live_detector.py``) fetches ``/metrics`` over the
network, so it depends on that bind address.

The scrape here is issued from a third container on the same user-defined
network, addressing the courier container by its DNS name. This tier publishes
no host ports, so it cannot collide with anything.

Every other container test that reads a metric sets ``prometheus_port`` in its
YAML; this one passes no ``extra_service_config``, so what answers is the
shipped default.

Everything read from the scrape is a traffic-driven counter, taken after a file
has crossed the whole pipeline. ``courier_service_uptime_seconds``,
``courier_service_health``, ``courier_broker_connected`` and
``courier_plugin_state`` are populated by the heartbeat loop whether or not
anything is flowing, so they do not show that the pipeline moved data.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests._helpers import poll_until
from tests.docker._pipeline import build_config, sample_value
from tests.docker.conftest import container_logs

if TYPE_CHECKING:
    from tests.docker._pipeline import Pipeline

# pytest-timeout applies this budget to each test separately. The gates below
# sum past the 300s default on a cold daemon: broker start-up 180s, two queue
# gates at 120s each, the endpoint gate at 120s, ``seed_until``'s 150s warm-up
# and a 120s poll for the dispatcher's counter. A smaller budget reports a bare
# timeout instead of the message from whichever gate gave up.
pytestmark = pytest.mark.timeout(900)

#: The port the shipped image listens on with nothing configured.
#: ``ServiceConfig.prometheus_port`` defaults to
#: ``int(os.environ.get("PROMETHEUS_PORT", "8000"))`` and the image sets no
#: ``PROMETHEUS_PORT``; adding one there means updating this constant. The
#: value is mirrored from ``src/courier/config.py``, since read back out of
#: ``ServiceConfig()`` it would only assert that the default equals itself.
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
    """The shipped image is scrapable from another container, unconfigured.

    The configuration does not mention the port, so the assertion is against
    the shipped default. Every number read comes from one scrape, taken after a
    file has reached ``/data/out``.

    Reverted check: pass ``addr="127.0.0.1"`` to the
    ``prometheus_client.start_http_server`` call in ``PrometheusManager.start``
    (``src/courier/managers/prometheus_manager.py``). The endpoint still
    answers the ``docker exec`` loopback scrape of
    :meth:`Pipeline.scrape_metrics`, and this test fails at
    ``await_metrics_endpoint``, because its request comes from a different
    container over the network.

    The received counter is pinned to this pipeline's namespaced per-builder
    queue, which also reads as the issue-44 check: a builder consuming from a
    server-named exclusive queue records its receipts under that name. The
    ``await_queue`` gate below cannot show the difference, because preflight
    predeclares the durable queue whether or not anything binds to it.
    """
    config = build_config(pipeline.namespace, pipeline.broker)
    container = pipeline.start_courier("metrics", config)

    files_found = f"{pipeline.namespace}-FilesFound-{BUILDER_ID}"

    # Gate on the broker topology first, by name. A config whose broker block
    # is wrong falls back to the in-memory transport without an error, and the
    # metrics below are exported either way. This gate passes only over real
    # AMQP.
    pipeline.await_queue(f"{pipeline.namespace}-JobReady-{DISPATCHER_ID}")
    pipeline.await_queue(files_found)

    pipeline.await_metrics_endpoint(container, DEFAULT_PROMETHEUS_PORT)

    assert pipeline.seed_until("scraped"), (
        f"no file crossed the pipeline, so there is no traffic to measure:\n"
        f"{container_logs(container)}"
    )

    # The dispatcher's counter is incremented after its script returns, so the
    # output file ``seed_until`` waited for can exist before the counter moves.
    # Poll the endpoint, and keep the body that satisfied the poll: every
    # number below has to come from the same scrape.
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

    # The poll above already read the dispatcher's counter as >= 1 from this body.
    seen = _files_seen(exposition)
    # Presence is enough. A labelled counter is created by its first .inc(),
    # and this one is only ever touched that way, so a series that exists holds
    # at least 1.
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
