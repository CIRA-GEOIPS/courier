"""Two dispatchers consuming at the same time must both keep dispatching.

The failure this guards against is silent and total. A service with two
dispatchers that can receive jobs concurrently would dispatch at most one job
and then stop forever, while every plugin reported ``PluginRunState.RUNNING``,
heartbeats kept beating and the ``Service`` health check kept returning
``True`` for every manager. Measured over one 240s window on a real
deployment: 44 files seen by the monitor, 88 jobs built and published to the
two ``JobReady`` queues, and exactly one ``Received Job`` line in the whole
service. It is a data-loss failure that no health signal reports.

The mechanism
~~~~~~~~~~~~~
``Dispatcher._emit_queue_depth`` ran once per received job, on the plugin's
own thread, and issued a synchronous ``queue_declare`` down the service's
single shared broker connection. kombu/py-amqp connections are not
thread-safe: two threads with an RPC in flight on one connection both read
from the same socket, and whichever reads first consumes the other's reply
frame. Both then block in ``amqp.transport.read_frame`` for a frame that has
already been taken -- with no timeout, and raising nothing, so the
``contextlib.suppress(Exception)`` around the call never saw it. A thread
parked there has not acknowledged the message it is holding, and at the
default ``broker_prefetch_count`` of 1 that single unacknowledged message is
enough for the broker to never deliver that consumer anything again.

Verified with a thread dump against a wedged process: both dispatcher threads
sat in ``_read``, reached from ``dispatchers.py`` through
``kombu.Connection.channel`` -> ``amqp.Channel.open`` -> ``blocking_read``.

Why the backlog is preloaded
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The jobs are published *before* either dispatcher attaches, and both are then
started together. That is what makes this deterministic rather than a race
worth a retry loop: both consumers are handed their first message in the same
instant, so both enter the probe together and the collision happens on the
first pair of jobs. Publishing afterwards instead hands the two queues their
first message milliseconds apart, and the two threads settle into a GIL
lock-step that can alternate for thousands of jobs without ever overlapping --
which is exactly why this went unnoticed. It is also what the real deployment
does: two builder threads publishing concurrently deliver to both dispatchers
at once.

What the rest of the suite could not see
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
``tests/unit_tests/routing/test_dispatcher_queue_depth.py`` drives the probe
against a stubbed channel on one thread, so no two callers ever meet.
``tests/docker/test_dispatcher_output_reenters.py`` runs two dispatchers in a
container but its two dispatches are strictly sequential -- chain one finishes
before chain two's job exists -- so its dispatchers never consume at the same
time. Nothing in the suite had two dispatchers consuming concurrently.

Reverted check
~~~~~~~~~~~~~~
Point ``Dispatcher._emit_queue_depth`` back at
``self.parent_service._broker_manager._connection`` instead of its own
``_queue_depth_connection()``. The test below then reports 0 of 10 jobs
dispatched, both dispatchers wedged, on every run.
"""

from __future__ import annotations

import contextlib
import threading
from pathlib import Path
from typing import TYPE_CHECKING

import kombu
import pytest

from courier.config import ServiceConfig
from courier.constants import job_ready_queue_for
from courier.interfaces.dispatchers import Dispatcher
from courier.service import Service
from courier.types.execution_log import ExecutionLog
from courier.types.file import File
from courier.types.job import Job
from tests._helpers import poll_until

if TYPE_CHECKING:
    from collections.abc import Iterator

#: Two is the smallest number that can collide, and is the arrangement the
#: incident was reported against.
DISPATCHERS = ("dispatch-a", "dispatch-b")

#: Jobs preloaded per dispatcher. The wedge lands on the first pair, so this
#: only has to be large enough that "some progress then silence" is
#: distinguishable from "everything drained".
JOBS_EACH = 5


class _RecordingDispatcher(Dispatcher):
    """Dispatcher that records the jobs it was asked to execute.

    Executing nothing is deliberate: a real payload would add its own
    failure modes, and the property under test is whether the consume loop
    keeps turning at all.
    """

    name = "recording_dispatcher"
    version = "test"

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.executed: list[str] = []
        self._executed_lock = threading.Lock()

    def get_execution_log(self, job: Job) -> list[ExecutionLog]:
        with self._executed_lock:
            self.executed.append(job.identifier)
        return [ExecutionLog(return_code=0, stdout="ok", stderr="", hostname="h")]

    @property
    def dispatched(self) -> int:
        """Return how many jobs this dispatcher has executed."""
        with self._executed_lock:
            return len(self.executed)


@pytest.fixture
def dispatcher_queues(amqp_url: str, namespace: str) -> Iterator[None]:
    """Delete the queues this module declares, whatever the test did.

    The shared ``raw_conn`` fixture cleans up a fixed list of names that does
    not include these, and a namespace is unique per test, so without this
    every run would leave queues behind on the broker.
    """
    yield
    conn = kombu.Connection(amqp_url)
    conn.ensure_connection(max_retries=3)
    try:
        with conn.channel() as channel:
            names = [f"{namespace}-JobReady-{ident}" for ident in DISPATCHERS]
            names.append(f"{namespace}-DispatcherQueue")
            for name in names:
                # Teardown is best effort: a test that failed before
                # declaring is not a test that should fail twice.
                with contextlib.suppress(Exception):
                    channel.queue_delete(name)
    finally:
        conn.release()


def _job(dispatcher: str, index: int) -> Job:
    """Return a job with an identifier unique across both dispatchers.

    Unique because the dispatcher's dedupe LRU silently drops a repeated
    identifier, which would look identical to the wedge being fixed.
    """
    return Job(
        "n",
        f"{dispatcher}-{index}",
        {},
        files=[File(file=Path(f"/data/{dispatcher}-{index}.nc")).freeze()],
    )


def test_two_dispatchers_consuming_at_once_both_keep_dispatching(
    amqp_url: str,
    namespace: str,
    dispatcher_queues: None,
) -> None:
    """Every preloaded job is dispatched when both consumers start together.

    Fails on a dispatcher that performs broker RPC on the connection it
    shares with the rest of the service: the two consumer threads deadlock
    against each other on the first pair of jobs and neither ever
    acknowledges, so the count stays at zero while both plugins go on
    reporting themselves healthy.

    Parameters
    ----------
    amqp_url : str
        Broker under test.
    namespace : str
        Namespace unique to this test, prefixing every queue name.
    dispatcher_queues : None
        Teardown fixture that removes this module's queues.
    """
    del dispatcher_queues
    config = ServiceConfig(
        broker_url=amqp_url,
        prometheus_port=0,
        namespace=namespace,
        tracing_enabled=False,
        broker_max_retries=1,
        # The default, stated rather than assumed: it is what turns one
        # parked thread into a permanently silent consumer.
        broker_prefetch_count=1,
    )
    service = Service(config)
    service.configure_routing(
        dispatcher_identifiers=set(DISPATCHERS),
        builder_targets={},
        builder_identifiers=set(),
    )
    service.preflight_check()
    # Opens the service's shared broker connection, which a running service
    # always has. It is the object the dispatchers must not reach into, so a
    # test that left it closed would pass against the defect.
    service._broker_manager.start()
    assert service._broker_manager.is_healthy()

    # Preloaded before anything attaches, so both consumers are handed their
    # first message at the same instant. See the module docstring.
    for index in range(JOBS_EACH):
        for identifier in DISPATCHERS:
            service.emit(
                queue=job_ready_queue_for(identifier),
                message=str(_job(identifier, index)),
            )

    dispatchers = [
        _RecordingDispatcher(service, {}, identifier=identifier)
        for identifier in DISPATCHERS
    ]
    total = JOBS_EACH * len(DISPATCHERS)
    try:
        for dispatcher in dispatchers:
            dispatcher.start()
        for dispatcher in dispatchers:
            assert dispatcher.wait_until_subscribed(
                timeout=30
            ), f"{dispatcher.identifier} never bound to its job queue"

        # 30s against a passing path that finishes in under two: the wedge is
        # immediate and permanent, so the budget only has to cover a loaded
        # box, not a slow drain.
        assert poll_until(
            lambda: sum(d.dispatched for d in dispatchers) == total,
            timeout=30,
        ), (
            "dispatch stopped with jobs still queued: "
            f"{ {d.identifier: d.dispatched for d in dispatchers} } of "
            f"{JOBS_EACH} each; every plugin still reports healthy="
            f"{ {d.identifier: d.is_healthy() for d in dispatchers} } and the "
            f"broker manager reports healthy={service._broker_manager.is_healthy()}"
        )
    finally:
        for dispatcher in dispatchers:
            dispatcher.stop()
        service._broker_manager.stop()
