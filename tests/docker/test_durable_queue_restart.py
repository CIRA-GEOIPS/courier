"""A job builder's subscription outlives the container that consumes it.

Issue #44: a job builder subscribed to the FilesFound fanout exchange by
declaring an exclusive queue under a server-generated name. An exclusive
queue is auto-delete, so the broker removed it the moment the builder
disconnected, and every file announced while no builder was attached was
discarded with no error, metric or log line. No other container could
predeclare the subscription on the builder's behalf, so a split deployment
lost everything published before the builder container's first start.

:mod:`tests.rabbitmq.test_file_found_durability` already covers a backlog
surviving a disconnect inside one living process, and checks the queue's
*properties* with a passive redeclare, which nothing here can do. What this
module adds is the container boundary: the producer is a separate container
running real inotify, the consumer is a process killed by docker, and the
backlog is watched leaving the queue and arriving as dispatcher output on a
shared volume. To the broker, a dropped connection and a stopped container
both read as zero consumers.

Two of the assertions below exist nowhere else in this tier. A producer-only
container declares ``<namespace>-FilesFound-<builder>`` before any builder
container has ever run, which is the half of #44 that made a first deployment
lose files (:meth:`courier.service.Service._predeclare_target_queues`). The
backlog is also required both to accumulate with nothing attached and to
reach zero once a consumer returns.

The class of bug this catches is any change that makes the subscription
co-terminous with the process consuming it: exclusivity, auto-delete, a
server-generated name, or a builder that declares its queue only at the
moment it is ready to consume. The in-memory transport never deletes a queue,
so none of those are observable there.

Every number this module asserts on is read from ``rabbitmqctl`` inside the
broker container rather than from log text. A configuration whose broker block
is wrong falls back to the in-memory transport with no error, in which case no
namespaced queue ever appears and the readiness gates fail. Those gates are
therefore also the check that AMQP was used.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests._helpers import poll_until, stays_false
from tests.docker._pipeline import build_config
from tests.docker.conftest import container_logs

if TYPE_CHECKING:
    from tests.docker._pipeline import Pipeline

#: Backstop above the roughly 1450 seconds the waits below permit in total.
#: Each of those waits fails with its own message and the container logs; a
#: module timeout under that sum would replace the diagnosis with "timed out".
pytestmark = pytest.mark.timeout(1800)

#: Files announced while no builder exists. Large enough that an exact count
#: is conclusive, small enough that a serial dispatcher drains it quickly.
BACKLOG_SIZE = 5

#: Seconds between broker polls. Every one of them is a ``docker exec``
#: starting an Erlang control node, which costs the better part of a second,
#: so the default 0.2 would issue back-to-back CLI invocations.
BROKER_POLL = 1.0

#: Tries before an unanswered broker query is reported as an error rather
#: than folded into the answer.
STATS_ATTEMPTS = 3


def _stats(pipeline: Pipeline, queue: str) -> tuple[int, int]:
    """Return ``(messages, consumers)`` for *queue*, retrying a failed query.

    :meth:`Pipeline.queue_stats` returns an empty mapping for any non-zero
    return from ``rabbitmqctl``, so "no such queue" and "the query did not
    run" arrive the same way. This module reads a missing queue as ``-1``,
    which is the failure it tests for, so a transient ``docker exec`` failure
    read that way would be reported as a lost backlog, and two such failures
    inside :func:`_settled_depth` would agree on ``-1`` and make it the
    baseline.

    An empty mapping is not a real observation here. Every call happens after
    a queue has been proved to exist, and preflight's queues are not deleted
    while a container is up, so the broker always has something to list.
    Retrying and then raising keeps a failed query out of the answers.

    Parameters
    ----------
    pipeline : Pipeline
        Helper owning the broker container.
    queue : str
        Fully namespaced queue name.

    Returns
    -------
    tuple[int, int]
        Messages held and consumers attached, or ``(-1, -1)`` when the broker
        answered and did not list the queue.

    Raises
    ------
    RuntimeError
        If the broker could not be queried.
    """
    for _ in range(STATS_ATTEMPTS):
        stats = pipeline.queue_stats()
        if stats:
            return stats.get(queue, (-1, -1))
    raise RuntimeError(
        f"the broker listed no queues at all on {STATS_ATTEMPTS} consecutive "
        f"attempts, so nothing read from it about {queue!r} is an observation",
    )


def _depth(pipeline: Pipeline, queue: str) -> int:
    """Return the broker-reported message count for *queue*.

    The count is ready plus unacknowledged, so a delivery a consumer is
    holding without acknowledging it is still counted here. A queue the broker
    does not list reads as ``-1`` rather than ``0``, so a vanished queue can
    never be mistaken for an empty one.

    Parameters
    ----------
    pipeline : Pipeline
        Helper owning the broker container.
    queue : str
        Fully namespaced queue name.

    Returns
    -------
    int
        Messages held, or ``-1`` when the queue is not declared.
    """
    return _stats(pipeline, queue)[0]


def _consumers(pipeline: Pipeline, queue: str) -> int:
    """Return the broker-reported consumer count for *queue*.

    Parameters
    ----------
    pipeline : Pipeline
        Helper owning the broker container.
    queue : str
        Fully namespaced queue name.

    Returns
    -------
    int
        Consumers attached, or ``-1`` when the queue is not declared. The
        tests below have to tell that apart from zero consumers.
    """
    return _stats(pipeline, queue)[1]


def _await_one_consumer(pipeline: Pipeline, queue: str, container: str) -> None:
    """Block until *queue* reports one consumer, or fail with the logs.

    :meth:`Pipeline.await_consumers` waits the same way, but its failure
    message cannot reach the container logs. A builder that cannot attach to
    its queue raises a fatal broker error and takes its container down with
    it, so from the broker side a dead container reads the same as one that
    has not attached yet.

    Parameters
    ----------
    pipeline : Pipeline
        Helper owning the broker container.
    queue : str
        Fully namespaced queue name.
    container : str
        Consumer container, quoted into the failure message.
    """
    assert poll_until(
        lambda: _consumers(pipeline, queue) == 1,
        timeout=120.0,
        interval=BROKER_POLL,
    ), (
        f"queue {queue!r} never reported one consumer; stats: "
        f"{pipeline.queue_stats().get(queue)} (-1 means no such queue); "
        f"running: {pipeline.is_running(container)}; logs:\n"
        f"{container_logs(container)}"
    )


def _settled_depth(
    pipeline: Pipeline,
    queue: str,
    window: float = 6.0,
    attempts: int = 5,
) -> int:
    """Return a depth that has stopped moving, for use as a baseline.

    Warm-up traffic can still be settling when the consumer container is
    stopped: the monitor announces every file it saw, not only the one whose
    output was observed, and the last of those publishes may land after the
    container is gone. Requeues are not what this absorbs.
    ``rabbitmqctl list_queues messages`` counts ready plus unacknowledged, so a
    delivery the dying builder never acknowledged moves between those two
    columns without changing the number read here.

    Parameters
    ----------
    pipeline : Pipeline
        Helper owning the broker container.
    queue : str
        Fully namespaced queue name.
    window : float, optional
        Seconds the reading must stay unchanged.  Default 6.
    attempts : int, optional
        Windows to try before giving up.  Default 5.

    Returns
    -------
    int
        The stable message count.

    Raises
    ------
    AssertionError
        If the depth never held still, leaving no usable baseline.
    """
    for _ in range(attempts):
        first = _depth(pipeline, queue)
        if stays_false(
            lambda expected=first: _depth(pipeline, queue) != expected,
            window=window,
            interval=BROKER_POLL,
        ):
            return first
    raise AssertionError(
        f"the depth of {queue!r} never held still for {window}s in {attempts} "
        f"attempts, with nothing publishing to it and nothing attached to it; "
        f"stats: {pipeline.queue_stats().get(queue)}",
    )


def test_a_stopped_builders_queue_keeps_its_backlog_until_the_container_returns(
    pipeline: Pipeline,
) -> None:
    """A builder container can die, and its files wait on the broker for it.

    Walks the whole property in one run, because the assertions are
    transitions: a producer-only container declares the queue before any
    builder has existed, a builder attaches to it, the container stops, the
    same queue is still declared with zero consumers, files announced during
    the outage pile up in it, nothing drains them, and the returning container
    turns every one of them into output and leaves the queue empty. The
    producer is never stopped or restarted, and the monitor is create-only
    inotify with no start-up scan, so each backlog file is announced once,
    while nothing was listening. Output for those files can therefore only
    have come out of the queue.

    Checked by reverting the fix: set ``auto_delete`` to ``True`` in
    ``MessageBrokerManager._file_found_queue_config``
    (``src/courier/broker/kombu.py``), the only place the file-found queue's
    properties are written, and rebuild the image. Every declaration still
    agrees, so there is no 406 and no 405; the builder attaches and the warm-up
    runs as it does now. The broker then deletes the queue when its last
    consumer disconnects, which is the #44 topology: the zero-consumer gate
    below reads ``-1`` and the run fails there, with ``declared:`` listing
    every queue except this one.

    The drain gate at the end needs its own revert, since nothing above it can
    fail for a backlog that is delivered but never released. Deleting the
    ``ack()`` that follows ``yield body, parent_ctx`` in ``Service._relay``
    (``src/courier/service.py``) and starting the consumer with
    ``env={"BROKER_PREFETCH_COUNT": "10"}`` leaves every gate above green: all
    five files are dispatched and all five appear in ``/data/out``. The run
    fails here alone, reporting the queue still holding every delivery
    (``stats: (6, 1)`` when checked: six messages, one attached consumer). The
    output directory cannot show that, because the shipped script copies to a
    fixed path.

    The wider prefetch window is part of that revert. At the shipped
    ``broker_prefetch_count`` of 1, a consumer that never acknowledges stalls
    on the first delivery and the run fails earlier, at the output gate with
    all five files missing. Replacing ``ack()`` with ``reject()`` fails the
    same way, since the requeued message lands back at the head of the queue
    and blocks everything behind it. Both were run.
    """
    config = build_config(pipeline.namespace, pipeline.broker)
    files_found = f"{pipeline.namespace}-FilesFound-create-jobs"

    # The producer starts first, and that ordering is the assertion: nothing
    # that consumes this queue has run yet, so the queue appearing below can
    # only be `Service._predeclare_target_queues` declaring it on the absent
    # builder's behalf.
    pipeline.start_courier("producer", config, only="watch-files")
    # Matched on the full name, because a fragment would also be satisfied by
    # the pre-fix `<ns>-FilesFoundExchange-fanout-<uuid>` queues.
    pipeline.await_queue(files_found)

    consumer = pipeline.start_courier(
        "consumer", config, only="create-jobs,process-files",
    )
    # No other signal shows a process that has started but not yet bound. A
    # count of one means the replicas share one queue; one count per replica
    # would mean each receives its own copy. Waiting here also covers the
    # job-ready queue:
    # preflight declares that one before any plugin thread starts, so nothing
    # can be attached here until it exists.
    _await_one_consumer(pipeline, files_found, consumer)

    assert pipeline.seed_until("warmup"), (
        "the split deployment never delivered a file end to end, so nothing "
        f"below would be attributable to the outage; consumer logs:\n"
        f"{container_logs(consumer)}"
    )

    # stop_courier blocks on `docker stop` and asserts its exit status, and no
    # restart policy is set, so the container is down here. A process can be
    # gone while the broker has not yet noticed the connection drop, which is
    # what the next gate waits for.
    pipeline.stop_courier(consumer)

    # A queue the broker does not list reads as -1 here, so a reading of zero
    # consumers says in one number that the queue is still declared and that
    # nothing is attached to it. The pre-fix topology cannot produce that
    # reading, because the queue left with its consumer.
    assert poll_until(
        lambda: _consumers(pipeline, files_found) == 0,
        timeout=120.0,
        interval=BROKER_POLL,
    ), (
        f"queue {files_found!r} never dropped to zero consumers after its "
        f"container stopped (-1 means the queue itself is gone, which is the "
        f"bug); stats: {pipeline.queue_stats().get(files_found)}; declared: "
        f"{sorted(pipeline.queue_names())}"
    )

    # Baseline after the zero-consumer gate: nothing is consuming, so any
    # warm-up message still in flight has already landed in it.
    baseline = _settled_depth(pipeline, files_found)
    expected = baseline + BACKLOG_SIZE

    # Names not used earlier in this run. The dispatcher drops a repeated job
    # identifier through an LRU, and the identifier is the file path, so a
    # reused name could vanish or satisfy the output assertion from the
    # warm-up.
    backlog = {f"backlog-{index}.dat" for index in range(BACKLOG_SIZE)}
    for name in sorted(backlog):
        pipeline.seed(f"/data/in/{name}")

    # Gated on `>=`: polling for equality can catch a count on its way up at
    # the expected value and read it as the settled answer. The reading below
    # is pinned after the count has stopped moving.
    assert poll_until(
        lambda: _depth(pipeline, files_found) >= expected,
        timeout=180.0,
        interval=BROKER_POLL,
    ), (
        f"files announced during the outage were not retained: expected at "
        f"least {expected} messages on {files_found!r} ({baseline} before "
        f"seeding {BACKLOG_SIZE} files), found "
        f"{pipeline.queue_stats().get(files_found)}"
    )
    held = _depth(pipeline, files_found)
    # With no consumer attached, no reading may move. A count that drops here
    # means something is draining the backlog.
    assert stays_false(
        lambda: _depth(pipeline, files_found) != held,
        window=6.0,
        interval=BROKER_POLL,
    ), (
        f"the depth of {files_found!r} moved from {held} while no consumer "
        f"was attached; stats: {pipeline.queue_stats().get(files_found)}"
    )
    assert held == expected, (
        f"{files_found!r} settled at {held} messages, not the {expected} its "
        f"{baseline}-message baseline plus {BACKLOG_SIZE} seeded files "
        f"account for, so the count is not attributable to the outage"
    )

    pipeline.restart_courier(consumer)
    # Separates "never came back" from "came back and failed to drain".
    _await_one_consumer(pipeline, files_found, consumer)

    assert poll_until(
        lambda: backlog <= set(pipeline.listdir("/data/out")),
        timeout=180.0,
        interval=BROKER_POLL,
    ), (
        f"the returning builder lost part of its backlog; missing "
        f"{sorted(backlog - set(pipeline.listdir('/data/out')))}; consumer "
        f"logs:\n{container_logs(consumer)}"
    )
    # Output does not prove release. The shipped script copies each input to a
    # fixed destination, so a builder that keeps every delivery unacknowledged,
    # or is handed the same messages again and again, writes byte-identical
    # output and satisfies the gate above; the dispatcher's job-id dedupe drops
    # the repeats before the script ever runs. Only the queue emptying
    # separates handled from held, and `messages` counts unacknowledged
    # deliveries too.
    assert poll_until(
        lambda: _depth(pipeline, files_found) == 0,
        timeout=180.0,
        interval=BROKER_POLL,
    ), (
        f"{files_found!r} never emptied after its consumer came back, so the "
        f"backlog was re-read or held rather than drained; stats: "
        f"{pipeline.queue_stats().get(files_found)}; consumer logs:\n"
        f"{container_logs(consumer)}"
    )
