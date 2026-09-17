"""A job builder's subscription outlives the container that consumes it.

Issue #44: a job builder subscribed to the FilesFound fanout exchange by
declaring an exclusive -- therefore auto-delete -- queue under a
server-generated name. The broker deleted that queue the moment the builder
disconnected, so every file announced while no builder was attached was
discarded with no error, no metric and no log line; no other container could
predeclare the subscription on the builder's behalf; and a split deployment
lost everything published before the builder container's first start.

What this module adds over the rabbitmq tier is the container boundary, not a
broker-observable distinction: a dropped connection and a stopped container
both read as zero consumers and RabbitMQ cannot tell them apart.
:mod:`tests.rabbitmq.test_file_found_durability` already proves that a backlog
survives a disconnect inside one living process, and proves the queue's
*properties* with a passive redeclare -- which nothing here can do, so nothing
here tries. What is new is that the producer is a separate container running
real inotify, that the consumer is a process killed by docker rather than a
thread asked to stop, and that the backlog is watched leaving the queue and
arriving as real dispatcher output on a shared volume.

Two of the assertions below exist nowhere else in this tier. A producer-only
container declares ``<namespace>-FilesFound-<builder>`` before any builder
container has ever run, which is the half of #44 that made a first deployment
lose files (:meth:`courier.service.Service._predeclare_target_queues`); and
the backlog is required both to accumulate with nothing attached and to reach
zero once a consumer returns.

The class of bug this catches is any change that makes the subscription
co-terminous with the process consuming it: exclusivity, auto-delete, a
server-generated name, or a builder that declares its queue only at the
moment it is ready to consume. None of those are observable on the in-memory
transport, which never deletes a queue, and every one of them loses files in
the split deployment the project documents.

Every number this module asserts on is read from ``rabbitmqctl`` inside the
broker container, never from log text. That is deliberate twice over: a
configuration whose broker block is wrong falls back silently to the in-memory
transport, in which case no namespaced queue ever appears and the readiness
gates fail, so the gates are also the assertion that AMQP was really used.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests._helpers import poll_until, stays_false
from tests.docker._pipeline import build_config
from tests.docker.conftest import container_logs

if TYPE_CHECKING:
    from tests.docker._pipeline import Pipeline

#: A backstop, not a budget. The waits below permit roughly 1450 seconds in
#: total, and every one of them fails with its own message and the container
#: logs; a module timeout below that sum would replace the diagnosis of a slow
#: failure with "timed out", which is the one outcome this module cannot use.
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

    :meth:`Pipeline.queue_stats` returns an empty mapping for ANY non-zero
    return from ``rabbitmqctl``, so "no such queue" and "the query did not
    run" arrive identically -- and this module reads the first as ``-1``, the
    value it accuses the system of. A single transient ``docker exec`` failure
    inside the hold below would otherwise fail the run with "the backlog moved
    while no consumer was attached", and two inside :func:`_settled_depth`
    would agree on ``-1`` and make it the baseline.

    An empty mapping is never a real observation here. Every call happens after
    a queue has been proved to exist, and preflight's queues are not deleted
    while a container is up, so the broker always has something to list.
    Retrying and then raising keeps "the query failed" out of the answers.

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
        If the broker could not be queried at all.
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
        sentinel matters: "zero consumers" and "no such queue" are the two
        outcomes this module has to tell apart.
    """
    return _stats(pipeline, queue)[1]


def _await_one_consumer(pipeline: Pipeline, queue: str, container: str) -> None:
    """Block until *queue* reports exactly one consumer, or fail loudly.

    :meth:`Pipeline.await_consumers` does the same waiting, but its failure
    message cannot reach the container. A builder that cannot attach to its
    queue raises a fatal broker error and takes its container down with it, so
    "no consumer yet" and "the process died two minutes ago" look identical
    from the broker side; without the logs a dead container reads as a slow
    one. This is also exactly where a regression on the queue's exclusivity
    lands, so it is the message most worth being good.

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
    container is already gone. Requeues are *not* what this absorbs --
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
        If the depth never held still, since a baseline taken from a moving
        count would make every number derived from it meaningless.
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

    Walks the whole property in one run, because the interesting assertions
    are transitions rather than states: a producer-only container declares the
    queue before any builder has ever existed, a builder attaches to it, the
    container stops, the same queue is still declared with zero consumers,
    files announced during the outage pile up in it, nothing drains them, and
    the returning container turns every last one into output *and* leaves the
    queue empty. The producer is never stopped or restarted, and the monitor
    is create-only inotify with no start-up scan, so each backlog file is
    announced exactly once -- while nothing was listening. Output for those
    files can therefore only have come out of the queue.

    Reverted check: in ``src/courier/broker/kombu.py``, set ``auto_delete`` to
    ``True`` in ``MessageBrokerManager._file_found_queue_config`` -- now the
    only place the file-found queue's properties are written -- and rebuild the
    image. Every declaration still agrees, so there is no 406 and no 405; the
    builder attaches and the warm-up runs exactly as it does now. The broker
    then deletes the queue when its last consumer disconnects, which is the
    #44 topology itself: the zero-consumer gate below reads ``-1``, and the run
    fails on that gate with ``declared:`` listing every queue except this one.
    Verified. Both literals have to move together: they are two independent
    declarations of one queue, and leaving them disagreeing exercises the
    broker's property-mismatch handling at start-up instead of anything about
    a queue outliving its consumer.

    The drain gate at the end needs its own revert, since nothing above it can
    fail for a backlog that is delivered but never released: delete the
    ``ack()`` that follows ``yield body, parent_ctx`` in ``Service._relay``
    (``src/courier/service.py``) and start the consumer with
    ``env={"BROKER_PREFETCH_COUNT": "10"}``. Every gate above stays green --
    all five files are dispatched and all five appear in ``/data/out`` -- and
    the run fails here alone, reporting the queue still holding every delivery
    (``stats: (6, 1)`` when checked: six messages, one attached consumer).
    Verified, and it is the false green this gate was added for, because the
    output directory cannot show it: the shipped script copies to a fixed path.

    The prefetch half of that revert is not decoration. At the shipped
    ``broker_prefetch_count`` of 1, a consumer that never acknowledges stalls
    on the first delivery and the run fails earlier, at the output gate with
    all five files missing -- as does replacing ``ack()`` with ``reject()``,
    whose requeue lands the same message back at the head of the queue and
    blocks everything behind it. Both were run. Only with a prefetch window
    wider than the backlog does the failure reach the assertion that is
    actually about draining.
    """
    config = build_config(pipeline.namespace, pipeline.broker)
    files_found = f"{pipeline.namespace}-FilesFound-create-jobs"

    # The producer half first, and that ordering is the assertion: nothing
    # that consumes this queue has run yet, so the queue appearing below can
    # only be `Service._predeclare_target_queues` declaring it on the absent
    # builder's behalf. Before #44 a producer could not do that, and a split
    # deployment lost every file published before its builder's first start.
    pipeline.start_courier("producer", config, only="watch-files")
    # The exact name, never a fragment: the subscription an operator can name
    # in advance is the whole of the fix, and a fragment match would also be
    # satisfied by the pre-fix `<ns>-FilesFoundExchange-fanout-<uuid>` queues.
    pipeline.await_queue(files_found)

    consumer = pipeline.start_courier(
        "consumer", config, only="create-jobs,process-files",
    )
    # A process that has started but not yet bound is invisible to every other
    # signal, and one consumer -- not one per replica -- is the shape that
    # makes replicas competing consumers. It also subsumes a queue-exists gate
    # on the job-ready queue: preflight declares that one before any plugin
    # thread starts, so nothing can be attached here until it exists.
    _await_one_consumer(pipeline, files_found, consumer)

    assert pipeline.seed_until("warmup"), (
        "the split deployment never delivered a file end to end, so nothing "
        f"below would be attributable to the outage; consumer logs:\n"
        f"{container_logs(consumer)}"
    )

    # stop_courier blocks on `docker stop` and asserts its exit status, and no
    # restart policy is set, so the container is provably down here. The gate
    # that matters is the broker's view of it, asserted next: a process can be
    # gone while the broker has not yet noticed the connection drop.
    pipeline.stop_courier(consumer)

    # The headline of issue #44, in one poll rather than two statements: a
    # queue the broker does not list reads as -1 here, so "exactly zero
    # consumers" says in one reading that the queue is still declared and that
    # nothing whatsoever is attached to it. The pre-fix topology cannot produce
    # that reading, because the queue left with its consumer.
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

    # Names never used before in this run. The dispatcher drops a repeated job
    # identifier -- which is the file path -- through an LRU, so a reused name
    # could either vanish or satisfy the output assertion from the warm-up.
    backlog = {f"backlog-{index}.dat" for index in range(BACKLOG_SIZE)}
    for name in sorted(backlog):
        pipeline.seed(f"/data/in/{name}")

    # Gated on `>=` rather than equality, because a count on its way up would
    # otherwise be caught in passing at exactly the expected value and read as
    # the settled answer. The reading is pinned after it has stopped moving.
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
    # Not a blip, and nothing is quietly eating the backlog: with no consumer
    # attached, no reading may move.
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
    # Output is not release. The shipped script copies each input to a fixed
    # destination, so a builder that keeps every delivery unacknowledged, or is
    # handed the same messages again and again, writes byte-identical output
    # and satisfies the gate above -- and the dispatcher's job-id dedupe drops
    # the repeats before the script ever runs, so nothing downstream notices
    # either. Only the queue emptying separates handled from held, and holding
    # cannot fake it: `messages` counts unacknowledged deliveries too.
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
