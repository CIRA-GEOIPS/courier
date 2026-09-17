"""Scaling a job builder up and down while files are flowing.

Replicas of one builder identifier now share ONE durable queue bound to the
file-found fanout, so they are competing consumers and each file is delivered
to exactly one of them. Under the old topology each replica declared its own
exclusive, server-named queue and bound that to the fanout, so every replica
received EVERY file: adding a replica silently doubled the work, and the
pipeline emitted two jobs for every input.

This module exists to catch that whole class of bug -- a topology change that
turns replicas from competing consumers into broadcast subscribers, or into
one working consumer and one attached spectator -- at the only level where it
is visible: several real containers, one real broker, and files arriving
throughout a runtime scale-up and scale-down.

Three observations are needed, and none of them is sufficient alone.

* A ledger on the data volume, one line appended per dispatcher execution.
  The shipped script copies its input to a fixed destination, so a second
  dispatch overwrites the first and the output directory looks identical
  either way. The ledger is the only record that can count executions.
* The dispatcher's own counters. A duplicate job never reaches the script:
  with ``files_per_job: 1`` the job identifier *is* the file path, both
  replicas mint the same identifier for the same file, and the dispatcher's
  consumer-side dedupe drops the repeat before executing anything. A
  ledger-only test therefore reads exactly-once whether or not the topology is
  broken. ``courier_dispatcher_dedupe_skips_total`` is what sees it.
* Each replica's own ``courier_job_builder_files_received_total``, scraped
  from that replica's own metrics endpoint. The first two observations answer
  "was any file handled twice"; neither answers "did the second replica handle
  anything at all". Every exactly-once assertion here is satisfied by a run in
  which the broker hands the whole batch to replica A while replica B sits
  attached and idle -- the shape of a queue with a single active consumer, or
  of a replica whose channel is never given a delivery. Splitting the count by
  replica is what makes COMPETING, rather than merely exactly-once, a measured
  property, and it re-proves exactly-once at the point of delivery, upstream of
  the dedupe that hides it everywhere else.

``files_per_job`` is 1 throughout, and that is load-bearing rather than
incidental: a builder that emits one job per file does not accumulate, so
replicating it needs no shared state and the scenario stays free of Redis
(see :meth:`courier.interfaces.job_builders.JobBuilder._check_replication_safety`).

Scope limit on the second half: only a DRAINED scale-down is covered. Both
queues are empty before replica B is stopped, so B holds nothing
unacknowledged and the broker has nothing to redeliver. A replica killed
mid-backlog, whose unacknowledged deliveries are redelivered to the survivor
and must then be absorbed by the dispatcher's dedupe, is a different property
and is deliberately not tested here -- arranging it would mean stalling a
builder that returns to idle in milliseconds.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests._helpers import poll_until, stays_false
from tests.docker._pipeline import build_config, sample_value
from tests.docker.conftest import container_logs

if TYPE_CHECKING:
    from tests.docker._pipeline import Pipeline

# Five containers and three seeded phases; the four short full-run tests set
# their own, much smaller, budget.
pytestmark = pytest.mark.timeout(900)

LEDGER_SCRIPT = (
    "printf '%s\\n' '{{ files[0].file }}' >> /data/ledger.txt\n"
    "cp {{ files[0].file }} /data/out/\n"
)

#: Port the metrics endpoint listens on inside each container.  Named rather
#: than defaulted so the scrape below cannot silently read a different server.
METRICS_PORT = 9187

#: Files seeded while two replicas are attached, and after one is stopped.
#: The property does not need volume; each file costs a ``docker exec``.
SCALED_UP_FILES = 6
SCALED_DOWN_FILES = 3

#: Dispatcher whose counters carry the duplicate-detection assertion.
DISPATCHER_ID = "process-files"

#: The replicated builder.  Both halves of its label pair are pinned when its
#: counter is read, because an exact label match is the only lookup that cannot
#: answer with a different series than the one being asked about.
BUILDER_ID = "create-jobs"
BUILDER_NAME = "filter_and_group"


def _files_received(pipeline: Pipeline, replica: str) -> float | None:
    """Return the files one replica has taken off the shared queue.

    The counter is incremented as each file-found message is parsed, before
    any job is built, so it measures delivery rather than outcome -- which is
    the point: everything downstream of it is deduplicated.

    ``None`` rather than ``0.0`` for an absent series, because a labelled
    counter is only materialised on first use and the caller is the only one
    that can say which reading it wants. A replica that received nothing and a
    scrape that did not answer both produce ``None`` here; they are told apart
    at the call site by summing the replicas against a known expected count.

    Parameters
    ----------
    pipeline : Pipeline
        Running pipeline.
    replica : str
        Container name of one builder replica.

    Returns
    -------
    float or None
        Files received by that replica since it started, or ``None`` when the
        series is absent from its exposition.
    """
    return sample_value(
        pipeline.scrape_metrics(replica, METRICS_PORT),
        "courier_job_builder_files_received_total",
        {"job_builder_name": BUILDER_NAME, "job_builder_identifier": BUILDER_ID},
    )


def _dedupe_evidence(
    pipeline: Pipeline,
    dispatcher: str,
) -> tuple[float | None, float | None, str]:
    """Return the dispatcher's consume and skip counters from ONE scrape.

    Both counters must come from the same exposition text. "No duplicates were
    skipped" is an assertion about an absent or zero series, which a misspelt
    name, a wrong port and a dead endpoint all satisfy; the consume counter
    read from the same text is the positive control that fails loudly first.

    Parameters
    ----------
    pipeline : Pipeline
        Running pipeline.
    dispatcher : str
        Container name of the dispatcher.

    Returns
    -------
    tuple[float or None, float or None, str]
        Jobs consumed, duplicate jobs skipped, and the exposition they were
        read from, for use in failure messages.  ``None`` means the series was
        never materialised.
    """
    exposition = pipeline.scrape_metrics(dispatcher, METRICS_PORT)
    labels = {"dispatcher_identifier": DISPATCHER_ID}
    return (
        sample_value(exposition, "courier_dispatcher_jobs_consumed_total", labels),
        sample_value(exposition, "courier_dispatcher_dedupe_skips_total", labels),
        exposition,
    )


def _settle(
    pipeline: Pipeline,
    queues: tuple[str, ...],
    window: float = 10.0,
    attempts: int = 4,
) -> list[str]:
    """Hold until the queues are idle and the ledger has stopped growing.

    A snapshot taken the moment the expected entries appear can be read on the
    way to twice as many, so every count in this module is taken after the
    ledger has been still, with both queues empty, for a while.

    An entry landing inside the window restarts the window rather than failing
    the test. A file can be invisible to both observations at once: the monitor
    holds it in an in-process queue between the inotify event and the publish,
    during which the broker is legitimately empty and the ledger legitimately
    unchanged. A late arrival therefore means the snapshot was taken early, not
    that a duplicate was dispatched -- and duplicates are counted by the
    per-phase assertions, which see the extra entry whenever it lands.

    Parameters
    ----------
    pipeline : Pipeline
        Running pipeline.
    queues : tuple[str, ...]
        Queues that must stay empty throughout the window.
    window : float, optional
        Seconds the ledger must stay unchanged.  Default 10.
    attempts : int, optional
        Windows to try before giving up.  Default 4.

    Returns
    -------
    list[str]
        The settled ledger.
    """

    def still(snapshot: list[str]) -> bool:
        return stays_false(
            lambda: pipeline.ledger() != snapshot or not pipeline.drained(queues),
            window=window,
            interval=1.0,
        )

    for _ in range(attempts):
        settled = pipeline.ledger()
        if still(settled):
            return settled
    raise AssertionError(
        f"the ledger never held still for {window}s in {attempts} attempts, so "
        f"no count taken from it describes a finished batch: {pipeline.ledger()}",
    )


def _await_dispatched(
    pipeline: Pipeline,
    expected: set[str],
    queues: tuple[str, ...],
    logs: dict[str, str],
) -> list[str]:
    """Block until every expected path is dispatched and the broker is idle.

    Parameters
    ----------
    pipeline : Pipeline
        Running pipeline.
    expected : set[str]
        Input paths that must appear in the ledger.
    queues : tuple[str, ...]
        Queues that must be empty before the count is taken.
    logs : dict[str, str]
        Container name to role, used to build a readable failure message.

    Returns
    -------
    list[str]
        The settled ledger, covering every phase so far.
    """

    def complete() -> bool:
        return expected <= set(pipeline.ledger()) and pipeline.drained(queues)

    assert poll_until(complete, timeout=240.0, interval=1.0), (
        f"not every seeded file was dispatched; missing "
        f"{sorted(expected - set(pipeline.ledger()))}, "
        f"queues {pipeline.queue_stats()}\n"
        + "\n".join(
            f"--- {role} ({name}) ---\n{container_logs(name)}"
            for name, role in logs.items()
        )
    )
    return _settle(pipeline, queues)


def _phase(ledger: list[str], expected: set[str]) -> list[str]:
    """Return the ledger entries belonging to one seeded batch.

    Parameters
    ----------
    ledger : list[str]
        Whole ledger.
    expected : set[str]
        Paths seeded for the phase.

    Returns
    -------
    list[str]
        Entries for those paths, duplicates included.
    """
    return [entry for entry in ledger if entry in expected]


def test_replicas_of_one_builder_share_the_files_and_dispatch_each_once(
    pipeline: Pipeline,
) -> None:
    """A second replica takes a share of the stream; no file is handled twice.

    Reverted check, **verified**: stop replica B immediately after the
    two-consumer gate, so it attaches and then does no work. ``took_b > 0``
    fails. That is the assertion carrying this test's headline claim, and the
    one a reader should trust.

    Two src-side reverts are worth naming but were *reasoned about, not run*,
    so they are recorded as predictions rather than observations. Setting
    ``exclusive=True`` in ``_file_found_queue_config``
    (src/courier/broker/kombu.py) does not reproduce the old topology by
    itself, because the queue is shared rather than per-connection, so the
    second builder's declaration should draw a 405 RESOURCE_LOCKED and the
    test should fail at the first gate. Reproducing
    pre-#44 behaviour faithfully needs the per-connection queue NAME too, and
    should also fail at the first gate, because the shared queue that gate
    names keeps its preflight declaration and never gains a consumer.

    The counters are for the partial regression instead: any change that leaves
    both replicas consuming the shared queue while some file still reaches both
    of them. The ledger cannot see that at all, since both replicas mint the
    same job identifier (the file path) and the dispatcher's dedupe drops the
    repeat before the script runs, so one line per file is written either way.
    The replicas' receipt counters would then sum to more than the batch,
    ``courier_dispatcher_jobs_consumed_total`` would exceed the ledger, and
    ``courier_dispatcher_dedupe_skips_total`` would rise off zero. Asserting
    that zero beside a positive control taken from the SAME scrape is what
    stops a misspelt metric name or an unreachable endpoint from reading as
    "no duplicates".

    Reverted check for the competing half, which is a different failure and
    needs its own: stop replica B immediately after the consumer gate, before
    the batch is seeded. Every count in this module stays green, because one
    replica dispatching six files exactly once is exactly what those counts ask
    for; only ``took_b`` goes to zero. The same reading is what a queue
    declared with ``x-single-active-consumer``, or a replica that binds but is
    never given a delivery, would produce while the broker still reports two
    consumers.
    """
    config = build_config(
        pipeline.namespace,
        pipeline.broker,
        files_per_job=1,
        script=LEDGER_SCRIPT,
        extra_service_config=f"prometheus_port: {METRICS_PORT}",
    )
    files_found = f"{pipeline.namespace}-FilesFound-{BUILDER_ID}"
    job_ready = f"{pipeline.namespace}-JobReady-{DISPATCHER_ID}"
    queues = (files_found, job_ready)

    dispatcher = pipeline.start_courier(
        "dispatcher", config, only=DISPATCHER_ID, env={"SERVICE_ID": "dispatcher"},
    )
    replica_a = pipeline.start_courier(
        "builder-a", config, only=BUILDER_ID, env={"SERVICE_ID": "builder-a"},
    )
    producer = pipeline.start_courier(
        "producer", config, only="watch-files", env={"SERVICE_ID": "producer"},
    )
    logs = {
        dispatcher: "dispatcher",
        replica_a: "builder replica A",
        producer: "producer",
    }

    pipeline.await_queue(job_ready)
    pipeline.await_queue(files_found)
    # Queue existence proves nothing about attachment here: every container,
    # the producer-only one included, predeclares this queue during preflight.
    pipeline.await_consumers(files_found, 1)

    # The monitor is edge-triggered inotify with no start-up scan and there is
    # no broker-side signal for "the watcher is watching", so the warm-up both
    # arms the path and tells us when it is live.  It creates an unknown number
    # of files, some of them before the observer existed, so it establishes a
    # baseline rather than a count.
    assert pipeline.seed_until("warmup"), (
        "the pipeline never produced any output at all\n"
        + "\n".join(
            f"--- {role} ({name}) ---\n{container_logs(name)}"
            for name, role in logs.items()
        )
    )
    assert poll_until(
        lambda: pipeline.drained(queues), timeout=120.0, interval=1.0,
    ), f"the warm-up files never cleared the queues: {pipeline.queue_stats()}"
    baseline = len(_settle(pipeline, queues))
    assert baseline > 0, "warm-up produced output but recorded no dispatch"

    # The one replica running so far did all of that work, so its counter is
    # the offset every later per-replica count is taken against -- and reading
    # it here, where the answer is already known from the ledger, is what
    # proves the per-replica scrape is live and correctly labelled before
    # anything is concluded from it.
    baseline_received = _files_received(pipeline, replica_a)
    assert baseline_received is not None, (
        f"replica A served no files-received sample for {BUILDER_ID!r} after "
        f"doing {baseline} dispatches, so the per-replica scrape cannot "
        f"attribute work to a replica\n{container_logs(replica_a)}"
    )
    assert baseline_received == baseline, (
        f"replica A received {baseline_received} files while the ledger "
        f"records {baseline} dispatches; the two instruments this test relies "
        f"on disagree before the scale-up has even started"
    )

    # -- scale up ---------------------------------------------------------
    replica_b = pipeline.start_courier(
        "builder-b", config, only=BUILDER_ID, env={"SERVICE_ID": "builder-b"},
    )
    logs[replica_b] = "builder replica B"
    pipeline.await_consumers(files_found, 2, timeout=180.0)
    scaled_up = set(pipeline.seed_many("scaled-up", SCALED_UP_FILES))
    ledger = _await_dispatched(pipeline, scaled_up, queues, logs)
    # Set equality is not asserted: _await_dispatched polls until every seeded
    # file appears, and _phase keeps only entries from this phase, so both
    # inclusions hold before this line runs. The count below is the live
    # assertion -- it is what a duplicate delivery breaks.
    dispatched = _phase(ledger, scaled_up)
    assert len(dispatched) == SCALED_UP_FILES, (
        f"expected {SCALED_UP_FILES} dispatches while scaled up, got "
        f"{len(dispatched)}: {dispatched}"
    )

    # Who actually did the work.  Both scrapes are taken with the batch
    # finished and both queues empty, so the two numbers describe the same
    # settled state even though they are two execs.
    received_a = _files_received(pipeline, replica_a)
    assert received_a is not None, (
        f"replica A stopped reporting its files-received counter\n"
        f"{container_logs(replica_a)}"
    )
    received_b = _files_received(pipeline, replica_b)
    took_a = received_a - baseline_received
    took_b = 0.0 if received_b is None else received_b
    assert took_a + took_b == SCALED_UP_FILES, (
        f"the replicas received {took_a + took_b} of the {SCALED_UP_FILES} "
        f"scaled-up files between them (A took {took_a}, B took {took_b}); "
        f"more than that means a file was delivered to both replicas, fewer "
        f"means a file was never delivered or a replica's endpoint went quiet"
    )
    assert took_b > 0, (
        f"replica B was attached to {files_found} as a second consumer for the "
        f"whole batch and received nothing: replica A took all "
        f"{SCALED_UP_FILES} files, so the replicas were not competing for them "
        f"and every exactly-once count above was measured on one replica"
    )

    consumed, skipped, exposition = _dedupe_evidence(pipeline, dispatcher)
    assert consumed == len(ledger), (
        f"the dispatcher consumed {consumed} jobs for {len(ledger)} dispatched "
        f"files, so the same file was built into a job more than once\n"
        f"{exposition}"
    )
    # ``None`` is the honest reading of "the skip path never ran": a labelled
    # counter is not materialised until its first increment.
    assert skipped is None or skipped == 0, (
        f"the dispatcher discarded {skipped} duplicate job(s): both replicas "
        f"received the same file\n{exposition}"
    )

    # -- scale back down --------------------------------------------------
    # Both queues are already empty, so replica B holds nothing unacknowledged
    # and stopping it cannot cause a legitimate redelivery.
    pipeline.stop_courier(replica_b)
    pipeline.await_consumers(files_found, 1, timeout=180.0)

    scaled_down = set(pipeline.seed_many("scaled-down", SCALED_DOWN_FILES))
    ledger = _await_dispatched(pipeline, scaled_down, queues, logs)
    remaining = _phase(ledger, scaled_down)
    assert len(remaining) == SCALED_DOWN_FILES, (
        f"expected {SCALED_DOWN_FILES} dispatches after scaling down, got "
        f"{len(remaining)}: {remaining}"
    )

    # -- whole-run invariants ---------------------------------------------
    assert len(ledger) == len(set(ledger)), (
        f"some file was dispatched twice over the whole run: {ledger}"
    )
    assert len(ledger) == baseline + SCALED_UP_FILES + SCALED_DOWN_FILES, (
        f"unexpected total dispatch count {len(ledger)} against a warm-up "
        f"baseline of {baseline}: {ledger}"
    )

    consumed, skipped, exposition = _dedupe_evidence(pipeline, dispatcher)
    assert consumed == len(ledger), (
        f"over the whole run the dispatcher consumed {consumed} jobs for "
        f"{len(ledger)} files\n{exposition}"
    )
    assert skipped is None or skipped == 0, (
        f"over the whole run the dispatcher discarded {skipped} duplicate "
        f"job(s)\n{exposition}"
    )
