"""A full pipeline driven by the message-queue monitor, not the filesystem one.

Every other full-run test in this tier drives ``file_system_poller_watchdog``:
a file appears on a volume, inotify fires, and the pipeline runs. But that is
not the monitor the project's own configurations reach for. ``config.yaml``
and ``tests/cira-data-inventory-example.yaml`` both drive ``rabbit_mq_watcher``,
which takes its input from a JSON notification on a broker queue and never
looks at a directory at all. That path had no end-to-end coverage whatsoever,
so the class of bug this module catches is "the queue-driven topology does not
work in the shipped image" -- a notification that parses into a File nobody can
use, a location that reassembles into the wrong path, a timestamp or a metadata
field dropped between the monitor and the job builder's filters, one
notification dispatched twice, or a monitor that connects to its own broker but
whose service never reaches AMQP at all.

That last one is worth spelling out, because it is a false green unique to this
scenario. ``rabbit_mq_watcher`` opens its **own** AMQP connection from its own
plugin config, entirely separate from ``spec.broker``. So if the service's
broker block were wrong, the schema would quietly infer the in-memory transport
and monitor, builder and dispatcher would still talk to each other inside one
process -- and an output-only assertion would pass while proving nothing about
the broker. Readiness is therefore gated on the namespaced queues appearing on
the real broker, exactly as the rest of the tier does, and those gates are the
assertion that AMQP was really the transport. Never relax them into log text.

Counting dispatches needs a ledger, not an output directory. The dispatcher's
script copies its input to a fixed destination, so two dispatches of one
notification leave exactly one output file and ``listdir('/data/out')`` reads
identically either way -- the trap :meth:`tests.docker._pipeline.Pipeline.
read_lines` documents, and the reason :mod:`tests.docker.test_scaling_mid_flight`
appends a line per execution instead. Duplicate delivery is the archetypal
regression for a queue-driven monitor (a reconnect with an unacknowledged
message, a requeue-on-error branch, ``max_retries: -1``), so it is the one
thing this module must be able to see. The script here appends before it
copies, and the line it appends carries the three values that have to survive
the trip: the File's timestamp, its hostname and its reassembled path.

Scope, stated precisely because an earlier draft of this docstring overclaimed
it: the *topology* under test is the shipped one, and the two shipped config
files now agree with it. They did not when this module was written, and the
two defects it was built around are worth recording, because each failed in a
way nothing reported.

* ``timestamp_field: time_range`` (``config.yaml``, ``tests/demo.yaml`` and the
  example config) was a silent no-op. ``_extract_timestamp`` took its
  explicit-field branch, walked to the ``time_range`` dict, and handed the dict
  to :func:`courier.utils.datetime_utils.parse_timestamp`, which returns
  ``None`` for anything that is not a string, a number or a datetime -- so
  every File those configs built carried ``timestamp=None``, which is also what
  a message with no timestamp produces. The key is gone from all three configs,
  the documented ``time_range`` -> ``lower``/``start`` default runs instead, and
  the monitor now warns once when a configured ``timestamp_field`` resolves to
  a container rather than a value.
* ``dir_path: dir_ath`` in ``tests/cira-data-inventory-example.yaml`` mapped the
  canonical ``dir_path`` onto a message key no producer sends. Note it did
  *not* fall back to ``file_path``: that branch tests whether the field_map
  declares ``dir_path``, and the merge against ``_DEFAULT_FIELD_MAP`` means it
  always does -- so the reassembly indexed a key that was absent and raised
  ``KeyError`` on every message, which the callback rejected without requeue.
  That config discarded its entire input stream.

Both are now guarded in ``tests/test_shipped_config_drift.py``, which checks
that a field_map names keys the documented schema contains and that the
configured timestamp actually resolves against a representative message.

No revert check applies to this module, and that is deliberate rather than an
omission: this is new coverage for a monitor that had no full-run test at all,
not a regression guard for the issue #44 durable-queue fix, so there is no
specific edit to name. Three vacuity checks stand in its place, because a
container test that cannot be made to fail is this tier's worst failure mode.
All three were run against this module and all three produced the failure
described.

* Point ``spec.broker.host`` at a hostname that does not resolve. The service
  never reaches AMQP, ``<ns>-FilesFound-create-jobs`` never appears, and the
  first readiness gate fails. If it still passed, the topology gates would
  have stopped being assertions.
* Publish the payload as a dict instead of a JSON string. kombu then tags it
  ``application/json``, the watcher's callback takes its ``body.decode``
  branch, raises ``AttributeError``, rejects the message without requeueing,
  and no output is ever produced. If it still passed, something other than the
  notification would be producing the output file.
* Publish the accepted notification twice. Each delivery builds its own File,
  the builder opens a successor job for the second (a fresh identifier, so the
  dispatcher's LRU dedupe does not absorb it), and the ledger holds two lines.
  The exactly-once assertion fails. If it still passed, "one notification, one
  dispatch" would not be a claim this module makes.

Unlike the filesystem tier there is no edge to miss here, so no ``seed_until``
retry loop is needed or wanted: the queue is durable and declared by the
publisher as well as the consumer, so a notification published before the
watcher attaches simply waits. The seeded files only have to exist before the
dispatcher runs, not before the monitor starts.
"""

from __future__ import annotations

import textwrap
from typing import TYPE_CHECKING

import pytest

from tests._helpers import poll_until, stays_false
from tests.docker.conftest import BROKER_PASSWORD, BROKER_USER, container_logs

if TYPE_CHECKING:
    from tests.docker._pipeline import Pipeline

# Six gates and two publishes, against the 300s this tier's ini default
# allows.  900 is what the other multi-gate modules in the tier use, and the
# session-scoped broker-image pull sits inside the first item's window too.
pytestmark = pytest.mark.timeout(900)

#: Platform and sensor the notification carries, and two of the three values
#: the job builder filters on.  Neither can be back-filled from the path: the
#: watcher infers a platform from a ``G16``..``G19`` token in the filename when
#: the message omits one, so a ``goes*`` platform would let the filter pass
#: even with ``platform_name`` deleted from the payload, and nothing infers a
#: sensor at all.  The generated filenames carry no uppercase ``G``.
PLATFORM = "himawari9"
SENSOR = "ahi"

#: The third filter value, and the only one that does **not** reach a ``File``
#: attribute.  ``product`` is not in the watcher's default field map, so it is
#: gathered into ``File.metadata`` instead, and the builder can only match it
#: through the metadata layer of ``_file_matches_filters``.  Filtering on it is
#: what makes "metadata survives the monitor -> builder hand-off" a claim
#: rather than a hope: with the default field map alone the metadata dict is
#: always empty and any assertion about it would be vacuous.
PRODUCT = "l1b-radiance"

#: Platform on the notification that must be **rejected**.  It gives the filter
#: a negative to go with its positive: same queue, same parse, same file on the
#: volume, and the only difference is a value the filter is configured against.
REJECTED_PLATFORM = "goes18"

#: Where the notification claims the file lives, split so that the reassembly
#: is actually pinned.  The watcher builds the path as
#: ``PurePosixPath(location_path) / PurePosixPath(dir_path).relative_to('/')
#: / file_name`` -- it CONCATENATES the two halves rather than merging them.
#: Both halves therefore have to be non-trivial: with a location path of ``/``
#: the ``relative_to`` call can be deleted and the result is unchanged, because
#: ``PurePosixPath('/') / '/data/in'`` is already ``/data/in``.  With ``/data``
#: and ``/in`` it is not -- dropping the ``relative_to`` yields ``/in/<name>``,
#: which does not exist, and the dispatch copies nothing.
LOCATION = "courier@datanode:/data"
HOSTNAME = "datanode"
DIR_PATH = "/in"

#: The directory the two halves above must reassemble into, and the only place
#: the files are actually seeded.
RESOLVED_DIR = "/data/in"

#: The notification's ``time_range``.  ``timestamp_field`` is left unset, so
#: the watcher reads ``lower`` through its documented default; ``upper`` is
#: carried only to prove the default picks the lower bound.  The expected
#: rendering is what ``File.to_dict`` produces after ``ensure_utc`` tags the
#: naive value as UTC, since the dispatcher's template sees the dict, not the
#: datetime.
TIME_RANGE_LOWER = "2026-01-29T09:10:00"
TIME_RANGE_UPPER = "2026-01-29T09:18:00"
EXPECTED_TIMESTAMP = "2026-01-29T09:10:00+00:00"

#: One line per dispatcher execution, outside both the input and the output
#: directory so no monitor can see it.  Appending before the copy means a
#: dispatch is recorded even if the copy then fails, which is what keeps
#: "nothing was dispatched" distinguishable from "the dispatch went wrong".
LEDGER = "/data/ledger.txt"


def _queue_driven_config(
    namespace: str,
    broker_host: str,
    inbound_queue: str,
) -> str:
    """Build the shipped queue-driven topology as a service configuration.

    The shared :func:`tests.docker._pipeline.build_config` cannot express this
    scenario -- it hardcodes the filesystem monitor and offers no builder
    filters -- so this module carries its own.  The shape follows ``config.yaml``
    and ``tests/cira-data-inventory-example.yaml``, with four deliberate
    departures, each of which the module docstring justifies.

    ``timestamp_field`` is omitted where the shipped configs set it to
    ``time_range``, which is a silent no-op, so the plugin's documented
    ``time_range.lower`` default runs instead.  ``field_map`` carries one extra
    key, ``product``, which is the only way to put anything into
    ``File.metadata``.  ``tracing_enabled`` is false, or the process spends its
    whole shutdown retrying an OTLP collector that is not there.  And
    ``targets`` is explicit, so the run does not depend on the implicit-target
    policy.

    Filter values are quoted.  ``filters`` is typed ``dict[str, str]`` and
    pydantic v2 does not coerce a bool back to a string, so an unquoted future
    value of ``on``, ``no``, ``y`` or a bare number would turn a one-word edit
    into a config-validation failure at container start.

    Parameters
    ----------
    namespace : str
        Service namespace, which prefixes every queue name the gates check.
    broker_host : str
        Container DNS name of the broker.  Used twice, and they are different
        things: once for the service's own broker, and once for the monitor's
        independent connection.
    inbound_queue : str
        Queue the monitor consumes notifications from.  Namespaced by the
        caller; the plugin uses this name verbatim and adds no prefix.

    Returns
    -------
    str
        YAML document.
    """
    return textwrap.dedent(
        f"""
        apiVersion: runcourier.dev/v1alpha1
        kind: Service
        metadata:
          name: container-test
          namespace: {namespace}
          description: Queue-driven container-tier pipeline under test.
        spec:
          service_config:
            heartbeat_interval: 2
            tracing_enabled: false
          broker:
            transport: amqp
            host: {broker_host}
            port: 5672
            username: {BROKER_USER}
            password: {BROKER_PASSWORD}
          run:
            - watch-queue:
                kind: data_monitor
                name: rabbit_mq_watcher
                config:
                  rabbitmq_host: {broker_host}
                  rabbitmq_port: 5672
                  rabbitmq_username: {BROKER_USER}
                  rabbitmq_password: {BROKER_PASSWORD}
                  rabbitmq_virtual_host: "/"
                  rabbitmq_queue: {inbound_queue}
                  rabbitmq_prefetch_count: 1
                  max_retries: -1
                  retry_delay_seconds: 2.0
                  retry_backoff_factor: 1.5
                  field_map:
                    location: location
                    dir_path: dir_path
                    file_name: file_name
                    platform: platform_name
                    sensor: source_name
                    time_range_key: time_range
                    time_range_lower_key: lower
                    time_range_start_key: start
                    product: product_name
                  location_format: user_at_host_colon_path
            - create-jobs:
                kind: job_builder
                name: filter_and_group
                config:
                  files_per_job: 1
                  filters:
                    source: "{PLATFORM}"
                    instrument: "{SENSOR}"
                    product: "{PRODUCT}"
                  targets: [process-files]
            - process-files:
                kind: dispatcher
                name: serial_bash
                config:
                  bash_script: |
                    printf '%s %s %s\\n' \\
                      '{{{{ files[0].timestamp }}}}' \\
                      '{{{{ files[0].hostname }}}}' \\
                      '{{{{ files[0].file }}}}' >> {LEDGER}
                    cp {{{{ files[0].file }}}} /data/out/
        """,
    ).lstrip()


def _notification(
    file_name: str,
    platform: str = PLATFORM,
) -> dict[str, object]:
    """Build one file notification in the schema the shipped field map expects.

    Parameters
    ----------
    file_name : str
        Basename the notification points at, relative to the reassembled
        directory.
    platform : str, optional
        Value for ``platform_name``, which becomes ``File.source``.  Default
        :data:`PLATFORM`, the value the builder's filter admits; pass
        :data:`REJECTED_PLATFORM` for a notification the filter must drop.

    Returns
    -------
    dict[str, object]
        Message body, serialised by the publisher.
    """
    return {
        "location": LOCATION,
        "dir_path": DIR_PATH,
        "file_name": file_name,
        "platform_name": platform,
        "source_name": SENSOR,
        "product_name": PRODUCT,
        "time_range": {
            "lower": TIME_RANGE_LOWER,
            "upper": TIME_RANGE_UPPER,
        },
    }


def _ledger(pipeline: Pipeline) -> list[str]:
    """Return the dispatch ledger, one entry per dispatcher execution.

    Parameters
    ----------
    pipeline : Pipeline
        The running pipeline.

    Returns
    -------
    list[str]
        ``"<timestamp> <hostname> <path>"`` per execution, in execution order.
        Empty when nothing has been dispatched yet -- a failure to read the
        volume raises rather than reading as an empty ledger.
    """
    return pipeline.read_lines(LEDGER)


def _drained(pipeline: Pipeline, queues: tuple[str, ...]) -> bool:
    """Return whether every named queue holds no messages at all.

    ``messages`` is ready plus unacknowledged, so this is the assertion that
    each notification was consumed AND acknowledged all the way down the
    chain.  Left unacked under a prefetch of one the monitor would wedge, and
    the message would be redelivered on every reconnect.

    A queue missing from the stats counts as *not* drained, so an unreadable
    broker can never be mistaken for a quiet one.

    Parameters
    ----------
    pipeline : Pipeline
        The running pipeline.
    queues : tuple[str, ...]
        Fully namespaced queue names.

    Returns
    -------
    bool
        ``True`` when every queue reports zero messages.
    """
    stats = pipeline.queue_stats()
    return all(stats.get(name, (1, 0))[0] == 0 for name in queues)


def _diagnosis(pipeline: Pipeline, container: str) -> str:
    """Return everything needed to attribute a failure, in one string.

    A notification the watcher cannot parse is rejected without requeueing and
    leaves only a log line, and a queue-property mismatch kills the listener
    thread and takes the container with it.  Both present as "no output", so
    every assertion in this module reports the container's liveness, the broker
    topology, the ledger and the container's logs rather than just the missing
    file.

    Parameters
    ----------
    pipeline : Pipeline
        The running pipeline.
    container : str
        Courier container name.

    Returns
    -------
    str
        Multi-line failure context.
    """
    return (
        f"running={pipeline.is_running(container)}\n"
        f"queues={pipeline.queue_stats()}\n"
        f"in={pipeline.listdir(RESOLVED_DIR)} out={pipeline.listdir('/data/out')}\n"
        f"ledger={_ledger(pipeline)}\n"
        f"logs:\n{container_logs(container)}"
    )


def test_one_broker_notification_produces_exactly_one_dispatch(
    pipeline: Pipeline,
) -> None:
    """One notification on a queue becomes one job and one dispatch, and no more.

    The first end-to-end run of the monitor the shipped topology uses.  A
    single container runs monitor, builder and dispatcher against the real
    broker; the test publishes two JSON notifications, one the builder's
    filters must admit and one they must reject, and asserts that the ledger
    holds exactly one line.

    Four things make the pass mean something more than "a file appeared".

    The count is exact.  The dispatcher appends a line per execution to a
    ledger outside both data directories, so a second dispatch of the same
    notification is visible -- which it is not in the output directory, where
    the copy has a fixed destination and any number of dispatches leave one
    file.  Both publishes are acknowledged and every queue is drained before
    the count is taken, and the ledger then has to hold still.

    The line's contents pin the parse.  The path is the one the watcher
    reassembled from ``location`` plus ``dir_path`` plus ``file_name``, and
    neither the location's path nor ``dir_path`` is trivial, so a
    concatenate-vs-merge regression produces a path that does not exist and
    the dispatch copies nothing (see :data:`LOCATION`).  The
    hostname is the one the location parser split out.  The timestamp is the
    one the plugin's ``time_range.lower`` default produced, normalised to UTC:
    nothing else downstream reads ``File.timestamp``, so without it in the
    ledger that whole branch could be deleted and this test would not notice.

    The rejected notification gives the filter a negative.  It is published
    first and travels the same queue, so by the time the admitted one has been
    dispatched the builder has already seen and refused it; if the filters
    stopped filtering, it would appear in the ledger and in the output.

    And the builder admits the admitted one only because three values survived
    the trip: ``platform_name`` and ``source_name`` into ``File.source`` and
    ``File.instrument``, and ``product_name`` into ``File.metadata``, which is
    a different layer of the filter and a different branch of the watcher's
    field-map split.  None of the three is recoverable from the path.
    """
    namespace = pipeline.namespace
    inbound_queue = f"{namespace}-inbound"
    expected_name = f"{namespace}-queued.dat"
    rejected_name = f"{namespace}-rejected.dat"
    expected_path = f"{RESOLVED_DIR}/{expected_name}"
    expected_line = f"{EXPECTED_TIMESTAMP} {HOSTNAME} {expected_path}"

    config = _queue_driven_config(namespace, pipeline.broker, inbound_queue)
    container = pipeline.start_courier("queue-driven", config)

    # Topology first: this queue exists only because the service spoke real
    # AMQP during preflight.  On the in-memory fallback the broker stays empty
    # and the run fails here, before a single message is published.
    pipeline.await_queue(f"{namespace}-DispatcherQueue")

    # Then the consumers, which is what makes "reaches the dispatcher" a
    # mechanical claim: the dispatcher's only input is its job-ready queue.
    # A consumer count also subsumes a queue-exists gate -- a queue that does
    # not exist cannot report one consumer -- so the two are not both spent.
    builder_queue = f"{namespace}-FilesFound-create-jobs"
    dispatcher_queue = f"{namespace}-JobReady-process-files"
    pipeline.await_consumers(builder_queue, 1)
    pipeline.await_consumers(dispatcher_queue, 1)

    # And the monitor's own queue, on its own connection.  This gate is unique
    # to this scenario and catches a silently-ignored typo in the monitor's
    # config block: its model ignores unknown keys, so a misspelt
    # ``rabbitmq_queue`` would default to ``file_catalog`` and the watcher
    # would sit listening to a queue nobody ever publishes to.
    pipeline.await_consumers(inbound_queue, 1)

    # seed() asserts its own exit status and the volume is fresh per test, so
    # the two files are known to exist; listing them back would only restate
    # the constants above.
    pipeline.seed(expected_path)
    pipeline.seed(f"{RESOLVED_DIR}/{rejected_name}")

    # Order is the whole negative case.  One queue, prefetch one, so the
    # rejected notification is parsed, published to the fanout and refused by
    # the builder before the admitted one is even read.  When the admitted one
    # reaches the ledger, the rejected one has already had its chance.
    pipeline.publish_message(
        inbound_queue,
        _notification(rejected_name, platform=REJECTED_PLATFORM),
    )
    pipeline.publish_message(inbound_queue, _notification(expected_name))

    queues = (inbound_queue, builder_queue, dispatcher_queue)

    assert poll_until(
        lambda: bool(_ledger(pipeline)),
        timeout=120.0,
        interval=1.0,
    ), f"the notification produced no dispatch:\n{_diagnosis(pipeline, container)}"

    assert poll_until(
        lambda: _drained(pipeline, queues),
        timeout=60.0,
        interval=1.0,
    ), (
        "the run never settled; something is still queued or unacknowledged, "
        f"so no count taken now would describe a finished run:\n"
        f"{_diagnosis(pipeline, container)}"
    )

    ledger = _ledger(pipeline)
    assert ledger == [expected_line], (
        f"expected exactly one dispatch of {expected_line!r}; the ledger holds "
        f"{ledger}. More than one line is a duplicate dispatch; a different "
        f"line is a mis-parsed notification:\n{_diagnosis(pipeline, container)}"
    )

    # Drained queues say nothing is in flight *now*; this says nothing arrives
    # late either, which is the shape a redelivery after a reconnect takes.
    assert stays_false(
        lambda: _ledger(pipeline) != ledger,
        window=10.0,
        interval=1.0,
    ), (
        "the ledger changed after the queues had drained, so the run dispatched "
        f"more than once:\n{_diagnosis(pipeline, container)}"
    )

