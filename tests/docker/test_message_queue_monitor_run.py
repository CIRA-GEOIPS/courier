"""A full pipeline run driven by the message-queue monitor.

Every other full-run test in this tier drives ``file_system_poller_watchdog``:
a file appears on a volume, inotify fires, and the pipeline runs. The
project's own configurations use a different monitor. ``config.yaml`` and
``tests/cira-data-inventory-example.yaml`` both drive ``rabbit_mq_watcher``,
which takes its input from a JSON notification on a broker queue and never
looks at a directory. That path had no end-to-end coverage. The bugs this
module catches all live in the queue-driven topology: a notification that
parses into an unusable File, a location that reassembles into the wrong path,
a timestamp or metadata field dropped between the monitor and the job
builder's filters, one notification dispatched twice, or a monitor that
connects to its own broker while its service never reaches AMQP.

That last one is a false green specific to this scenario. ``rabbit_mq_watcher``
opens its own AMQP connection from its own plugin config, separate from
``spec.broker``. A wrong broker block makes the schema infer the in-memory
transport, and monitor, builder and dispatcher still talk to each other inside
one process, so an output-only assertion would pass without the broker being
involved. Readiness is gated on the namespaced queues appearing on the real
broker, as elsewhere in this tier; those gates are what assert AMQP was the
transport, and log text is no substitute for them.

Counting dispatches needs a ledger. The dispatcher's script copies its input
to a fixed destination, so two dispatches of one notification leave one output
file and ``listdir('/data/out')`` reads the same either way. That is the trap
:meth:`tests.docker._pipeline.Pipeline.read_lines` documents, and the reason
:mod:`tests.docker.test_scaling_mid_flight` appends a line per execution.
Duplicate delivery is a common regression for a queue-driven monitor: a
reconnect with an unacknowledged message, a requeue-on-error branch,
``max_retries: -1``. The script here appends before it copies, and the line it
appends carries the three values that have to survive the trip: the File's
timestamp, its hostname and its reassembled path.

The topology under test is the shipped one, and the two shipped config files
now agree with it. They did not when this module was written.
``timestamp_field: time_range`` was a silent no-op: ``_extract_timestamp``
handed the ``time_range`` dict to
:func:`courier.utils.datetime_utils.parse_timestamp`, which returns ``None``
for anything that is not a string, a number or a datetime, so every File those
configs built carried ``timestamp=None``. ``dir_path: dir_ath`` in
``tests/cira-data-inventory-example.yaml`` mapped the canonical ``dir_path``
onto a message key no producer sends, so the reassembly raised ``KeyError`` on
every message and the callback rejected it without requeue; that config
discarded its entire input stream. Both keys are fixed, the documented
``time_range`` -> ``lower``/``start`` default now runs, the monitor warns once
when a configured ``timestamp_field`` resolves to a container, and
``tests/test_shipped_config_drift.py`` guards both defects.

No revert check applies to this module: it is new coverage for a monitor that
had no full-run test, not a regression guard for the issue #44 durable-queue
fix, so there is no specific edit to name. Three vacuity checks stand in its
place. Each was run against this module and produced the failure described.

* Point ``spec.broker.host`` at a hostname that does not resolve. The service
  never reaches AMQP, ``<ns>-FilesFound-create-jobs`` never appears, and the
  first readiness gate fails.
* Publish the payload as a dict instead of a JSON string. kombu tags it
  ``application/json``, the watcher's callback takes its ``body.decode``
  branch, raises ``AttributeError`` and rejects the message without
  requeueing, and no output is produced.
* Publish the accepted notification twice. Each delivery builds its own File,
  the builder opens a successor job for the second (a fresh identifier, so the
  dispatcher's LRU dedupe does not absorb it), and the ledger holds two lines.
  The exactly-once assertion fails.

No ``seed_until`` retry loop is needed here. The queue is durable and declared
by the publisher as well as the consumer, so a notification published before
the watcher attaches waits. The seeded files only have to exist before the
dispatcher runs.
"""

from __future__ import annotations

import textwrap
from typing import TYPE_CHECKING

import pytest

from tests._helpers import poll_until, stays_false
from tests.docker._pipeline import LEDGER
from tests.docker.conftest import BROKER_PASSWORD, BROKER_USER, container_logs

if TYPE_CHECKING:
    from tests.docker._pipeline import Pipeline

# Six gates and two publishes do not fit the 300s this tier's ini default
# allows.  900 matches the other multi-gate modules in the tier, and the
# session-scoped broker-image pull falls inside the first item's window.
pytestmark = pytest.mark.timeout(900)

#: Platform and sensor the notification carries, two of the three values the
#: job builder filters on.  The watcher infers a platform from a ``G16`` to
#: ``G19`` token in the filename when the message omits one, so a ``goes*``
#: platform here would let the filter pass with ``platform_name`` deleted from
#: the payload.  The generated filenames carry no uppercase ``G``, and nothing
#: infers a sensor.
PLATFORM = "himawari9"
SENSOR = "ahi"

#: The third filter value.  ``product`` is absent from the watcher's default
#: field map, so it is gathered into ``File.metadata`` instead of a ``File``
#: attribute, and the builder matches it through the metadata layer of
#: ``_file_matches_filters``.  Under the default field map that dict is empty,
#: so filtering on ``product`` is what covers the metadata hand-off from the
#: monitor to the builder.
PRODUCT = "l1b-radiance"

#: Platform on the notification the builder must reject.  It travels the same
#: queue, parses the same way and names a file that exists on the volume; the
#: filtered value is its only difference from the admitted notification.
REJECTED_PLATFORM = "goes18"

#: Where the notification claims the file lives, split into two non-trivial
#: halves so the reassembly is pinned.  The watcher concatenates them as
#: ``PurePosixPath(location_path) / PurePosixPath(dir_path).relative_to('/')
#: / file_name``.  A location path of ``/`` would hide a dropped
#: ``relative_to``, since ``PurePosixPath('/') / '/data/in'`` is already
#: ``/data/in``.  With ``/data`` and ``/in``, dropping it yields
#: ``/in/<name>``, which does not exist, and the dispatch copies nothing.
LOCATION = "courier@datanode:/data"
HOSTNAME = "datanode"
DIR_PATH = "/in"

#: The directory the two halves above must reassemble into, and the only place
#: the files are seeded.
RESOLVED_DIR = "/data/in"

#: The notification's ``time_range``.  ``timestamp_field`` is left unset, so
#: the watcher reads ``lower`` through its documented default; ``upper`` is
#: carried to show the default picks the lower bound.  The dispatcher's
#: template renders ``File.to_dict``, so the expected value is what that
#: produces after ``ensure_utc`` tags the naive value as UTC.
TIME_RANGE_LOWER = "2026-01-29T09:10:00"
TIME_RANGE_UPPER = "2026-01-29T09:18:00"
EXPECTED_TIMESTAMP = "2026-01-29T09:10:00+00:00"


def _queue_driven_config(
    namespace: str,
    broker_host: str,
    inbound_queue: str,
) -> str:
    """Build the shipped queue-driven topology as a service configuration.

    The shared :func:`tests.docker._pipeline.build_config` hardcodes the
    filesystem monitor and offers no builder filters, so this module carries
    its own.  The shape follows ``config.yaml`` and
    ``tests/cira-data-inventory-example.yaml``, with four departures.

    ``timestamp_field`` is omitted where the shipped configs set it to
    ``time_range``, which is a silent no-op, so the plugin's documented
    ``time_range.lower`` default runs.  ``field_map`` carries one extra key,
    ``product``, the only way to put anything into ``File.metadata``.
    ``tracing_enabled`` is false; otherwise the process spends its shutdown
    retrying an OTLP collector that is not there.  ``targets`` is explicit, so
    the run does not depend on the implicit-target policy.

    Filter values are quoted because ``filters`` is typed ``dict[str, str]``
    and pydantic v2 does not coerce a bool back to a string.  An unquoted
    ``on``, ``no``, ``y`` or bare number would fail config validation at
    container start.

    Parameters
    ----------
    namespace : str
        Service namespace, which prefixes every queue name the gates check.
    broker_host : str
        Container DNS name of the broker.  Used twice: once for the service's
        own broker, once for the monitor's independent connection.
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


def _diagnosis(pipeline: Pipeline, container: str) -> str:
    """Return everything needed to attribute a failure, in one string.

    Two failures present as "no output".  A notification the watcher cannot
    parse is rejected without requeueing and leaves only a log line, and a
    queue-property mismatch kills the listener thread and takes the container
    with it.  Every assertion in this module therefore reports the container's
    liveness, the broker topology, the ledger and the container's logs.

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
        f"ledger={pipeline.ledger()}\n"
        f"logs:\n{container_logs(container)}"
    )


def test_one_broker_notification_produces_exactly_one_dispatch(
    pipeline: Pipeline,
) -> None:
    """One notification on a queue becomes one job and one dispatch.

    The first end-to-end run of the monitor the shipped topology uses.  A
    single container runs monitor, builder and dispatcher against the real
    broker.  The test publishes two JSON notifications, one the builder's
    filters must admit and one they must reject, and asserts that the ledger
    holds one line.

    The count is exact.  The dispatcher appends a line per execution to a
    ledger outside both data directories, so a second dispatch of the same
    notification is visible there; in the output directory the copy has a
    fixed destination and any number of dispatches leave one file.  Both
    publishes are acknowledged and every queue is drained before the count is
    taken, and the ledger then has to hold still.

    The line's contents pin the parse.  The path is the one the watcher
    reassembled from ``location``, ``dir_path`` and ``file_name``, and neither
    the location's path nor ``dir_path`` is trivial (see :data:`LOCATION`), so
    a concatenate-vs-merge regression produces a path that does not exist and
    the dispatch copies nothing.  The hostname is the one the location parser
    split out.  The timestamp is the one the plugin's ``time_range.lower``
    default produced, normalised to UTC; nothing else downstream reads
    ``File.timestamp``, so the ledger is the only place that branch is covered.

    The rejected notification is published first and travels the same queue, so
    by the time the admitted one has been dispatched the builder has already
    seen and refused it.  The admitted one gets through because three values
    survived the trip: ``platform_name`` and ``source_name`` into
    ``File.source`` and ``File.instrument``, and ``product_name`` into
    ``File.metadata``, which is a different layer of the filter and a different
    branch of the watcher's field-map split.  None of the three is recoverable
    from the path.
    """
    namespace = pipeline.namespace
    inbound_queue = f"{namespace}-inbound"
    expected_name = f"{namespace}-queued.dat"
    rejected_name = f"{namespace}-rejected.dat"
    expected_path = f"{RESOLVED_DIR}/{expected_name}"
    expected_line = f"{EXPECTED_TIMESTAMP} {HOSTNAME} {expected_path}"

    config = _queue_driven_config(namespace, pipeline.broker, inbound_queue)
    container = pipeline.start_courier("queue-driven", config)

    # Topology first.  This queue exists only because the service spoke real
    # AMQP during preflight.  On the in-memory fallback the broker stays empty
    # and the run fails here, before any message is published.
    pipeline.await_queue(f"{namespace}-DispatcherQueue")

    # Then the consumers.  The dispatcher's only input is its job-ready queue,
    # so a consumer on that queue is what "reaches the dispatcher" means here.
    # A consumer count subsumes a queue-exists gate, since a queue that does
    # not exist cannot report one consumer.
    builder_queue = f"{namespace}-FilesFound-create-jobs"
    dispatcher_queue = f"{namespace}-JobReady-process-files"
    pipeline.await_consumers(builder_queue, 1)
    pipeline.await_consumers(dispatcher_queue, 1)

    # And the monitor's own queue, on its own connection.  This gate catches a
    # typo in the monitor's config block, which is otherwise ignored: the
    # model drops unknown keys, so a misspelt ``rabbitmq_queue`` defaults to
    # ``file_catalog`` and the watcher listens to a queue nobody publishes to.
    pipeline.await_consumers(inbound_queue, 1)

    # seed() asserts its own exit status and the volume is fresh per test, so
    # the two files are known to exist without a listing.
    pipeline.seed(expected_path)
    pipeline.seed(f"{RESOLVED_DIR}/{rejected_name}")

    # Publish order carries the negative case.  One queue at prefetch one, so
    # the rejected notification is parsed, published to the fanout and refused
    # by the builder before the admitted one is read.  By the time the admitted
    # one reaches the ledger, the rejected one has had its chance.
    pipeline.publish_message(
        inbound_queue,
        _notification(rejected_name, platform=REJECTED_PLATFORM),
    )
    pipeline.publish_message(inbound_queue, _notification(expected_name))

    queues = (inbound_queue, builder_queue, dispatcher_queue)

    assert poll_until(
        lambda: bool(pipeline.ledger()),
        timeout=120.0,
        interval=1.0,
    ), f"the notification produced no dispatch:\n{_diagnosis(pipeline, container)}"

    assert poll_until(
        lambda: pipeline.drained(queues),
        timeout=60.0,
        interval=1.0,
    ), (
        "the run never settled; something is still queued or unacknowledged, "
        f"so no count taken now would describe a finished run:\n"
        f"{_diagnosis(pipeline, container)}"
    )

    ledger = pipeline.ledger()
    assert ledger == [expected_line], (
        f"expected exactly one dispatch of {expected_line!r}; the ledger holds "
        f"{ledger}. More than one line is a duplicate dispatch; a different "
        f"line is a mis-parsed notification:\n{_diagnosis(pipeline, container)}"
    )

    # Drained queues say nothing is in flight now.  This says nothing arrives
    # late either, which is the shape a redelivery after a reconnect takes.
    assert stays_false(
        lambda: pipeline.ledger() != ledger,
        window=10.0,
        interval=1.0,
    ), (
        "the ledger changed after the queues had drained, so the run dispatched "
        f"more than once:\n{_diagnosis(pipeline, container)}"
    )
