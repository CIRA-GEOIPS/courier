"""A dispatcher's output re-enters the pipeline and is processed again.

Two pipelines chained through the filesystem: chain one's dispatcher writes
into a directory chain two's monitor is watching. No test in the repository
covered that arrangement, at any tier. The chaining the project documents is a
different code path, which this module switches off.

A unit test cannot stand in for this one. The hand-off crosses a process and a
thread boundary: the first dispatcher forks ``bash``, ``bash`` creates a file,
an inotify observer in another thread has to already be armed on that
directory, and the resulting ``File`` has to survive a round trip through a
fanout exchange before a second builder will look at it.

The bug class this catches is a wired chain that nothing crosses: a second
monitor whose directory did not exist and whose thread died at start-up,
a second builder whose subscription never bound, routing that collapsed both
chains onto one dispatcher, or a ``File`` whose attributes did not survive
serialisation so the second builder rejected everything. Each of those leaves a
service that looks healthy and produces the first artefact.

How the evidence is constructed
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Three sibling directories under ``/data``. ``chain-src`` is seeded by the test
and watched by chain one. ``chain-mid`` is written by chain one's dispatcher and
watched by chain two; it is the re-entry point. ``chain-dst`` is written by
chain two's dispatcher and watched by nobody, which stops the pipeline feeding
itself forever. The observer is scheduled ``recursive=True``, so an output
directory underneath an input directory would feed the pipeline its own
output, so the three are siblings.

The proof lives in the filename. Each dispatcher appends its own suffix, so a
seeded ``relay-3.dat`` can only reach ``chain-dst`` as
``relay-3.dat.stage1.stage2``, created twice, once by each dispatcher, with a
monitor observing in between. Filenames are complete at creation time, and
inotify fires on ``open(O_CREAT)`` before ``cp`` has written a byte, so any
assertion about file *contents* would be a coin flip.

Configuration choices
~~~~~~~~~~~~~~~~~~~~~
``FilesFound`` is a service-wide fanout: every builder sees every monitor's
files regardless of which chain it belongs to. Each monitor therefore stamps a
distinct ``hostname`` and each builder filters on it. The filter keeps chain
two's builder from receiving the seeded file directly, and keeps chain one from
re-ingesting its own output and rewriting it as ``.stage1.stage1`` until the
volume fills. Both halves of that defence are asserted, so neither ``filters:``
block can be deleted without a red test.

``serial_bash``'s ``output_files`` block is left unset on both dispatchers.
Setting it makes a dispatcher publish its outputs straight back to the fanout
exchange, bypassing the filesystem relay this module tests; that is the chaining
the project documents (``sphinx/api-reference/plugins.md``, "Pipeline Feedback
with ``emit_file``").

What this leaves uncovered
~~~~~~~~~~~~~~~~~~~~~~~~~~
The documented path is as covered after this module as it was before it.
``tests/unit_tests/plugins/test_output_scanner.py`` drives
``_scan_and_emit_output_files`` against a mocked ``emit_file`` callback, and the
``if self.validated.output_files`` branch that calls it in
``src/courier/plugins/dispatchers/serial_bash.py`` has never run through a
broker, a container or a real dispatcher.

Exactly-once across a chain is uncovered too. Both scripts name their output
after ``basename "$src"``, so a job dispatched twice at either hop overwrites
its own output and leaves no trace. ``tests/docker/test_scaling_mid_flight.py``
counts duplicate dispatch with a ledger, but for one hop only.

One product observation, found while running the self-check below. Feed both
builders from the *same* file, as either self-check edit does, and the service
stops dispatching. Measured over four minutes: 44 files seen, 88 jobs built and
published to the two ``JobReady`` queues, one job consumed, while every plugin
reported ``RUNNING``, heartbeats continued and the health checks stayed green.
An edit that should surface as a misrouted artefact in seconds therefore
sometimes surfaces as no artefact. The test survives either shape. The wedge
is a bug in ``src/``.

Revert check
~~~~~~~~~~~~
No edit under ``src/`` is caught by this module alone. Chained pipelines predate
and are orthogonal to the durable ``FilesFound`` queues of issue #44, so the gap
here was integration coverage rather than a regression guard. The test does
still gate on the names ``<ns>-FilesFound-build-source`` and
``<ns>-FilesFound-build-relay``, which exist only because of that fix
(``Service._predeclare_target_queues`` and ``constants.file_found_queue_for``).
A revert to server-named exclusive queues would redden the ``await_consumers``
gates: predeclaration creates those two queues through ``add_file_found_queue``
whether or not anything binds to them, so they would exist under the reverted
topology and never gain a consumer. The ``Self-check:`` paragraph on the test
below names the edits to this file that must make it fail.
"""

from __future__ import annotations

import textwrap
import time
from typing import TYPE_CHECKING

import pytest

from tests._helpers import poll_until, stays_false
from tests.docker.conftest import BROKER_PASSWORD, BROKER_USER, container_logs, run

if TYPE_CHECKING:
    from pathlib import Path

    from tests.docker._pipeline import Pipeline

#: Matches the heavier modules of this tier.  This module's internal budgets sum
#: to more than 600 seconds in the worst case: four queue gates, four consumer
#: gates and the seeding loop.  The item timer also spans the session-scoped
#: broker pull and the broker's own readiness poll when this is the first module
#: to run.  At 600 a slow box reports a bare ``Timeout`` instead of the failure
#: messages below.
pytestmark = pytest.mark.timeout(900)

#: Seeded by the test, watched by chain one.
SOURCE_DIR = "/data/chain-src"
#: Written by chain one, watched by chain two.  The re-entry point.
RELAY_DIR = "/data/chain-mid"
#: Written by chain two, watched by nobody.  Terminates the pipeline.
TERMINAL_DIR = "/data/chain-dst"

#: Every script's first line.  It is kept out of the f-strings below because
#: ``{{ }}`` is Jinja2 for the dispatcher to render, and an f-string would
#: consume the braces.
_TAKE_SOURCE = 'src="{{ files[0].file }}"\n'

#: Chain one's dispatcher: copy the job's file into the relay directory under
#: a name that records this stage.  ``basename`` keeps a second seeded file from
#: overwriting the first and preserves the stem the terminal assertion matches
#: on.
STAGE_ONE_SCRIPT = _TAKE_SOURCE + (
    f'cp "$src" "{RELAY_DIR}/$(basename "$src").stage1"\n'
)

#: Chain two's dispatcher: the same shape one directory further on.  The two
#: suffixes compose into a chain of custody: only the first dispatcher can
#: produce ``.stage1``, and only the second can append ``.stage2`` to it.
STAGE_TWO_SCRIPT = _TAKE_SOURCE + (
    f'cp "$src" "{TERMINAL_DIR}/$(basename "$src").stage2"\n'
)


def build_chained_config(namespace: str, broker_host: str) -> str:
    """Build a service configuration holding two chained pipelines.

    Scenario-local, because :func:`tests.docker._pipeline.build_config` builds
    one monitor, one builder and one dispatcher, and can express none of the
    parts that make this a chain: six run steps, three directories, a
    per-monitor ``hostname``, a per-builder ``filters`` block and explicit
    ``targets``.

    Three of those settings are required.  ``targets`` is required once two
    dispatchers exist: a builder that declares none raises
    ``AmbiguousImplicitTargetError`` at preflight, and the container exits
    before declaring a single queue.  ``files_per_job: 1`` is required because
    the default is five, so an unset value means neither builder ever emits.
    ``filters`` keeps the service-wide fanout from delivering every monitor's
    files to every builder.

    Parameters
    ----------
    namespace : str
        Service namespace, which prefixes every queue name.
    broker_host : str
        Container DNS name of the broker.

    Returns
    -------
    str
        YAML document.
    """
    # The scripts are spliced in through placeholder lines after the dedent.
    # ``dedent`` measures the common indentation of the *already interpolated*
    # string, so a multi-line script pasted in at column 4 drags the whole
    # document to column 0 and the YAML then parses as a different pipeline.
    body = textwrap.dedent(
        f"""
        apiVersion: runcourier.dev/v1alpha1
        kind: Service
        metadata:
          name: chained-container-test
          namespace: {namespace}
          description: Two chained pipelines, the second fed by the first.
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
            - watch-source:
                kind: data_monitor
                name: file_system_poller_watchdog
                config:
                  path: {SOURCE_DIR}
                  hostname: source-stage
            - build-source:
                kind: job_builder
                name: filter_and_group
                config:
                  files_per_job: 1
                  filters:
                    hostname: source-stage
                  targets: [dispatch-source]
            - dispatch-source:
                kind: dispatcher
                name: serial_bash
                config:
                  bash_script: |
        __STAGE_ONE__
            - watch-relay:
                kind: data_monitor
                name: file_system_poller_watchdog
                config:
                  path: {RELAY_DIR}
                  hostname: relay-stage
            - build-relay:
                kind: job_builder
                name: filter_and_group
                config:
                  files_per_job: 1
                  filters:
                    hostname: relay-stage
                  targets: [dispatch-relay]
            - dispatch-relay:
                kind: dispatcher
                name: serial_bash
                config:
                  bash_script: |
        __STAGE_TWO__
        """,
    ).strip()
    for marker, script in (
        ("__STAGE_ONE__", STAGE_ONE_SCRIPT),
        ("__STAGE_TWO__", STAGE_TWO_SCRIPT),
    ):
        body = body.replace(
            marker,
            textwrap.indent(textwrap.dedent(script).strip(), " " * 12),
        )
    return f"{body}\n"


def prepare_chain_directories(pipeline: Pipeline, *paths: str) -> None:
    """Create *paths* inside the data volume, owned by the image's user.

    The container tier's data helper prepares ``/data/in`` and ``/data/out``
    only, and it is memoised, so its recursive ``chown`` runs once and does not
    reach a directory created later.  This does its own ``chown``: a fresh named
    volume is root-owned and the image runs as uid 1000, so a directory left
    root-owned is one the dispatcher cannot write into.

    Call this before the service starts.  The filesystem monitor is
    inotify with no start-up scan, ``observer.schedule`` raises on a missing path, and the
    data-monitor base class turns any exception from its find-and-emit loop into
    ``os._exit(1)``, so one absent directory takes the whole container down.

    It uses a one-shot ``docker run -u root`` so that it reaches for no private
    attribute of the shared pipeline helper.

    Parameters
    ----------
    pipeline : Pipeline
        Pipeline whose volume and image are used.
    *paths : str
        Absolute directory paths inside ``/data``.
    """
    joined = " ".join(paths)
    result = run(
        [
            "docker", "run", "--rm", "-u", "root",
            "-v", f"{pipeline.volume}:/data",
            pipeline.image, "sh", "-c",
            f"mkdir -p {joined} && chown 1000:1000 {joined}",
        ],
    )
    assert result.returncode == 0, result.stderr


def seed_until_decided(
    pipeline: Pipeline,
    prefix: str,
    timeout: float = 240.0,
    interval: float = 5.0,
) -> None:
    """Seed fresh files until the run has produced evidence either way.

    The scenario-specific counterpart of
    :meth:`tests.docker._pipeline.Pipeline.seed_until`, which hardcodes
    ``/data/in`` and ``/data/out`` and would poll a directory nothing in this
    scenario ever writes to.

    It stops on a misrouted artefact as readily as on the wanted one, and
    returns nothing; the caller asserts on the directories afterwards.  Waiting
    only for the wanted artefact would spend the whole budget on a run whose
    verdict was already on disk.

    The retry loop exists because neither monitor announces itself.  Both are
    edge-triggered inotify with no start-up scan, so a file created before an
    observer is armed is missed permanently, and the broker carries no signal
    for "the watcher is now watching".  Seeding a fresh name repeatedly is the
    only gate there is.  The name has to be fresh for two reasons: rewriting a
    path is a modify and never re-triggers the monitor, and the dispatcher keys
    its already-executed cache on the source path, so a repeat would be dropped
    as a duplicate even if it were seen.

    Each attempt is given a longer window than the single-chain helper allows,
    because an attempt here has to cross two builder hops and two ``bash`` forks
    before anything can appear.

    Parameters
    ----------
    pipeline : Pipeline
        Pipeline owning the data volume.
    prefix : str
        Basename stem, unique to this test.
    timeout : float, optional
        Seconds to keep trying.  Default 240.
    interval : float, optional
        Seconds to wait for each attempt to traverse both chains.  Default 5.
    """

    def decided() -> bool:
        return bool(
            terminal_artefacts(pipeline, prefix)
            or misrouted_artefacts(pipeline, prefix),
        )

    attempt = 0
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        attempt += 1
        pipeline.seed(f"{SOURCE_DIR}/{prefix}-{attempt}.dat")
        if poll_until(decided, timeout=interval, interval=0.5):
            return


def terminal_artefacts(pipeline: Pipeline, prefix: str) -> list[str]:
    """Return terminal entries that record passage through *both* stages.

    The match is on the per-test stem and the double suffix, so a stray file the
    dispatcher left behind, or a file that short-circuited chain one, cannot
    satisfy the assertion.

    Parameters
    ----------
    pipeline : Pipeline
        Pipeline owning the data volume.
    prefix : str
        Basename stem the seeded files were given.

    Returns
    -------
    list[str]
        Entry names of the form ``<prefix>-N.dat.stage1.stage2``.
    """
    return [
        name
        for name in pipeline.listdir(TERMINAL_DIR)
        if name.startswith(f"{prefix}-") and name.endswith(".dat.stage1.stage2")
    ]


def misrouted_artefacts(pipeline: Pipeline, prefix: str) -> list[str]:
    """Return every entry that exists only because a ``filters:`` block failed.

    There is one shape per half of the defence, and no correctly routed run
    produces either name.

    ``<prefix>-N.dat.stage2`` in the terminal directory carries the second stage
    with no first: chain two's builder was handed the seeded file directly, so
    the service-wide fanout reached across chains.
    ``<prefix>-N.dat.stage1.stage1`` in the relay directory was written by chain
    one's dispatcher from a file chain one's dispatcher had already written.
    That shape has no terminating condition, because every rewrite is a fresh
    creation the relay monitor reports.

    One function covers both, because the callers want the same thing of each:
    stop waiting, and name what went wrong.  Neither can be read off the
    terminal artefact, since a misrouting chain may go on producing the
    legitimate ``<prefix>-N.dat.stage1.stage2`` alongside the garbage.

    Parameters
    ----------
    pipeline : Pipeline
        Pipeline owning the data volume.
    prefix : str
        Basename stem the seeded files were given.

    Returns
    -------
    list[str]
        Offending entry names, empty when both filters held.
    """
    short_circuited = [
        name
        for name in pipeline.listdir(TERMINAL_DIR)
        if name.startswith(f"{prefix}-") and name.endswith(".dat.stage2")
    ]
    recycled = [
        name
        for name in pipeline.listdir(RELAY_DIR)
        if name.startswith(f"{prefix}-") and name.endswith(".stage1.stage1")
    ]
    return short_circuited + recycled


def test_config_with_two_chained_pipelines_is_accepted(
    docker_image: str,
    tmp_path: Path,
) -> None:
    """The generated two-chain document validates inside the shipped image.

    This runs first because it is cheap and the failure it removes is indirect.
    The YAML is assembled from two spliced block scalars, and mis-indentation
    produces a document that still parses, as a different pipeline.  Left to the
    container run, that surfaces two minutes later as a queue that never
    appeared, which reads like a broker problem.  Asking for the image rather
    than the pipeline fixture keeps the cost at nothing: no broker, no volume,
    no network.

    It proves only that the document is well formed.  Routing, targets and
    subscriptions are settled in ``Service.preflight_check``, which validation
    never reaches, so the broker gates in the test below remain the real
    evidence.

    Parameters
    ----------
    docker_image : str
        Image under test.
    tmp_path : Path
        Directory the generated configuration is written to.
    """
    config_path = tmp_path / "chained.yaml"
    config_path.write_text(build_chained_config("ctvalidate", "broker-host"))

    result = run(
        [
            "docker", "run", "--rm",
            "-v", f"{config_path}:/cfg/service.yaml:ro",
            docker_image, "courier", "validate", "/cfg/service.yaml",
        ],
    )

    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
    # A count per kind: the failure mode being excluded is a step that parsed as
    # part of its neighbour and was silently dropped.
    assert "2 data monitors, 2 job builders, 2 dispatchers" in result.stdout, (
        result.stdout
    )


def test_a_dispatchers_output_is_observed_and_processed_again(
    pipeline: Pipeline,
) -> None:
    """A file written by one dispatcher is re-ingested by the next pipeline.

    The terminal artefact carries both stage suffixes, which no single hop can
    produce: chain one's dispatcher is the only thing that can create a
    ``.stage1`` name, and chain two's is the only thing that can append
    ``.stage2`` to one.  Between those two writes sits the property under test,
    a monitor observing a directory another pipeline writes into.

    Self-check: two edits to :func:`build_chained_config`, one per half of the
    ``filters:`` defence.  Both were run, twice each, and both reddened this
    test.  Deleting the block from ``build-relay`` makes the service-wide fanout
    hand the seeded ``chain-src`` file straight to chain two's builder, which
    writes ``<prefix>-N.dat.stage2``.  Deleting it from ``build-source`` makes
    chain one re-ingest its own output as ``<prefix>-N.dat.stage1.stage1``.  The
    negative assertion below names either within seconds, on the runs where the
    file is written.  In half the observed runs it was not: feeding both builders
    from one file wedges dispatch after at most one job (the module docstring
    has the counts), the run produces nothing more, and the *last* assertion is
    what goes red four minutes later.  Both shapes appeared for both edits.

    If either edit does not fail this test, that half of the defence can be
    deleted silently and the terminal assertion no longer demonstrates
    re-ingestion.

    Parameters
    ----------
    pipeline : Pipeline
        Container helper with the broker already running.
    """
    prepare_chain_directories(pipeline, SOURCE_DIR, RELAY_DIR, TERMINAL_DIR)
    config = build_chained_config(pipeline.namespace, pipeline.broker)
    container = pipeline.start_courier("chained", config)

    chain_queues = (
        f"{pipeline.namespace}-FilesFound-build-source",
        f"{pipeline.namespace}-FilesFound-build-relay",
        f"{pipeline.namespace}-JobReady-dispatch-source",
        f"{pipeline.namespace}-JobReady-dispatch-relay",
    )

    # Both chains reached a real broker.  A wrong broker block falls back to the
    # in-memory transport, which runs the whole two-chain pipeline inside one
    # container and declares no queue the broker can see.  Gating on all four
    # queues also catches a step lost during config parsing, which is otherwise
    # indistinguishable from a working service.
    for queue in chain_queues:
        pipeline.await_queue(queue)

    assert pipeline.is_running(container), (
        f"service exited during start-up:\n{container_logs(container)}"
    )

    # Every queue above is predeclared from the YAML before a single plugin
    # thread starts, so all four exist even if neither chain ever attached.  A
    # consumer count is the only broker-visible proof that the plugins are
    # running, and seeding before they bind is how this test would become a
    # flake.  Gating on all four catches a chain one that never bound, which the
    # seeding loop below would otherwise hide for its whole four-minute budget.
    for queue in chain_queues:
        pipeline.await_consumers(queue, 1)

    prefix = "relay"
    seed_until_decided(pipeline, prefix)

    # The negative first: misrouting skips a hop, so it lands first when it
    # happens, and this assertion names the defect where the one below could
    # only report that nothing arrived.  It samples over a settle window because
    # a misrouted file and the legitimate artefact are triggered by the same
    # event, so at the instant the artefact that ended the seeding loop
    # appeared, the competing path can still be one ``bash`` fork behind.
    assert stays_false(
        lambda: bool(misrouted_artefacts(pipeline, prefix)),
        window=15.0,
        interval=1.0,
    ), (
        "a builder consumed the other chain's files: "
        f"{sorted(misrouted_artefacts(pipeline, prefix))}; "
        f"{RELAY_DIR}={pipeline.listdir(RELAY_DIR)} "
        f"{TERMINAL_DIR}={pipeline.listdir(TERMINAL_DIR)}"
    )

    assert terminal_artefacts(pipeline, prefix), (
        "no twice-processed artefact appeared; "
        f"{RELAY_DIR}={pipeline.listdir(RELAY_DIR)} "
        f"{TERMINAL_DIR}={pipeline.listdir(TERMINAL_DIR)}\n"
        f"{container_logs(container)}"
    )

    assert pipeline.is_running(container), (
        f"the chained service died while producing output:\n"
        f"{container_logs(container)}"
    )
