"""Full pipeline runs inside real containers against a real AMQP broker.

Every existing integration test runs on the in-memory transport in a single
process.  Nothing had ever put a message through a real broker, crossed a
container boundary, or exercised ``--only`` outside a mocked plugin registry.

The container plumbing -- readiness gates, the data volume, the broker probe --
lives in :mod:`tests.docker._pipeline`, which documents why each gate is shaped
the way it is.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests._helpers import stays_false
from tests.docker._pipeline import build_config
from tests.docker.conftest import container_logs, run

if TYPE_CHECKING:
    from tests.docker._pipeline import Pipeline

pytestmark = pytest.mark.timeout(600)


def test_single_container_pipeline_over_real_amqp(pipeline: Pipeline) -> None:
    """A whole pipeline runs in one container against a real broker.

    The first test in this repository to put a message through AMQP rather than
    the in-memory transport, and the first to run the shipped image.
    """
    config = build_config(pipeline.namespace, pipeline.broker)
    container = pipeline.start_courier("all", config)

    pipeline.await_queue(f"{pipeline.namespace}-JobReady-process-files")
    pipeline.await_queue(f"{pipeline.namespace}-DispatcherQueue")

    assert pipeline.seed_until("alpha"), (
        f"no output produced:\n{container_logs(container)}"
    )


def test_split_containers_share_one_config_over_amqp(pipeline: Pipeline) -> None:
    """Two containers split one config with ``--only`` and still deliver.

    This is the deployment the project documents, and the first test to cross a
    process *and* a container boundary.

    The consumer is started first here only to keep the test's gates simple.
    It is no longer *required*: since preflight predeclares every builder's
    durable queue, a producer that starts alone publishes into a bound queue
    and nothing is discarded.  ``tests/docker/test_durable_queue_restart.py``
    asserts that directly by starting the producer first.
    """
    config = build_config(pipeline.namespace, pipeline.broker)

    consumer = pipeline.start_courier(
        "consumer", config, only="create-jobs,process-files",
    )
    pipeline.await_queue(f"{pipeline.namespace}-JobReady-process-files")
    pipeline.await_queue_matching(f"{pipeline.namespace}-FilesFound")

    pipeline.start_courier("producer", config, only="watch-files")

    assert pipeline.seed_until("split"), (
        f"split deployment produced nothing:\n{container_logs(consumer)}"
    )


def test_container_exits_promptly_on_sigterm(pipeline: Pipeline) -> None:
    """The container stops well inside the grace period and is never killed.

    Validates the init process: PID 1 does not reap orphans, and the bash
    dispatchers fork ``/bin/bash`` children.  Exit code 137 would mean the
    signal was ignored and docker had to resort to SIGKILL.
    """
    config = build_config(pipeline.namespace, pipeline.broker)
    container = pipeline.start_courier("sigterm", config)

    # Wait for real output first, so every thread is provably live.
    pipeline.await_queue(f"{pipeline.namespace}-JobReady-process-files")
    assert pipeline.seed_until("live"), (
        f"pipeline never produced output:\n{container_logs(container)}"
    )

    stopped = run(["docker", "stop", "--time", "30", container], timeout=60.0)
    assert stopped.returncode == 0, stopped.stderr

    inspected = run(
        ["docker", "inspect", "-f", "{{.State.ExitCode}} {{.State.OOMKilled}}", container],
    )
    exit_code, oom_killed = inspected.stdout.split()
    assert oom_killed == "false"
    assert exit_code != "137", (
        "container ignored SIGTERM and had to be killed; "
        f"logs:\n{container_logs(container)}"
    )


def test_failed_dispatch_does_not_kill_the_service(pipeline: Pipeline) -> None:
    """A dispatcher script exiting non-zero leaves the service running.

    No existing integration test asserts a *failing* dispatch; every one
    asserts the happy path.
    """
    script = (
        "case '{{ files[0].file }}' in *boom*) exit 3;; esac\n"
        "cp {{ files[0].file }} /data/out/\n"
    )
    config = build_config(pipeline.namespace, pipeline.broker, script=script)
    container = pipeline.start_courier("failing", config)

    pipeline.await_queue(f"{pipeline.namespace}-JobReady-process-files")
    pipeline.seed("/data/in/boom-1.dat")

    def exited() -> bool:
        result = run(["docker", "inspect", "-f", "{{.State.Running}}", container])
        return result.stdout.strip() != "true"

    assert stays_false(exited, window=10.0), (
        f"a failing dispatch stopped the service:\n{container_logs(container)}"
    )

    assert pipeline.seed_until("ok"), (
        f"service did not recover after a failed dispatch:\n{container_logs(container)}"
    )
