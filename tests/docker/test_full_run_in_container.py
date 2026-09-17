"""Full pipeline runs inside real containers against a real AMQP broker.

The other integration tiers run on the in-memory transport in a single process.

The container plumbing lives in :mod:`tests.docker._pipeline`: readiness gates,
the data volume and the broker probe, with the reasoning for each gate.
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
    """A whole pipeline runs in one container against a real broker."""
    config = build_config(pipeline.namespace, pipeline.broker)
    container = pipeline.start_courier("all", config)

    pipeline.await_queue(f"{pipeline.namespace}-JobReady-process-files")
    pipeline.await_queue(f"{pipeline.namespace}-DispatcherQueue")

    assert pipeline.seed_until("alpha"), (
        f"no output produced:\n{container_logs(container)}"
    )


def test_split_containers_share_one_config_over_amqp(pipeline: Pipeline) -> None:
    """Two containers split one config with ``--only`` and still deliver.

    This split is the deployment the project documents.

    The consumer starts first to keep the gates simple.  Preflight predeclares
    every builder's durable queue, so a producer that starts alone publishes
    into a bound queue and nothing is discarded;
    ``tests/docker/test_durable_queue_restart.py`` starts the producer first.
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
    """The container stops inside the grace period and is never killed.

    Covers the init process.  The bash dispatchers fork ``/bin/bash`` children,
    and PID 1 does not reap orphans.  Exit code 137 means the signal was
    ignored and docker had to send SIGKILL.
    """
    config = build_config(pipeline.namespace, pipeline.broker)
    container = pipeline.start_courier("sigterm", config)

    # Wait for output first, so every thread is running.
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
    """A dispatcher script exiting non-zero leaves the service running."""
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
