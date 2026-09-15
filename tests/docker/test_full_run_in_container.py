"""Full pipeline runs inside real containers against a real AMQP broker.

Every existing integration test runs on the in-memory transport in a single
process.  Nothing had ever put a message through a real broker, crossed a
container boundary, or exercised ``--only`` outside a mocked plugin registry.

Design notes that keep these honest rather than flaky:

* Readiness is gated on **broker topology** -- the namespaced queues actually
  existing -- not on log text and never on a fixed sleep.  That gate doubles as
  the positive assertion that the pipeline really reached AMQP: a config whose
  broker block is wrong silently falls back to the in-memory transport, and
  those queues would then never appear.
* Data lives on a named volume, seeded and read through ``docker exec``.  The
  filesystem monitor is pure inotify with no start-up scan, and inotify over a
  host bind mount is unreliable on Docker Desktop.
* The containers publish no host ports and reach the broker by container DNS,
  so this tier cannot collide with anything already bound to 5672.
"""

from __future__ import annotations

import textwrap
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests._helpers import poll_until, stays_false
from tests.docker.conftest import (
    BROKER_PASSWORD,
    BROKER_USER,
    container_logs,
    run,
)

pytestmark = pytest.mark.timeout(600)


class Pipeline:
    """Helper owning the containers one test needs.

    Parameters
    ----------
    image : str
        Courier image under test.
    network : str
        User-defined docker network every container joins.
    volume : str
        Named volume mounted at ``/data``.
    tmp_path : Path
        Directory for generated configuration files.
    broker_image : str
        Pre-pulled RabbitMQ image.
    """

    def __init__(
        self,
        image: str,
        network: str,
        volume: str,
        tmp_path: Path,
        broker_image: str,
    ) -> None:
        self.image = image
        self.broker_image = broker_image
        self.network = network
        self.volume = volume
        self.tmp_path = tmp_path
        self.namespace = f"ct{uuid.uuid4().hex[:8]}"
        self.broker = f"rabbit-{uuid.uuid4().hex[:8]}"
        self._containers: list[str] = []

    # -- lifecycle ---------------------------------------------------------

    def start_broker(self) -> None:
        """Start RabbitMQ and block until it answers a ping."""
        result = run(
            [
                "docker", "run", "-d",
                "--name", self.broker,
                "--network", self.network,
                "-e", f"RABBITMQ_DEFAULT_USER={BROKER_USER}",
                "-e", f"RABBITMQ_DEFAULT_PASS={BROKER_PASSWORD}",
                self.broker_image,
            ],
            timeout=300.0,
        )
        assert result.returncode == 0, result.stderr
        self._containers.append(self.broker)

        assert poll_until(self._broker_ready, timeout=180.0, interval=1.0), (
            f"rabbitmq never became ready:\n{container_logs(self.broker)}"
        )

    def _broker_ready(self) -> bool:
        """Return whether the broker answers a diagnostics ping.

        The probe runs as ``rabbitmq``, never as root, and that is load-bearing.
        An Erlang tool run as root before the entrypoint has finished will
        CREATE ``/var/lib/rabbitmq/.erlang.cookie`` owned by root; the broker
        then starts as ``rabbitmq``, cannot read its own cookie, and dies with
        ``eacces``.  Polling from t=0 as root reproduces that every time.
        """
        probe = run(
            [
                "docker", "exec", "-u", "rabbitmq", self.broker,
                "rabbitmq-diagnostics", "-q", "ping",
            ],
            timeout=30.0,
        )
        return probe.returncode == 0

    def start_courier(self, name: str, config: str, only: str | None = None) -> str:
        """Start a courier container running *config*.

        Parameters
        ----------
        name : str
            Container name suffix.
        config : str
            YAML written to the host and mounted read-only.
        only : str or None, optional
            Value for ``--only``.  ``None`` runs every plugin.

        Returns
        -------
        str
            The container name.
        """
        # The watched directory must exist before the monitor starts: it is
        # pure inotify, and watching a missing path is a fatal plugin error.
        self._data_helper()

        config_path = self.tmp_path / f"{name}.yaml"
        config_path.write_text(config)
        container = f"courier-{name}-{uuid.uuid4().hex[:6]}"

        command = [
            "docker", "run", "-d",
            "--name", container,
            "--network", self.network,
            "-v", f"{self.volume}:/data",
            "-v", f"{config_path}:/cfg/service.yaml:ro",
            self.image,
            "courier", "run", "/cfg/service.yaml",
        ]
        if only is not None:
            command += ["--only", only]

        result = run(command)
        assert result.returncode == 0, result.stderr
        self._containers.append(container)
        return container

    def cleanup(self) -> None:
        """Remove every container this pipeline started."""
        for container in reversed(self._containers):
            run(["docker", "rm", "-f", container])
        self._containers.clear()

    # -- observation -------------------------------------------------------

    def queue_names(self) -> set[str]:
        """Return the queue names currently declared on the broker."""
        result = run(
            [
                "docker", "exec", "-u", "rabbitmq", self.broker,
                "rabbitmqctl", "list_queues", "-s", "name",
            ],
            timeout=60.0,
        )
        if result.returncode != 0:
            return set()
        return {line.strip() for line in result.stdout.splitlines() if line.strip()}

    def await_queue_matching(self, fragment: str, timeout: float = 120.0) -> None:
        """Block until some declared queue name contains *fragment*.

        The builder's subscription queue carries a random suffix today, so the
        split-deployment test cannot wait on an exact name.  Waiting on the
        dispatcher's queue is not enough: it exists before the builder has
        bound to the fanout exchange, and a fanout discards anything published
        while nothing is bound.

        Parameters
        ----------
        fragment : str
            Substring the queue name must contain.
        timeout : float, optional
            Seconds to wait.  Default 120.
        """
        assert poll_until(
            lambda: any(fragment in name for name in self.queue_names()),
            timeout=timeout,
        ), (
            f"no queue containing {fragment!r} appeared; "
            f"declared: {sorted(self.queue_names())}"
        )

    def await_queue(self, name: str, timeout: float = 120.0) -> None:
        """Block until *name* exists on the broker.

        Parameters
        ----------
        name : str
            Fully namespaced queue name.
        timeout : float, optional
            Seconds to wait.  Default 120.
        """
        assert poll_until(lambda: name in self.queue_names(), timeout=timeout), (
            f"queue {name!r} never appeared; declared: {sorted(self.queue_names())}"
        )

    def seed(self, path: str, content: str = "x") -> None:
        """Create a file inside the data volume via the broker-side container.

        Parameters
        ----------
        path : str
            Absolute path inside ``/data``.
        content : str, optional
            File contents.  Default ``"x"``.
        """
        helper = self._data_helper()
        result = run(
            ["docker", "exec", helper, "sh", "-c", f"printf '{content}' > {path}"],
        )
        assert result.returncode == 0, result.stderr

    def seed_until(
        self,
        prefix: str,
        timeout: float = 150.0,
        interval: float = 3.0,
    ) -> bool:
        """Drop files named ``<prefix>-N.dat`` until one comes out the far end.

        Two properties of the monitor force this shape.  It is edge-triggered
        inotify with no start-up scan, so anything created before the observer
        is watching is missed permanently, and there is no broker-side signal
        for "the watcher is now watching".  And it handles *creation* events
        only, so rewriting the same path is a modify and would never
        re-trigger -- each attempt must be a new filename.

        Parameters
        ----------
        prefix : str
            Basename stem.  Outputs are matched on the same stem.
        timeout : float, optional
            Seconds to keep trying.  Default 150.
        interval : float, optional
            Seconds to wait for each attempt to land.  Default 3.

        Returns
        -------
        bool
            ``True`` once an output file with this stem exists.
        """
        import time

        def arrived() -> bool:
            return any(n.startswith(prefix) for n in self.listdir("/data/out"))

        attempt = 0
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            attempt += 1
            self.seed(f"/data/in/{prefix}-{attempt}.dat")
            if poll_until(arrived, timeout=interval, interval=0.5):
                return True
        return arrived()

    def listdir(self, path: str) -> list[str]:
        """Return the entries of *path* inside the data volume.

        Parameters
        ----------
        path : str
            Absolute directory path inside ``/data``.

        Returns
        -------
        list[str]
            Entry names, empty when the directory is missing.
        """
        helper = self._data_helper()
        result = run(["docker", "exec", helper, "sh", "-c", f"ls -1 {path} 2>/dev/null"])
        return [line.strip() for line in result.stdout.splitlines() if line.strip()]

    def _data_helper(self) -> str:
        """Return a long-lived container with the data volume mounted."""
        if getattr(self, "_helper", None):
            return self._helper
        name = f"data-{uuid.uuid4().hex[:8]}"
        result = run(
            [
                "docker", "run", "-d", "--name", name,
                "-v", f"{self.volume}:/data",
                self.image, "sleep", "3600",
            ],
        )
        assert result.returncode == 0, result.stderr
        self._containers.append(name)

        # A fresh named volume is owned by root, while the courier image runs
        # unprivileged -- so both the input directory the monitor watches and
        # the output directory the dispatcher writes to must be handed over.
        prepared = run(
            [
                "docker", "exec", "-u", "root", name, "sh", "-c",
                "mkdir -p /data/in /data/out && chown -R 1000:1000 /data",
            ],
        )
        assert prepared.returncode == 0, prepared.stderr
        self._helper = name
        return name


def _config(
    namespace: str,
    broker_host: str,
    *,
    files_per_job: int = 1,
    script: str = "cp {{ files[0].file }} /data/out/",
) -> str:
    """Build a single-pipeline service configuration.

    Parameters
    ----------
    namespace : str
        Service namespace, which prefixes every queue name.
    broker_host : str
        Container DNS name of the broker.
    files_per_job : int, optional
        Files the builder groups before emitting.  Default 1.
    script : str, optional
        Dispatcher bash script.

    Returns
    -------
    str
        YAML document.
    """
    body = textwrap.dedent(
        f"""
        apiVersion: runcourier.dev/v1alpha1
        kind: Service
        metadata:
          name: container-test
          namespace: {namespace}
          description: Container-tier pipeline under test.
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
            - watch-files:
                kind: data_monitor
                name: file_system_poller_watchdog
                config:
                  path: /data/in
            - create-jobs:
                kind: job_builder
                name: filter_and_group
                config:
                  files_per_job: {files_per_job}
                  targets: [process-files]
            - process-files:
                kind: dispatcher
                name: serial_bash
                config:
                  bash_script: |
        """,
    ).strip()
    indented = textwrap.indent(textwrap.dedent(script).strip(), " " * 12)
    return f"{body}\n{indented}\n"


@pytest.fixture
def pipeline(
    docker_image: str,
    docker_network: str,
    docker_volume: str,
    tmp_path: Path,
    broker_image: str,
) -> Iterator[Pipeline]:
    """Provide a pipeline helper and tear its containers down.

    Yields
    ------
    Pipeline
        Helper with the broker already running.
    """
    helper = Pipeline(
        docker_image, docker_network, docker_volume, tmp_path, broker_image,
    )
    try:
        helper.start_broker()
        yield helper
    finally:
        helper.cleanup()


def test_single_container_pipeline_over_real_amqp(pipeline: Pipeline) -> None:
    """A whole pipeline runs in one container against a real broker.

    The first test in this repository to put a message through AMQP rather than
    the in-memory transport, and the first to run the shipped image.
    """
    config = _config(pipeline.namespace, pipeline.broker)
    container = pipeline.start_courier("all", config)

    pipeline.await_queue(f"{pipeline.namespace}-JobReady-process-files")
    pipeline.await_queue(f"{pipeline.namespace}-DispatcherQueue")

    assert pipeline.seed_until("alpha"), (
        f"no output produced:\n{container_logs(container)}"
    )


def test_split_containers_share_one_config_over_amqp(pipeline: Pipeline) -> None:
    """Two containers split one config with ``--only`` and still deliver.

    This is the deployment the project documents, and the first test to cross a
    process *and* a container boundary.  The consumer starts first because a
    fanout exchange discards anything published before a queue is bound.
    """
    config = _config(pipeline.namespace, pipeline.broker)

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
    config = _config(pipeline.namespace, pipeline.broker)
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
    config = _config(pipeline.namespace, pipeline.broker, script=script)
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
