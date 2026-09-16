"""Container-pipeline helper shared by every module in the container tier.

Extracted from the first full-run module once a second one needed it. Keeping
one helper matters more than it looks: the readiness gates here encode several
properties of the system that are easy to get wrong and that fail as flakes
rather than as errors.

* Readiness is gated on **broker topology** -- the namespaced queues actually
  existing -- not on log text and never on a fixed sleep. That gate doubles as
  the positive assertion that the pipeline really reached AMQP: a config whose
  broker block is wrong silently falls back to the in-memory transport, and
  those queues would then never appear.
* Data lives on a named volume, seeded and read through ``docker exec``. The
  filesystem monitor is pure inotify with no start-up scan, and inotify over a
  host bind mount is unreliable on Docker Desktop.
* The containers publish no host ports and reach the broker by container DNS,
  so this tier cannot collide with anything already bound to 5672.
"""

from __future__ import annotations

import textwrap
import time
import uuid
from typing import TYPE_CHECKING

from tests._helpers import poll_until
from tests.docker.conftest import (
    BROKER_PASSWORD,
    BROKER_USER,
    container_logs,
    run,
)

if TYPE_CHECKING:
    from pathlib import Path


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

    def start_courier(
        self,
        name: str,
        config: str,
        only: str | None = None,
        env: dict[str, str] | None = None,
    ) -> str:
        """Start a courier container running *config*.

        Parameters
        ----------
        name : str
            Container name suffix.
        config : str
            YAML written to the host and mounted read-only.
        only : str or None, optional
            Value for ``--only``.  ``None`` runs every plugin.
        env : dict[str, str] or None, optional
            Extra environment variables for the container.

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
        ]
        for key, value in (env or {}).items():
            command += ["-e", f"{key}={value}"]
        command += [self.image, "courier", "run", "/cfg/service.yaml"]
        if only is not None:
            command += ["--only", only]

        result = run(command)
        assert result.returncode == 0, result.stderr
        self._containers.append(container)
        return container

    def stop_courier(self, container: str, grace: int = 30) -> None:
        """Stop *container*, leaving it removable and restartable.

        Parameters
        ----------
        container : str
            Container name.
        grace : int, optional
            Seconds docker waits before escalating to SIGKILL.  Default 30.
        """
        result = run(
            ["docker", "stop", "--time", str(grace), container],
            timeout=float(grace) + 30.0,
        )
        assert result.returncode == 0, result.stderr

    def restart_courier(self, container: str) -> None:
        """Start a previously stopped container again.

        Restarting the same container rather than creating a new one keeps the
        config mount and the container's identity fixed, so a test can attribute
        a behaviour change to the downtime rather than to a new configuration.

        Parameters
        ----------
        container : str
            Container name.
        """
        result = run(["docker", "start", container])
        assert result.returncode == 0, result.stderr

    def is_running(self, container: str) -> bool:
        """Return whether *container* is currently running.

        Parameters
        ----------
        container : str
            Container name.

        Returns
        -------
        bool
            ``True`` while docker reports the container as running.
        """
        result = run(["docker", "inspect", "-f", "{{.State.Running}}", container])
        return result.stdout.strip() == "true"

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

    def queue_stats(self) -> dict[str, tuple[int, int]]:
        """Return ``{queue name: (ready messages, consumers)}`` for the broker.

        Both numbers come from one call so they describe the same instant. A
        queue that exists with zero consumers and a rising message count is the
        exact shape of the bug this tier exists to prove fixed: before the queue
        was durable it simply vanished with its consumer, taking the backlog.

        Returns
        -------
        dict[str, tuple[int, int]]
            Mapping of queue name to ready-message count and consumer count.
            Empty when the broker cannot be queried.
        """
        result = run(
            [
                "docker", "exec", "-u", "rabbitmq", self.broker,
                "rabbitmqctl", "list_queues", "-s", "name", "messages", "consumers",
            ],
            timeout=60.0,
        )
        if result.returncode != 0:
            return {}
        stats: dict[str, tuple[int, int]] = {}
        for line in result.stdout.splitlines():
            fields = line.split()
            expected = 3
            if len(fields) != expected:
                continue
            name, messages, consumers = fields
            if not messages.isdigit() or not consumers.isdigit():
                continue
            stats[name] = (int(messages), int(consumers))
        return stats

    def await_queue_matching(self, fragment: str, timeout: float = 120.0) -> None:
        """Block until some declared queue name contains *fragment*.

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

    def await_consumers(
        self,
        name: str,
        count: int,
        timeout: float = 120.0,
    ) -> None:
        """Block until *name* reports exactly *count* consumers.

        Scaling tests need this rather than a queue-exists gate: a replica that
        has started its process but not yet bound is invisible to every other
        signal, and seeding files before it binds is how a scaling test turns
        into a flake.

        Parameters
        ----------
        name : str
            Fully namespaced queue name.
        count : int
            Consumer count to wait for.
        timeout : float, optional
            Seconds to wait.  Default 120.
        """
        assert poll_until(
            lambda: self.queue_stats().get(name, (0, -1))[1] == count,
            timeout=timeout,
        ), (
            f"queue {name!r} never reported {count} consumers; "
            f"stats: {self.queue_stats().get(name)}"
        )

    def seed(self, path: str, content: str = "x") -> None:
        """Create a file inside the data volume via a helper container.

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
        result = run(
            ["docker", "exec", helper, "sh", "-c", f"ls -1 {path} 2>/dev/null"],
        )
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


def build_config(
    namespace: str,
    broker_host: str,
    *,
    files_per_job: int = 1,
    script: str = "cp {{ files[0].file }} /data/out/",
    watch_path: str = "/data/in",
    extra_service_config: str = "",
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
    watch_path : str, optional
        Directory the filesystem monitor watches.  Default ``/data/in``.
    extra_service_config : str, optional
        Additional ``service_config`` keys, already indented to zero and joined
        by newlines.  Used to switch on the metrics endpoint.

    Returns
    -------
    str
        YAML document.
    """
    settings = ["heartbeat_interval: 2", "tracing_enabled: false"]
    settings += textwrap.dedent(extra_service_config).strip().splitlines()
    service_config = "\n".join(line for line in settings if line.strip())

    # A placeholder line rather than a direct interpolation: dedent measures
    # the common prefix of the *already interpolated* string, so a multi-line
    # value pasted in at column 4 drags the whole document's indentation to
    # zero and produces YAML that parses as something else entirely.
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
            __SERVICE_CONFIG__
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
                  path: {watch_path}
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
    body = body.replace(
        "    __SERVICE_CONFIG__",
        textwrap.indent(service_config, " " * 4),
    )
    indented = textwrap.indent(textwrap.dedent(script).strip(), " " * 12)
    return f"{body}\n{indented}\n"
