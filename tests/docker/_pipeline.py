"""Container-pipeline helper shared by every module in the container tier.

* Readiness is gated on the namespaced queues existing on the broker, not on
  log text or a fixed sleep. A config with a wrong broker block falls back to
  the in-memory transport without reporting an error, and those queues then
  never appear, so the gate also shows that the pipeline reached AMQP.
* Data lives on a named volume, seeded and read through ``docker exec``. The
  filesystem monitor is inotify with no start-up scan, and inotify over a host
  bind mount is unreliable on Docker Desktop.
* The containers publish no host ports and reach the broker by container DNS,
  so this tier cannot collide with anything already bound to 5672.
"""

from __future__ import annotations

import json
import textwrap
import time
import uuid
from typing import TYPE_CHECKING, Any

from tests._helpers import poll_until
from tests.docker.conftest import (
    BROKER_PASSWORD,
    BROKER_USER,
    container_logs,
    run,
)

if TYPE_CHECKING:
    from pathlib import Path

#: One line per dispatcher execution, written inside the data volume and
#: outside the watched tree. The shipped scripts append before copying, so a
#: dispatch is recorded even if the copy fails; the serial dispatcher runs one
#: script at a time, so the appends cannot interleave.
LEDGER = "/data/ledger.txt"

#: Fields rabbitmqctl prints per queue: name, messages, consumers.
QUEUE_STATS_FIELDS = 3


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

    def start_broker(self) -> None:
        """Start RabbitMQ and block until it answers a ping."""
        result = run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                self.broker,
                "--network",
                self.network,
                "-e",
                f"RABBITMQ_DEFAULT_USER={BROKER_USER}",
                "-e",
                f"RABBITMQ_DEFAULT_PASS={BROKER_PASSWORD}",
                self.broker_image,
            ],
            timeout=300.0,
        )
        assert result.returncode == 0, result.stderr
        self._containers.append(self.broker)

        assert poll_until(
            self._broker_ready, timeout=180.0, interval=1.0
        ), f"rabbitmq never became ready:\n{container_logs(self.broker)}"

    def _broker_ready(self) -> bool:
        """Return whether the broker answers a diagnostics ping.

        Run as the ``rabbitmq`` user. An Erlang tool run as root before the
        entrypoint finishes creates a root-owned
        ``/var/lib/rabbitmq/.erlang.cookie``, and the broker then cannot read
        its own cookie and exits with ``eacces``.
        """
        probe = run(
            [
                "docker",
                "exec",
                "-u",
                "rabbitmq",
                self.broker,
                "rabbitmq-diagnostics",
                "-q",
                "ping",
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
        # inotify-based, and watching a missing path is a fatal plugin error.
        self._data_helper()

        config_path = self.tmp_path / f"{name}.yaml"
        config_path.write_text(config)
        container = f"courier-{name}-{uuid.uuid4().hex[:6]}"

        command = [
            "docker",
            "run",
            "-d",
            "--name",
            container,
            "--network",
            self.network,
            "-v",
            f"{self.volume}:/data",
            "-v",
            f"{config_path}:/cfg/service.yaml:ro",
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

        Restarting the same container keeps the config mount and the
        container's identity fixed, so a test can attribute a behaviour change
        to the downtime.

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

    def queue_names(self) -> set[str]:
        """Return the queue names currently declared on the broker."""
        result = run(
            [
                "docker",
                "exec",
                "-u",
                "rabbitmq",
                self.broker,
                "rabbitmqctl",
                "list_queues",
                "-s",
                "name",
            ],
            timeout=60.0,
        )
        if result.returncode != 0:
            return set()
        return {line.strip() for line in result.stdout.splitlines() if line.strip()}

    def queue_stats(self) -> dict[str, tuple[int, int]]:
        """Return ``{queue name: (undelivered messages, consumers)}``.

        Both numbers come from one call, so they describe the same instant.

        The count is ``rabbitmqctl``'s ``messages``, which is ready plus
        unacknowledged. A message handed to a consumer that has not yet
        acknowledged it still counts, so a queue reaching zero means the work
        finished.

        A queue with zero consumers and a rising message count is the bug this
        tier exists to prove fixed: before the queue was durable it vanished
        with its consumer and took the backlog with it.

        Returns
        -------
        dict[str, tuple[int, int]]
            Mapping of queue name to undelivered-message count and consumer
            count.  Empty when the broker cannot be queried.
        """
        result = run(
            [
                "docker",
                "exec",
                "-u",
                "rabbitmq",
                self.broker,
                "rabbitmqctl",
                "list_queues",
                "-s",
                "name",
                "messages",
                "consumers",
            ],
            timeout=60.0,
        )
        if result.returncode != 0:
            return {}
        stats: dict[str, tuple[int, int]] = {}
        for line in result.stdout.splitlines():
            fields = line.split()
            if len(fields) != QUEUE_STATS_FIELDS:
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
        assert poll_until(
            lambda: name in self.queue_names(), timeout=timeout
        ), f"queue {name!r} never appeared; declared: {sorted(self.queue_names())}"

    def await_consumers(
        self,
        name: str,
        count: int,
        timeout: float = 120.0,
    ) -> None:
        """Block until *name* reports exactly *count* consumers.

        Scaling tests gate on this instead of on queue existence: a replica
        that has started its process but not yet bound to the queue is
        invisible to every other signal, and files seeded before it binds make
        the test flaky.

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

        The monitor is edge-triggered inotify with no start-up scan, so files
        created before the observer is watching are missed, and nothing on the
        broker reports when it starts watching. It handles creation events
        only, so each attempt must use a new filename; rewriting the same path
        is a modify and never re-triggers.

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

    def _read_from_volume(self, command: str, attempts: int = 3) -> list[str]:
        """Run a read-only shell *command* on the volume and return its lines.

        The ``|| true`` separates two cases. Without it, "the file does not
        exist yet" and "the docker daemon failed" both arrive as a non-zero
        exit with empty output, so a transient exec failure reads as an
        observation of an empty directory. With it, a non-zero return means
        the exec itself failed, and that is retried and then raised.

        Parameters
        ----------
        command : str
            Shell command whose stdout is the observation.
        attempts : int, optional
            Tries before giving up.  Default 3.

        Returns
        -------
        list[str]
            Stripped non-empty output lines.

        Raises
        ------
        RuntimeError
            If the command could not be executed.
        """
        helper = self._data_helper()
        last = ""
        for _ in range(attempts):
            result = run(["docker", "exec", helper, "sh", "-c", f"{command} || true"])
            if result.returncode == 0:
                return [
                    line.strip() for line in result.stdout.splitlines() if line.strip()
                ]
            last = result.stderr.strip()
        raise RuntimeError(
            f"could not read the data volume ({command!r}) after {attempts} "
            f"attempts; last error: {last}",
        )

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
        return self._read_from_volume(f"ls -1 {path} 2>/dev/null")

    def read_lines(self, path: str) -> list[str]:
        """Return the non-empty lines of a text file inside the data volume.

        The output directory cannot tell one dispatch from two: the shipped
        script copies the input to a fixed destination, so a second dispatch
        overwrites the first. A script that appends a line per execution
        leaves the record on the volume, which is reachable only through the
        helper container.

        Parameters
        ----------
        path : str
            Absolute file path inside ``/data``.

        Returns
        -------
        list[str]
            Stripped lines, empty when the file does not exist yet.

        Raises
        ------
        RuntimeError
            If the volume could not be read, so a failed read is never
            mistaken for an empty file.
        """
        return self._read_from_volume(f"cat {path} 2>/dev/null")

    def ledger(self) -> list[str]:
        """Return the dispatch ledger, one entry per dispatcher execution.

        The line format differs per test module; the path is fixed. The
        scripts append before copying, so a dispatch is recorded even when the
        copy then fails, which keeps "nothing was dispatched" distinguishable
        from "the dispatch went wrong".

        Returns
        -------
        list[str]
            One entry per execution, in execution order. Empty when nothing
            has been dispatched yet. A failed read of the volume raises.
        """
        return self.read_lines(LEDGER)

    def drained(self, queues: tuple[str, ...]) -> bool:
        """Return whether every named queue holds no messages.

        ``messages`` counts ready plus unacknowledged, so zero means nothing
        is still in flight. A replica stopped while holding an unacknowledged
        file requeues it, and the redelivery is a duplicate.

        A queue missing from the stats counts as not drained, so an unreadable
        broker is never mistaken for a quiet one.

        Parameters
        ----------
        queues : tuple[str, ...]
            Fully namespaced queue names.

        Returns
        -------
        bool
            ``True`` when every queue reports zero messages.
        """
        stats = self.queue_stats()
        return all(stats.get(name, (1, 0))[0] == 0 for name in queues)

    def seed_many(self, prefix: str, count: int) -> list[str]:
        """Create ``<prefix>-1.dat`` .. ``<prefix>-<count>.dat`` and return them.

        The counted counterpart of :meth:`seed_until`, which creates an unknown
        number of files and so cannot support an exactly-once count. Each phase
        of a test needs its own prefix: the monitor fires on creation only, so
        a reused name is never seen again.

        Parameters
        ----------
        prefix : str
            Basename stem, unique to the phase being measured.
        count : int
            Number of files to create.

        Returns
        -------
        list[str]
            Absolute paths created, in creation order.
        """
        paths = [f"/data/in/{prefix}-{index}.dat" for index in range(1, count + 1)]
        for path in paths:
            self.seed(path)
        return paths

    def scrape_metrics(self, container: str, port: int = 9187) -> str:
        """Return a container's Prometheus exposition text.

        The scrape runs inside the target container. The data helper is
        started without ``--network`` and this tier publishes no host ports,
        so 127.0.0.1 inside the container is the only reachable address. Each
        call is one scrape, so every number read from it describes the same
        instant.

        Parameters
        ----------
        container : str
            Container name to scrape.
        port : int, optional
            Port the service's metrics endpoint listens on.  Default 9187.

        Returns
        -------
        str
            Exposition text, empty when the endpoint could not be read.
        """
        result = run(
            [
                "docker",
                "exec",
                container,
                "python",
                "-c",
                "import urllib.request;"
                "print(urllib.request.urlopen("
                f"'http://127.0.0.1:{port}/metrics', timeout=5).read().decode())",
            ],
            timeout=60.0,
        )
        return result.stdout if result.returncode == 0 else ""

    def publish_message(
        self,
        queue: str,
        payload: dict[str, Any],
        timeout: float = 120.0,
    ) -> None:
        """Publish one JSON notification into a broker queue, and confirm it.

        Used by tests that drive a queue-consuming data monitor, where the
        input to the pipeline is a broker message and not a file creation.

        The publish runs in a one-shot container built from the image under
        test. The broker publishes no host ports and is reached only by
        container DNS on a per-test user-defined network, so the host venv
        cannot reach it, and the data helper is started without ``--network``
        and cannot resolve the broker's name. ``rabbitmqadmin`` inside the
        broker is not an option either: it is a python script in the
        management plugin's web assets, it is not on PATH in the alpine broker
        image, and that image has no interpreter to run it. The image under
        test ships kombu as a hard dependency and has ``python`` on PATH, and
        its entrypoint is ``tini --``, so an arbitrary command runs.

        The body is published as a string. Under kombu's default serializer a
        str is sent as ``text/plain`` and arrives at the consumer as a str,
        which is what courier's own publish path produces and what the
        queue-consuming monitors assume; a dict is tagged
        ``application/json``, arrives as a dict, and is dropped by a consumer
        that calls ``.decode()`` on it. Serialising here keeps callers from
        getting that wrong.

        The queue is declared ``durable=True``, matching the properties the
        monitor declares. A mismatch is an AMQP 406 that escapes the monitor's
        retry handling and kills its listener thread.

        Publisher confirms are requested through the connection's transport
        options. That form makes ``publish`` wait for the broker's
        acknowledgement; ``confirm_select`` on the channel puts it in confirm
        mode but leaves the publish asynchronous, so a rejected message would
        still be reported here as a success.

        Parameters
        ----------
        queue : str
            Fully namespaced queue name to publish into.
        payload : dict[str, Any]
            Notification body, serialised to compact JSON.
        timeout : float, optional
            Seconds to allow the publisher container.  Default 120.
        """
        url = f"amqp://{BROKER_USER}:{BROKER_PASSWORD}@{self.broker}:5672/"
        snippet = textwrap.dedent(
            """
            import os

            import kombu

            target = kombu.Queue(os.environ["QUEUE"], durable=True)
            connection = kombu.Connection(
                os.environ["BROKER_URL"],
                transport_options={"confirm_publish": True},
            )
            with connection as conn:
                conn.ensure_connection(
                    max_retries=10, interval_start=0.5, interval_step=0.5,
                )
                with kombu.Producer(conn) as producer:
                    producer.publish(
                        os.environ["BODY"],
                        routing_key=target.name,
                        exchange="",
                        declare=[target],
                    )
            print("published")
            """,
        )
        publisher = f"publish-{uuid.uuid4().hex[:8]}"
        self._containers.append(publisher)
        result = run(
            [
                "docker",
                "run",
                "--rm",
                "--name",
                publisher,
                "--network",
                self.network,
                "-e",
                f"BROKER_URL={url}",
                "-e",
                f"QUEUE={queue}",
                "-e",
                f"BODY={json.dumps(payload, separators=(',', ':'))}",
                self.image,
                "python",
                "-c",
                snippet,
            ],
            timeout=timeout,
        )
        assert (
            result.returncode == 0
        ), f"publishing to {queue!r} failed:\n{result.stdout}\n{result.stderr}"

    def _data_helper(self) -> str:
        """Return a long-lived container with the data volume mounted."""
        if getattr(self, "_helper", None):
            return self._helper
        name = f"data-{uuid.uuid4().hex[:8]}"
        result = run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                name,
                "-v",
                f"{self.volume}:/data",
                self.image,
                "sleep",
                "3600",
            ],
        )
        assert result.returncode == 0, result.stderr
        self._containers.append(name)

        # A fresh named volume is owned by root and the courier image runs
        # unprivileged, so the input directory the monitor watches and the
        # output directory the dispatcher writes to are chowned here.
        prepared = run(
            [
                "docker",
                "exec",
                "-u",
                "root",
                name,
                "sh",
                "-c",
                "mkdir -p /data/in /data/out && chown -R 1000:1000 /data",
            ],
        )
        assert prepared.returncode == 0, prepared.stderr
        self._helper = name
        return name

    def _metrics_scraper(self) -> str:
        """Return a long-lived container that can reach the pipeline by name.

        The data helper cannot stand in: it is started without ``--network``,
        sits on the default bridge, and cannot resolve a courier container's
        name. :meth:`scrape_metrics` reaches the target's own ``127.0.0.1``
        through ``docker exec``, which a listener bound to the loopback
        interface answers just as happily, so it cannot tell that listener
        from one bound to ``0.0.0.0``.

        Returns
        -------
        str
            Container name, created on first use and reused thereafter.
        """
        if getattr(self, "_scraper", None):
            return self._scraper
        name = f"scrape-{uuid.uuid4().hex[:8]}"
        result = run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                name,
                "--network",
                self.network,
                self.image,
                "sleep",
                "3600",
            ],
        )
        assert result.returncode == 0, result.stderr
        self._containers.append(name)
        self._scraper = name
        return name

    def scrape_over_network(
        self,
        container: str,
        port: int,
        path: str = "/metrics",
    ) -> str:
        """Return *container*'s exposition text, fetched from another container.

        The request addresses the target by its container DNS name, which
        distinguishes a listener on ``0.0.0.0`` from one on the loopback
        interface. No host port is published; the scrape stays inside the
        user-defined network.

        An empty return is ambiguous: a refused connection, a process that
        never started its server, and an empty body all read the same. Callers
        gate on :meth:`await_metrics_endpoint` first, and an assertion that
        something is absent needs a body already known to be non-empty.

        Parameters
        ----------
        container : str
            Container name to scrape, resolved by docker's embedded DNS.
        port : int
            Port the target's metrics endpoint listens on.
        path : str, optional
            Request path.  Default ``"/metrics"``.

        Returns
        -------
        str
            Exposition text, empty when the endpoint could not be read.
        """
        # The URL is passed as its own argv element. ``run`` executes a fixed
        # argv with no shell, so the snippet stays constant and nothing in the
        # address is re-interpreted.
        result = run(
            [
                "docker",
                "exec",
                self._metrics_scraper(),
                "python",
                "-c",
                "import sys, urllib.request;"
                "sys.stdout.write("
                "urllib.request.urlopen(sys.argv[1], timeout=5).read().decode())",
                f"http://{container}:{port}{path}",
            ],
            timeout=60.0,
        )
        return result.stdout if result.returncode == 0 else ""

    def await_metrics_endpoint(
        self,
        container: str,
        port: int,
        timeout: float = 120.0,
    ) -> None:
        """Block until *container* serves at least one courier metric sample.

        The gate is stricter than "the socket answers". ``prometheus_client``
        exports its own ``python_*`` and ``process_*`` collectors, so a
        body-is-non-empty check would pass on a service that registered no
        metrics of its own. Comment lines do not count either: ``# HELP
        courier_...`` is emitted for every declared metric whether or not a
        sample exists.

        Parameters
        ----------
        container : str
            Container name to scrape.
        port : int
            Port the target's metrics endpoint listens on.
        timeout : float, optional
            Seconds to wait.  Default 120.
        """

        def answering() -> bool:
            body = self.scrape_over_network(container, port)
            return any(line.startswith("courier_") for line in body.splitlines())

        assert poll_until(answering, timeout=timeout, interval=1.0), (
            f"no courier metrics were served on {container}:{port} within "
            f"{timeout}s:\n{container_logs(container)}"
        )


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

    # A placeholder line, substituted after the dedent. ``textwrap.dedent``
    # measures the common prefix of the already-interpolated string, so a
    # multi-line value pasted in at column 4 pulls the whole document's
    # indentation to zero and changes what the YAML means.
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


def sample_value(
    exposition: str,
    name: str,
    labels: dict[str, str],
) -> float | None:
    """Return the value of one fully-labelled series, or ``None`` if absent.

    ``None`` and ``0.0`` say different things. A labelled series is
    materialised on first use, so "this queue was never counted" and "this
    queue is counted and currently holds nothing" are different answers.

    The metric name is matched for equality, which keeps the two label families
    of the file-found path apart. ``courier_broker_messages_pending`` is
    labelled with the per-builder queue name, ``courier_broker_messages_sent_total``
    on the next line is labelled with the exchange name, and ``<ns>-FilesFound``
    is a prefix of both, so an ``in`` or ``startswith`` test reads the fixed and
    the pre-fix behaviour identically. Equality also drops the companion series
    ``prometheus_client`` exports beside a counter or a histogram
    (``_created``, ``_bucket``, ``_sum``, ``_count``).

    Comment lines are skipped because ``# HELP`` and ``# TYPE`` carry the name
    of every declared metric, so a substring search over the raw text finds a
    metric that was never recorded.

    Parameters
    ----------
    exposition : str
        Text returned by :meth:`Pipeline.scrape_over_network`.
    name : str
        Full metric name, including any ``_total`` suffix.
    labels : dict[str, str]
        The series' complete label set.  A sample carrying any other label,
        or missing one of these, does not match.  Label values containing a
        comma or a quote are not unquoted correctly; no courier label
        contains one.

    Returns
    -------
    float or None
        The sample value, or ``None`` when no such series exists.
    """
    for raw in exposition.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        head, _, value = line.rpartition(" ")
        series, _, label_text = head.partition("{")
        if series.strip() != name:
            continue
        present: dict[str, str] = {}
        for part in label_text.rstrip("}").split(","):
            key, _, raw_value = part.partition("=")
            if key.strip():
                present[key.strip()] = raw_value.strip().strip('"')
        if present == labels:
            return float(value)
    return None
