"""Watches a RabbitMQ queue for new messages and yields them as files."""

import json
import queue
import re
import threading
import time
from collections.abc import Callable, Generator
from datetime import datetime
from pathlib import PurePosixPath
from typing import Any

import pika
import pika.exceptions
from prometheus_client import Gauge

from geoips_driver.interfaces.module_based.data_monitors import DataMonitorBasePlugin
from geoips_driver.interfaces.module_based.service import Service
from geoips_driver.types.file import File

interface: str = "data_monitors"
family: str = "standard"
name: str = "rabbit_mq_watcher"

# Registry mapping a format name to a callable (raw_location: str) -> (hostname, path)
_LOCATION_PARSERS: dict[str, Any] = {}


def _register_location_parser(fmt_name: str) -> Callable:
    """Register a location parser under a given format name.

    This is a decorator factory that can be used to register location parsing functions
    """

    def decorator(fn: Callable) -> Callable:
        _LOCATION_PARSERS[fmt_name] = fn
        return fn

    return decorator


@_register_location_parser("user_at_host_colon_path")
def _parse_user_at_host_colon_path(location: str) -> tuple[str, str]:
    """Parse 'user@hostname:/mount/path' format.

    Example: ``admin@28bde7b8-2ab2-11f0-9b65-3cecefb7c814.sat=/:``
    """
    if "@" not in location:
        raise ValueError(
            f"Expected 'user@hostname:/path' location format, got: {location!r}",
        )
    after_at = location.rsplit("@", maxsplit=1)[1]
    if ":" not in after_at:
        raise ValueError(
            f"Expected ':' separating hostname and path in location {location!r}",
        )
    hostname, _, path = after_at.partition(":")
    if not hostname:
        raise ValueError(f"Empty hostname in location {location!r}")
    return hostname, path


@_register_location_parser("hostname_only")
def _parse_hostname_only(location: str) -> tuple[str, str]:
    """Parse a bare hostname with no path component (path defaults to '/')."""
    if not location:
        raise ValueError("Empty location string for 'hostname_only' format")
    return location, "/"


@_register_location_parser("regex")
def _parse_regex(
    location: str,
    *,
    pattern: str,
    hostname_group: str = "hostname",
    path_group: str = "path",
) -> tuple[str, str]:
    """Parse a location string using a named-group regex.

    The regex must contain at least a ``(?P<hostname>...)`` group; the
    ``(?P<path>...)`` group is optional and defaults to ``'/'``.
    """
    m = re.match(pattern, location)
    if not m:
        raise ValueError(
            f"Location {location!r} did not match regex pattern {pattern!r}",
        )
    hostname = m.group(hostname_group)
    try:
        path = m.group(path_group)
    except IndexError:
        path = "/"
    return hostname, path or "/"


def _parse_timestamp(raw: Any, fmt: str | None) -> datetime | None:
    """Parse *raw* into a :class:`datetime`.

    Parameters
    ----------
    raw:
        The raw value extracted from the message.  May be an ISO-8601 string,
        a Unix epoch (int/float), or ``None``.
    fmt:
        Optional ``strptime`` format string.  When ``None`` the value is
        treated as ISO-8601 (or epoch if numeric).
    """
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return datetime.fromtimestamp(float(raw))
    if not isinstance(raw, str):
        return None
    if fmt:
        return datetime.strptime(raw, fmt)
    return datetime.fromisoformat(raw)


class RabbitMQWatcher(DataMonitorBasePlugin):
    """RabbitMQ Data Monitor Plugin.

    Configuration keys
    ------------------
    rabbitmq_host : str
        Hostname of the RabbitMQ broker.  Default: ``"localhost"``.
    rabbitmq_port : int
        Port of the RabbitMQ broker.  Default: ``5672``.
    rabbitmq_vhost : str
        Virtual host.  Default: ``"/"``.
    rabbitmq_queue : str
        Queue name to consume from.  Default: ``"file_catalog"``.
    rabbitmq_username : str
        Broker username.  Default: ``"guest"``.
    rabbitmq_password : str
        Broker password.  Default: ``"guest"``.
    rabbitmq_prefetch_count : int
        Basic QoS prefetch count.  Default: ``1``.
    max_retries : int
        Maximum reconnection attempts before giving up.  ``-1`` means retry
        forever.  Default: ``-1``.
    retry_delay_seconds : float
        Initial delay between reconnection attempts in seconds.  Default: ``2``.
    retry_backoff_factor : float
        Multiplier applied to the delay after each failed attempt.
        Default: ``1.5``.
    field_map : dict
        Mapping of canonical field names to the actual keys present in the
        incoming JSON message.  Canonical keys and their defaults::

            {
                "location":             "location",
                "dir_path":             "dir_path",
                "file_name":            "file_name",
                "platform":             "platform_name",
                "sensor":               "source_name",
                "time_range_key":       "time_range",
                "time_range_lower_key": "lower",
                "time_range_start_key": "start",
            }

    location_format : str
        Name of the location parser to use.  Built-in options:
        ``"user_at_host_colon_path"`` (default), ``"hostname_only"``,
        ``"regex"``.
    location_format_regex : str
        Regex pattern (with named groups ``hostname`` and optionally ``path``)
        used when ``location_format`` is ``"regex"``.
    timestamp_field : str
        Dot-separated path into the message dict from which to read the
        timestamp.  Default: derived from ``field_map`` (``time_range`` →
        ``lower``/``start``).  Set this to a top-level key name (e.g.
        ``"created_at"``) to read a flat timestamp instead.
    timestamp_format : str | None
        ``strptime`` format string for the timestamp value.  When absent the
        value is assumed to be ISO-8601 or a Unix epoch.
    """

    # Class-level name used by the plugin registry
    name = "rabbit_mq_watcher"
    version = "1.0.0"

    def __init__(self, service: Service, config: dict) -> None:
        super().__init__(service, config)
        self.health = False

        self.rabbitmq_host: str = config.get("rabbitmq_host", "localhost")
        self.rabbitmq_port: int = int(config.get("rabbitmq_port", 5672))
        self.rabbitmq_virtual_host: str = config.get("rabbitmq_virtual_host", "/")
        self.rabbitmq_queue: str = config.get("rabbitmq_queue", "file_catalog")
        self.rabbitmq_username: str = config.get("rabbitmq_username", "guest")
        self.rabbitmq_password: str = config.get("rabbitmq_password", "guest")
        self.rabbitmq_prefetch_count: int = int(
            config.get("rabbitmq_prefetch_count", 1),
        )

        self.max_retries: int = int(config.get("max_retries", -1))
        self.retry_delay_seconds: float = float(config.get("retry_delay_seconds", 2.0))
        self.retry_backoff_factor: float = float(
            config.get("retry_backoff_factor", 1.5),
        )

        self.field_map: dict[str, str] = config.get("field_map", {})

        self.location_format: str = config.get(
            "location_format",
            "user_at_host_colon_path",
        )
        if self.location_format not in _LOCATION_PARSERS:
            raise ValueError(
                f"Unknown location_format {self.location_format!r}. "
                f"Valid options: {list(_LOCATION_PARSERS)}",
            )
        self.location_format_regex: str | None = config.get("location_format_regex")
        if self.location_format == "regex" and not self.location_format_regex:
            raise ValueError(
                "'location_format_regex' must be set when location_format is 'regex'",
            )

        self.timestamp_field: str | None = config.get("timestamp_field")
        self.timestamp_format: str | None = config.get("timestamp_format")

        metric_suffix = self.rabbitmq_queue.replace("-", "_").replace(".", "_")
        self.last_file_processed_timestamp = Gauge(
            f"last_file_emitted_timestamp_seconds_{metric_suffix}",
            "Unix timestamp when the last file was processed",
            labelnames=["queue"],
        )

        # Queue used to surface listener thread errors back to the generator
        self._error_queue: queue.Queue[Exception] = queue.Queue()
        self._stop_event = threading.Event()

    def stop(self) -> None:
        """Stop the RabbitMQ watcher."""
        self._stop_event.set()

    def is_healthy(self) -> bool:
        """Return ``True`` while the listener thread is consuming messages."""
        return self.health

    def _parse_location(self, location: str) -> tuple[str, str]:
        """Dispatch to the configured location parser and return (hostname, path)."""
        parser = _LOCATION_PARSERS[self.location_format]
        if self.location_format == "regex":
            return parser(location, pattern=self.location_format_regex)
        return parser(location)

    def _extract_timestamp(self, message: dict[str, Any]) -> datetime | None:
        """Extract and parse a timestamp from *message* using the plugin config."""
        fm = self.field_map

        if self.timestamp_field:
            parts = self.timestamp_field.split(".")
            raw: Any = message
            for part in parts:
                if not isinstance(raw, dict):
                    return None
                raw = raw.get(part)

            # Handle PostgreSQL-style array string:
            # eg. "2026-01-29 09:10:00", "2026-01-29 09:18:00"
            if isinstance(raw, str):
                try:
                    parsed = json.loads(raw)
                    if isinstance(parsed, list) and parsed:
                        raw = parsed[0]  # take the lower bound
                except (json.JSONDecodeError, ValueError):
                    pass  # fall through to _parse_timestamp as-is

            return _parse_timestamp(raw, self.timestamp_format)

        # Default: time_range dict with lower/start sub-keys
        time_range = message.get(fm["time_range_key"])
        if isinstance(time_range, dict):
            raw = time_range.get(fm["time_range_lower_key"]) or time_range.get(
                fm["time_range_start_key"],
            )
            return _parse_timestamp(raw, self.timestamp_format)

        # time_range itself might already be a datetime (e.g. ORM objects)
        if isinstance(time_range, datetime):
            return time_range

        return None

    def _build_connection_params(self) -> pika.ConnectionParameters:
        credentials = pika.PlainCredentials(
            username=self.rabbitmq_username,
            password=self.rabbitmq_password,
        )
        return pika.ConnectionParameters(
            host=self.rabbitmq_host,
            port=self.rabbitmq_port,
            virtual_host=self.rabbitmq_virtual_host,
            credentials=credentials,
        )

    def _listen_to_rabbit_mq(
        self,
        file_queue: queue.Queue[File],
    ) -> None:
        """Listen to RabbitMQ for files and add to **file_queue** as Files.

        Listen to the configured RabbitMQ queue, placing :class:`File` objects
        onto *file_queue*.  Reconnects automatically on connection errors according
        to the configured retry policy.  Any fatal error is placed onto
        ``self._error_queue``.

        Intended to be run in a daemon thread.
        """
        attempt = 0
        delay = self.retry_delay_seconds

        while not self._stop_event.is_set():
            try:
                self._connect_and_consume(file_queue)
                # _connect_and_consume only returns if consuming ends cleanly;
                # treat that as a reason to reconnect as well.
                self._logger.warning(
                    "RabbitMQ consumer stopped unexpectedly; reconnecting...",
                )
            except pika.exceptions.AMQPConnectionError as exc:
                attempt += 1
                if self.max_retries != -1 and attempt > self.max_retries:
                    self._logger.error(  # noqa: TRY400
                        f"Exceeded max_retries ({self.max_retries}). "
                        f"Giving up after error: {exc}",
                    )
                    self._error_queue.put(exc)
                    return
                self._logger.warning(
                    f"Connection error (attempt {attempt}): {exc}. "
                    f"Retrying in {delay:.1f}s...",
                )
                time.sleep(delay)
                delay = min(delay * self.retry_backoff_factor, 60.0)
            except Exception as exc:
                self._logger.exception("Unrecoverable error in RabbitMQ listener")
                self._error_queue.put(exc)
                return

    def _connect_and_consume(self, file_queue: queue.Queue[File]) -> None:
        """Open a single connection+channel and block until consumption ends."""
        params = self._build_connection_params()
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.queue_declare(queue=self.rabbitmq_queue, durable=True)
        self._logger.debug(
            f"Connected to RabbitMQ at {self.rabbitmq_host}:{self.rabbitmq_port}"
            f" (virtual_host={self.rabbitmq_virtual_host!r}), "
            f"waiting for messages from queue {self.rabbitmq_queue!r}...",
        )

        fm = self.field_map

        def callback(
            ch: pika.adapters.blocking_connection.BlockingChannel,
            method: pika.spec.Basic.Deliver,
            properties: pika.spec.BasicProperties,  # noqa: ARG001
            body: bytes,
        ) -> None:
            """Handle an incoming RabbitMQ message."""
            self._logger.debug(f"Received message: {body}")
            try:
                loaded_file_info: dict[str, Any] | str = json.loads(
                    body.decode("utf-8"),
                )
                if isinstance(loaded_file_info, str):
                    self._logger.debug(
                        f"Received string instead of dict: {loaded_file_info}"
                        ".. casting into dict. This means the producer is sending "
                        "JSON-encoded strings instead of objects.",
                    )
                    file_info: dict[str, Any] = json.loads(loaded_file_info)
                else:
                    file_info = loaded_file_info

                location: str = file_info.get(fm["location"], "")
                hostname, location_path = self._parse_location(location)

                full_path = (
                    PurePosixPath(location_path)
                    / PurePosixPath(file_info[fm["dir_path"]]).relative_to("/")
                    / file_info[fm["file_name"]]
                )

                timestamp = self._extract_timestamp(file_info)

                file = File(
                    file=full_path,
                    hostname=hostname,
                    platform=file_info.get(fm["platform"]),
                    sensor=file_info.get(fm["sensor"]),
                    timestamp=timestamp,
                )
                file_queue.put(file)
            except Exception:
                self._logger.exception(
                    "Failed to process message; negative acknowledging. "
                    f"Body: {body!r}",
                )
                ch.basic_nack(
                    delivery_tag=method.delivery_tag,
                    requeue=False,
                )
                return

            ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_qos(prefetch_count=self.rabbitmq_prefetch_count)
        channel.basic_consume(queue=self.rabbitmq_queue, on_message_callback=callback)

        while not self._stop_event.is_set():
            connection.process_data_events(time_limit=1.0)
        channel.stop_consuming()
        connection.close()

    def find_file(self) -> Generator[File, None, None]:
        """Watch the configured RabbitMQ queue and yield :class:`File` objects.

        Starts :meth:`_listen_to_rabbit_mq` in a background daemon thread.
        Thread errors are surfaced here and re-raised so callers are not
        silently blocked forever.  Sets :attr:`health` to ``True`` while
        running and resets it to ``False`` on exit.

        Yields
        ------
        File
            A :class:`File` object constructed from each incoming RabbitMQ message.

        Raises
        ------
        Exception
            Re-raises any fatal exception that occurred in the listener thread.
        """
        self._stop_event.clear()
        file_queue: queue.Queue[File] = queue.Queue()
        listener = threading.Thread(
            target=self._listen_to_rabbit_mq,
            args=(file_queue,),
            daemon=True,
            name=f"rmq-listener-{self.rabbitmq_queue}",
        )
        listener.start()
        try:
            self.health = True
            while not self._stop_event.is_set():
                # Poll with a timeout so we can detect a dead listener thread
                # and surface any errors it placed on the error queue.
                while True:
                    try:
                        yield file_queue.get(timeout=5.0)
                        self.last_file_processed_timestamp.labels(
                            queue=self.rabbitmq_queue,
                        ).set_to_current_time()
                        break
                    except queue.Empty as e:
                        # Check if the listener thread died unexpectedly
                        if not listener.is_alive():
                            try:
                                exc = self._error_queue.get_nowait()
                            except queue.Empty:
                                exc = RuntimeError(
                                    "RabbitMQ listener thread exited unexpectedly "
                                    "without an error on the queue.",
                                )
                            raise exc from e
        finally:
            self.health = False


def call() -> None:
    """Raise error if called directly."""
    raise NotImplementedError("You cannot call this plugin directly.")


if __name__ == "__main__":
    call()
