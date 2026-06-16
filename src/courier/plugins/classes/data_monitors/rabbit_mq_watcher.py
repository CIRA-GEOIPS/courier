"""Watches a RabbitMQ queue for new messages and yields them as files."""

from __future__ import annotations

import json
import queue
import re
import threading
import time
import types
from contextlib import suppress
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any, ClassVar, cast

import kombu
from kombu.exceptions import OperationalError
from pydantic import BaseModel, Field, field_validator, model_validator

from courier.interfaces.module_based.data_monitors import DataMonitorBasePlugin
from courier.metrics import RABBITMQ_LAST_FILE_EMITTED_TIMESTAMP
from courier.types.file import File
from courier.utils.datetime_utils import parse_timestamp as _parse_timestamp

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

    from courier.service import Service

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
    if "ceph-IPs" in location:
        return (
            "ceph",
            "/",
        )  # special case for ceph-IPs which don't have a hostname and just use the location as the path  # noqa: E501
    if "@" not in location:
        raise ValueError(
            f"Expected 'user@hostname:/path' location format, got: {location!r}",
        )
    after_at = location.rsplit("@", maxsplit=1)[1]
    if ":" in after_at:
        hostname, _, path = after_at.partition(":")
    elif "=" in after_at:
        hostname, path = after_at.split("=", maxsplit=1)
    else:
        raise ValueError(
            f"Expected colon ':' or '=' separating hostname and path"
            f" in location {location!r}, "
            f"e.g. 'user@host:/path' or 'user@host=/path'",
        )
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


class RabbitMQWatcherConfig(BaseModel, frozen=True):
    """Validated configuration for :class:`RabbitMQWatcher`."""

    rabbitmq_host: str = Field(
        default="localhost",
        description="Hostname of the RabbitMQ broker",
    )
    rabbitmq_port: int = Field(
        default=5672,
        ge=1,
        le=65535,
        description="Port of the RabbitMQ broker",
    )
    rabbitmq_virtual_host: str = Field(default="/", description="Virtual host")
    rabbitmq_queue: str = Field(
        default="file_catalog",
        description="Queue name to consume from",
    )
    rabbitmq_username: str = Field(
        default="guest",
        description="Broker username",
    )
    rabbitmq_password: str = Field(
        default="guest",
        description="Broker password",
    )
    rabbitmq_prefetch_count: int = Field(
        default=1,
        ge=1,
        description="Basic QoS prefetch count",
    )
    max_retries: int = Field(
        default=-1,
        ge=-1,
        description="Maximum reconnection attempts; -1 = forever",
    )
    retry_delay_seconds: float = Field(
        default=2.0,
        gt=0,
        description="Initial delay between reconnect attempts",
    )
    retry_backoff_factor: float = Field(
        default=1.5,
        gt=1.0,
        description="Multiplier for retry delay after each attempt",
    )
    field_map: dict[str, str] = Field(
        default_factory=dict,
        description="Mapping of canonical field names to message keys",
    )
    location_format: str = Field(
        default="user_at_host_colon_path",
        description=(
            "Location parser: 'user_at_host_colon_path', 'hostname_only', or 'regex'"
        ),
    )
    location_format_regex: str | None = Field(
        default=None,
        description=(
            "Regex pattern for location_format='regex' "
            "(required when format is 'regex')"
        ),
    )
    timestamp_field: str | None = Field(
        default=None,
        description="Dot-separated path to timestamp in message dict",
    )
    timestamp_format: str | None = Field(
        default=None,
        description="strptime format for the timestamp value",
    )
    rate_limit_per_second: float = Field(
        default=0.0,
        ge=0,
        description="Maximum number of files per second to yield (0.0 = disabled)",
    )

    @field_validator("location_format")
    @classmethod
    def _validate_location_format(cls, v: str) -> str:
        """Validate that the location format is a known parser."""
        if v not in _LOCATION_PARSERS:
            raise ValueError(
                f"Unknown location_format {v!r}. Valid: {list(_LOCATION_PARSERS)}",
            )
        return v

    @model_validator(mode="after")
    def _require_regex_for_regex_format(self) -> RabbitMQWatcherConfig:
        """location_format_regex is required when location_format is 'regex'."""
        if self.location_format == "regex" and not self.location_format_regex:
            raise ValueError(
                "'location_format_regex' must be set when location_format is 'regex'",
            )
        return self


_DEFAULT_FIELD_MAP: dict[str, str] = {
    "location": "location",
    "dir_path": "dir_path",
    "file_name": "file_name",
    "platform": "platform_name",
    "sensor": "source_name",
    "time_range_key": "time_range",
    "time_range_lower_key": "lower",
    "time_range_start_key": "start",
}

_FIELD_MAP_KEYS_EXCLUDED_FROM_METADATA = frozenset({"platform", "sensor"})


class RabbitMQWatcher(DataMonitorBasePlugin):
    """RabbitMQ Data Monitor Plugin.

    Configuration keys
    ------------------
    rabbitmq_host : str
        Hostname of the RabbitMQ broker.  Default: ``"localhost"``.
    rabbitmq_port : int
        Port of the RabbitMQ broker.  Default: ``5672``.
    rabbitmq_virtual_host : str
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
    rate_limit_per_second : float
        Maximum number of files per second to yield.  ``0.0`` (the default)
        disables rate limiting and preserves the original unbounded behaviour.

    Field Map and Metadata
    ----------------------

    How ``field_map`` works
    ~~~~~~~~~~~~~~~~~~~~~~~
    The ``field_map`` dict maps canonical field names to the actual keys in the
    incoming JSON message.  Default keys from ``_DEFAULT_FIELD_MAP`` map
    directly to :class:`~courier.types.file.File` constructor arguments
    (``platform`` → ``source``, ``sensor`` → ``instrument``).  Extra
    user-override keys — those not present in the default map — are collected
    into the ``metadata`` dict on each ``File`` object.

    Metadata construction
    ~~~~~~~~~~~~~~~~~~~~~
    For each ``field_map`` entry, if the canonical key is in
    ``_FIELD_MAP_KEYS_EXCLUDED_FROM_METADATA`` (currently ``{"platform",
    "sensor"}``), the resolved value is set as a ``File`` constructor argument.
    All other user-override keys are gathered into a ``metadata`` dict and
    passed as ``File(metadata={...})``.

    Debug logging
    ~~~~~~~~~~~~~
    A ``DEBUG``-level log line shows the field_map resolution for every
    message — each canonical key, its resolved value, and whether it was routed
    to a ``File`` attribute or to ``metadata``.

    Usage example
    ~~~~~~~~~~~~~
    A YAML configuration with both default and extra ``field_map`` keys::

        field_map:
          file_name: "custom_file_name_key"
          location: "custom_location_key"
          # platform and sensor use defaults (platform_name, source_name)
    """

    # Class-level name used by the plugin registry
    interface: ClassVar[str] = "data_monitors"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "rabbit_mq_watcher"
    version: ClassVar[str] = "1.0.0"
    _min_sleep_seconds: ClassVar[float] = 0.001

    def __init__(
        self,
        service: Service | types.ModuleType | None = None,
        config: dict | None = None,
    ) -> None:
        super().__init__(service, config)
        if service is None or isinstance(service, types.ModuleType):
            return
        self.validated = RabbitMQWatcherConfig.model_validate(config or {})
        self.health = False

        self.rabbitmq_host = self.validated.rabbitmq_host
        self.rabbitmq_port = self.validated.rabbitmq_port
        self.rabbitmq_virtual_host = self.validated.rabbitmq_virtual_host
        self.rabbitmq_queue = self.validated.rabbitmq_queue
        self.rabbitmq_username = self.validated.rabbitmq_username
        self.rabbitmq_password = self.validated.rabbitmq_password
        self.rabbitmq_prefetch_count = self.validated.rabbitmq_prefetch_count
        self.max_retries = self.validated.max_retries
        self.retry_delay_seconds = self.validated.retry_delay_seconds
        self.retry_backoff_factor = self.validated.retry_backoff_factor
        self.field_map = {**_DEFAULT_FIELD_MAP, **self.validated.field_map}
        # Validate required keys are present after merge (fail fast)
        for required_key in ("location", "platform", "sensor", "time_range_key"):
            if required_key not in self.field_map:
                raise ValueError(
                    f"field_map missing required key {required_key!r}. "
                    f"Available keys: {list(self.field_map.keys())}",
                )
        self.location_format = self.validated.location_format
        self.location_format_regex = self.validated.location_format_regex
        self.timestamp_field = self.validated.timestamp_field
        self.timestamp_format = self.validated.timestamp_format
        self.rate_limit_per_second = self.validated.rate_limit_per_second

        self.last_file_processed_timestamp = RABBITMQ_LAST_FILE_EMITTED_TIMESTAMP

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
            return cast(
                "tuple[str, str]",
                parser(location, pattern=self.location_format_regex),
            )
        return cast("tuple[str, str]", parser(location))

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

    def _build_broker_url(self) -> str:
        """Build the AMQP broker URL from the plugin's connection config."""
        return (
            f"amqp://{self.rabbitmq_username}:{self.rabbitmq_password}"
            f"@{self.rabbitmq_host}:{self.rabbitmq_port}"
            f"{self.rabbitmq_virtual_host}"
        )

    def _listen_to_broker(
        self,
        file_queue: queue.Queue[File],
    ) -> None:
        """Listen to the broker queue and place files onto *file_queue*.

        Reconnects automatically on connection errors according to the
        configured retry policy.  Any fatal error is placed onto
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
                    "Broker consumer stopped unexpectedly; reconnecting...",
                )
            except OperationalError as exc:
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
            except (OSError, ValueError, RuntimeError) as exc:
                self._logger.exception("Unrecoverable error in broker listener")
                self._error_queue.put(exc)
                return

    def _connect_and_consume(self, file_queue: queue.Queue[File]) -> None:  # noqa: PLR0915
        """Open a single broker connection and block until consumption ends."""
        url = self._build_broker_url()
        self._logger.debug(f"Attempting to connect to broker at {url!r}")

        connection = kombu.Connection(url)
        connection.ensure_connection(max_retries=1, interval_start=0, interval_step=0)

        queue_obj = kombu.Queue(self.rabbitmq_queue, durable=True)
        queue_obj = queue_obj.bind(connection)
        queue_obj.declare()

        self._logger.debug(
            f"Connected to broker at {self.rabbitmq_host}:{self.rabbitmq_port}"
            f" (virtual_host={self.rabbitmq_virtual_host!r}), "
            f"waiting for messages from queue {self.rabbitmq_queue!r}...",
        )

        fm = self.field_map

        def callback(body: Any, message: kombu.Message) -> None:
            """Handle an incoming broker message."""
            self._logger.debug(f"Received message: {body!r}")
            try:
                raw = body if isinstance(body, str) else body.decode("utf-8")
                loaded: dict[str, Any] | str = json.loads(raw)
                if isinstance(loaded, str):
                    self._logger.debug(
                        f"Received string instead of dict: {loaded}"
                        " — double-decoding. Producer is sending JSON-encoded strings.",
                    )
                    file_info: dict[str, Any] = json.loads(loaded)
                else:
                    file_info = loaded

                # Debug logging for field_map resolution
                self._logger.debug(
                    "field_map resolution for message: %s",
                    {
                        k: (
                            file_info.get(v),
                            "attr" if k in _DEFAULT_FIELD_MAP else "metadata",
                        )
                        for k, v in fm.items()
                    },
                )

                location: str = file_info.get(fm["location"], "")
                hostname, location_path = self._parse_location(location)

                if fm.get("file_name") is None or fm.get("dir_path") is None:
                    self._logger.debug(
                        "Field map missing 'file_name' or 'dir_path'; ",
                    )
                    if fm.get("file_path") is not None:
                        self._logger.debug(
                            "Received message with file_path but missing "
                            "dir_path/file_name; "
                            "attempting to parse file_path into components.",
                        )
                        full_path_str = file_info.get(fm["file_path"])
                        if full_path_str is None:
                            raise ValueError("file_path key not found in message")  # noqa: TRY301
                        full_path = Path(full_path_str)
                        hostname = (
                            hostname or full_path.parts[0]
                        )  # maybe the hostname is in the path?
                        location_path = "/" + "/".join(full_path.parts[1:-1])
                    else:
                        raise ValueError(  # noqa: TRY301
                            f"Message missing required file path components "
                            f"according to field_map. "
                            f"Got: {file_info!r}, expected keys: "
                            f"'dir_path' and 'file_name'",
                            "or a single 'file_path' key.",
                        )
                else:
                    full_path = Path(
                        PurePosixPath(location_path)
                        / PurePosixPath(file_info[fm["dir_path"]]).relative_to("/")
                        / file_info[fm["file_name"]],
                    )

                timestamp = self._extract_timestamp(file_info)

                # Build metadata from extra field_map entries not directly mapped to File attrs  # noqa: E501
                metadata: dict[str, Any] = {}
                for key, msg_key in fm.items():
                    if key in _FIELD_MAP_KEYS_EXCLUDED_FROM_METADATA:
                        continue
                    if key in _DEFAULT_FIELD_MAP:
                        continue
                    value = file_info.get(msg_key)
                    if value is not None:
                        metadata[key] = value

                file = File(
                    file=full_path,
                    hostname=hostname,
                    source=file_info.get(fm["platform"]),
                    instrument=file_info.get(fm["sensor"])
                    or ("goes18" if "G18" in str(full_path) else None),
                    timestamp=timestamp,
                    metadata=metadata,
                )
                file_queue.put(file)
                message.ack()
            except (ValueError, KeyError, UnicodeDecodeError, AttributeError):
                self._logger.exception(
                    f"Failed to process message; rejecting. Body: {body!r}",
                )
                message.reject(requeue=False)

        with kombu.Consumer(
            connection,
            queues=[queue_obj],
            callbacks=[callback],
            prefetch_count=self.rabbitmq_prefetch_count,
        ):
            while not self._stop_event.is_set():
                with suppress(TimeoutError):
                    connection.drain_events(timeout=1.0)

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
            target=self._listen_to_broker,
            args=(file_queue,),
            daemon=True,
            name=f"broker-listener-{self.rabbitmq_queue}",
        )
        listener.start()
        _last_yield_time = 0.0  # monotonic timestamp for rate limiting
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
                            monitor_identifier=self.identifier,
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
                # Rate limiting: throttle to rate_limit_per_second files/sec
                if self.rate_limit_per_second > 0:
                    interval = 1.0 / self.rate_limit_per_second
                    elapsed = time.monotonic() - _last_yield_time
                    remaining = interval - elapsed
                    if remaining >= self._min_sleep_seconds:
                        time.sleep(remaining)
                    _last_yield_time = time.monotonic()
        finally:
            self.health = False


PLUGIN_CLASS = RabbitMQWatcher
