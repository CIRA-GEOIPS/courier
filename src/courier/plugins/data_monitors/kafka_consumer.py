"""Consume messages from a Kafka topic and emit them as File objects.

Requires the optional ``data-courier[kafka]`` extra (``kafka-python``).

Messages are expected to be JSON-encoded dicts. A ``field_map`` allows the
operator to translate producer-specific key names into the canonical
``File`` fields. Uses a daemon listener thread and an internal
``queue.Queue[File]`` to bridge the Kafka consumer into the
``DataMonitorBasePlugin`` generator contract, following the same pattern
as :class:`courier.plugins.modules.data_monitors.rabbit_mq_watcher.RabbitMQWatcher`.
"""

from __future__ import annotations

import contextlib
import json
import queue
import threading
import time
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from pydantic import BaseModel, Field, model_validator

from courier.errors import InvalidPluginConfigError
from courier.interfaces.data_monitors import DataMonitorBasePlugin
from courier.metrics import (
    DATA_MONITOR_CONNECTION_STATUS,
    DATA_MONITOR_CONSUMER_LAG,
    DATA_MONITOR_POLL_ERRORS,
)
from courier.types.file import File, parse_location
from courier.utils.datetime_utils import parse_timestamp

if TYPE_CHECKING:
    from collections.abc import Generator

    from courier.service import Service

_DEFAULT_FIELD_MAP: dict[str, str] = {
    "file": "file",
    "hostname": "hostname",
    "source": "source",
    "instrument": "instrument",
    "processing_stage": "processing_stage",
    "domain": "domain",
    "timestamp": "timestamp",
}

_FIELD_MAP_KEYS_EXCLUDED_FROM_METADATA = frozenset(_DEFAULT_FIELD_MAP.keys())

SaslMechanism = Literal["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"]
OffsetReset = Literal["latest", "earliest"]


class KafkaConsumerConfig(BaseModel, frozen=True):
    """Validated configuration for :class:`KafkaConsumer`."""

    bootstrap_servers: list[str] = Field(min_length=1)
    topic: str
    group_id: str
    auto_offset_reset: OffsetReset = "latest"
    field_map: dict[str, str] = Field(default_factory=dict)
    timestamp_format: str | None = None
    sasl_mechanism: SaslMechanism | None = None
    sasl_plain_username: str | None = None
    sasl_plain_password: str | None = None
    security_protocol: Literal[
        "PLAINTEXT",
        "SSL",
        "SASL_PLAINTEXT",
        "SASL_SSL",
    ] = "PLAINTEXT"
    ssl_cafile: str | None = None
    ssl_certfile: str | None = None
    ssl_keyfile: str | None = None
    ssl_check_hostname: bool = True
    poll_timeout_seconds: float = Field(default=1.0, gt=0)
    max_retries: int = Field(default=-1, ge=-1)
    retry_delay_seconds: float = Field(default=2.0, gt=0)
    retry_backoff_factor: float = Field(default=1.5, ge=1.0)
    max_retry_delay_seconds: float = Field(default=60.0, gt=0)
    lag_report_interval_seconds: float = Field(default=30.0, gt=0)

    @model_validator(mode="after")
    def _check_sasl(self) -> KafkaConsumerConfig:
        if self.sasl_mechanism is not None and (
            self.sasl_plain_username is None or self.sasl_plain_password is None
        ):
            msg = (
                "sasl_mechanism requires both sasl_plain_username and "
                "sasl_plain_password to be set"
            )
            raise ValueError(msg)
        return self


class KafkaConsumer(DataMonitorBasePlugin):
    """Consume JSON messages from a Kafka topic and yield ``File`` objects.

    Thread-safe: the listener thread owns the ``kafka.KafkaConsumer``
    exclusively and communicates with :meth:`find_file` via an internal
    ``queue.Queue[File]``. Fatal errors are surfaced on a separate error
    queue and re-raised in the generator thread so dead listeners cannot
    silently stall the pipeline.
    """

    interface: ClassVar[str] = "data_monitors"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "kafka_consumer"
    version: ClassVar[str] = "0.1.0"

    def __init__(
        self,
        service: Service,
        config: dict[str, Any] | None = None,
        identifier: str | None = None,
    ) -> None:
        super().__init__(service, config, identifier=identifier)
        self.validated = KafkaConsumerConfig.model_validate(config or {})
        self.field_map = {**_DEFAULT_FIELD_MAP, **self.validated.field_map}
        self._stop_event = threading.Event()
        self._error_queue: queue.Queue[Exception] = queue.Queue()
        self.health = False
        self._listener_thread: threading.Thread | None = None

    def stop(self) -> None:
        """Signal the listener to exit and join the main thread."""
        self._stop_event.set()
        super().stop()

    def is_healthy(self) -> bool:
        """Return ``True`` while the listener thread is consuming messages."""
        return self.health and (
            self._listener_thread is not None and self._listener_thread.is_alive()
        )

    def _build_consumer(self) -> Any:
        """Create the underlying ``kafka.KafkaConsumer`` (lazy import)."""
        try:
            from kafka import KafkaConsumer as _KafkaConsumer  # noqa: PLC0415
        except ImportError as exc:
            raise InvalidPluginConfigError(
                "kafka_consumer requires the kafka extra: "
                "pip install data-courier[kafka]",
            ) from exc

        kwargs: dict[str, Any] = {
            "bootstrap_servers": self.validated.bootstrap_servers,
            "group_id": self.validated.group_id,
            "auto_offset_reset": self.validated.auto_offset_reset,
            "enable_auto_commit": True,
            "security_protocol": self.validated.security_protocol,
            "consumer_timeout_ms": int(self.validated.poll_timeout_seconds * 1000),
        }
        if self.validated.sasl_mechanism is not None:
            kwargs["sasl_mechanism"] = self.validated.sasl_mechanism
            kwargs["sasl_plain_username"] = self.validated.sasl_plain_username
            kwargs["sasl_plain_password"] = self.validated.sasl_plain_password
        if self.validated.ssl_cafile is not None:
            kwargs["ssl_cafile"] = self.validated.ssl_cafile
        if self.validated.ssl_certfile is not None:
            kwargs["ssl_certfile"] = self.validated.ssl_certfile
        if self.validated.ssl_keyfile is not None:
            kwargs["ssl_keyfile"] = self.validated.ssl_keyfile
        kwargs["ssl_check_hostname"] = self.validated.ssl_check_hostname

        return _KafkaConsumer(self.validated.topic, **kwargs)

    def _message_to_file(self, payload: dict[str, Any]) -> File | None:
        """Translate a decoded message dict into a :class:`File`."""
        fm = self.field_map
        file_raw = payload.get(fm["file"])
        if file_raw is None:
            self._logger.warning(
                f"Kafka message missing required field {fm['file']!r}: {payload!r}",
            )
            return None
        timestamp_raw = payload.get(fm["timestamp"])
        timestamp = (
            parse_timestamp(timestamp_raw, self.validated.timestamp_format)
            if timestamp_raw is not None
            else None
        )
        # Build metadata from user-override field_map entries not mapped to File attrs
        metadata: dict[str, Any] = {}
        for key, msg_key in fm.items():
            if key in _FIELD_MAP_KEYS_EXCLUDED_FROM_METADATA:
                continue
            value = payload.get(msg_key)
            if value is not None:
                metadata[key] = value
        return File(
            # parse_location keeps URIs verbatim and converts plain
            # filesystem paths to Path.
            file=parse_location(str(file_raw)),
            hostname=payload.get(fm["hostname"]),
            source=payload.get(fm["source"]),
            instrument=payload.get(fm["instrument"]),
            processing_stage=payload.get(fm["processing_stage"]),
            domain=payload.get(fm["domain"]),
            timestamp=timestamp,
            metadata=metadata,
        )

    def _decode_value(self, raw: bytes | str | None) -> dict[str, Any] | None:
        """Decode a Kafka message value into a dict, or ``None`` on failure."""
        if raw is None:
            return None
        try:
            text = raw if isinstance(raw, str) else raw.decode("utf-8")
            loaded = json.loads(text)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            self._logger.warning(f"Discarding non-JSON Kafka message: {exc}")
            DATA_MONITOR_POLL_ERRORS.labels(
                monitor_name=self.name,
                monitor_identifier=self.identifier,
                error_type="decode",
            ).inc()
            return None
        if not isinstance(loaded, dict):
            self._logger.warning(
                f"Discarding non-dict Kafka message: {type(loaded).__name__}",
            )
            return None
        return loaded

    def _report_lag(self, consumer: Any) -> None:
        """Read assigned partition end-offsets and publish consumer lag."""
        try:
            assignment = consumer.assignment()
            if not assignment:
                return
            end_offsets = consumer.end_offsets(list(assignment))
            total_lag = 0
            for tp in assignment:
                position = consumer.position(tp)
                end = end_offsets.get(tp, position)
                total_lag += max(0, end - position)
            DATA_MONITOR_CONSUMER_LAG.labels(
                monitor_name=self.name,
                monitor_identifier=self.identifier,
                topic=self.validated.topic,
            ).set(total_lag)
        except (OSError, ValueError) as exc:
            self._logger.debug(f"Failed to report Kafka lag: {exc}")

    def _consume_loop(self, file_queue: queue.Queue[File]) -> None:
        """Run the listener loop, reconnecting on transient errors."""
        try:
            from kafka.errors import KafkaError  # noqa: PLC0415
        except ImportError as exc:
            self._error_queue.put(exc)
            return

        attempt = 0
        delay = self.validated.retry_delay_seconds

        while not self._stop_event.is_set():
            try:
                consumer = self._build_consumer()
            except KafkaError as exc:
                should_stop, attempt, delay = self._handle_connect_error(
                    exc,
                    attempt,
                    delay,
                )
                if should_stop:
                    return
                continue

            DATA_MONITOR_CONNECTION_STATUS.labels(
                monitor_name=self.name,
                monitor_identifier=self.identifier,
            ).set(1)
            attempt = 0
            delay = self.validated.retry_delay_seconds
            try:
                self._poll_until_error(consumer, file_queue, KafkaError)
            finally:
                with contextlib.suppress(KafkaError):
                    consumer.close(autocommit=True)
                DATA_MONITOR_CONNECTION_STATUS.labels(
                    monitor_name=self.name,
                    monitor_identifier=self.identifier,
                ).set(0)

    def _handle_connect_error(
        self,
        exc: Exception,
        attempt: int,
        delay: float,
    ) -> tuple[bool, int, float]:
        """Record a connect failure; return ``(stop, attempt, next_delay)``."""
        attempt += 1
        DATA_MONITOR_POLL_ERRORS.labels(
            monitor_name=self.name,
            monitor_identifier=self.identifier,
            error_type="connect",
        ).inc()
        if self.validated.max_retries != -1 and attempt > self.validated.max_retries:
            self._logger.error(
                f"Kafka connect failed after {attempt} attempts: {exc}",
            )
            self._error_queue.put(exc)
            return True, attempt, delay
        self._logger.warning(
            f"Kafka connect error (attempt {attempt}): {exc}. "
            f"Retrying in {delay:.1f}s...",
        )
        if self._stop_event.wait(timeout=delay):
            return True, attempt, delay
        next_delay = min(
            delay * self.validated.retry_backoff_factor,
            self.validated.max_retry_delay_seconds,
        )
        return False, attempt, next_delay

    def _poll_until_error(
        self,
        consumer: Any,
        file_queue: queue.Queue[File],
        kafka_error_cls: type[Exception],
    ) -> None:
        """Poll the consumer, publishing files until an error or stop event."""
        last_lag_report = 0.0
        try:
            while not self._stop_event.is_set():
                records = consumer.poll(
                    timeout_ms=int(self.validated.poll_timeout_seconds * 1000),
                )
                for partition_records in records.values():
                    for record in partition_records:
                        payload = self._decode_value(record.value)
                        if payload is None:
                            continue
                        file = self._message_to_file(payload)
                        if file is not None:
                            file_queue.put(file)
                now = time.time()
                if now - last_lag_report >= self.validated.lag_report_interval_seconds:
                    self._report_lag(consumer)
                    last_lag_report = now
        except kafka_error_cls as exc:
            self._logger.warning(f"Kafka consume error, reconnecting: {exc}")
            DATA_MONITOR_POLL_ERRORS.labels(
                monitor_name=self.name,
                monitor_identifier=self.identifier,
                error_type="consume",
            ).inc()

    def find_file(self) -> Generator[File, None, None]:
        """Start the listener thread and yield decoded files from its queue."""
        self._stop_event.clear()
        file_queue: queue.Queue[File] = queue.Queue()
        listener = threading.Thread(
            target=self._consume_loop,
            args=(file_queue,),
            daemon=True,
            name=f"kafka-listener-{self.validated.topic}",
        )
        listener.start()
        self._listener_thread = listener

        try:
            self.health = True
            while not self._stop_event.is_set():
                try:
                    yield file_queue.get(timeout=5.0)
                except queue.Empty as e:
                    if not listener.is_alive():
                        try:
                            exc = self._error_queue.get_nowait()
                        except queue.Empty:
                            exc = RuntimeError(
                                "Kafka listener thread exited unexpectedly "
                                "without an error on the queue.",
                            )
                        raise exc from e
        finally:
            self.health = False

