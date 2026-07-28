"""Poll an S3 bucket on a fixed interval and emit new object keys.

Requires the optional ``courier[s3]`` extra (``boto3`` + ``botocore``).

Configuration lives in :class:`S3PollerConfig`. Credentials can be supplied
explicitly (``aws_access_key_id`` / ``aws_secret_access_key``) or left to
boto3's credential chain (env vars, shared config, IAM role).
"""

from __future__ import annotations

import threading
import time
import types
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import BaseModel, Field, field_validator, model_validator

from courier.errors import InvalidPluginConfigError
from courier.interfaces.module_based.data_monitors import DataMonitorBasePlugin
from courier.metrics import (
    DATA_MONITOR_LAST_SCAN_TIMESTAMP,
    DATA_MONITOR_POLL_ERRORS,
    DATA_MONITOR_SCAN_DURATION,
)
from courier.types.file import File
from courier.utils.deduplication import BoundedSeenSet
from courier.utils.polling import interruptible_sleep

if TYPE_CHECKING:
    from collections.abc import Generator

    from courier.service import Service


class S3PollerConfig(BaseModel, frozen=True):
    """Validated configuration for :class:`S3Poller`."""

    bucket: str
    prefix: str = ""
    suffix_filter: list[str] = Field(default_factory=list)
    region: str = "us-east-1"
    endpoint_url: str | None = None
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    poll_interval_seconds: float = Field(default=60.0, ge=1.0)
    max_seen_keys: int = Field(default=100_000, ge=1)
    run_on_start: bool = True
    ignore_existing: bool = False
    hostname: str = "s3"

    @field_validator("suffix_filter")
    @classmethod
    def _normalize_suffixes(cls, v: list[str]) -> list[str]:
        out: list[str] = []
        for raw in v:
            suffix = raw.lower().strip()
            if suffix and not suffix.startswith("."):
                suffix = "." + suffix
            out.append(suffix)
        return out

    @model_validator(mode="after")
    def _check_credentials(self) -> S3PollerConfig:
        has_key = self.aws_access_key_id is not None
        has_secret = self.aws_secret_access_key is not None
        if has_key != has_secret:
            msg = (
                "aws_access_key_id and aws_secret_access_key must be supplied "
                "together (or both omitted to use the boto3 credential chain)"
            )
            raise ValueError(msg)
        return self


class S3Poller(DataMonitorBasePlugin):
    """Poll an S3 bucket on an interval and emit new object URIs.

    Uses ``BoundedSeenSet`` for in-memory deduplication, keyed by the full
    ``s3://bucket/key`` URI. Restarts re-emit all matching keys unless
    ``ignore_existing=True``.
    """

    interface: ClassVar[str] = "data_monitors"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "s3_poller"
    version: ClassVar[str] = "0.1.0"

    def __init__(
        self,
        service: Service | types.ModuleType | None = None,
        config: dict[str, Any] | None = None,
        identifier: str | None = None,
    ) -> None:
        super().__init__(service, config, identifier=identifier)
        if service is None or isinstance(service, types.ModuleType):
            return
        self.validated = S3PollerConfig.model_validate(config or {})
        self._seen: BoundedSeenSet[str] = BoundedSeenSet(self.validated.max_seen_keys)
        self._stop_event = threading.Event()
        self.health = False

    def stop(self) -> None:
        """Signal the polling loop to exit."""
        self._stop_event.set()
        super().stop()

    def is_healthy(self) -> bool:
        """Return ``True`` while the polling loop is running."""
        return self.health

    def _build_client(self) -> Any:
        """Create the boto3 S3 client. Raises if boto3 is not installed."""
        try:
            import boto3  # noqa: PLC0415
        except ImportError as exc:
            raise InvalidPluginConfigError(
                "s3_poller requires the s3 extra: pip install courier[s3]",
            ) from exc
        kwargs: dict[str, Any] = {"region_name": self.validated.region}
        if self.validated.endpoint_url:
            kwargs["endpoint_url"] = self.validated.endpoint_url
        if self.validated.aws_access_key_id:
            kwargs["aws_access_key_id"] = self.validated.aws_access_key_id
            kwargs["aws_secret_access_key"] = self.validated.aws_secret_access_key
        return boto3.client("s3", **kwargs)

    def _matches_suffix(self, key: str) -> bool:
        suffixes = self.validated.suffix_filter
        if not suffixes:
            return True
        lowered = key.lower()
        return any(lowered.endswith(s) for s in suffixes)

    def _key_to_uri(self, key: str) -> str:
        return f"s3://{self.validated.bucket}/{key}"

    def _scan_bucket(self, client: Any) -> Generator[File, None, None]:
        """Paginate the bucket; yield File objects for unseen keys."""
        scan_start = time.time()
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(
            Bucket=self.validated.bucket,
            Prefix=self.validated.prefix,
        ):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not self._matches_suffix(key):
                    continue
                uri = self._key_to_uri(key)
                if uri in self._seen:
                    continue
                self._seen.add(uri)
                last_modified = obj.get("LastModified")
                timestamp = (
                    last_modified if isinstance(last_modified, datetime) else None
                )
                yield File(
                    # Kept as a str: PurePosixPath would collapse "s3://"
                    # into "s3:/" and corrupt the URI.
                    file=uri,
                    hostname=self.validated.hostname,
                    timestamp=timestamp,
                )
        DATA_MONITOR_LAST_SCAN_TIMESTAMP.labels(
            monitor_name=self.name,
            monitor_identifier=self.identifier,
        ).set(time.time())
        DATA_MONITOR_SCAN_DURATION.labels(
            monitor_name=self.name,
            monitor_identifier=self.identifier,
        ).observe(time.time() - scan_start)

    def _seed_seen(self, client: Any) -> None:
        """Pre-populate the seen-set without yielding."""
        paginator = client.get_paginator("list_objects_v2")
        count = 0
        for page in paginator.paginate(
            Bucket=self.validated.bucket,
            Prefix=self.validated.prefix,
        ):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not self._matches_suffix(key):
                    continue
                self._seen.add(self._key_to_uri(key))
                count += 1
        self._logger.debug(f"Pre-seeded {count} existing S3 keys into seen-set")

    def find_file(self) -> Generator[File, None, None]:
        """Poll the bucket and yield newly discovered object URIs."""
        try:
            from botocore.exceptions import (  # noqa: PLC0415
                ClientError,
                EndpointConnectionError,
            )
        except ImportError as exc:
            raise InvalidPluginConfigError(
                "s3_poller requires the s3 extra: pip install courier[s3]",
            ) from exc

        client = self._build_client()
        try:
            self.health = True

            if self.validated.ignore_existing:
                try:
                    self._seed_seen(client)
                except ClientError as exc:
                    self._handle_client_error(exc)
                    return

            if self.validated.run_on_start:
                try:
                    yield from self._scan_bucket(client)
                except ClientError as exc:
                    self._handle_client_error(exc)
                    return

            while not self._stop_event.is_set():
                if interruptible_sleep(
                    self.validated.poll_interval_seconds,
                    self._stop_event,
                ):
                    return
                try:
                    yield from self._scan_bucket(client)
                except EndpointConnectionError as exc:
                    self._logger.warning(f"S3 endpoint unreachable: {exc}")
                    DATA_MONITOR_POLL_ERRORS.labels(
                        monitor_name=self.name,
                        monitor_identifier=self.identifier,
                        error_type="endpoint_unreachable",
                    ).inc()
                except ClientError as exc:
                    if not self._handle_client_error(exc):
                        return
        finally:
            self.health = False

    def _handle_client_error(self, exc: Exception) -> bool:
        """Return ``True`` if the error is transient; ``False`` if fatal."""
        code = getattr(exc, "response", {}).get("Error", {}).get("Code", "Unknown")
        if code in {"NoSuchBucket", "AccessDenied", "InvalidAccessKeyId"}:
            self._logger.error(f"Fatal S3 error ({code}): {exc}")
            DATA_MONITOR_POLL_ERRORS.labels(
                monitor_name=self.name,
                monitor_identifier=self.identifier,
                error_type=code,
            ).inc()
            raise InvalidPluginConfigError(f"S3 {code}: {exc}") from exc
        self._logger.warning(f"Transient S3 error ({code}): {exc}")
        DATA_MONITOR_POLL_ERRORS.labels(
            monitor_name=self.name,
            monitor_identifier=self.identifier,
            error_type=code,
        ).inc()
        return True


PLUGIN_CLASS = S3Poller
