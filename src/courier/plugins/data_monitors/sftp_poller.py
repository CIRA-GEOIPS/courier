"""Poll an SFTP server for new files matching a glob pattern.

Requires the optional ``data-courier[sftp]`` extra (``paramiko``).

Configuration lives in :class:`SftpPollerConfig`. Supports either password or
private-key authentication (exactly one must be supplied). The connection is
held open across scans and reconnected with exponential backoff on transient
network errors.
"""

from __future__ import annotations

import contextlib
import fnmatch
import threading
import time
from datetime import datetime
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import BaseModel, Field, model_validator

from courier.errors import InvalidPluginConfigError
from courier.interfaces.data_monitors import DataMonitorBasePlugin
from courier.metrics import (
    DATA_MONITOR_CONNECTION_STATUS,
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


class SftpPollerConfig(BaseModel, frozen=True):
    """Validated configuration for :class:`SftpPoller`."""

    host: str
    port: int = Field(default=22, ge=1, le=65535)
    username: str
    password: str | None = None
    private_key_path: str | None = None
    private_key_passphrase: str | None = None
    remote_path: str = "/"
    glob_pattern: str = "*"
    poll_interval_seconds: float = Field(default=60.0, ge=1.0)
    max_seen_files: int = Field(default=100_000, ge=1)
    run_on_start: bool = True
    ignore_existing: bool = False
    hostname: str | None = None
    connection_timeout_seconds: float = Field(default=30.0, gt=0)
    max_retries: int = Field(default=5, ge=-1)
    retry_delay_seconds: float = Field(default=2.0, gt=0)
    retry_backoff_factor: float = Field(default=1.5, ge=1.0)
    max_retry_delay_seconds: float = Field(default=60.0, gt=0)

    @model_validator(mode="after")
    def _check_auth(self) -> SftpPollerConfig:
        has_password = self.password is not None
        has_key = self.private_key_path is not None
        if has_password == has_key:
            msg = (
                "SftpPollerConfig requires exactly one of 'password' or "
                "'private_key_path' (received "
                f"password={'set' if has_password else 'unset'}, "
                f"private_key_path={'set' if has_key else 'unset'})"
            )
            raise ValueError(msg)
        return self


class SftpPoller(DataMonitorBasePlugin):
    """Poll an SFTP server on an interval and emit new file URIs.

    Uses ``BoundedSeenSet`` for in-memory deduplication keyed by the full
    ``sftp://user@host:port/path`` URI. The SFTP connection is held open
    across scans to amortize authentication cost and is reconnected with
    exponential backoff on transient ``SSHException`` / ``EOFError``.
    """

    interface: ClassVar[str] = "data_monitors"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "sftp_poller"
    version: ClassVar[str] = "0.1.0"

    def __init__(
        self,
        service: Service,
        config: dict[str, Any] | None = None,
        identifier: str | None = None,
    ) -> None:
        super().__init__(service, config, identifier=identifier)
        self.validated = SftpPollerConfig.model_validate(config or {})
        self._seen: BoundedSeenSet[str] = BoundedSeenSet(self.validated.max_seen_files)
        self._stop_event = threading.Event()
        self.health = False
        self._client: Any = None
        self._sftp: Any = None

    def stop(self) -> None:
        """Signal the polling loop to exit."""
        self._stop_event.set()
        super().stop()

    def is_healthy(self) -> bool:
        """Return ``True`` while the polling loop is running."""
        return self.health

    @property
    def _hostname_label(self) -> str:
        return self.validated.hostname or self.validated.host

    def _uri_for(self, path: str) -> str:
        return (
            f"sftp://{self.validated.username}@{self.validated.host}:"
            f"{self.validated.port}{path}"
        )

    def _connect(self) -> None:
        """Open a paramiko SSH/SFTP connection. Raises on fatal errors."""
        try:
            import paramiko  # noqa: PLC0415
        except ImportError as exc:
            raise InvalidPluginConfigError(
                "sftp_poller requires the sftp extra: pip install data-courier[sftp]",
            ) from exc

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # noqa: S507

        connect_kwargs: dict[str, Any] = {
            "hostname": self.validated.host,
            "port": self.validated.port,
            "username": self.validated.username,
            "timeout": self.validated.connection_timeout_seconds,
            "allow_agent": False,
            "look_for_keys": False,
        }
        if self.validated.password is not None:
            connect_kwargs["password"] = self.validated.password
        else:
            key = self._load_private_key(paramiko)
            connect_kwargs["pkey"] = key

        client.connect(**connect_kwargs)
        self._client = client
        self._sftp = client.open_sftp()
        DATA_MONITOR_CONNECTION_STATUS.labels(
            monitor_name=self.name,
            monitor_identifier=self.identifier,
        ).set(1)
        self._logger.info(
            f"Connected to sftp://{self.validated.username}@"
            f"{self.validated.host}:{self.validated.port}",
        )

    def _load_private_key(self, paramiko: Any) -> Any:
        """Load an RSA/Ed25519/ECDSA private key from the configured path."""
        path = self.validated.private_key_path
        passphrase = self.validated.private_key_passphrase
        for key_cls in (
            paramiko.Ed25519Key,
            paramiko.ECDSAKey,
            paramiko.RSAKey,
            paramiko.DSSKey,
        ):
            try:
                return key_cls.from_private_key_file(path, password=passphrase)
            except paramiko.SSHException:
                continue
        raise InvalidPluginConfigError(
            f"Could not load private key at {path!r} as any known key type",
        )

    def _disconnect(self) -> None:
        """Close the SFTP channel and SSH transport, swallowing errors."""
        if self._sftp is not None:
            with contextlib.suppress(OSError):
                self._sftp.close()
            self._sftp = None
        if self._client is not None:
            with contextlib.suppress(OSError):
                self._client.close()
            self._client = None
        DATA_MONITOR_CONNECTION_STATUS.labels(
            monitor_name=self.name,
            monitor_identifier=self.identifier,
        ).set(0)

    def _matches_pattern(self, filename: str) -> bool:
        return fnmatch.fnmatchcase(filename, self.validated.glob_pattern)

    def _scan_remote(self) -> Generator[File, None, None]:
        """List the remote directory; yield File objects for unseen matches."""
        if self._sftp is None:
            return
        scan_start = time.time()
        entries = self._sftp.listdir_attr(self.validated.remote_path)
        for entry in entries:
            filename = entry.filename
            if not self._matches_pattern(filename):
                continue
            full_path = str(
                PurePosixPath(self.validated.remote_path) / filename,
            )
            uri = self._uri_for(full_path)
            if uri in self._seen:
                continue
            self._seen.add(uri)
            mtime = getattr(entry, "st_mtime", None)
            timestamp = (
                datetime.fromtimestamp(mtime)
                if isinstance(mtime, int | float)
                else None
            )
            yield File(
                # Kept as a str: PurePosixPath would collapse "sftp://"
                # into "sftp:/" and corrupt the URI.
                file=uri,
                hostname=self._hostname_label,
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

    def _seed_seen(self) -> None:
        """Pre-populate the seen-set without yielding."""
        if self._sftp is None:
            return
        entries = self._sftp.listdir_attr(self.validated.remote_path)
        count = 0
        for entry in entries:
            if not self._matches_pattern(entry.filename):
                continue
            full_path = str(
                PurePosixPath(self.validated.remote_path) / entry.filename,
            )
            self._seen.add(self._uri_for(full_path))
            count += 1
        self._logger.debug(f"Pre-seeded {count} existing SFTP files into seen-set")

    def _connect_with_retries(self, paramiko_exc: Any) -> bool:
        """Try to connect up to ``max_retries`` times. Returns True on success."""
        attempt = 0
        delay = self.validated.retry_delay_seconds
        while not self._stop_event.is_set():
            try:
                self._connect()
            except paramiko_exc.AuthenticationException as exc:
                DATA_MONITOR_POLL_ERRORS.labels(
                    monitor_name=self.name,
                    monitor_identifier=self.identifier,
                    error_type="authentication",
                ).inc()
                raise InvalidPluginConfigError(
                    f"SFTP authentication failed for "
                    f"{self.validated.username}@{self.validated.host}: {exc}",
                ) from exc
            except (paramiko_exc.SSHException, OSError, EOFError) as exc:
                attempt += 1
                DATA_MONITOR_POLL_ERRORS.labels(
                    monitor_name=self.name,
                    monitor_identifier=self.identifier,
                    error_type="connection",
                ).inc()
                if (
                    self.validated.max_retries != -1
                    and attempt > self.validated.max_retries
                ):
                    self._logger.error(  # noqa: TRY400
                        f"SFTP connect failed after {attempt} attempts: {exc}",
                    )
                    return False
                self._logger.warning(
                    f"SFTP connect error (attempt {attempt}): {exc}. "
                    f"Retrying in {delay:.1f}s...",
                )
                if interruptible_sleep(delay, self._stop_event):
                    return False
                delay = min(
                    delay * self.validated.retry_backoff_factor,
                    self.validated.max_retry_delay_seconds,
                )
            else:
                return True
        return False

    def find_file(self) -> Generator[File, None, None]:
        """Poll the SFTP server and yield newly discovered file URIs."""
        try:
            import paramiko  # noqa: PLC0415
        except ImportError as exc:
            raise InvalidPluginConfigError(
                "sftp_poller requires the sftp extra: pip install data-courier[sftp]",
            ) from exc

        try:
            self.health = True
            if not self._connect_with_retries(paramiko):
                return

            if self.validated.ignore_existing:
                try:
                    self._seed_seen()
                except (paramiko.SSHException, OSError) as exc:
                    self._logger.warning(f"Failed to seed seen-set: {exc}")

            if self.validated.run_on_start:
                try:
                    yield from self._scan_remote()
                except (paramiko.SSHException, OSError) as exc:
                    self._logger.warning(f"Initial SFTP scan failed: {exc}")
                    self._disconnect()

            while not self._stop_event.is_set():
                if interruptible_sleep(
                    self.validated.poll_interval_seconds,
                    self._stop_event,
                ):
                    return

                if self._sftp is None and not self._connect_with_retries(paramiko):
                    return

                try:
                    yield from self._scan_remote()
                except FileNotFoundError as exc:
                    self._logger.error(  # noqa: TRY400
                        f"Remote path {self.validated.remote_path!r} not found: {exc}",
                    )
                    DATA_MONITOR_POLL_ERRORS.labels(
                        monitor_name=self.name,
                        monitor_identifier=self.identifier,
                        error_type="missing_remote_path",
                    ).inc()
                except (paramiko.SSHException, OSError, EOFError) as exc:
                    self._logger.warning(f"Transient SFTP error, reconnecting: {exc}")
                    DATA_MONITOR_POLL_ERRORS.labels(
                        monitor_name=self.name,
                        monitor_identifier=self.identifier,
                        error_type="transient",
                    ).inc()
                    self._disconnect()
        finally:
            self._disconnect()
            self.health = False
