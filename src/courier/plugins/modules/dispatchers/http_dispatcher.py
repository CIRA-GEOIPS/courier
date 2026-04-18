"""HTTP Dispatcher — POST/PUT a rendered JSON payload per job.

Requires the optional ``courier[http]`` extra (``httpx``). The payload is
generated from a Jinja2 template with a rich job/file context. Transient
transport errors are retried with exponential backoff; HTTP status codes
listed in ``success_status_codes`` mark the request as successful.
"""

from __future__ import annotations

import socket
import time
from typing import TYPE_CHECKING, Any, Literal

import jinja2
from pydantic import BaseModel, Field, field_validator, model_validator

from courier.errors import InvalidPluginConfigError
from courier.interfaces.module_based.dispatchers import Dispatcher
from courier.metrics import (
    DISPATCHER_HTTP_REQUEST_DURATION,
    DISPATCHER_HTTP_RESPONSE_CODES,
)
from courier.types.execution_log import ExecutionLog

if TYPE_CHECKING:
    from courier.service import Service
    from courier.types.job import Job

interface: str = "dispatchers"
family: str = "standard"
name: str = "http_dispatcher"

AuthType = Literal["none", "bearer", "basic"]
HttpMethod = Literal["POST", "PUT", "PATCH"]

_MAX_STDOUT_BYTES = 65536
_HTTP_5XX_MIN = 500
_HTTP_5XX_MAX = 600


class HttpDispatcherConfig(BaseModel, frozen=True):
    """Validated configuration for :class:`HttpDispatcher`."""

    url: str
    method: HttpMethod = "POST"
    headers: dict[str, str] = Field(default_factory=dict)
    auth_type: AuthType = "none"
    token: str | None = None
    username: str | None = None
    password: str | None = None
    payload_template: str = '{"job_id": "{{ job_id }}"}'
    content_type: str = "application/json"
    timeout_seconds: float = Field(default=30.0, gt=0)
    retry_count: int = Field(default=3, ge=0)
    retry_delay_seconds: float = Field(default=1.0, gt=0)
    retry_backoff_factor: float = Field(default=2.0, ge=1.0)
    max_retry_delay_seconds: float = Field(default=60.0, gt=0)
    verify_ssl: bool = True
    success_status_codes: list[int] = Field(
        default_factory=lambda: [200, 201, 202, 204],
    )

    @field_validator("url")
    @classmethod
    def _validate_url(cls, v: str) -> str:
        if not (v.startswith("http://") or v.startswith("https://")):
            raise ValueError(f"url must begin with http:// or https://, got {v!r}")
        return v

    @field_validator("payload_template")
    @classmethod
    def _validate_template(cls, v: str) -> str:
        try:
            jinja2.Environment(autoescape=False).parse(v)  # noqa: S701
        except jinja2.TemplateSyntaxError as exc:
            raise ValueError(f"Invalid payload_template: {exc}") from exc
        return v

    @model_validator(mode="after")
    def _check_auth(self) -> HttpDispatcherConfig:
        if self.auth_type == "bearer" and not self.token:
            raise ValueError("auth_type='bearer' requires token to be set")
        if self.auth_type == "basic" and (not self.username or not self.password):
            raise ValueError(
                "auth_type='basic' requires both username and password",
            )
        return self


class HttpDispatcher(Dispatcher):
    """Dispatcher that POST/PUTs a rendered payload per :class:`Job`."""

    interface = "dispatchers"
    name = "http_dispatcher"
    version = "0.1.0"

    def __init__(self, service: Service, config: dict[str, Any]) -> None:
        super().__init__(service, config)
        self.validated = HttpDispatcherConfig.model_validate(config)
        self._template = jinja2.Environment(autoescape=False).from_string(  # noqa: S701
            self.validated.payload_template,
        )

    def is_healthy(self) -> bool:
        """Stateless; always healthy once loaded."""
        return True

    def _build_context(self, job: Job) -> dict[str, Any]:
        """Build the Jinja2 render context from job + first file metadata."""
        files = list(job.files)
        first = files[0] if files else None
        return {
            "job_id": job.identifier,
            "job_name": job.name,
            "files": [str(f.file) for f in files if f.file is not None],
            "file_count": len(files),
            "last_modified": job.last_modified,
            "source": getattr(first, "source", None),
            "instrument": getattr(first, "instrument", None),
            "processing_stage": getattr(first, "processing_stage", None),
            "domain": getattr(first, "domain", None),
            "hostname": getattr(first, "hostname", None),
            "timestamp": (
                first.timestamp.isoformat()
                if first is not None and first.timestamp is not None
                else None
            ),
        }

    def _build_client(self) -> Any:
        """Create an :class:`httpx.Client` with auth and headers configured."""
        try:
            import httpx  # noqa: PLC0415
        except ImportError as exc:
            raise InvalidPluginConfigError(
                "http_dispatcher requires the http extra: pip install courier[http]",
            ) from exc

        headers = dict(self.validated.headers)
        headers.setdefault("content-type", self.validated.content_type)
        auth: Any = None
        if self.validated.auth_type == "bearer":
            headers["authorization"] = f"Bearer {self.validated.token}"
        elif self.validated.auth_type == "basic":
            auth = httpx.BasicAuth(
                self.validated.username or "",
                self.validated.password or "",
            )

        return httpx.Client(
            headers=headers,
            auth=auth,
            timeout=self.validated.timeout_seconds,
            verify=self.validated.verify_ssl,
        )

    def _send_with_retries(
        self,
        client: Any,
        payload: str,
    ) -> tuple[int | None, str, str | None]:
        """POST/PUT *payload*, retrying on transport errors.

        Returns
        -------
        tuple[int | None, str, str | None]
            ``(status_code, body, error)``. On total failure, ``status_code``
            is ``None`` and ``error`` carries a diagnostic string.
        """
        import httpx  # noqa: PLC0415

        delay = self.validated.retry_delay_seconds
        last_error: str | None = None

        for attempt in range(self.validated.retry_count + 1):
            start = time.time()
            try:
                response = client.request(
                    self.validated.method,
                    self.validated.url,
                    content=payload,
                )
            except httpx.TransportError as exc:
                last_error = f"TransportError (attempt {attempt + 1}): {exc}"
                self._logger.warning(last_error)
                DISPATCHER_HTTP_RESPONSE_CODES.labels(
                    dispatcher_name=self.name,
                    status_code="transport_error",
                ).inc()
                if attempt >= self.validated.retry_count:
                    return None, "", last_error
                time.sleep(delay)
                delay = min(
                    delay * self.validated.retry_backoff_factor,
                    self.validated.max_retry_delay_seconds,
                )
                continue
            finally:
                DISPATCHER_HTTP_REQUEST_DURATION.labels(
                    dispatcher_name=self.name,
                ).observe(time.time() - start)

            status_code = response.status_code
            DISPATCHER_HTTP_RESPONSE_CODES.labels(
                dispatcher_name=self.name,
                status_code=str(status_code),
            ).inc()
            body = response.text[:_MAX_STDOUT_BYTES]

            if status_code in self.validated.success_status_codes:
                return status_code, body, None

            if (
                _HTTP_5XX_MIN <= status_code < _HTTP_5XX_MAX
                and attempt < self.validated.retry_count
            ):
                last_error = f"HTTP {status_code} (attempt {attempt + 1})"
                self._logger.warning(f"{last_error}; retrying")
                time.sleep(delay)
                delay = min(
                    delay * self.validated.retry_backoff_factor,
                    self.validated.max_retry_delay_seconds,
                )
                continue

            return status_code, body, f"HTTP {status_code}"

        return None, "", last_error

    def get_execution_log(self, job: Job) -> list[ExecutionLog]:
        """Render payload, send the request, and return a single log entry."""
        hostname = socket.gethostname()
        try:
            payload = self._template.render(**self._build_context(job))
        except jinja2.TemplateError as exc:
            return [
                ExecutionLog(
                    return_code=-1,
                    stdout="",
                    stderr=f"payload_template render failed: {exc}",
                    hostname=hostname,
                ),
            ]

        client = self._build_client()
        try:
            status_code, body, error = self._send_with_retries(client, payload)
        finally:
            client.close()

        if status_code is None:
            return_code = -1
        elif status_code in self.validated.success_status_codes:
            return_code = status_code
        else:
            return_code = status_code

        return [
            ExecutionLog(
                return_code=return_code,
                stdout=body,
                stderr=error,
                hostname=hostname,
            ),
        ]


def call() -> None:
    """Raise error if called directly."""
    raise NotImplementedError("You cannot call this plugin directly.")
