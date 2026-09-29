"""Base plugin for the payload interface.

A **payload** describes *what* is to be executed: a language/representation
(Python lowers to bash lowers to sh) plus the template and arguments needed to
launch it.  Job builders nest a payload plugin, render its template once, and
attach the serialized result to each :class:`~courier.types.job.Job`; the
dispatcher that receives the job hydrates a payload instance from that spec and
executes it.

Payloads are sub-plugins: they are discovered through the ``courier.payloads``
entry-point group but are never runnable pipeline steps of their own.
"""

from __future__ import annotations

import base64
import os
import re
import secrets
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, NoReturn, Self

import jinja2
from jinja2.sandbox import SandboxedEnvironment
from pydantic import BaseModel, Field, ValidationInfo, field_validator, model_validator

from courier.dispatchers._output_file_pattern import OutputFilePattern  # noqa: TC001
from courier.errors import CourierError
from courier.interfaces.discovery import ENTRY_POINT_PREFIX, ClassPluginRegistry
from courier.interfaces.plugin_protocol import ServicePlugin
from courier.metrics import (
    PAYLOAD_JOB_EXECUTION_DURATION,
    PAYLOAD_JOBS_PROCESSED,
    collect_labeled,
)
from courier.types.execution_log import ExecutionLog
from courier.types.payload import PayloadSpec
from courier.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Mapping

    from courier.service import Service
    from courier.types.job import Job


class DispatcherGroupConfig(BaseModel):
    """Validated configuration for the entire dispatcher group."""

    timeout_seconds: float = Field(default=3600.0, gt=0)
    log_to_logger: bool = Field(default=False)
    log_to_file: bool = Field(default=False)
    log_dir: str = Field(default="")
    log_only_errors: bool = Field(default=False)
    scan_stderr: bool = Field(default=False)
    #: Patterns that discover output files in a job's stdout/stderr; each match
    #: is re-emitted into the file-found exchange so chained pipelines work.
    output_files: list[OutputFilePattern] | None = Field(default=None)

    @model_validator(mode="after")
    def _validate_logging_config(self) -> Self:
        if self.log_to_file and not self.log_dir:
            raise ValueError("log_dir is required when log_to_file=True")
        if self.log_to_file:
            log_dir_path = Path(self.log_dir)
            if not log_dir_path.is_dir():
                log_dir_path.mkdir(parents=True, exist_ok=True)
            elif not os.access(self.log_dir, os.W_OK):
                raise ValueError(f"log_dir is not writable: {self.log_dir}")
        return self


class PayloadConfig(BaseModel):
    """Validated configuration for a Payload."""

    file: Path | None = None
    script: str | None = None
    toolchain: list[str] = Field(default_factory=list)
    toolchain_prepend: list[str] = Field(default_factory=list)
    prefix_args: list[str] = Field(default_factory=list)
    suffix_args: list[str] = Field(default_factory=list)
    binary: str | None = None
    default_binary: str | None = None

    @field_validator("file")
    @classmethod
    def validate_file(
        cls,
        value: Path | None,
        info: ValidationInfo,
    ) -> Path | None:
        """Validate that the provided file exists.

        Skipped when hydrating a serialized job spec: there the rendered script
        is authoritative and the template is not required on the executing host.
        """
        context = info.context or {}
        if (
            value is not None
            and not context.get("hydrating")
            and not value.exists()
        ):
            raise ValueError(f"File does not exist: {value}")
        return value

    @model_validator(mode="after")
    def validate_payload_source(self) -> Self:
        """Require a template file, an inline script, or a binary."""
        if self.file is None and self.binary is None and self.script is None:
            raise ValueError(
                "Either 'file', 'script', or 'binary' must be provided",
            )
        return self


class DeferredExpressionError(CourierError):
    """A dispatcher-only value was used where only the builder can resolve it."""


#: Prefix of a pass-one deferred marker.  Markers read
#: ``\x00COURIER-DEFER:<nonce>:<base64(expression)>\x00``.
_DEFER_PREFIX = "COURIER-DEFER:"
_DEFER_MARKER_RE = re.compile(
    "\x00" + re.escape(_DEFER_PREFIX) + r"([0-9a-fA-F]+):([A-Za-z0-9+/=]*)\x00",
)

#: Sandbox reused to evaluate authenticated markers in pass two.  It holds no
#: per-call state, so ``compile_expression`` is shared safely across threads.
_RESOLVE_ENV = SandboxedEnvironment(
    undefined=jinja2.StrictUndefined,
    autoescape=False,
)


def _deferred_marker(nonce: str, expression: str) -> str:
    """Encode *expression* as a marker only *nonce* can authenticate."""
    encoded = base64.b64encode(expression.encode()).decode()
    return f"\x00{_DEFER_PREFIX}{nonce}:{encoded}\x00"


class _DeferredValue(jinja2.Undefined):
    """Undefined that defers dispatcher-only expressions to pass two.

    Pass one runs on the builder, where values such as ``dispatcher`` or
    ``script_path`` do not exist yet.  Referencing one emits an unforgeable
    marker instead of failing; the dispatcher later resolves only markers that
    carry the nonce it received on the job, and never re-parses surrounding
    text as a template.  Using a deferred value in a conditional, loop, or
    length test -- which cannot be decided on the builder -- raises
    :class:`DeferredExpressionError` rather than silently rendering the wrong
    branch.
    """

    _nonce: ClassVar[str] = ""

    def __getattr__(self, name: str) -> _DeferredValue:
        # Restore Jinja's dunder guard: probes like ``__html__`` must raise
        # AttributeError rather than return a deferred marker.
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)
        # The ``attr`` filter passes an arbitrary (possibly attacker-controlled)
        # name here.  Only accept real identifiers so untrusted data can never
        # be spliced into the pass-two expression.
        if not (isinstance(name, str) and name.isidentifier()):
            raise DeferredExpressionError(
                f"dispatcher-only expression {self._undefined_name!r} cannot "
                f"be indexed with the unsafe attribute name {name!r}; "
                f"two-pass templates support leaf interpolation only",
            )
        return type(self)(name=f"{self._undefined_name}.{name}")

    def __getitem__(self, name: object) -> _DeferredValue:
        return type(self)(name=f"{self._undefined_name}[{name!r}]")

    def __str__(self) -> str:
        return _deferred_marker(self._nonce, str(self._undefined_name))

    def _unsupported(self, construct: str) -> NoReturn:
        raise DeferredExpressionError(
            f"dispatcher-only expression {self._undefined_name!r} cannot be "
            f"used in {construct}; two-pass templates support leaf "
            f"interpolation only",
        )

    def _fail_with_undefined_error(
        self,
        *_args: object,
        **_kwargs: object,
    ) -> NoReturn:
        # Jinja routes arithmetic, comparison, calling and every other
        # unsupported operation here.  Raise our own error type so the failure
        # is a CourierError rather than a bare jinja2.UndefinedError.
        self._unsupported("this operation")

    # Jinja binds these operations to the *parent's* helper at class-creation
    # time, so an override of ``_fail_with_undefined_error`` alone is bypassed.
    # Rebind them here to raise DeferredExpressionError instead of a bare
    # jinja2.UndefinedError.
    __add__ = __radd__ = __sub__ = __rsub__ = _fail_with_undefined_error
    __mul__ = __rmul__ = __div__ = __rdiv__ = _fail_with_undefined_error
    __truediv__ = __rtruediv__ = _fail_with_undefined_error
    __floordiv__ = __rfloordiv__ = _fail_with_undefined_error
    __mod__ = __rmod__ = _fail_with_undefined_error
    __pos__ = __neg__ = _fail_with_undefined_error
    __call__ = _fail_with_undefined_error
    __lt__ = __le__ = __gt__ = __ge__ = _fail_with_undefined_error
    __int__ = __float__ = __complex__ = _fail_with_undefined_error
    __pow__ = __rpow__ = _fail_with_undefined_error

    def __eq__(self, _other: object) -> bool:
        return self._unsupported("a comparison")

    def __ne__(self, _other: object) -> bool:
        return self._unsupported("a comparison")

    # Defining __eq__ above resets __hash__ to None; keep the parent's identity
    # hash so deferred values remain usable in sets/dicts.
    __hash__ = jinja2.Undefined.__hash__

    def __bool__(self) -> bool:
        return self._unsupported("a conditional ({% if %})")

    def __iter__(self) -> object:
        return self._unsupported("a loop ({% for %})")

    def __len__(self) -> int:
        return self._unsupported("a filter or length test")


def _deferred_undefined(nonce: str) -> type[jinja2.Undefined]:
    """Return an Undefined class bound to *nonce* for pass-one rendering."""
    return type("_BoundDeferredValue", (_DeferredValue,), {"_nonce": nonce})


def _deferred_default(
    value: Any,
    default_value: Any = "",
    boolean: bool = False,
) -> Any:
    """Refuse to treat a deferred dispatcher value as undefined.

    The stock ``default`` filter special-cases ``jinja2.Undefined``; because
    :class:`_DeferredValue` subclasses it, a fallback would otherwise be
    substituted silently for a value only the dispatcher can supply.
    """
    if isinstance(value, _DeferredValue):
        raise DeferredExpressionError(
            f"dispatcher-only expression {value._undefined_name!r} cannot be "
            f"used with the 'default' filter; two-pass templates support leaf "
            f"interpolation only",
        )
    if isinstance(value, jinja2.Undefined) or (boolean and not value):
        return default_value
    return value


def _resolve_deferred_expressions(
    text: str,
    context: Mapping[str, Any],
    nonce: str,
) -> str:
    """Replace nonce-authenticated deferred markers with their values.

    Only markers carrying *nonce* are evaluated; anything else in *text* -- in
    particular job data that happens to contain template syntax -- is left
    literal.  Expressions are evaluated in a sandboxed environment.
    """
    if not text:
        return text
    environment = _RESOLVE_ENV

    def _replace(match: re.Match[str]) -> str:
        if match.group(1) != nonce:
            raise DeferredExpressionError(
                "deferred expression failed authentication; refusing to "
                "evaluate a marker this job did not produce",
            )
        try:
            expression = base64.b64decode(match.group(2)).decode()
            value = environment.compile_expression(expression)(**context)
        except Exception as exc:
            raise DeferredExpressionError(
                f"Could not resolve dispatcher-only expression for marker "
                f"{match.group(0)!r}: {exc}",
            ) from exc
        return "" if value is None else str(value)

    resolved = _DEFER_MARKER_RE.sub(_replace, text)
    # A filter (e.g. ``{{ x | upper }}``) would have mutated the marker text;
    # detect that rather than write a broken marker into the script.  Only a
    # null-delimited marker prefix counts -- a script or file path that merely
    # contains the literal text ``COURIER-DEFER:`` is not a marker.
    if (
        ("\x00" + _DEFER_PREFIX).lower() in resolved.lower()
        and _DEFER_MARKER_RE.search(resolved) is None
    ):
        raise DeferredExpressionError(
            "a dispatcher-only expression was altered before resolution; "
            "filters over dispatcher-only values are not supported",
        )
    return resolved


def _config_from_job_spec(spec: PayloadSpec) -> PayloadConfig:
    """Build a payload config from a serialized job spec.

    The template file is not required to exist on the host hydrating the spec:
    ``PayloadSpec.script`` already carries the builder-rendered contents, so the
    ``file`` field is path metadata only.  Validation still runs (types and the
    file/script/binary requirement); only the exists-on-disk check is skipped,
    via the ``hydrating`` validation context.
    """
    return PayloadConfig.model_validate(spec.config, context={"hydrating": True})


class Payload(ServicePlugin):
    """Base class for Payloads."""

    interface: ClassVar[str] = "payloads"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "payload"

    #: Payloads are sub-plugins with no run loop of their own; the plugin
    #: manager registers them without forking a thread.
    threaded: ClassVar[bool] = False

    #: Interpreter used when the config does not set ``default_binary``.
    default_binary: ClassVar[str | None] = None
    #: Suffix for scripts written without one (and for the temp-file fallback).
    file_suffix: ClassVar[str] = ".sh"

    base_config: DispatcherGroupConfig

    def __init__(
        self,
        service: Service,
        config: dict | None = None,
        identifier: str | None = None,
    ) -> None:
        if identifier is None:
            raise ValueError(
                f"Payload {type(self).__name__} requires an identifier",
            )
        self._bootstrap(
            service,
            PayloadConfig.model_validate(config),
            identifier,
        )

    def _bootstrap(
        self,
        service: Service,
        config: PayloadConfig,
        identifier: str,
    ) -> None:
        """Initialize shared instance state from a validated config."""
        self.identifier = identifier
        self._logger = get_logger("plugin", self.name, service.config)
        self.config = config
        self._job_execution_duration = PAYLOAD_JOB_EXECUTION_DURATION
        self._jobs_processed = PAYLOAD_JOBS_PROCESSED
        self._configure_from_config()

    def _configure_from_config(self) -> None:
        """Resolve the effective interpreter and script suffix.

        ``config.default_binary`` overrides the class-level ``default_binary``.
        Subclasses override the class attributes (or this hook for behaviour
        beyond those two values) so that instances hydrated from a job spec
        share exactly the same setup path.
        """
        self._default_binary = self.config.default_binary or self.default_binary
        self._file_suffix = self.file_suffix

    @classmethod
    def from_job_spec(
        cls,
        spec: PayloadSpec,
        service: Service,
        base_config: DispatcherGroupConfig | None = None,
    ) -> Payload:
        """Hydrate a payload instance from a serialized job spec.

        Parameters
        ----------
        spec : PayloadSpec
            Serialized payload attached to a job by its builder.
        service : Service
            Parent service, used for logging configuration.
        base_config : DispatcherGroupConfig or None, optional
            Dispatcher-group config supplying execution timeouts and logging
            flags.  The dispatcher passes its own config here.

        Returns
        -------
        Payload
            A new instance of this representation class.
        """
        instance = cls.__new__(cls)
        instance._bootstrap(service, _config_from_job_spec(spec), spec.identifier)
        if base_config is not None:
            instance.base_config = base_config
        return instance

    @classmethod
    def get_representation_hierarchy(cls) -> list[type[Payload]]:
        """Generate the Payload representation hierarchy.

        Returns
        -------
        list[type[Payload]]
            Payload subclasses in base-to-most-specific inheritance order.
        """
        return [
            parent
            for parent in reversed(cls.__mro__)
            if issubclass(parent, Payload) and parent is not Payload
        ]

    def validate_toolchain_arg(self, value: str) -> list[ExecutionLog]:  # noqa: ARG002
        """Validate a toolchain argument through a self-defined method.

        Parameters
        ----------
        value : str
            Tool or executable name to validate.

        Returns
        -------
        list[ExecutionLog]
            Execution logs describing the result of validation.
        """
        return []

    def _probe_toolchain(self, command: list[str]) -> list[ExecutionLog]:
        """Run a toolchain probe and log the per-command return codes."""
        payload = self.get_payload_from_job(command)
        self._logger.debug(
            f"Toolchain validation command {command} returned:"
            f"{[p.return_code for p in payload]}",
        )
        return payload

    def get_payload_from_job(
        self,
        command: list[str],  # noqa: ARG002
        job: Job | None = None,  # noqa: ARG002
        log_prefix: str = "",  # noqa: ARG002
        log_file_path: Path | None = None,  # noqa: ARG002
    ) -> list[ExecutionLog]:
        """Get an execution log from executing a command.

        Parameters
        ----------
        command : list[str]
            Command and arguments to execute.
        log_prefix : str, optional
            Prefix to apply to generated log output.
        log_file_path : Path | None, optional
            Optional path for persisted execution logs.

        Returns
        -------
        list[ExecutionLog]
            Execution logs produced by the command.
        """
        return [
            ExecutionLog(),
        ]

    def declare_command(self, path: Path | None = None) -> list[str]:  # noqa: ARG002
        """Declare the syntax to call a command.

        Parameters
        ----------
        path : Path | None, optional
            Path to the rendered script or executable.

        Returns
        -------
        list[str]
            Command arguments required to execute the Payload.
        """
        return []

    def generate_calling_method(self) -> list[str]:
        """Declare the first part of a command, e.g. `python3 -c` or `bash -c`.

        Returns
        -------
        list[str]
            Command arguments used to invoke this Payload representation.
        """
        return []

    @staticmethod
    def _template_context(
        job: Job,
        extra_context: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Return the base Jinja context shared by both render passes."""
        context: dict[str, Any] = {
            "files": [
                f.to_dict() for f in sorted(job.files, key=lambda f: str(f.file))
            ],
            "job": {
                "name": job.name,
                "identifier": job.identifier,
                "config": job.config,
                "last_modified": job.last_modified,
                "timeout": job.timeout,
                "correlation_id": job.correlation_id,
                "emit_time": job.emit_time,
                "targets": list(job.targets),
            },
            "config": job.config,
        }
        if extra_context:
            context.update(extra_context)
        return context

    def render_script(
        self,
        job: Job,
        script: str,
        extra_context: Mapping[str, Any] | None = None,
        *,
        defer_nonce: str | None = None,
    ) -> str:
        """Render a Jinja2 template with job and payload context.

        Parameters
        ----------
        job : Job
            The job supplying template values and file metadata.
        script : str
            Jinja2 template to render.
        extra_context : Mapping[str, Any] or None, optional
            Additional, namespaced context supplied by the caller.  Used by the
            builder (``builder``, ``targets``) and by the dispatcher
            (``dispatcher``, ``script_path``, ``hostname``, ``output_dir``) so
            each side can fill in what it alone knows.
        defer_nonce : str or None, optional
            When given (the builder's pass one), values this side cannot resolve
            are emitted as authenticated deferred markers for the dispatcher's
            pass two instead of raising.  When ``None`` (a strict single pass),
            an unresolved variable is an error.

        Returns
        -------
        str
            The rendered script or command.

        Notes
        -----
        Two-pass templates support leaf interpolation of dispatcher-only values
        only; conditionals, loops and filters over them raise
        :class:`DeferredExpressionError`.
        """
        context = self._template_context(job, extra_context)
        undefined = (
            _deferred_undefined(defer_nonce)
            if defer_nonce is not None
            else jinja2.StrictUndefined
        )
        environment = SandboxedEnvironment(
            undefined=undefined,
            autoescape=False,
            finalize=lambda value: (
                value
                if isinstance(value, _DeferredValue)
                else ("" if value is None or value == [] else value)
            ),
        )
        if defer_nonce is not None:
            # ``default``/``d`` would otherwise treat the Undefined-derived
            # deferred value as "missing" and silently substitute a fallback.
            environment.filters["default"] = _deferred_default
            environment.filters["d"] = _deferred_default
        return environment.from_string(script).render(**context)

    def resolve_deferred_expressions(
        self,
        script: str,
        job: Job,
        extra_context: Mapping[str, Any] | None = None,
        *,
        defer_nonce: str = "",
    ) -> str:
        """Resolve the deferred markers a builder left in *script*.

        This is the dispatcher's half of the two-pass render.  It evaluates only
        markers authenticated by *defer_nonce*; the rest of the text is never
        parsed, so job data cannot inject template expressions.
        """
        return _resolve_deferred_expressions(
            script,
            self._template_context(job, extra_context),
            defer_nonce,
        )

    def write_script(self, text: str, path: Path | None = None) -> Path:
        """Persist already-rendered *text* as an executable file.

        Parameters
        ----------
        text : str
            Fully rendered script contents.
        path : Path or None, optional
            File to write.  When ``None`` a temporary file is created.

        Returns
        -------
        Path
            Path to the executable.
        """
        if path is None:
            with tempfile.NamedTemporaryFile(
                mode="w",
                suffix=self._file_suffix,
                dir="/tmp/",
                delete=False,
            ) as script_file:
                script_file.write(text)
                target = Path(script_file.name)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
            target = path
        target.chmod(0o755)
        return target

    def to_job_spec(self, job: Job, builder: Any | None = None) -> PayloadSpec:
        """Render this payload for *job* and serialize it for the wire.

        This is pass one: the builder fills in everything it knows (job files,
        its own identity and targets) and leaves authenticated markers for the
        values only the dispatcher can supply.

        Parameters
        ----------
        job : Job
            Job the payload will travel with.
        builder : JobBuilder or None, optional
            Owning builder, exposed to the template under ``builder``.

        Returns
        -------
        PayloadSpec
            Serialized payload, ready to attach to ``job.payload``.
        """
        extra: dict[str, Any] = {}
        if builder is not None:
            extra["builder"] = {
                "name": builder.name,
                "identifier": builder.identifier,
                "targets": list(job.targets),
            }
        if self.config.file is not None:
            template: str | None = self.config.file.read_text()
            suffix = self.config.file.suffix or self._file_suffix
        else:
            template = self.config.script
            suffix = self._file_suffix
        nonce = secrets.token_hex(16)
        script = (
            self.render_script(job, template, extra, defer_nonce=nonce)
            if template is not None
            else None
        )
        return PayloadSpec(
            name=self.name,
            identifier=self.identifier,
            config=self.config.model_dump(mode="json"),
            script=script,
            suffix=suffix,
            defer_nonce=nonce,
        )

    def get_metrics(self) -> dict[str, Any]:
        """Return plugin-specific metrics."""
        return {
            **collect_labeled(
                PAYLOAD_JOB_EXECUTION_DURATION,
                "payload_name",
                self.name,
            ),
            **collect_labeled(PAYLOAD_JOBS_PROCESSED, "payload_name", self.name),
        }

    def start(self) -> None:
        """Start execution of the payload."""
        return

    def stop(self) -> None:
        """Stop execution of the payload."""
        return

    def is_healthy(self) -> bool:
        """Declare the health of the payload."""
        return True


payloads = ClassPluginRegistry(
    name="payloads",
    group=f"{ENTRY_POINT_PREFIX}.payloads",
    expected_base=Payload,
)
