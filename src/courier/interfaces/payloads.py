"""Base plugin for the payload interface.

A **payload** describes *what* a job executes: a representation (Python
lowers to bash lowers to sh) plus the template and arguments that launch it.
A job builder constructs the payload its ``payload`` block names, renders the
template once per job (pass one) and attaches the result to the job as a
:class:`~courier.types.payload.PayloadSpec`.  The dispatcher that receives the
job hydrates a payload from that spec (:meth:`Payload.from_job_spec`), fills
in what only it knows (pass two) and executes it.

Pass one is a strict Jinja render: every name the builder owns (``files``,
``job``, ``config``, ``builder``) must resolve.  Only the names in
:data:`DISPATCHER_CONTEXT_NAMES` are deferred, each as a marker carrying its
access path and a per-job nonce.  Pass two replaces each marker carrying the
job's nonce with the value its path names in the dispatcher's context.  It
evaluates nothing, so job data is never evaluated as a template.

A dispatcher-only name can only be written as a **bare value**: a
``{{ ... }}`` holding just the name or, for ``dispatcher``, an access path of
``.attribute`` and constant ``['key']`` or ``[0]`` steps on it, with any
literal text around it, at top level or in the body of an ``{% if %}`` or a
non-recursive ``{% for %}``.  Any other use is rejected when the payload is
constructed (see :data:`_BARE_VALUES_ONLY`).

In both passes ``config`` is the plain JSON form of the job's config, and
``None`` and ``[]`` render as ``''``.
"""

from __future__ import annotations

import base64
import copy
import json
import os
import re
import secrets
import unicodedata
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Self, cast

import jinja2
from jinja2 import nodes
from jinja2.sandbox import SandboxedEnvironment
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from courier.dispatchers._output_file_pattern import OutputFilePattern  # noqa: TC001
from courier.errors import CourierError, UnexecutableJobError
from courier.interfaces.discovery import ENTRY_POINT_PREFIX, ClassPluginRegistry
from courier.metrics import PAYLOAD_JOB_EXECUTION_DURATION, PAYLOAD_JOBS_PROCESSED
from courier.types.execution_log import ExecutionLog
from courier.types.job import json_default
from courier.types.payload import PayloadSpec
from courier.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence, Set

    from courier.service import Service
    from courier.types.job import Job

#: Top-level template names only a dispatcher can supply.  Pass one defers
#: exactly these; a template may use them only as bare values.
DISPATCHER_CONTEXT_NAMES: frozenset[str] = frozenset(
    {"dispatcher", "script_path", "hostname", "output_dir"},
)

_UPGRADE_GUIDE = "see 'Removed configuration keys' in the upgrade guide"
_PARALLEL_REMOVED = (
    "per-file parallel execution within one job is not supported; scale out "
    f"with more dispatcher replicas or smaller jobs ({_UPGRADE_GUIDE})"
)

#: Dispatcher config keys earlier releases accepted, mapped to what to do
#: instead.  They are rejected with this advice.
REMOVED_DISPATCHER_KEYS: dict[str, str] = {
    "bash_script": (
        f"set the script in the job builder's nested payload block ({_UPGRADE_GUIDE})"
    ),
    "fail_fast": _PARALLEL_REMOVED,
    "python_venv": (
        f"set the payload's `default_binary` to the venv's interpreter "
        f"({_UPGRADE_GUIDE})"
    ),
    "sbatch_template": (
        f"use slurm_dispatcher's scheduler options, or #SBATCH lines in the "
        f"payload script ({_UPGRADE_GUIDE})"
    ),
}


def _check_keys(
    data: Any,
    known: Mapping[str, Any],
    other: type[BaseModel],
    advice: str,
) -> Any:
    """Reject the removed keys of *data*, then those that are *other*'s fields.

    Keys the validating model defines itself (*known*) are left alone, so a
    subclass may reintroduce one of these names.

    Raises
    ------
    ValueError
        Naming every removed key with its advice, or every misplaced key
        followed by *advice*.
    """
    if not isinstance(data, dict):
        return data
    foreign = [key for key in data if key not in known]
    removed = [key for key in foreign if key in REMOVED_DISPATCHER_KEYS]
    if removed:
        raise ValueError(
            " ".join(
                f"{key!r} is no longer supported: {REMOVED_DISPATCHER_KEYS[key]}"
                for key in removed
            ),
        )
    misplaced = [key for key in foreign if key in other.model_fields]
    if misplaced:
        raise ValueError(f"{', '.join(map(repr, misplaced))}: {advice}")
    return data


class DispatcherGroupConfig(BaseModel):
    """Validated configuration shared by every dispatcher.

    Unknown keys are rejected; a removed key or a :class:`PayloadConfig`
    setting is rejected with advice.  With ``log_to_file``, ``log_dir`` is
    created if missing and must be writable, unless the config is validated
    with the context ``{"offline": True}`` (as ``courier validate`` does).
    """

    model_config = ConfigDict(extra="forbid")

    timeout_seconds: float = Field(default=3600.0, gt=0)
    log_to_logger: bool = Field(default=False)
    log_to_file: bool = Field(default=False)
    log_dir: str = Field(default="")
    log_only_errors: bool = Field(default=False)
    scan_stderr: bool = Field(default=False)
    #: Patterns that discover output files in a job's stdout/stderr; each match
    #: is re-emitted into the file-found exchange so chained pipelines work.
    output_files: list[OutputFilePattern] | None = Field(default=None)
    #: How many jobs this dispatcher runs at once.
    max_workers: int = Field(default=1, ge=1)

    @model_validator(mode="before")
    @classmethod
    def _screen_keys(cls, data: Any) -> Any:
        return _check_keys(
            data,
            cls.model_fields,
            PayloadConfig,
            "payload setting(s) set in a dispatcher block; move them to the job "
            "builder's nested payload block (e.g. `script:` or `file:` for the "
            "script)",
        )

    @model_validator(mode="after")
    def _validate_logging_config(self, info: ValidationInfo) -> Self:
        if self.log_to_file and not self.log_dir:
            raise ValueError("log_dir is required when log_to_file=True")
        if self.log_to_file and not (info.context or {}).get("offline"):
            _prepare_log_dir(self.log_dir)
        return self


def _prepare_log_dir(log_dir: str) -> None:
    """Create *log_dir* if it is missing and check it is writable.

    Raises
    ------
    ValueError
        If it is not a directory, cannot be created or is not writable (an
        ``OSError`` would escape pydantic's validation-error handling).
    """
    path = Path(log_dir)
    if path.exists() and not path.is_dir():
        raise ValueError(f"log_dir is not a directory: {log_dir}")
    if not path.is_dir():
        try:
            path.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise ValueError(f"log_dir cannot be created: {log_dir}: {exc}") from exc
    elif not os.access(log_dir, os.W_OK):
        raise ValueError(f"log_dir is not writable: {log_dir}")


class PayloadConfig(BaseModel):
    """Validated configuration for a payload.

    Unknown keys are rejected; a removed key or a
    :class:`DispatcherGroupConfig` setting is rejected with advice.
    """

    model_config = ConfigDict(extra="forbid")

    file: Path | None = None
    script: str | None = None
    #: Executables the dispatcher probes for on its own host before it runs a
    #: job (a success is cached); a missing one parks the job as unexecutable.
    toolchain: list[str] = Field(default_factory=list)
    #: Arguments python_payload puts in front of the interpreter in each
    #: ``toolchain`` probe; never part of the job's command.
    toolchain_prepend: list[str] = Field(default_factory=list)
    prefix_args: list[str] = Field(default_factory=list)
    suffix_args: list[str] = Field(default_factory=list)
    binary: str | None = None
    default_binary: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _screen_keys(cls, data: Any) -> Any:
        return _check_keys(
            data,
            cls.model_fields,
            DispatcherGroupConfig,
            "dispatcher option(s) set in a payload block; move them to the "
            "dispatcher's config",
        )

    @field_validator("file")
    @classmethod
    def validate_file(
        cls,
        value: Path | None,
        info: ValidationInfo,
    ) -> Path | None:
        """Require the template file to exist, except when hydrating a job spec."""
        context = info.context or {}
        if value is not None and not context.get("hydrating") and not value.exists():
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
    """A dispatcher-only value is used other than as a bare value, or is missing.

    Raised by the template check, and by a dispatcher's pass two for a
    malformed marker or a value that dispatcher does not define.
    """


#: Start of a pass-one marker.  Markers read
#: ``\x00COURIER-DEFER:<nonce>:<base64 of the JSON list of path steps>\x00``.
_DEFER_PREFIX = "COURIER-DEFER:"
_MARKER_START = "\x00" + _DEFER_PREFIX
_NONCE_RE = re.compile(r"[0-9a-f]+")

#: Dispatcher-only names whose values are strings, so no path step follows.
_STRING_VALUED_NAMES = frozenset({"script_path", "hostname", "output_dir"})

#: How a dispatcher-only name may be written; ends every template-check error.
_BARE_VALUES_ONLY = (
    f"a dispatcher-only name ({', '.join(sorted(DISPATCHER_CONTEXT_NAMES))}) "
    "can only be written as a bare value: a {{ ... }} holding just the name or, "
    "for dispatcher, an access path on it, such as {{ script_path }}, "
    "{{ dispatcher.config.log_dir }} or {{ dispatcher.config['log_dir'] }}, "
    "with any literal text around it ({{ script_path }}.log), at top level or "
    "in the body of an {% if %} or a non-recursive {% for %}. Not supported: "
    "filters, tests, calls, operators or slices over it, a condition or loop "
    "on it, {% set %}, {% with %}, {% block %}, {% autoescape %}, macros, call "
    "blocks, filter blocks or recursive loops holding it, rebinding it, and "
    "spelling it with look-alike non-ASCII letters"
)

#: Nodes that bind a name without a :class:`jinja2.nodes.Name` node for it.
_BINDING_NODES = (nodes.Macro, nodes.Import, nodes.FromImport, nodes.NSRef)


def _deferred_marker(nonce: str, path: Sequence[str | int]) -> str:
    """Encode the access *path* as a marker carrying *nonce*."""
    encoded = base64.b64encode(json.dumps(list(path)).encode()).decode()
    return f"{_MARKER_START}{nonce}:{encoded}\x00"


class _DeferredValue:
    """Placeholder for a dispatcher-only value during the builder's pass one.

    Attribute and item access extend its access path; :class:`str` gives the
    marker.  The template check guarantees a template does nothing else with
    one.  It defines no public attribute, which would shadow a path step.
    """

    __slots__ = ("_nonce", "_path")

    def __init__(self, nonce: str, path: tuple[str | int, ...]) -> None:
        self._nonce = nonce
        self._path = path

    def __getattr__(self, name: str) -> _DeferredValue:
        # Underscore names (dunder probes, the unset slots of a copy made
        # without __init__) must see a missing attribute, not a path step.
        if name.startswith("_"):
            raise AttributeError(name)
        return _DeferredValue(self._nonce, (*self._path, name))

    def __getitem__(self, key: str | int) -> _DeferredValue:
        return _DeferredValue(self._nonce, (*self._path, key))

    def __str__(self) -> str:
        return _deferred_marker(self._nonce, self._path)


def _finalize(value: Any) -> Any:
    """Map ``None`` and ``[]`` to ``''``, comparing nothing with ``==``."""
    if value is None or (isinstance(value, list) and not value):
        return ""
    return value


#: The one Jinja environment: pass one, strict single-pass renders and the
#: dispatcher's argument templates.  Pass two does not use Jinja.
_ENV = SandboxedEnvironment(
    undefined=jinja2.StrictUndefined,
    autoescape=False,
    finalize=_finalize,
)


# ── the template check: dispatcher-only names as bare values only ───────────


def _is_const(node: nodes.Node, kind: type) -> bool:
    """Return whether *node* is a constant of exactly the type *kind*."""
    return isinstance(node, nodes.Const) and type(node.value) is kind


def _is_path_step(node: nodes.Getattr | nodes.Getitem) -> bool:
    """Return whether *node* is ``.name`` or a constant ``[key]`` subscript.

    The name must not start with ``_``; the key is a string or an integer
    (``[-1]`` included), never a boolean, ``none`` or a slice.
    """
    if isinstance(node, nodes.Getattr):
        return not node.attr.startswith("_")
    if isinstance(node.arg, nodes.Neg):
        return _is_const(node.arg.node, int)
    return _is_const(node.arg, str) or _is_const(node.arg, int)


def _bare_value_root(node: nodes.Node) -> nodes.Name | None:
    """Return the dispatcher-only name *node* is a bare access path on, if any."""
    while isinstance(node, (nodes.Getattr, nodes.Getitem)):
        if not _is_path_step(node):
            return None
        node = node.node
    if isinstance(node, nodes.Name) and node.name in DISPATCHER_CONTEXT_NAMES:
        return node
    return None


def _bare_value_outputs(body: Iterable[nodes.Node]) -> Iterator[nodes.Output]:
    """Yield the outputs in *body* where a bare dispatcher-only value may sit.

    These are its top-level outputs and, recursively, those in the bodies
    (``elif`` and ``else`` included) of its ``{% if %}`` and non-recursive
    ``{% for %}`` tags.
    """
    for node in body:
        if isinstance(node, nodes.Output):
            yield node
        elif isinstance(node, nodes.If):
            yield from _bare_value_outputs([*node.body, *node.elif_, *node.else_])
        elif isinstance(node, nodes.For) and not node.recursive:
            yield from _bare_value_outputs([*node.body, *node.else_])


def _bare_values(outputs: Iterable[nodes.Output]) -> dict[int, nodes.Node]:
    """Map the id of each bare value's root name in *outputs* to the value."""
    return {
        id(root): expression
        for output in outputs
        for expression in output.nodes
        if (root := _bare_value_root(expression)) is not None
    }


def _bound_names(node: nodes.Node) -> list[str]:
    """Return the names a macro, an import or ``{% set ns.x %}`` binds."""
    if isinstance(node, (nodes.Macro, nodes.NSRef)):
        return [node.name]
    if isinstance(node, nodes.Import):
        return [node.target]
    # The last of _BINDING_NODES: {% from ... import name, other as alias %}.
    imported = cast("nodes.FromImport", node)
    return [name if isinstance(name, str) else name[1] for name in imported.names]


def _reads_as_reserved(name: str) -> bool:
    """Return whether the template name *name* is a dispatcher-only name.

    Names are compared in NFKC form: Python NFKC-normalizes the identifiers
    Jinja compiles them into, so a fullwidth or mathematical look-alike would
    be the same variable as ``hostname``.
    """
    return unicodedata.normalize("NFKC", name) in DISPATCHER_CONTEXT_NAMES


def _name_problem(
    name: nodes.Name,
    bare: Mapping[int, nodes.Node],
    placed: Set[int],
) -> str | None:
    """Say what is wrong with dispatcher-only *name*, if anything.

    *bare* maps the root of each bare value in the template to the value;
    *placed* holds the roots of those in a place one may stand.
    """
    if name.ctx != "load":
        return "is assigned or rebound"
    if not name.name.isascii():
        return "is spelled with look-alike non-ASCII letters"
    value = bare.get(id(name))
    if value is None:
        return "is used other than as a bare value"
    if id(name) not in placed:
        return (
            "is a bare value inside a tag that cannot hold one (only the top "
            "level and the bodies of {% if %} and non-recursive {% for %} can)"
        )
    if value is not name and name.name in _STRING_VALUED_NAMES:
        return (
            f"is a string, so no attribute or item can follow it (write "
            f"{{{{ {name.name} }}}} on its own)"
        )
    return None


def _unsupported_use(name: str, line: int, problem: str) -> DeferredExpressionError:
    """Build the template-check error for dispatcher-only *name* on *line*."""
    canonical = unicodedata.normalize("NFKC", name)
    spelled = repr(name) if canonical == name else f"{name!r} (reads as {canonical!r})"
    return DeferredExpressionError(
        f"line {line}: dispatcher-only name {spelled} {problem}; {_BARE_VALUES_ONLY}",
    )


def _check_dispatcher_names(template: nodes.Template) -> None:
    """Reject any use of a dispatcher-only name in *template* but a bare value.

    Raises
    ------
    DeferredExpressionError
        Naming the offending name and its line, and how to write it instead.
    """
    bare = _bare_values(template.find_all(nodes.Output))
    placed = _bare_values(_bare_value_outputs(template.body)).keys()
    for name in template.find_all(nodes.Name):
        if _reads_as_reserved(name.name) and (
            problem := _name_problem(name, bare, placed)
        ):
            raise _unsupported_use(name.name, name.lineno, problem)
    for node in template.find_all(_BINDING_NODES):
        bound = [n for n in _bound_names(node) if _reads_as_reserved(n)]
        if bound:
            raise _unsupported_use(bound[0], node.lineno, "is assigned or rebound")


def _compile_checked(source: str) -> jinja2.Template:
    """Parse *source*, check it with :func:`_check_dispatcher_names`, compile it.

    Raises
    ------
    jinja2.TemplateSyntaxError
        If *source* is not valid Jinja (an unknown filter included).
    DeferredExpressionError
        If it uses a dispatcher-only name other than as a bare value.
    """
    template = _ENV.parse(source)
    _check_dispatcher_names(template)
    return _ENV.template_class.from_code(
        _ENV,
        _ENV.compile(template),
        _ENV.make_globals(None),
        None,
    )


# ── pass two: look the marked access paths up ───────────────────────────────

#: A key or index that is not there.
_MISSING = object()


def _decode_path(encoded: str) -> list[str | int]:
    """Decode the access path a marker carries.

    Raises
    ------
    DeferredExpressionError
        If it is not base64-encoded JSON, or not a list of string and integer
        steps rooted in one of :data:`DISPATCHER_CONTEXT_NAMES`.
    """
    try:
        path = json.loads(base64.b64decode(encoded, validate=True))
    except ValueError as exc:  # binascii.Error, UnicodeDecodeError, JSONDecodeError
        raise DeferredExpressionError(
            f"malformed dispatcher-only value marker {encoded!r}: {exc}",
        ) from exc
    if not (
        isinstance(path, list)
        and path
        and isinstance(path[0], str)
        and path[0] in DISPATCHER_CONTEXT_NAMES
        and all(type(key) in {str, int} for key in path[1:])
    ):
        raise DeferredExpressionError(
            f"malformed dispatcher-only value marker: {path!r} is not an access "
            f"path rooted in one of {sorted(DISPATCHER_CONTEXT_NAMES)}",
        )
    return path


def _path_text(path: Sequence[str | int]) -> str:
    """Spell *path* as a template would: ``dispatcher.config['weird key']``."""
    root, *keys = path
    return str(root) + "".join(
        f".{key}" if isinstance(key, str) and key.isidentifier() else f"[{key!r}]"
        for key in keys
    )


def _look_up(path: list[str | int], context: Mapping[str, Any]) -> str:
    """Return the value at *path* in *context*, finalized as pass one renders it.

    Each step indexes a mapping by key or a list or tuple by position; nothing
    is evaluated or called.

    Raises
    ------
    DeferredExpressionError
        If *context* does not define the value, or a step follows a value that
        is not a mapping, list or tuple.
    """
    value: object = context
    for depth, key in enumerate(path, start=1):
        if value is None:
            raise DeferredExpressionError(
                f"dispatcher-only value {_path_text(path)!r} is not defined by "
                f"this dispatcher ({_path_text(path[: depth - 1])!r} is null)",
            )
        if isinstance(value, Mapping):
            value = value.get(key, _MISSING)
        elif isinstance(value, (list, tuple)):
            value = (
                value[key]
                if isinstance(key, int) and -len(value) <= key < len(value)
                else _MISSING
            )
        else:
            raise DeferredExpressionError(
                f"dispatcher-only value {_path_text(path)!r} cannot be looked "
                f"up: {_path_text(path[: depth - 1])!r} is a "
                f"{type(value).__name__}, and only a dictionary key or a list "
                f"index can follow a value",
            )
        if value is _MISSING:
            raise DeferredExpressionError(
                f"dispatcher-only value {_path_text(path)!r} is not defined by "
                f"this dispatcher (it has no {_path_text(path[:depth])!r}); for "
                f"example 'output_dir' exists only on dispatchers that provide one",
            )
    return str(_finalize(value))


def _resolve_deferred_expressions(
    text: str,
    context: Mapping[str, Any],
    nonce: str,
) -> str:
    """Pass two; see :meth:`Payload.resolve_deferred_expressions`."""
    if not nonce:
        return text
    marker = re.compile(re.escape(f"{_MARKER_START}{nonce}:") + r"([^\x00]*)\x00")
    return marker.sub(
        lambda match: _look_up(_decode_path(match.group(1)), context),
        text,
    )


class Payload:
    """Base class for payloads.

    Notes
    -----
    A payload is built two ways: by its job builder through ``__init__``,
    which reads, checks and compiles the template once (so a bad template
    fails at startup), and by a dispatcher through :meth:`from_job_spec`,
    which hydrates an instance from a job's spec **without calling
    ``__init__``**.  Subclasses therefore put per-instance setup in
    :meth:`_configure_from_config`, which both paths run, and declare extra
    config fields on a :class:`PayloadConfig` subclass named by
    :attr:`config_class`.  A subclass that overrides ``__init__`` must call
    ``super().__init__``: it compiles the template :meth:`to_job_spec` renders.
    """

    interface: ClassVar[str] = "payloads"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "payload"

    #: Interpreter used when the config does not set ``default_binary``.
    default_binary: ClassVar[str | None] = None
    #: Suffix of the script a dispatcher writes, unless ``file`` has one.
    file_suffix: ClassVar[str] = ".sh"
    #: Model that validates this payload's config, on both construction paths.
    config_class: ClassVar[type[PayloadConfig]] = PayloadConfig

    identifier: str
    config: PayloadConfig
    base_config: DispatcherGroupConfig
    #: Entry-point name of the payload plugin that was configured.  It labels
    #: the metrics, also when a dispatcher runs it as a lower representation.
    payload_name: str
    #: Class in this payload's hierarchy that the dispatcher runs it as: its
    #: own class, or a lower representation (see :meth:`render_script`).
    representation: type[Payload]
    #: Compiled pass-one template; ``None`` for a binary-only payload and for
    #: an instance hydrated from a job spec.
    _template: jinja2.Template | None = None
    #: Set by :meth:`from_job_spec`: ``config.script`` is already rendered.
    _hydrated: bool = False

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
            type(self).config_class.model_validate(
                config if config is not None else {},
            ),
            identifier,
        )
        self._template = self._compile_template()
        self._check_argument_templates()

    def _bootstrap(
        self,
        service: Service,
        config: PayloadConfig,
        identifier: str,
        *,
        payload_name: str | None = None,
        base_config: DispatcherGroupConfig | None = None,
    ) -> None:
        """Initialize the state both construction paths share."""
        self.identifier = identifier
        self.payload_name = payload_name or self.name
        self.representation = type(self)
        self._logger = get_logger("plugin", self.name, service.config)
        self.config = config
        self.base_config = (
            base_config if base_config is not None else DispatcherGroupConfig()
        )
        self._job_execution_duration = PAYLOAD_JOB_EXECUTION_DURATION
        self._jobs_processed = PAYLOAD_JOBS_PROCESSED
        self._configure_from_config()

    def _configure_from_config(self) -> None:
        """Resolve the effective interpreter (``config.default_binary`` wins).

        Subclasses override the class attributes, or this hook for setup
        beyond them, so that hydrated instances share the same setup path.
        """
        self._default_binary = self.config.default_binary or self.default_binary

    @classmethod
    def from_job_spec(
        cls,
        spec: PayloadSpec,
        service: Service,
        base_config: DispatcherGroupConfig | None = None,
        representation: type[Payload] | None = None,
    ) -> Self:
        """Hydrate a payload from a job's serialized spec, without ``__init__``.

        Keys :attr:`config_class` does not define are dropped first (a builder
        running another version of the plugin may send more), and
        ``config.script`` is set to the builder-rendered ``spec.script``,
        replacing any raw template an older builder sent.  The template file
        need not exist on this host.  The result executes that script; it
        cannot render a job (:meth:`to_job_spec` refuses).

        Parameters
        ----------
        spec : PayloadSpec
            Serialized payload attached to a job by its builder.
        service : Service
            Parent service, used for logging configuration.
        base_config : DispatcherGroupConfig or None, optional
            The dispatcher's config (timeouts, logging flags); defaults to a
            default :class:`DispatcherGroupConfig`.
        representation : type[Payload] or None, optional
            The class in this payload's hierarchy the dispatcher runs it as;
            defaults to this class.  See :meth:`render_script`.

        Returns
        -------
        Payload
            An instance of this class whose :attr:`payload_name` is
            ``spec.name``.

        Raises
        ------
        pydantic.ValidationError
            If ``spec.config`` is not a valid :attr:`config_class`.
        TypeError
            If *representation* is not in this class's hierarchy.
        """
        if representation is not None and not issubclass(cls, representation):
            raise TypeError(
                f"{cls.__name__} cannot run as {representation.__name__}: it is "
                f"not in its representation hierarchy",
            )
        fields = cls.config_class.model_fields
        config = {
            key: value
            for key, value in spec.config.items()
            if key in fields and key != "script"
        }
        if spec.script is not None:
            config["script"] = spec.script
        instance = cls.__new__(cls)
        instance._bootstrap(
            service,
            cls.config_class.model_validate(config, context={"hydrating": True}),
            spec.identifier,
            payload_name=spec.name,
            base_config=base_config,
        )
        if representation is not None:
            instance.representation = representation
        instance._hydrated = True
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

    @property
    def lowered(self) -> bool:
        """Whether the dispatcher runs this payload as a lower representation."""
        return self.representation is not type(self)

    @classmethod
    def wrap_command(cls, command: list[str]) -> list[str]:  # noqa: ARG003
        """Return an argv that runs *command*, another payload's argv, as this class.

        A class that a dispatcher can list in ``representations`` implements
        this, so a payload lowered to it still runs with its own interpreter.
        Every element of *command* must stay a separate argument: it is never
        joined into one string to be parsed again.

        Raises
        ------
        UnexecutableJobError
            Always, here: the base class cannot run anything.
        """
        raise UnexecutableJobError(
            f"{cls.__name__} cannot run the command of another payload",
        )

    def render_script(self, command: list[str]) -> list[str]:
        """Rewrite *command*, this payload's own argv, to run as :attr:`representation`.

        The lowering hook.  A payload run as its own class returns *command*
        unchanged.  One lowered to an ancestor (a ``python_payload`` on a
        dispatcher that lists only ``BashPayload``) still has to run with its
        own interpreter, so by default *command* is handed to that ancestor's
        :meth:`wrap_command`: ``bash -c '"$@"' bash python <script>``.  The
        dispatcher passes every command it runs for the payload through here,
        the job's and each toolchain probe's.  Override it to lower a payload
        differently.

        Parameters
        ----------
        command : list[str]
            The argv that runs this payload as its own class.

        Returns
        -------
        list[str]
            The argv the dispatcher runs.
        """
        if not self.lowered:
            return command
        return self.representation.wrap_command(command)

    def _probe_toolchain(self, command: list[str]) -> list[ExecutionLog]:
        """Run a toolchain probe (``probe=True``: no log file, no job metrics).

        *command* is this payload's own; it is lowered like a job's command.
        """
        command = self.render_script(command)
        payload = self.get_payload_from_job(command, probe=True)
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
        *,
        probe: bool = False,  # noqa: ARG002
    ) -> list[ExecutionLog]:
        """Get an execution log from executing a command.

        Parameters
        ----------
        command : list[str]
            Command and arguments to execute.
        job : Job or None, optional
            Job being executed; ``None`` for a command that is not a job.
        log_prefix : str, optional
            Prefix to apply to generated log output.
        log_file_path : Path | None, optional
            Optional path for persisted execution logs.
        probe : bool, optional
            Mark a toolchain probe rather than a job run.  A probe must never
            write a log file, must not apply ``log_only_errors``, and must not
            record payload job metrics.

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

    def _compile_template(self) -> jinja2.Template | None:
        """Read, check and compile the pass-one template, if there is one.

        Raises
        ------
        ValueError
            If the template cannot be read, is not valid Jinja, or uses a
            dispatcher-only name other than as a bare value (chained from the
            :class:`DeferredExpressionError`); the message names the file (or
            ``inline script``) and the line.
        """
        file = self.config.file
        source: str | None = self.config.script
        origin = "inline script"
        if file is not None:
            origin = f"template file {str(file)!r}"
            try:
                source = file.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError) as exc:
                raise ValueError(
                    f"Payload {self.identifier!r}: cannot read {origin}: {exc}",
                ) from exc
        if source is None:
            return None
        try:
            return _compile_checked(source)
        except jinja2.TemplateSyntaxError as exc:
            raise ValueError(
                f"Payload {self.identifier!r}: invalid Jinja template in "
                f"{origin}, line {exc.lineno}: {exc.message}",
            ) from exc
        except DeferredExpressionError as exc:
            raise ValueError(
                f"Payload {self.identifier!r}: unsupported template in {origin}, {exc}",
            ) from exc

    def _check_argument_templates(self) -> None:
        """Compile ``binary``/``prefix_args``/``suffix_args`` templates.

        They are rendered on the dispatcher; checking them here turns a syntax
        error into a startup failure rather than a failure of every job.

        Raises
        ------
        ValueError
            If any of them is not valid Jinja.
        """
        arguments = [
            ("binary", self.config.binary),
            *(
                (f"prefix_args[{i}]", arg)
                for i, arg in enumerate(self.config.prefix_args)
            ),
            *(
                (f"suffix_args[{i}]", arg)
                for i, arg in enumerate(self.config.suffix_args)
            ),
        ]
        for origin, argument in arguments:
            if argument is None:
                continue
            try:
                _ENV.from_string(argument)
            except jinja2.TemplateSyntaxError as exc:
                raise ValueError(
                    f"Payload {self.identifier!r}: invalid Jinja template in "
                    f"{origin}, line {exc.lineno}: {exc.message}",
                ) from exc

    @staticmethod
    def _template_context(
        job: Job,
        extra_context: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Return the Jinja context of a render: the job's values and *extra_context*.

        The job's values are in their JSON wire form, so ``config`` is the
        plain dict a dispatcher sees, even when the builder holds a pydantic
        model.
        """
        context: dict[str, Any] = json.loads(
            json.dumps(
                {
                    "files": [
                        f.to_dict()
                        for f in sorted(job.files, key=lambda f: str(f.file))
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
                },
                default=json_default,
            ),
        )
        if extra_context:
            context.update(extra_context)
        return context

    @classmethod
    def _render(
        cls,
        template: jinja2.Template,
        job: Job,
        extra_context: Mapping[str, Any] | None,
        defer_nonce: str | None,
    ) -> str:
        """Render a checked *template*; with a *defer_nonce*, defer dispatcher names.

        Each name in :data:`DISPATCHER_CONTEXT_NAMES` that *extra_context*
        does not supply is then a :class:`_DeferredValue`.
        """
        context = cls._template_context(job, extra_context)
        if defer_nonce is not None:
            if not _NONCE_RE.fullmatch(defer_nonce):
                raise ValueError("defer_nonce must be a non-empty lowercase hex string")
            for name in DISPATCHER_CONTEXT_NAMES:
                context.setdefault(name, _DeferredValue(defer_nonce, (name,)))
        return template.render(context)

    def render_template(
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
            Additional context: ``builder`` on the builder, ``dispatcher``,
            ``script_path``, ``hostname`` and ``output_dir`` on a dispatcher.
        defer_nonce : str or None, optional
            When given (a pass one), *script* is first checked to use
            dispatcher-only names as bare values only, and the ones
            *extra_context* does not supply become markers carrying this nonce.
            When ``None`` (a strict single pass), every name must resolve.

        Returns
        -------
        str
            The rendered script or command.

        Raises
        ------
        jinja2.UndefinedError
            If a name the caller should supply is undefined (a typo, a missing
            metadata or config key, an out-of-range index).
        DeferredExpressionError
            With a *defer_nonce*, if *script* uses a dispatcher-only name
            other than as a bare value.
        """
        template = (
            _ENV.from_string(script)
            if defer_nonce is None
            else _compile_checked(script)
        )
        return self._render(template, job, extra_context, defer_nonce)

    def with_rendered_arguments(
        self,
        job: Job,
        extra_context: Mapping[str, Any] | None = None,
    ) -> Self:
        """Return a copy whose ``binary``/``prefix_args``/``suffix_args`` are rendered.

        Each is rendered on its own, in one strict pass with the dispatcher's
        *extra_context*.  Everything else :meth:`declare_command` returns (the
        script path, the interpreter, wrapper snippets) is used literally, so
        a ``{{`` in ``TMPDIR`` or ``slurm_output_dir`` is never evaluated.

        Parameters
        ----------
        job : Job
            The job supplying template values.
        extra_context : Mapping[str, Any] or None, optional
            The dispatcher's context (``dispatcher``, ``script_path``, ...).

        Returns
        -------
        Payload
            A shallow copy of this payload with a rendered config.

        Raises
        ------
        jinja2.UndefinedError
            If an argument references an undefined name.
        ValueError
            If a configured ``binary`` renders to an empty string.
        """
        config = self.config
        context = self._template_context(job, extra_context)

        def render(argument: str) -> str:
            return _ENV.from_string(argument).render(context)

        binary = None if config.binary is None else render(config.binary)
        if config.binary and not binary:
            raise ValueError(
                f"Payload {self.identifier!r}: binary {config.binary!r} "
                f"rendered to an empty string",
            )
        rendered = copy.copy(self)
        rendered.config = config.model_copy(
            update={
                "binary": binary,
                "prefix_args": [render(arg) for arg in config.prefix_args],
                "suffix_args": [render(arg) for arg in config.suffix_args],
            },
        )
        return rendered

    def resolve_deferred_expressions(
        self,
        script: str,
        job: Job,  # noqa: ARG002 -- part of the signature dispatchers call
        extra_context: Mapping[str, Any] | None = None,
        *,
        defer_nonce: str = "",
    ) -> str:
        """Resolve the markers a builder's pass one left in *script* (pass two).

        Each marker carrying *defer_nonce* is replaced with the value its
        access path names in *extra_context*, rendered as pass one renders a
        value; the rest of the text is left exactly as it is.

        Parameters
        ----------
        script : str
            The builder's rendered script (``PayloadSpec.script``).
        job : Job
            The job the script belongs to; it supplies no values.
        extra_context : Mapping[str, Any] or None, optional
            The dispatcher's context (``dispatcher``, ``script_path``,
            ``hostname`` and, where it supplies one, ``output_dir``).
        defer_nonce : str, optional
            The job's ``PayloadSpec.defer_nonce``; empty resolves nothing.

        Returns
        -------
        str
            *script* with its markers resolved.

        Raises
        ------
        DeferredExpressionError
            If a marker carrying *defer_nonce* is malformed, or names a value
            this dispatcher does not define.
        """
        return _resolve_deferred_expressions(script, extra_context or {}, defer_nonce)

    def to_job_spec(self, job: Job, builder: Any | None = None) -> PayloadSpec:
        """Render this payload for *job* (pass one) and serialize it for the wire.

        The template compiled at construction is rendered with the job's
        values and, under ``builder``, the owning builder's identity; the
        dispatcher-only names become markers carrying a fresh random nonce.

        Parameters
        ----------
        job : Job
            Job the payload will travel with.
        builder : JobBuilder or None, optional
            Owning builder, exposed to the template under ``builder``.

        Returns
        -------
        PayloadSpec
            Serialized payload whose ``script`` is the rendered script, the
            only copy of the template the job carries (``config`` omits
            ``script``).

        Raises
        ------
        jinja2.UndefinedError
            If the template references a builder-side value that is missing.
        CourierError
            If this payload was hydrated from a job spec: rendering its
            already-rendered script again would evaluate job data.
        """
        if self._hydrated:
            raise CourierError(
                f"Payload {self.identifier!r} was hydrated from a job spec: its "
                f"script is already rendered, so it cannot render a job",
            )
        extra: dict[str, Any] = {}
        if builder is not None:
            extra["builder"] = {
                "name": builder.name,
                "identifier": builder.identifier,
                "targets": list(job.targets),
            }
        nonce = secrets.token_hex(16)
        template = self._template
        file = self.config.file
        return PayloadSpec(
            name=self.name,
            identifier=self.identifier,
            config=self.config.model_dump(mode="json", exclude={"script"}),
            script=(
                None if template is None else self._render(template, job, extra, nonce)
            ),
            suffix=(file.suffix if file is not None else "") or self.file_suffix,
            defer_nonce=nonce,
        )


payloads = ClassPluginRegistry(
    name="payloads",
    group=f"{ENTRY_POINT_PREFIX}.payloads",
    expected_base=Payload,
)
