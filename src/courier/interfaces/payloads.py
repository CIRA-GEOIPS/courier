"""Base plugin for the payload interface.

A **payload** describes *what* is to be executed: a language/representation
(Python lowers to bash lowers to sh) plus the template and arguments needed to
launch it.  Job builders nest a payload plugin, render its template once, and
attach the serialized result to each :class:`~courier.types.job.Job`; the
dispatcher that receives the job hydrates a payload instance from that spec and
executes it.

Payloads are sub-plugins: they are discovered through the ``courier.payloads``
entry-point group but are never runnable pipeline steps of their own.

Two-pass rendering
------------------
The template is rendered in two passes.  Pass one runs on the job builder with
ordinary strict Jinja semantics: every name the builder owns (``files``,
``job``, ``config``, ``builder``) must resolve, so a typo or a missing
metadata key fails the job at the builder exactly as in a single-pass render,
and ``| default``, ``is defined`` and ``{% if %}`` over builder-side data
behave as stock Jinja.  Only the top-level names in
:data:`DISPATCHER_CONTEXT_NAMES`, which the builder cannot know, are deferred:
each access path on them (``dispatcher.config.log_dir``) is written into the
script as a marker authenticated by a per-job nonce.  Pass two, on the
dispatcher, evaluates only authenticated markers whose expression is an access
path rooted in one of those names; the rest of the script is never parsed
again, so job data can never be evaluated as a template.

Dispatcher-only values support leaf interpolation only: ``{{ script_path }}``,
concatenation with ``~``, plain string conversion (inside ``| join`` over a
list, ``'%s' %``, ``'{}'.format``), and being passed through a call that only
stores or returns them (a macro, ``namespace()``, ``dict()``, ``cycler()`` or
``loop.cycle()``, ``list.append()``, the default of ``dict.get()``).
Filters, tests, conditionals, loops, operators and comparisons over them, and
calls that would compute with them -- string methods, a lookup keyed by one,
``%`` or ``str.format`` conversions other than a plain ``%s`` or ``{}`` --
raise :class:`DeferredExpressionError` at the builder, because the builder
cannot compute their result.  A string built from one with ``~`` holds its
marker and is treated the same way; its own methods and subscripts are refused
too.

Pass one then checks its output: every marker must be intact, carry the job's
nonce and be one this render emitted, and the nonce may not appear outside a
marker -- nor, when the render emitted any marker, may a NUL character -- so a
marker that a call or filter altered or escaped (``| tojson`` or
``| urlencode`` over a container, slicing, NUL-stripping) is refused rather
than left in the script.  What cannot be intercepted is *inspecting* such a
string: comparisons, ``in``, truthiness, iteration, sorting, de-duplication
and searching one (``list.count``) see the marker text, not the dispatcher's
value, so templates must not branch on them.

Only the rendered script travels to the dispatcher: :meth:`Payload.to_job_spec`
puts it in :attr:`PayloadSpec.script <courier.types.payload.PayloadSpec.script>`
and leaves the raw ``script`` template out of the serialized config.

In both passes ``config`` (and ``job.config``) is the plain JSON form of the
job's config -- what a dispatcher sees after the job crosses the broker --
and ``None`` and ``[]`` render as the empty string.
"""

# cspell:ignore binop binops

from __future__ import annotations

import base64
import binascii
import copy
import functools
import json
import os
import re
import secrets
import string
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, NoReturn, Self, cast

import jinja2
from jinja2 import nodes
from jinja2.parser import Parser
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
from courier.errors import CourierError
from courier.interfaces.discovery import ENTRY_POINT_PREFIX, ClassPluginRegistry
from courier.interfaces.plugin_protocol import ServicePlugin
from courier.metrics import (
    PAYLOAD_JOB_EXECUTION_DURATION,
    PAYLOAD_JOBS_PROCESSED,
    collect_labeled,
)
from courier.types.execution_log import ExecutionLog
from courier.types.job import json_default
from courier.types.payload import PayloadSpec
from courier.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from jinja2.runtime import Context

    from courier.service import Service
    from courier.types.job import Job


#: Top-level template names only a dispatcher can supply.  Pass one defers
#: exactly these (and access paths rooted in them) to the dispatcher; every
#: other name must resolve on the builder.
DISPATCHER_CONTEXT_NAMES: frozenset[str] = frozenset(
    {"dispatcher", "script_path", "hostname", "output_dir"},
)

_PARALLEL_REMOVED = (
    "parallel_bash was removed, and per-file parallel execution within one job "
    "is not supported: a dispatcher runs one job at a time. Scale out with "
    "more dispatcher replicas, or emit smaller jobs (e.g. files_per_job: 1)."
)

#: Dispatcher config keys that earlier releases accepted, mapped to what to do
#: instead.  They are rejected with this advice rather than the generic
#: "extra inputs are not permitted".
REMOVED_DISPATCHER_KEYS: dict[str, str] = {
    "bash_script": (
        "the script moved from the dispatcher to the job builder's nested "
        "payload block: set it there as `script:` (inline) or `file:` (a "
        "template path)."
    ),
    "max_workers": _PARALLEL_REMOVED,
    "fail_fast": _PARALLEL_REMOVED,
    "python_venv": (
        "select the venv in the job builder's payload instead: for a "
        "python_payload set `default_binary` to the venv's interpreter (e.g. "
        "`default_binary: /opt/venv/bin/python`); for a shell or bash script, "
        "activate it at the top of the script (`source /opt/venv/bin/activate`, "
        "or `export PATH=/opt/venv/bin:$PATH`), which is what python_venv did. "
        "The payload's `toolchain_prepend` does not do this: it never touches "
        "the job's command (python_payload puts it in front of the interpreter "
        "in its `toolchain` probes only; shell_payload and bash_payload ignore "
        "it)."
    ),
    "falcon": (
        "falcons were replaced by payloads: configure the script as the job "
        "builder's nested `payload:` block (kind: payload, e.g. name: "
        "bash_payload)."
    ),
    "falconer": (
        "falconers were replaced by dispatchers: the dispatcher itself "
        "(local_dispatcher or slurm_dispatcher) executes the job builder's "
        "payload, so remove this block."
    ),
    "sbatch_template": (
        "slurm_dispatcher no longer renders its own template. Pass scheduler "
        "options with `partition`, `account`, `qos`, `time_limit`, `ntasks`, "
        "`mem_per_node` or `sbatch_extra_args`; or put #SBATCH directives at "
        "the top of a shell_payload/bash_payload script with no `binary` or "
        "`prefix_args` (only such a script is submitted as the batch script; "
        "any other payload is submitted with --wrap, which ignores them). The "
        "dispatcher's --job-name, --output and --error, and the options above, "
        "take precedence over #SBATCH lines."
    ),
}


class DispatcherGroupConfig(BaseModel):
    """Validated configuration for the entire dispatcher group.

    Unknown keys are rejected; keys removed in earlier releases are rejected
    with migration advice (see :data:`REMOVED_DISPATCHER_KEYS`), and a payload
    setting (``script``, ``prefix_args``...) with where it belongs.

    With ``log_to_file``, ``log_dir`` is created if missing and must be
    writable.  Validating with the context ``{"offline": True}`` (as
    ``courier validate`` does) skips that filesystem check, so validating a
    config never creates directories on the validating host.
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

    @model_validator(mode="before")
    @classmethod
    def _reject_removed_keys(cls, data: Any) -> Any:
        """Turn a key from an earlier release, or a payload key, into advice.

        A payload key is any setting of an installed payload's config model
        (see :func:`payload_setting`) that this model does not define.
        """
        if not isinstance(data, dict):
            return data
        _raise_for_removed_keys(data, cls.model_fields)
        misplaced = _describe_misplaced(
            data,
            cls.model_fields,
            _payload_setting_owners,
        )
        if misplaced:
            raise ValueError(
                f"{misplaced}: payload setting(s) set in a dispatcher block; "
                f"move them to the job builder's nested payload block (e.g. "
                f"`script:` or `file:` for the script)",
            )
        return data

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
        If it is not a directory, cannot be created or is not writable.
        (pydantic reports only ``ValueError`` and ``AssertionError`` as
        validation errors; a bare ``OSError`` would escape it.)
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


def _raise_for_removed_keys(data: Mapping[str, Any], known: Mapping[str, Any]) -> None:
    """Raise a migration hint for each key of *data* in REMOVED_DISPATCHER_KEYS.

    Keys the validating model defines itself (*known*) are left alone, so a
    subclass may legitimately reintroduce a name.

    Raises
    ------
    ValueError
        Naming every removed key and what to do instead.
    """
    removed = [
        key for key in data if key in REMOVED_DISPATCHER_KEYS and key not in known
    ]
    if removed:
        raise ValueError(
            " ".join(
                f"{key!r} is no longer supported: {REMOVED_DISPATCHER_KEYS[key]}"
                for key in removed
            ),
        )


def _installed_settings(registry: ClassPluginRegistry) -> dict[str, list[str]]:
    """Map each config key of every installed *registry* plugin to its plugins.

    Only each plugin's declared ``config_class`` is read.  A plugin that
    cannot be loaded is skipped: this runs only to explain a key a model
    rejected, and must not fail because some other plugin is broken.
    """
    owners: dict[str, list[str]] = {}
    for name in registry.names():
        try:
            model = getattr(registry.get_plugin(name), "config_class", None)
        except Exception:  # noqa: S112 -- see the docstring
            continue
        if isinstance(model, type) and issubclass(model, BaseModel):
            for key in model.model_fields:
                owners.setdefault(key, []).append(name)
    return owners


def _setting_owners(
    key: str,
    base: type[BaseModel],
    registry: ClassPluginRegistry,
) -> list[str] | None:
    """Return the installed *registry* plugins whose config defines *key*.

    Returns
    -------
    list[str] or None
        ``[]`` when every such plugin accepts it (a field of *base*, the
        model all of them extend), their sorted entry-point names when only
        some define it, and ``None`` when none does.
    """
    if key in base.model_fields:
        return []
    owners = _installed_settings(registry).get(key)
    return sorted(owners) if owners else None


def _dispatcher_setting_owners(key: str) -> list[str] | None:
    """Return the installed dispatchers defining *key*; see :func:`_setting_owners`."""
    # Imported here: courier.interfaces.dispatchers imports this module.
    from courier.interfaces.dispatchers import dispatchers  # noqa: PLC0415

    return _setting_owners(key, DispatcherGroupConfig, dispatchers)


def _payload_setting_owners(key: str) -> list[str] | None:
    """Return the installed payloads defining *key*; see :func:`_setting_owners`."""
    return _setting_owners(key, PayloadConfig, payloads)


def _setting_phrase(label: str, owners: list[str] | None) -> str | None:
    """Phrase *owners* (from :func:`_setting_owners`) as ``a <label> setting``."""
    if owners is None:
        return None
    return f"a {' / '.join(owners) or label} setting"


def dispatcher_setting(key: str) -> str | None:
    """Describe *key* as a dispatcher setting, if any installed dispatcher has it.

    Parameters
    ----------
    key : str
        A config key.

    Returns
    -------
    str or None
        ``"a dispatcher setting"`` for a key every dispatcher accepts (a
        :class:`DispatcherGroupConfig` field), ``"a slurm_dispatcher
        setting"`` for one that only some installed dispatchers' config models
        define (their entry-point names, joined with ``" / "``), and ``None``
        when no installed dispatcher defines it.
    """
    return _setting_phrase("dispatcher", _dispatcher_setting_owners(key))


def payload_setting(key: str) -> str | None:
    """Describe *key* as a payload setting, if any installed payload has it.

    The counterpart of :func:`dispatcher_setting`: ``"a payload setting"``
    for a :class:`PayloadConfig` field, ``"a <name> setting"`` for a field
    only some installed payloads' config models define, else ``None``.
    """
    return _setting_phrase("payload", _payload_setting_owners(key))


def _describe_misplaced(
    data: Mapping[str, Any],
    known: Mapping[str, Any],
    owners_of: Callable[[str], list[str] | None],
) -> str:
    """List the keys of *data* that *known* lacks and *owners_of* recognises.

    A key every plugin of the other kind accepts is listed bare
    (``'timeout_seconds'``), one only some of them define with their names
    (``'partition' (slurm_dispatcher)``).  Nothing is looked up for a key
    *known* defines, so a valid block never loads another plugin.

    Returns
    -------
    str
        The comma-separated list, or ``""`` when there is none.
    """
    described = [
        (key, owners)
        for key in data
        if key not in known and (owners := owners_of(key)) is not None
    ]
    return ", ".join(
        f"{key!r} ({' / '.join(owners)})" if owners else repr(key)
        for key, owners in described
    )


class PayloadConfig(BaseModel):
    """Validated configuration for a Payload.

    Unknown keys are rejected; a dispatcher setting or a key removed in an
    earlier release is rejected with advice on where it belongs.  (When a
    dispatcher hydrates a job's payload it may use a lower representation
    than the builder configured, so :meth:`Payload.from_job_spec` drops the
    keys that representation does not define before validating: the
    builder-rendered script is authoritative.)
    """

    model_config = ConfigDict(extra="forbid")

    file: Path | None = None
    script: str | None = None
    #: Executables the dispatcher probes for on its own host before it runs a
    #: job (a success is cached per payload configuration); a missing one
    #: parks the job as unexecutable.
    toolchain: list[str] = Field(default_factory=list)
    #: Arguments python_payload puts in front of the interpreter in each
    #: ``toolchain`` probe (e.g. ``["env", "LD_LIBRARY_PATH=/opt/lib"]``).
    #: Never part of the job's command; shell_payload and bash_payload ignore
    #: it.
    toolchain_prepend: list[str] = Field(default_factory=list)
    prefix_args: list[str] = Field(default_factory=list)
    suffix_args: list[str] = Field(default_factory=list)
    binary: str | None = None
    default_binary: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _screen_keys(cls, data: Any) -> Any:
        """Explain a dispatcher setting or a removed key in a payload block.

        A dispatcher setting is any field of an installed dispatcher's config
        model (see :func:`dispatcher_setting`), e.g. ``timeout_seconds`` or
        slurm_dispatcher's ``partition``, that this model does not define.
        """
        if not isinstance(data, dict):
            return data
        _raise_for_removed_keys(data, cls.model_fields)
        misplaced = _describe_misplaced(
            data,
            cls.model_fields,
            _dispatcher_setting_owners,
        )
        if misplaced:
            raise ValueError(
                f"{misplaced}: dispatcher option(s) set in a payload block; "
                f"move them to the dispatcher's config",
            )
        return data

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
    """A dispatcher-only value was used where only the builder can resolve it."""


_LEAF_ONLY = "two-pass templates support leaf interpolation only"

#: Prefix of a pass-one deferred marker.  Markers read
#: ``\x00COURIER-DEFER:<nonce>:<base64(expression)>\x00``.
_DEFER_PREFIX = "COURIER-DEFER:"
_MARKER_START = "\x00" + _DEFER_PREFIX
#: The nonce is lowercase hex (``secrets.token_hex``); a filter that changed
#: its case therefore breaks the match and is reported as an altered marker.
_DEFER_MARKER_RE = re.compile(
    re.escape(_MARKER_START) + r"([0-9a-f]+):([A-Za-z0-9+/=]*)\x00",
)
_NONCE_RE = re.compile(r"[0-9a-f]+")

#: Attributes Jinja probes on any value it is about to call.  A deferred value
#: must not answer them with another deferred value.
_JINJA_PROTOCOL_ATTRIBUTES = frozenset(
    {"jinja_pass_arg", "unsafe_callable", "alters_data"},
)


def _deferred_marker(nonce: str, expression: str) -> str:
    """Encode *expression* as a marker only *nonce* can authenticate."""
    encoded = base64.b64encode(expression.encode()).decode()
    return f"{_MARKER_START}{nonce}:{encoded}\x00"


def _refuse(action: str) -> Callable[..., NoReturn]:
    """Build a method refusing *action* (``"be called"``) on a deferred value."""

    def refuse(self: _DeferredValue, *_args: object, **_kwargs: object) -> NoReturn:
        self._unsupported(action)

    return refuse


_REFUSE_ARITHMETIC = _refuse("be used in arithmetic")


class _DeferredValue:
    """Placeholder for a dispatcher-only value during the builder's pass one.

    Pass one binds each name in :data:`DISPATCHER_CONTEXT_NAMES` to one of
    these.  Attribute and item access builds up the full access path taken in
    the template source (``dispatcher.config['k']``); converting the value to a
    string emits a marker carrying that path, authenticated by the job's nonce,
    and records it in *emitted* -- shared by every value of one render -- so
    the render's output can be checked against exactly what it produced.
    Everything the builder cannot decide -- conditionals, loops, filters,
    operators, comparisons, calling the value -- raises
    :class:`DeferredExpressionError`.

    The class deliberately defines no public attributes: any it defined would
    shadow the template attribute of the same name.
    """

    __slots__ = ("_emitted", "_expression", "_nonce")

    def __init__(
        self,
        nonce: str,
        expression: str,
        emitted: set[str] | None = None,
    ) -> None:
        self._nonce = nonce
        self._expression = expression
        self._emitted = emitted if emitted is not None else set()

    def _unsupported(self, action: str) -> NoReturn:
        raise DeferredExpressionError(
            f"dispatcher-only expression {self._expression!r} cannot {action}; "
            f"{_LEAF_ONLY}",
        )

    def __getattr__(self, name: str) -> _DeferredValue:
        # Dunder and Jinja-protocol probes must see a missing attribute, not a
        # deferred marker.  So must every other underscore name: the sandbox
        # refuses them as attributes anyway (falling back to ``obj['_x']``
        # exactly as for a dict), and a copy made without ``__init__`` must not
        # recurse through here looking up its own unset slots.
        if name.startswith("_") or name in _JINJA_PROTOCOL_ATTRIBUTES:
            raise AttributeError(name)
        # Only real identifiers are spliced into the pass-two expression.
        if not name.isidentifier():
            raise DeferredExpressionError(
                f"dispatcher-only expression {self._expression!r} cannot be "
                f"indexed with the attribute name {name!r}; {_LEAF_ONLY}",
            )
        return self._extend(f"{self._expression}.{name}")

    def __getitem__(self, key: object) -> _DeferredValue:
        # Only a plain string or integer key becomes a literal in the pass-two
        # expression; slices, None, booleans and deferred values cannot.
        if isinstance(key, str):
            literal = repr(str(key))
        elif isinstance(key, int) and not isinstance(key, bool):
            literal = repr(int(key))
        else:
            raise DeferredExpressionError(
                f"dispatcher-only expression {self._expression!r} can only be "
                f"indexed with a string or integer key, not "
                f"{type(key).__name__}; {_LEAF_ONLY}",
            )
        return self._extend(f"{self._expression}[{literal}]")

    def _extend(self, expression: str) -> _DeferredValue:
        """Return the deferred value one access step further along."""
        return _DeferredValue(self._nonce, expression, self._emitted)

    def __str__(self) -> str:
        marker = _deferred_marker(self._nonce, self._expression)
        self._emitted.add(marker)
        return marker

    def __format__(self, spec: str) -> str:
        if spec:
            self._unsupported(f"be formatted with the specification {spec!r}")
        return str(self)

    def __repr__(self) -> str:
        # repr() is how a list or dict prints its items; the marker is not a
        # repr of the dispatcher's value, so refuse instead of rendering junk.
        self._unsupported("be passed to repr() (e.g. printed inside a list or dict)")

    __hash__ = _refuse("be used as a lookup key")
    __bool__ = _refuse("be used in a conditional ({% if %})")
    __iter__ = _refuse("be used in a loop ({% for %})")
    __len__ = _refuse("be used in a length test")
    __contains__ = _refuse("be used in an 'in' test")
    __eq__ = __ne__ = _refuse("be used in a comparison")
    __lt__ = __le__ = __gt__ = __ge__ = _refuse("be used in a comparison")
    __add__ = __radd__ = __sub__ = __rsub__ = _REFUSE_ARITHMETIC
    __mul__ = __rmul__ = __truediv__ = __rtruediv__ = _REFUSE_ARITHMETIC
    __floordiv__ = __rfloordiv__ = __mod__ = __rmod__ = _REFUSE_ARITHMETIC
    __pow__ = __rpow__ = __pos__ = __neg__ = __abs__ = _REFUSE_ARITHMETIC
    __int__ = __float__ = __complex__ = __index__ = _refuse("be converted to a number")
    __call__ = _refuse("be called")


def _finalize(value: Any) -> Any:
    """Map ``None`` and ``[]`` to ``''``; shared by both passes.

    A deferred value passes through untouched (its ``__eq__`` refuses), and
    nothing else is compared with ``==`` either, so a value with an unusual
    ``__eq__`` renders as it would without finalize.
    """
    if value is None or (isinstance(value, list) and not value):
        return ""
    return value


def _carries_marker(value: object) -> bool:
    """Return whether *value* is a string holding a deferred marker.

    Such a string was built from a dispatcher-only value (``hostname ~ '/x'``)
    and is as unknown to the builder as the value itself.
    """
    return isinstance(value, str) and "\x00" in value and _has_marker_start(value)


def _refuse_dispatcher_values(values: Iterable[object], action: str) -> None:
    """Raise if any of *values* is dispatcher-only or a string built from one.

    Raises
    ------
    DeferredExpressionError
        Naming the expression (or the marker-bearing string) and *action*.
    """
    for value in values:
        if isinstance(value, _DeferredValue):
            value._unsupported(action)
        _refuse_marked_string(value, action)


def _refuse_marked_string(value: object, action: str) -> None:
    """Raise if *value* is a string built from a dispatcher-only value.

    Raises
    ------
    DeferredExpressionError
        Naming *action*.
    """
    if _carries_marker(value):
        raise DeferredExpressionError(
            f"a string built from a dispatcher-only value (e.g. with '~') "
            f"cannot {action}; {_LEAF_ONLY}",
        )


def _mod_operands(right: object) -> tuple[object, ...]:
    """Return the values a ``%`` right operand substitutes, one level deep."""
    if isinstance(right, tuple):
        return right
    if isinstance(right, dict):
        return tuple(right.values())
    return (right,)


#: A printf-style conversion: ``%`` [``(key)``] [flags/width/precision] type.
_PRINTF_CONVERSION_RE = re.compile(
    r"%(?:\((?P<key>[^)]*)\))?(?P<conversion>[^a-zA-Z%]*[a-zA-Z%])",
)


def _is_plain_printf(template: str) -> bool:
    """Return whether every conversion in *template* is ``%s`` or ``%%``.

    A mapping key containing ``(`` makes the template not plain: Python
    matches nested parentheses in a key (``%(a(b)s)200s`` has the key
    ``a(b)s`` and pads to 200), which this scan does not, so it could not
    tell where such a conversion really starts.
    """
    return all(
        "(" not in (match.group("key") or "")
        and match.group("conversion") in {"s", "%"}
        for match in _PRINTF_CONVERSION_RE.finditer(template)
    )


def _is_plain_str_format(template: str) -> bool:
    """Return whether no field of a ``str.format`` *template* has a spec.

    A conversion (``!s``) or format spec (``:>9``) would be applied to the
    marker text, not the dispatcher's value.
    """
    try:
        fields = list(string.Formatter().parse(template))
    except ValueError:
        return False
    return all(not spec and conversion is None for _, _, spec, conversion in fields)


def _str_format_target(obj: object) -> str | None:
    """Return the format string of a sandboxed ``str.format`` wrapper, if any."""
    wrapped = getattr(obj, "__wrapped__", None)
    owner = getattr(wrapped, "__self__", None)
    if isinstance(owner, str) and getattr(wrapped, "__name__", "") in {
        "format",
        "format_map",
    }:
        return owner
    return None


def _refuse_deferred_arguments(
    func: Callable[..., Any],
    kind: str,
    name: str,
) -> Callable[..., Any]:
    """Wrap a Jinja filter or test so it rejects dispatcher-only arguments.

    A deferred value, or a string built from one, as any positional or keyword
    argument raises.  ``functools.wraps`` copies ``jinja_pass_arg``, so
    ``pass_context`` and its siblings keep working on the wrapped callable.
    """
    action = (
        f"be passed to the {name!r} {kind}: filters and tests over "
        f"dispatcher-only values are not supported"
    )

    @functools.wraps(func)
    def guarded(*args: Any, **kwargs: Any) -> Any:
        _refuse_dispatcher_values((*args, *kwargs.values()), action)
        return func(*args, **kwargs)

    return guarded


#: Mapping methods whose first argument is a lookup key.
_KEYED_MAPPING_METHODS = frozenset({"get", "pop", "setdefault"})


def _refuse_padded_format(
    template: str,
    args: tuple[Any, ...],
    kwargs: Mapping[str, Any],
) -> None:
    """Refuse a ``str.format``/``format_map`` that would convert a marker.

    Raises
    ------
    DeferredExpressionError
        If *template* has a conversion or format spec and a dispatcher-only
        value is among the values it formats.
    """
    if _is_plain_str_format(template):
        return
    arguments = (*args, *kwargs.values())
    if len(args) == 1 and isinstance(args[0], dict) and not kwargs:
        arguments = (*arguments, *args[0].values())  # format_map
    _refuse_dispatcher_values(
        arguments,
        "be formatted with a conversion or format specification",
    )


def _refuse_computing_call(
    obj: object,
    args: tuple[Any, ...],
    kwargs: Mapping[str, Any],
) -> None:
    """Refuse a call that would compute with a dispatcher-only argument.

    A call that only stores or returns its arguments -- a macro,
    ``namespace()``, ``dict()``, ``cycler()``, ``loop.cycle()``,
    ``list.append()``, the default of ``dict.get()`` -- is allowed: the
    pass-one output check refuses any marker it altered.  Refused are
    ``str.format`` conversions with a spec, every string method (they
    transform or search their arguments: ``'a/b'.split(hostname ~ '')``),
    and a dispatcher-only lookup key (``config.get(hostname ~ '')``).

    Raises
    ------
    DeferredExpressionError
        Naming what the call would have done with the value.
    """
    template = _str_format_target(obj)
    if template is not None:
        _refuse_padded_format(template, args, kwargs)
        return
    owner = getattr(obj, "__self__", None)
    method = getattr(obj, "__name__", "")
    if isinstance(owner, str):
        _refuse_dispatcher_values(
            (*args, *kwargs.values()),
            f"be passed to the string method {method!r}",
        )
    elif isinstance(owner, Mapping) and method in _KEYED_MAPPING_METHODS:
        _refuse_dispatcher_values(args[:1], "be used as a lookup key")


class _PassOneEnvironment(SandboxedEnvironment):
    """Builder-side sandbox that refuses to compute with dispatcher-only values.

    On top of the guarded filters and tests (see :func:`_pass_one_environment`)
    it refuses method access and subscripts on a string that carries a marker,
    ``%`` or ``str.format`` conversions that would pad, truncate or convert
    one, and calls that would compute with one (see
    :func:`_refuse_computing_call`).  A call that merely passes such a value
    through -- a macro, whose body renders under the same guards, or
    ``namespace()``, ``dict()`` and the like -- may receive it.
    """

    intercepted_binops: frozenset[str] = frozenset({"%"})

    def getattr(self, obj: Any, attribute: str) -> Any:
        """Refuse attribute access (e.g. a method) on a marker-bearing string."""
        _refuse_marked_string(obj, f"have its attribute {attribute!r} used")
        return super().getattr(obj, attribute)

    def getitem(self, obj: Any, argument: Any) -> Any:
        """Refuse subscripting or slicing a marker-bearing string."""
        _refuse_marked_string(obj, "be subscripted or sliced")
        return super().getitem(obj, argument)

    def call_binop(
        self,
        context: Context,
        operator: str,
        left: Any,
        right: Any,
    ) -> Any:
        """Allow only plain ``%s`` conversions of dispatcher-only values."""
        if operator == "%" and isinstance(left, str) and not _is_plain_printf(left):
            _refuse_dispatcher_values(
                _mod_operands(right),
                "be formatted by a '%' conversion other than a plain %s",
            )
        return super().call_binop(context, operator, left, right)

    def call(
        self,
        context: Context,
        obj: Any,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Refuse a call that would compute with a dispatcher-only argument."""
        _refuse_computing_call(obj, args, kwargs)
        return super().call(context, obj, *args, **kwargs)


def _pass_one_environment() -> SandboxedEnvironment:
    """Build the builder-side environment: strict, with guarded filters/tests."""
    environment = _PassOneEnvironment(
        undefined=jinja2.StrictUndefined,
        autoescape=False,
        finalize=_finalize,
    )
    environment.filters = {
        name: _refuse_deferred_arguments(func, "filter", name)
        for name, func in environment.filters.items()
    }
    # Jinja's untyped TESTS table is inferred as ``dict[str, function]``.
    environment.tests = {
        name: _refuse_deferred_arguments(cast("Callable[..., Any]", func), "test", name)
        for name, func in environment.tests.items()
    }
    return environment


#: Pass one (builder).  Holds no per-render state -- the nonce lives in the
#: deferred values placed in each render's context -- so it is shared safely.
_PASS_ONE_ENV = _pass_one_environment()

#: Strict single-pass renders and pass-two expression evaluation (dispatcher).
_STRICT_ENV = SandboxedEnvironment(
    undefined=jinja2.StrictUndefined,
    autoescape=False,
    finalize=_finalize,
)


def _is_literal_key(node: nodes.Node) -> bool:
    """Return whether *node* is a literal a deferred ``__getitem__`` emits."""
    if isinstance(node, nodes.Neg):
        node = node.node
        return isinstance(node, nodes.Const) and type(node.value) is int
    return isinstance(node, nodes.Const) and type(node.value) in {str, int}


@functools.lru_cache(maxsize=1024)
def _access_path_root(expression: str) -> str | None:
    """Return the root name of *expression* if it is a pure access path.

    An access path is a name followed only by ``.attr`` and literal
    ``[key]`` subscripts -- the only shape pass one ever emits.  The result
    depends only on *expression*, so it is cached across jobs.
    """
    try:
        parser = Parser(_STRICT_ENV, expression, state="variable")
        node = parser.parse_expression()
        if not parser.stream.eos:
            return None
    except jinja2.TemplateSyntaxError:
        return None
    while isinstance(node, (nodes.Getattr, nodes.Getitem)):
        if isinstance(node, nodes.Getitem) and not _is_literal_key(node.arg):
            return None
        node = node.node
    return node.name if isinstance(node, nodes.Name) else None


def _decode_marker(match: re.Match[str], nonce: str) -> str:
    """Authenticate *match* against *nonce* and return its expression."""
    if not nonce or match.group(1) != nonce:
        raise DeferredExpressionError(
            "deferred expression failed authentication; refusing to "
            "evaluate a marker this job did not produce",
        )
    try:
        return base64.b64decode(match.group(2), validate=True).decode()
    except (binascii.Error, UnicodeDecodeError) as exc:
        raise DeferredExpressionError(
            f"Could not resolve dispatcher-only expression for marker "
            f"{match.group(0)!r}: {exc}",
        ) from exc


@functools.lru_cache(maxsize=1024)
def _compile_access_path(expression: str) -> jinja2.environment.TemplateExpression:
    """Compile an allowed access path once; the compiled form is reusable."""
    return _STRICT_ENV.compile_expression(expression, undefined_to_none=False)


def _evaluate_deferred(expression: str, context: Mapping[str, Any]) -> str:
    """Evaluate an authenticated access path in the dispatcher's context."""
    root = _access_path_root(expression)
    if root not in DISPATCHER_CONTEXT_NAMES:
        raise DeferredExpressionError(
            f"refusing to evaluate deferred expression {expression!r}: only "
            f"access paths rooted in {sorted(DISPATCHER_CONTEXT_NAMES)} are "
            f"resolved by the dispatcher",
        )
    try:
        value = _compile_access_path(expression)(dict(context))
    except jinja2.UndefinedError as exc:
        raise _not_defined_here(expression, str(exc)) from exc
    except Exception as exc:
        raise DeferredExpressionError(
            f"Could not resolve dispatcher-only expression {expression!r}: {exc}",
        ) from exc
    if isinstance(value, jinja2.Undefined):
        raise _not_defined_here(expression, value._undefined_message)
    if callable(value):
        # e.g. ``hostname.upper`` or a data-chosen key naming a dict method:
        # the path names a method, not a value, and calls are not supported.
        raise DeferredExpressionError(
            f"dispatcher-only expression {expression!r} names a method, not a "
            f"value; {_LEAF_ONLY}",
        )
    return str(_finalize(value))


def _not_defined_here(expression: str, reason: str) -> DeferredExpressionError:
    """Build the error for a deferred value this dispatcher does not supply."""
    return DeferredExpressionError(
        f"dispatcher-only expression {expression!r} is not defined by this "
        f"dispatcher ({reason}); for example 'output_dir' exists only on "
        f"dispatchers that provide one",
    )


def _resolve_deferred_expressions(
    text: str,
    context: Mapping[str, Any],
    nonce: str,
) -> str:
    """Replace nonce-authenticated deferred markers with their values.

    Only markers carrying *nonce* whose expression is an access path rooted in
    :data:`DISPATCHER_CONTEXT_NAMES` are evaluated; anything else in *text* --
    in particular job data that happens to contain template syntax -- is left
    literal.  Expressions are evaluated in the sandbox, with the same
    ``finalize`` as pass one.
    """
    if not _has_marker_start(text):
        return text
    _intact_markers(text)
    # The context is fixed for this call, so each distinct marker is decoded,
    # checked and evaluated once however often the script repeats it.
    resolved: dict[str, str] = {}

    def resolve(match: re.Match[str]) -> str:
        marker = match.group(0)
        if marker not in resolved:
            resolved[marker] = _evaluate_deferred(
                _decode_marker(match, nonce),
                context,
            )
        return resolved[marker]

    return _DEFER_MARKER_RE.sub(resolve, text)


def _has_marker_start(text: str) -> bool:
    """Return whether *text* contains a marker prefix, in any letter case.

    Only a null-delimited prefix counts: a script or file path that merely
    contains the literal text ``COURIER-DEFER:`` is not a marker.
    """
    return bool(text) and _MARKER_START.lower() in text.lower()


def _intact_markers(text: str) -> list[re.Match[str]]:
    """Return the markers in *text*, refusing any that a filter altered.

    A filter applied to text that contains a marker (``{% filter upper %}``,
    or ``| replace`` over a string built with ``~``) mutates the marker;
    detect that rather than leave a broken marker in the script.

    Raises
    ------
    DeferredExpressionError
        If a marker prefix is not followed by a well-formed marker.
    """
    matches = list(_DEFER_MARKER_RE.finditer(text))
    if text.lower().count(_MARKER_START.lower()) != len(matches):
        raise DeferredExpressionError(
            "a dispatcher-only expression was altered after interpolation "
            "(e.g. by {% filter %} or a filter over a string containing it); "
            "filters over dispatcher-only values are not supported",
        )
    return matches


def _check_pass_one_markers(text: str, nonce: str, emitted: set[str]) -> None:
    """Refuse pass-one output whose markers are not exactly those it emitted.

    Pass one knows the nonce and the markers it produced (*emitted*), so it
    rejects at the builder what would otherwise fail -- or silently leave
    junk in the script -- on the dispatcher.  Every marker in *text* must be
    intact, carry *nonce* and be one of *emitted*; outside the markers there
    must be no trace of *nonce* and, when any marker was emitted, no NUL
    character.  Together these catch a marker that was rewritten, truncated
    or had its prefix removed, one that was escaped into another form
    (``| tojson``, ``| urlencode`` or ``| pprint`` over a container holding
    it, NUL-stripping), and a marker-shaped run of text from job data.  (The
    nonce is random per job, so job data cannot contain it.)  A render that
    emitted no marker has none to truncate, so a NUL in its job data is kept.

    Raises
    ------
    DeferredExpressionError
        If a marker was altered, escaped or carries another nonce, or the
        text of a render that emitted a marker holds a NUL character outside
        one.
    """
    markers = _intact_markers(text)
    if any(match.group(1) != nonce for match in markers):
        raise DeferredExpressionError(
            "the rendered script contains a deferred-expression marker this "
            "render did not emit (for example from job data); refusing to "
            "pass it to the dispatcher",
        )
    outside = _DEFER_MARKER_RE.sub("", text) if markers else text
    # A NUL is how a truncated marker shows; with no marker emitted there is
    # none to truncate, so a NUL can only be job data and is left alone.
    if emitted and "\x00" in outside:
        raise DeferredExpressionError(
            "the rendered script contains a NUL character outside a "
            "dispatcher-only value: either such a value was altered after "
            "interpolation (sliced, or filtered as part of a longer string), "
            "which is not supported, or job data contains a NUL character, "
            "which is refused in a script that uses a dispatcher-only value",
        )
    if nonce in outside or any(match.group(0) not in emitted for match in markers):
        raise DeferredExpressionError(
            "a dispatcher-only expression was altered or escaped after "
            "interpolation (e.g. by | tojson, | urlencode or | pprint over a "
            "container holding it, or by rewriting its text); only leaf "
            "interpolation, concatenation with '~' and plain string "
            "conversion are supported",
        )


def _to_wire_form(value: Any) -> Any:
    """Return *value* as a dispatcher sees it after the JSON round trip."""
    return json.loads(json.dumps(value, default=json_default))


def _config_from_job_spec(
    spec: PayloadSpec,
    config_class: type[PayloadConfig] = PayloadConfig,
) -> PayloadConfig:
    """Build a payload config from a serialized job spec.

    The template is not sent: ``PayloadSpec.script`` carries the builder's
    rendered script, the only copy of it on the wire.  The hydrated config's
    ``script`` is set to that rendered text, so a spec with a script
    satisfies the file/script/binary requirement and dispatcher-side logic
    that looks at ``config.script`` sees what will run.  A spec from an older
    builder that still carries the raw template in ``config.script`` is
    hydrated the same way: the rendered copy replaces it.

    The template file is not required to exist on the host hydrating the
    spec, so the ``file`` field is path metadata only.  Validation still runs
    (types and the file/script/binary requirement); only the exists-on-disk
    check is skipped, via the ``hydrating`` validation context.

    Keys *config_class* does not define are dropped first: the dispatcher may
    hydrate a lower representation than the builder configured (whose config
    model is a subset), and the rendered script is authoritative.
    """
    known = {
        key: value
        for key, value in spec.config.items()
        if key in config_class.model_fields and key != "script"
    }
    if spec.script is not None and "script" in config_class.model_fields:
        known["script"] = spec.script
    return config_class.model_validate(known, context={"hydrating": True})


class Payload(ServicePlugin):
    """Base class for Payloads.

    Notes
    -----
    A payload is built two ways: by the plugin manager through ``__init__`` on
    the builder side, and by a dispatcher through :meth:`from_job_spec`, which
    hydrates an instance from a job's serialized spec **without calling
    ``__init__``**.  Subclasses must therefore put per-instance setup in
    :meth:`_configure_from_config`, which both paths run, and declare extra
    config fields on a :class:`PayloadConfig` subclass named by
    :attr:`config_class`.

    The builder-side ``__init__`` reads and compiles the template once, so a
    Jinja syntax error or an unreadable template fails plugin construction at
    startup; editing the template file afterwards takes effect on restart.
    """

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
    #: Model that validates this payload's config, on both construction paths.
    config_class: ClassVar[type[PayloadConfig]] = PayloadConfig

    identifier: str
    config: PayloadConfig
    base_config: DispatcherGroupConfig
    #: Entry-point name of the payload plugin that was configured.  A
    #: dispatcher may hydrate a lower representation (``self.name`` differs);
    #: metrics are labelled with this name so payload types stay distinct.
    payload_name: str
    #: Pass-one template compiled from ``file``/``script``; ``None`` for a
    #: binary-only payload and for instances hydrated from a job spec.
    _template: jinja2.Template | None
    #: Set on an instance built by :meth:`from_job_spec`.  Its
    #: ``config.script`` is the builder's *rendered* script, which must never
    #: be rendered again (see :meth:`to_job_spec`).
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
        """Initialize shared instance state from a validated config."""
        self.identifier = identifier
        self.payload_name = payload_name or self.name
        self._logger = get_logger("plugin", self.name, service.config)
        self.config = config
        self.base_config = (
            base_config if base_config is not None else DispatcherGroupConfig()
        )
        self._template = None
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
    ) -> Self:
        """Hydrate a payload instance from a serialized job spec.

        ``__init__`` is not called (see the class notes): the template is not
        read or compiled, because ``spec.script`` is already rendered.

        Parameters
        ----------
        spec : PayloadSpec
            Serialized payload attached to a job by its builder.
        service : Service
            Parent service, used for logging configuration.
        base_config : DispatcherGroupConfig or None, optional
            Dispatcher-group config supplying execution timeouts and logging
            flags.  The dispatcher passes its own config here; defaults to a
            default :class:`DispatcherGroupConfig`.

        Returns
        -------
        Payload
            A new instance of this representation class, whose
            :attr:`payload_name` is ``spec.name`` and whose ``config.script``
            is the rendered ``spec.script`` (see :func:`_config_from_job_spec`).
            It executes that script; it cannot render a job
            (:meth:`to_job_spec` refuses).

        Raises
        ------
        pydantic.ValidationError
            If ``spec.config`` is not a valid :attr:`config_class`.
        """
        instance = cls.__new__(cls)
        instance._bootstrap(
            service,
            _config_from_job_spec(spec, cls.config_class),
            spec.identifier,
            payload_name=spec.name,
            base_config=base_config,
        )
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

    def _probe_toolchain(self, command: list[str]) -> list[ExecutionLog]:
        """Run a toolchain probe and log the per-command return codes.

        The probe is not a job: it is run with ``probe=True``, so it writes no
        log file and records no payload job metrics.
        """
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

    def _template_source(self) -> tuple[str | None, str]:
        """Return the raw template (``None`` if there is none) and its origin.

        Raises
        ------
        ValueError
            If the template file cannot be read.
        """
        file = self.config.file
        if file is None:
            return self.config.script, "inline script"
        try:
            source = file.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            raise ValueError(
                f"Payload {self.identifier!r}: cannot read template file "
                f"{str(file)!r}: {exc}",
            ) from exc
        return source, f"template file {str(file)!r}"

    def _compile_template(self) -> jinja2.Template | None:
        """Read and compile the payload template for pass one.

        Raises
        ------
        ValueError
            If the template cannot be read or is not valid Jinja; the message
            names the file (or ``inline script``) and the line.
        """
        source, origin = self._template_source()
        if source is None:
            return None
        try:
            return _PASS_ONE_ENV.from_string(source)
        except jinja2.TemplateSyntaxError as exc:
            raise ValueError(
                f"Payload {self.identifier!r}: invalid Jinja template in "
                f"{origin}, line {exc.lineno}: {exc.message}",
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
                _STRICT_ENV.from_string(argument)
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
        """Return the base Jinja context shared by both render passes.

        The job's values are normalized to their JSON wire form, so ``config``
        (and ``job.config``) is the plain dict a dispatcher sees after the job
        crosses the broker, even when the builder holds a pydantic model.
        """
        context: dict[str, Any] = _to_wire_form(
            {
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
            },
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
        """Render a compiled *template*; defer dispatcher names in pass one."""
        context = cls._template_context(job, extra_context)
        if defer_nonce is None:
            return template.render(context)
        if not _NONCE_RE.fullmatch(defer_nonce):
            raise ValueError("defer_nonce must be a non-empty lowercase hex string")
        emitted: set[str] = set()
        for name in DISPATCHER_CONTEXT_NAMES:
            context.setdefault(name, _DeferredValue(defer_nonce, name, emitted))
        rendered = template.render(context)
        _check_pass_one_markers(rendered, defer_nonce, emitted)
        return rendered

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
            builder (``builder``) and by the dispatcher (``dispatcher``,
            ``script_path``, ``hostname``, ``output_dir``) so each side can
            fill in what it alone knows.
        defer_nonce : str or None, optional
            When given (the builder's pass one), the names in
            :data:`DISPATCHER_CONTEXT_NAMES` that *extra_context* does not
            supply are emitted as markers authenticated by this nonce, for the
            dispatcher's pass two.  When ``None`` (a strict single pass), every
            name must resolve.

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
            If a dispatcher-only value is used in anything but leaf
            interpolation, concatenation, string conversion or a call that
            only passes it through.

        Notes
        -----
        Two-pass templates support leaf interpolation of dispatcher-only values
        only; conditionals, loops, filters, tests, operators, comparisons and
        calls that compute with them (string methods, lookups keyed by one,
        padded conversions) -- and filters, tests, methods and subscripts over
        a string built from one with ``~`` -- raise
        :class:`DeferredExpressionError`, while a call that merely stores or
        returns one (a macro, ``namespace()``, ``dict()``) may receive it (see
        the module notes, also for what cannot be intercepted).  Builder-side
        values follow ordinary strict Jinja semantics, including
        ``| default`` and ``is defined``.
        """
        environment = _PASS_ONE_ENV if defer_nonce is not None else _STRICT_ENV
        return self._render(
            environment.from_string(script),
            job,
            extra_context,
            defer_nonce,
        )

    def with_rendered_arguments(
        self,
        job: Job,
        extra_context: Mapping[str, Any] | None = None,
    ) -> Self:
        """Return a copy whose ``binary``/``prefix_args``/``suffix_args`` are rendered.

        These config values are the only templates in a payload's command.
        Each is rendered on its own, in one strict pass with the dispatcher's
        *extra_context*; everything else :meth:`declare_command` returns --
        the script path, the interpreter, wrapper snippets -- is used
        literally, so text such as a ``{{`` in ``TMPDIR`` or
        ``slurm_output_dir`` is never evaluated as a template.

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
            return _STRICT_ENV.from_string(argument).render(context)

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
        job: Job,
        extra_context: Mapping[str, Any] | None = None,
        *,
        defer_nonce: str = "",
    ) -> str:
        """Resolve the deferred markers a builder left in *script*.

        This is the dispatcher's half of the two-pass render.  It evaluates only
        markers authenticated by *defer_nonce* whose expression is an access
        path rooted in :data:`DISPATCHER_CONTEXT_NAMES`; the rest of the text is
        never parsed, so job data cannot inject template expressions.

        Raises
        ------
        DeferredExpressionError
            If a marker fails authentication, was altered, is not an allowed
            access path, or names a value this dispatcher does not define.
        """
        if not _has_marker_start(script):
            return script
        return _resolve_deferred_expressions(
            script,
            self._template_context(job, extra_context),
            defer_nonce,
        )

    def write_script(self, text: str, path: Path | None = None) -> Path:
        """Persist already-rendered *text* as an executable file.

        The file is always newly created (``O_EXCL``): an existing file or a
        symlink at *path* is never followed or overwritten.

        Parameters
        ----------
        text : str
            Fully rendered script contents.
        path : Path or None, optional
            File to create.  When ``None`` a uniquely named file is created in
            :func:`tempfile.gettempdir` (which honours ``TMPDIR``).

        Returns
        -------
        Path
            Path to the executable.

        Raises
        ------
        FileExistsError
            If *path* already exists (including as a symlink).
        """
        if path is None:
            fd, name = tempfile.mkstemp(
                suffix=self._file_suffix,
                dir=tempfile.gettempdir(),
            )
            target = Path(name)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
            fd = os.open(path, flags, 0o700)
            target = path
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                os.fchmod(handle.fileno(), 0o755)
                handle.write(text)
        except BaseException:
            target.unlink(missing_ok=True)
            raise
        return target

    def _script_suffix(self) -> str:
        """Return the suffix the dispatcher should give the rendered script."""
        if self.config.file is not None:
            return self.config.file.suffix or self._file_suffix
        return self._file_suffix

    def to_job_spec(self, job: Job, builder: Any | None = None) -> PayloadSpec:
        """Render this payload for *job* and serialize it for the wire.

        This is pass one: the builder fills in everything it knows (job files,
        its own identity and targets) and leaves authenticated markers for the
        values only the dispatcher can supply.  The template compiled at
        construction is reused; the file is not re-read.

        Parameters
        ----------
        job : Job
            Job the payload will travel with.
        builder : JobBuilder or None, optional
            Owning builder, exposed to the template under ``builder``.

        Returns
        -------
        PayloadSpec
            Serialized payload, ready to attach to ``job.payload``.  Its
            ``script`` is the rendered script -- the only copy of the template
            the job carries: ``config`` is this payload's config without
            ``script``.

        Raises
        ------
        jinja2.UndefinedError
            If the template references a builder-side value that is missing.
        DeferredExpressionError
            If a dispatcher-only value is used unsupportedly.
        CourierError
            If this payload was hydrated from a job spec: its ``config.script``
            is already rendered, and rendering it again would evaluate job
            data as a template.
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
        template = self._template
        if template is None:
            # A subclass that bypassed __init__; compile on demand.
            template = self._template = self._compile_template()
        nonce = secrets.token_hex(16)
        script = (
            self._render(template, job, extra, nonce) if template is not None else None
        )
        return PayloadSpec(
            name=self.name,
            identifier=self.identifier,
            config=self.config.model_dump(mode="json", exclude={"script"}),
            script=script,
            suffix=self._script_suffix(),
            defer_nonce=nonce,
        )

    def get_metrics(self) -> dict[str, Any]:
        """Return plugin-specific metrics, labelled with :attr:`payload_name`."""
        return {
            **collect_labeled(
                PAYLOAD_JOB_EXECUTION_DURATION,
                "payload_name",
                self.payload_name,
            ),
            **collect_labeled(
                PAYLOAD_JOBS_PROCESSED,
                "payload_name",
                self.payload_name,
            ),
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
