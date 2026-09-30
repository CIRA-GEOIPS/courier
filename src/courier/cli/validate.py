"""CLI `validate` command — validates a service config file.

Two layers are checked, both offline:

* the service schema (``apiVersion``, ``metadata``, the ``spec.run`` shape),
  exactly as ``courier run`` loads it; and
* the plugins each step names: that every kind is one a step can have, that
  every plugin is installed, that each job builder nests exactly one payload,
  that dispatcher and payload settings pass their plugin's config model, that
  each payload's template compiles, and that each payload can run on the
  dispatchers its builder targets.

The second layer is what turns a half-migrated config -- a dispatcher still
carrying ``bash_script``, say -- into an error here rather than a surprise at
``courier run``. Data monitor and job builder settings are left to their
constructors: some of those need optional dependencies or the network, which
``validate`` must not.
"""

from __future__ import annotations

import difflib
import os
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Annotated, Any, cast

import typer
from pydantic import BaseModel, ValidationError

from courier.cli.feedback import (
    format_problem_report,
    humanise_message,
    load_config_or_exit,
    shell_quote,
)
from courier.cli.init_helpers import declared_config_model
from courier.cli.plugins import KIND_INFO, PLUGIN_REGISTRIES, RUN_KINDS, normalize_kind
from courier.errors import CourierError
from courier.interfaces.discovery import REMOVED_PLUGINS, ClassPluginRegistry
from courier.interfaces.job_builders import block_error_location
from courier.interfaces.payloads import (
    REMOVED_DISPATCHER_KEYS,
    DispatcherGroupConfig,
    PayloadConfig,
    dispatcher_setting,
    payload_setting,
)
from courier.schema.v1alpha1.service_config import MicroserviceModel

if TYPE_CHECKING:
    from collections.abc import Callable

    from pydantic_core import ErrorDetails

    from courier.interfaces.dispatchers import Dispatcher
    from courier.interfaces.payloads import Payload
    from courier.schema.v1alpha1.service_config import ServiceConfigModel
    from courier.service import Service

#: Interface name -> what to call one of them when talking to a human.
_KIND_LABELS = {
    "data_monitors": "data monitor",
    "job_builders": "job builder",
    "dispatchers": "dispatcher",
}

#: Interfaces whose config block is checked against the plugin's config model.
#: Their models are pure data: validating one needs no network, no optional
#: dependency and no running service.
_MODEL_CHECKED_KINDS = frozenset({"dispatchers", "payloads"})

#: The model each checked interface validates with when a plugin class does
#: not name its own ``config_class``.
_DEFAULT_MODELS: dict[str, type[BaseModel]] = {
    "dispatchers": DispatcherGroupConfig,
    "payloads": PayloadConfig,
}

#: A setting that belongs to the other kind of block is almost always in the
#: wrong place, so it is reported with where it goes instead of as unknown.
#: Each entry names how to recognise one -- a field of any installed plugin's
#: config model of the other kind (``slurm_dispatcher``'s ``partition`` as
#: well as every dispatcher's ``timeout_seconds``) -- and where it goes.
_MISPLACED_HINTS: dict[str, tuple[Callable[[str], str | None], str]] = {
    "dispatchers": (payload_setting, "put it in the job builder's payload block"),
    "payloads": (dispatcher_setting, "put it in the dispatcher's config"),
}


#: Start of the message :class:`PayloadConfig` raises for a template file that
#: is not there. See :meth:`_PluginChecker._split_missing_template`.
_MISSING_TEMPLATE = "File does not exist"


@dataclass
class PluginCheck:
    """What :func:`check_plugins` found beyond the service schema.

    Attributes
    ----------
    problems : list[tuple[str, str]]
        ``(location, message)`` pairs that stop the config from running.
    notes : list[str]
        Things worth knowing that are not errors here, e.g. a payload
        template that is not visible from the current directory.
    payloads : list[tuple[str, str, str]]
        ``(builder identifier, payload identifier, payload plugin name)`` for
        every well-formed payload block, for the pipeline summary.
    """

    problems: list[tuple[str, str]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    payloads: list[tuple[str, str, str]] = field(default_factory=list)


@dataclass(frozen=True)
class _BuilderPayload:
    """A job builder whose payload plugin resolved, for the routing check."""

    builder: str
    targets: tuple[str, ...]
    payload_class: type[Payload]
    location: str


def _singular(kind: str) -> str:
    """Return the YAML ``kind`` spelling of registry key *kind*."""
    return KIND_INFO.get(kind, ("", None, ""))[1] or kind


def _label(kind: str) -> str:
    """Return what to call one *kind* plugin in a sentence, e.g. ``job builder``."""
    return KIND_INFO.get(kind, (kind, None, ""))[0].lower()


def _join(location: str, loc: tuple[int | str, ...]) -> str:
    """Append a pydantic error location to a config *location*."""
    return ".".join([location, *(str(part) for part in loc)])


def _config_model(kind: str, plugin_class: type) -> type[BaseModel]:
    """Return the model a *kind* plugin validates its config block with.

    That is the class's ``config_class`` -- what both ``Dispatcher.__init__``
    and ``Payload.__init__`` validate with -- or, for a class that names none,
    the interface's base model.
    """
    return declared_config_model(plugin_class) or _DEFAULT_MODELS[kind]


def _misplaced_key(kind: str, model: type[BaseModel], key: str) -> str | None:
    """Say why *key* does not belong in a *kind* block, or ``None`` if it may.

    Only keys with a known better home are explained: settings removed in an
    earlier release (with their replacement) and settings of the other block,
    i.e. of any installed plugin of the other kind.  Any other unknown key is
    left for *model* to reject.  ``courier run`` gives the same advice: the
    config models raise it themselves.
    """
    if key in model.model_fields:
        return None
    if key in REMOVED_DISPATCHER_KEYS:
        # Both config models reject these with the same advice.
        return f"no longer supported: {REMOVED_DISPATCHER_KEYS[key]}"
    describe, hint = _MISPLACED_HINTS[kind]
    owner = describe(key)
    if owner is not None:
        return f"not a {_label(kind)} setting; it is {owner}: {hint}"
    return None


def _explain(model: type[BaseModel], error: ErrorDetails) -> str:
    """Humanise one *model* error, suggesting the setting a typo was meant as."""
    message = humanise_message(error["msg"])
    loc = error["loc"]
    if error["type"] != "extra_forbidden" or len(loc) != 1:
        return message
    close = difflib.get_close_matches(str(loc[0]), list(model.model_fields), n=1)
    return f"{message}; did you mean {close[0]!r}?" if close else message


def _log_dir_state(log_dir: Path) -> str | None:
    """Say what ``courier run`` would do with *log_dir* as it is on this host.

    Returns
    -------
    str or None
        ``None`` for a writable directory; otherwise the rest of a note
        saying what is wrong with it here and what ``courier run`` does about
        that: it creates a missing directory when it builds the dispatcher,
        but does not change the permissions of an existing one or replace a
        file.
    """
    if log_dir.is_dir():
        if os.access(log_dir, os.W_OK):
            return None
        return (
            "exists here but is not writable; `courier run` does not change "
            "its permissions, and fails to start unless it is writable where "
            "it runs"
        )
    if log_dir.exists():
        return (
            "exists here but is not a directory; `courier run` fails to start "
            "unless it is a writable directory, or can be created as one, "
            "where it runs"
        )
    return (
        "does not exist here; `courier run` creates it when it builds the "
        "dispatcher at startup, and fails to start if it cannot create or "
        "write it where it runs"
    )


def _declared_targets(cfg: dict[str, Any]) -> tuple[str, ...]:
    """Return the dispatcher identifiers a job builder config routes to.

    Mirrors what ``courier run`` hands preflight: every route's ``targets``
    for a routing builder, otherwise the builder's own ``targets``.
    """
    routes = cfg.get("routes")
    if isinstance(routes, list):
        blocks = [route.get("targets") for route in routes if isinstance(route, dict)]
    else:
        blocks = [cfg.get("targets")]
    return tuple(
        target
        for block in blocks
        if isinstance(block, list)
        for target in block
        if isinstance(target, str)
    )


def _offline_service() -> Service:
    """Return the stand-in service a payload is constructed against here.

    A payload reads only ``service.config`` (for its logger) while it is being
    built; ``None`` gives console-only logging, so nothing is contacted.
    """
    return cast("Service", SimpleNamespace(config=None))


class _PluginChecker:
    """Walk ``spec.run`` recursively, collecting problems into a PluginCheck."""

    def __init__(self, config: ServiceConfigModel) -> None:
        self.result = PluginCheck()
        self._allow_implicit_target = config.spec.allow_implicit_target
        # Seeded with every step: the schema already made those unique, so a
        # clash can only come from a nested sub-plugin, which is what is named.
        self._identifiers = {entry.identifier for entry in config.spec.run}
        #: Every ``spec.run`` dispatcher; the class is ``None`` when it did not
        #: resolve (already reported), so routing is judged on the rest.
        self._dispatchers: dict[str, type[Dispatcher] | None] = {
            entry.identifier: None
            for entry in config.spec.run
            if normalize_kind(entry.spec.kind) == "dispatchers"
        }
        self._builders: list[_BuilderPayload] = []

    def _problem(self, location: str, message: str) -> None:
        self.result.problems.append((location, message))

    def check(
        self,
        entry: MicroserviceModel,
        location: str,
        expected_kind: str | None = None,
    ) -> type | None:
        """Check one run step, or one sub-plugin nested under *expected_kind*.

        Returns
        -------
        type or None
            The plugin class, when it is one whose config is checked here and
            it loaded; ``None`` otherwise.
        """
        kind = self._kind(entry, location, expected_kind)
        if kind is None:
            return None
        cfg = self._settings(entry, location)
        if cfg is None:
            return None
        plugin_class = self._resolve(kind, entry, location)
        if plugin_class is not None:
            self._check_config(kind, plugin_class, cfg, entry.identifier, location)
        if kind == "dispatchers":
            self._dispatchers[entry.identifier] = cast(
                "type[Dispatcher] | None",
                plugin_class,
            )
        for key in PLUGIN_REGISTRIES[kind].nested_values:
            nested_class = self._check_nested(
                key,
                cfg,
                entry.identifier,
                kind,
                location,
            )
            if kind == "job_builders" and nested_class is not None:
                self._builders.append(
                    _BuilderPayload(
                        builder=entry.identifier,
                        targets=_declared_targets(cfg),
                        payload_class=cast("type[Payload]", nested_class),
                        location=f"{location}.config.{key}",
                    ),
                )
        return plugin_class

    def _settings(
        self,
        entry: MicroserviceModel,
        location: str,
    ) -> dict[str, Any] | None:
        """Return *entry*'s config block, or report that it is not a mapping."""
        raw = entry.spec.config
        if raw is None:
            return {}
        if isinstance(raw, dict):
            return raw
        self._problem(f"{location}.config", "should be a mapping of settings")
        return None

    def _claim(self, identifier: str, location: str) -> None:
        """Record a nested *identifier*, reporting it if it is already taken.

        ``spec.run`` duplicates are rejected by the schema; this catches a
        nested payload reusing another plugin's identifier, which
        ``courier run`` refuses because both would register under one name.
        """
        if identifier in self._identifiers:
            self._problem(
                location,
                f"identifier {identifier!r} is already used by another plugin; "
                "every run step and nested payload needs its own",
            )
        self._identifiers.add(identifier)

    def _kind(
        self,
        entry: MicroserviceModel,
        location: str,
        expected_kind: str | None,
    ) -> str | None:
        """Return *entry*'s registry key, or report why it cannot go here."""
        kind = normalize_kind(entry.spec.kind)
        if expected_kind is None:
            if kind in RUN_KINDS:
                return kind
            allowed = ", ".join(_singular(k) for k in sorted(RUN_KINDS))
            self._problem(
                f"{location}.kind",
                f"{entry.spec.kind!r} is not a pipeline step; use one of {allowed}",
            )
            return None
        if kind == expected_kind:
            return kind
        self._problem(
            f"{location}.kind",
            f"{entry.spec.kind!r} cannot be nested here; this block takes a "
            f"{_singular(expected_kind)}",
        )
        return None

    def _resolve(
        self,
        kind: str,
        entry: MicroserviceModel,
        location: str,
    ) -> type | None:
        """Check the plugin is installed; load it when its config is checked.

        Names come from entry-point metadata and import nothing. Only plugins
        in :data:`_MODEL_CHECKED_KINDS` are imported, to reach their model.
        """
        registry = PLUGIN_REGISTRIES[kind]
        names = registry.names()
        name = entry.spec.name
        if name not in names:
            available = ", ".join(names) or "none are installed"
            removed = REMOVED_PLUGINS.get((kind, name))
            self._problem(
                f"{location}.name",
                f"{name!r} was removed: {removed}"
                if removed
                else f"no {_label(kind)} plugin named {name!r}; available: {available}",
            )
            return None
        if kind not in _MODEL_CHECKED_KINDS or not isinstance(
            registry,
            ClassPluginRegistry,
        ):
            return None
        try:
            return registry.get_plugin(name)
        except CourierError as exc:
            self._problem(f"{location}.name", str(exc))
            return None

    def _check_config(
        self,
        kind: str,
        plugin_class: type,
        cfg: dict[str, Any],
        identifier: str,
        location: str,
    ) -> None:
        """Validate *cfg* against the plugin's config model.

        Keys with a known better home are reported one by one first and set
        aside, so the model's verdict on everything else is reported in the
        same run rather than after the first fix. A payload that passes is
        then built exactly as ``courier run`` builds it on the job builder,
        which also compiles its template.
        """
        where = f"{location}.config"
        model = _config_model(kind, plugin_class)
        misplaced = {
            key: reason
            for key in cfg
            if (reason := _misplaced_key(kind, model, key)) is not None
        }
        for key, reason in misplaced.items():
            self._problem(f"{where}.{key}", reason)
        remaining = {key: value for key, value in cfg.items() if key not in misplaced}
        validated = self._passes_model(model, remaining, where)
        if validated is None:
            return
        if kind == "dispatchers":
            self._note_log_dir(validated, where)
        elif not misplaced:
            self._build_payload(plugin_class, cfg, identifier, where)

    def _passes_model(
        self,
        model: type[BaseModel],
        cfg: dict[str, Any],
        where: str,
    ) -> BaseModel | None:
        """Validate *cfg* with *model*, reporting each error at its key.

        Validation runs with the ``offline`` context, so nothing on this host
        is touched: a dispatcher's ``log_dir`` is not created (see
        :meth:`_note_log_dir`).

        Returns
        -------
        BaseModel or None
            The validated config, only when *cfg* validated outright; a
            template that is not visible here (a note, not a problem) also
            yields ``None``, since there is then nothing to compile.
        """
        try:
            return model.model_validate(cfg, context={"offline": True})
        except ValidationError as exc:
            for error in self._split_missing_template(exc, where):
                self._problem(_join(where, error["loc"]), _explain(model, error))
        except (ValueError, TypeError, OSError) as exc:
            self._problem(where, str(exc))
        return None

    def _note_log_dir(self, config: BaseModel, where: str) -> None:
        """Note a ``log_to_file`` directory that is not usable as it is here.

        ``courier run`` validates the dispatcher's config when it builds the
        dispatcher, at startup, on the host (or in the container) it runs on,
        which is often not this one: a missing ``log_dir`` is created then,
        and startup fails if it cannot be created or is not writable.  So, as
        for a template file, its state here is a note, not a problem, and
        ``validate`` never creates it.  See :func:`_log_dir_state`.
        """
        if not getattr(config, "log_to_file", False):
            return
        log_dir = Path(str(getattr(config, "log_dir", "")))
        state = _log_dir_state(log_dir)
        if state is not None:
            self.result.notes.append(f"{where}.log_dir: {log_dir} {state}")

    def _split_missing_template(
        self,
        exc: ValidationError,
        where: str,
    ) -> list[ErrorDetails]:
        """Turn a missing template file into a note; return the other errors.

        A relative ``file`` is resolved against wherever ``courier run`` starts
        the builder, and an absolute one often names a path inside a container
        image, so its absence here is not proof it will be absent there. Only
        the existence check is set aside: every other setting was still
        validated in the same pass.
        """
        errors: list[ErrorDetails] = []
        for error in exc.errors(include_url=False):
            if error["loc"] == ("file",) and _MISSING_TEMPLATE in error["msg"]:
                template = error.get("input")
                self.result.notes.append(
                    f"{where}.file: {template} is not visible from {Path.cwd()}, "
                    "so its template was not checked; the job builder will not "
                    "start unless it exists where `courier run` is started",
                )
            else:
                errors.append(error)
        return errors

    def _build_payload(
        self,
        plugin_class: type,
        cfg: dict[str, Any],
        identifier: str,
        where: str,
    ) -> None:
        """Construct the payload offline, reporting what its constructor rejects.

        Construction reads and compiles the template (and the ``binary`` and
        argument templates), so a Jinja syntax error or an unreadable template
        is reported here instead of when the job builder starts.
        """
        try:
            plugin_class(_offline_service(), config=cfg, identifier=identifier)
        except ValidationError as exc:
            for error in exc.errors(include_url=False):
                self._problem(
                    _join(where, error["loc"]),
                    humanise_message(error["msg"]),
                )
        except Exception as exc:  # a third-party constructor may raise anything
            self._problem(where, str(exc))

    def _check_nested(
        self,
        key: str,
        cfg: dict[str, Any],
        parent_identifier: str,
        parent_kind: str,
        location: str,
    ) -> type | None:
        """Check the single sub-plugin a *parent_kind* must nest under *key*.

        Returns
        -------
        type or None
            The nested plugin's class, when it resolved.
        """
        nested = self._nested_entry(key, cfg, parent_kind, location)
        if nested is None:
            return None
        self.result.payloads.append(
            (parent_identifier, nested.identifier, nested.spec.name),
        )
        nested_location = f"{location}.config.{key}.{nested.identifier}"
        self._claim(nested.identifier, nested_location)
        return self.check(nested, nested_location, expected_kind=normalize_kind(key))

    def _nested_entry(
        self,
        key: str,
        cfg: dict[str, Any],
        parent_kind: str,
        location: str,
    ) -> MicroserviceModel | None:
        """Parse the block nested under *key*, or report why it is unusable."""
        label = _label(normalize_kind(key))
        where = f"{location}.config.{key}"
        raw = cfg.get(key)
        if raw is None:
            # Worded like the error JobBuilder.__init__ raises at `courier run`.
            self._problem(
                where,
                f"required, but missing: every {_label(parent_kind)} needs a "
                f"{label} block: its config nests exactly one {label} plugin "
                f"under `{key}:`, which is what its jobs execute",
            )
            return None
        if not isinstance(raw, dict):
            self._problem(where, f"should be a mapping describing one {label} plugin")
            return None
        if "identifier" not in raw and "spec" not in raw and len(raw) != 1:
            found = f": {', '.join(map(str, raw))}" if raw else ""
            self._problem(
                where,
                f"takes exactly one {label} plugin; found {len(raw)}{found}",
            )
            return None
        try:
            return MicroserviceModel.model_validate(raw)
        except ValidationError as exc:
            for error in exc.errors(include_url=False):
                self._problem(
                    _join(where, block_error_location(raw, error["loc"])),
                    humanise_message(error["msg"]),
                )
            return None

    def _receivers(self, builder: _BuilderPayload) -> list[str]:
        """Return the dispatchers *builder*'s jobs reach, as preflight wires them.

        A builder that declares no targets is auto-wired to the sole
        dispatcher when ``allow_implicit_target`` allows it.
        """
        if builder.targets:
            return list(dict.fromkeys(builder.targets))
        if self._allow_implicit_target and len(self._dispatchers) == 1:
            return list(self._dispatchers)
        return []

    def check_routing(self) -> None:
        """Report payloads that a dispatcher they are routed to cannot run.

        Unknown targets and ambiguous implicit routing are left to preflight,
        which owns those rules; only an incompatible pairing is reported.
        """
        for builder in self._builders:
            for target in self._receivers(builder):
                dispatcher = self._dispatchers.get(target)
                if dispatcher is None:
                    continue
                if dispatcher.compatible_representation(builder.payload_class):
                    continue
                self._problem(
                    builder.location,
                    f"payload plugin {builder.payload_class.name!r} cannot run on "
                    f"dispatcher {target!r} ({dispatcher.name}), which supports "
                    f"{dispatcher.representation_names()}",
                )


def check_plugins(config: ServiceConfigModel) -> PluginCheck:
    """Check every plugin a schema-valid config names, without running any.

    Parameters
    ----------
    config : ServiceConfigModel
        A config that already passed schema validation.

    Returns
    -------
    PluginCheck
        Problems that would stop ``courier run``, plus non-fatal notes.
    """
    checker = _PluginChecker(config)
    for entry in config.spec.run:
        checker.check(entry, entry.identifier)
    checker.check_routing()
    return checker.result


def _describe_pipeline(config: Any, payloads: list[tuple[str, str, str]]) -> list[str]:
    """Summarise what was validated, for the operator to sanity-check.

    ``Config valid`` alone answered "did it parse", but not the question the
    operator actually has -- did it parse *as the pipeline I meant*. A count
    per kind catches a step that was silently dropped or duplicated, and the
    payload lines show what each job builder will have executed.
    """
    counts: Counter[str] = Counter(
        normalize_kind(entry.spec.kind) for entry in config.spec.run
    )
    parts = [
        f"{counts[kind]} {label}{'s' if counts[kind] != 1 else ''}"
        for kind, label in _KIND_LABELS.items()
        if counts[kind]
    ]
    total = sum(counts.values())
    lines = [f"  {total} pipeline step{'s' if total != 1 else ''}: {', '.join(parts)}"]
    lines.extend(
        f"  {builder} runs payload {payload} ({name})"
        for builder, payload, name in payloads
    )

    transport = getattr(getattr(config.spec, "broker", None), "transport", None)
    if transport:
        lines.append(f"  broker: {transport}")
    return lines


def validate(
    config_file: Annotated[
        Path,
        typer.Argument(
            metavar="CONFIG",
            help="Service YAML to check. Nothing is started.",
        ),
    ],
) -> None:
    """Validate a service config file without running the service."""
    config = load_config_or_exit(config_file)
    check = check_plugins(config)
    if check.problems:
        typer.echo(
            format_problem_report(
                config_file,
                check.problems,
                next_step=(
                    "Fix these, then re-run:  "
                    f"courier validate {shell_quote(config_file)}"
                ),
            ),
        )
        raise typer.Exit(1)

    typer.echo(f"{config_file} is valid.")
    for line in _describe_pipeline(config, check.payloads):
        typer.echo(line)
    for note in check.notes:
        typer.echo(f"  note: {note}")
    typer.echo(f"\nRun it:  courier run {shell_quote(config_file)}")
