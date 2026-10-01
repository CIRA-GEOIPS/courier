"""CLI `validate` command — validates a service config file.

Checks offline, with the code ``courier run`` uses at startup: the service
schema; each step's kind and plugin name; each job builder's ``payload`` block,
parsed and built as the job builder does (which compiles its template); each
dispatcher's settings, validated as the dispatcher does but creating nothing;
payload identifiers; and that each payload can run on every dispatcher its
builder reaches. Data monitor and job builder settings are left to ``courier
run``: some of those plugins need optional dependencies or the network.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Annotated, Any, cast

import typer
from pydantic import ValidationError

from courier.cli.feedback import (
    format_problem_report,
    humanise_message,
    load_config_or_exit,
    shell_quote,
)
from courier.cli.plugins import KIND_INFO, PLUGIN_REGISTRIES, RUN_KINDS, normalize_kind
from courier.cli.run import (
    _check_payload_identifiers,
    _collect_topology,
    _payload_blocks,
)
from courier.errors import CourierError
from courier.interfaces.job_builders import PAYLOAD_KEY, parse_payload_block
from courier.service import _require_compatible

if TYPE_CHECKING:
    from courier.schema.v1alpha1.service_config import (
        MicroserviceModel,
        ServiceConfigModel,
    )
    from courier.service import Service

#: Interface name -> what to call one of them when talking to a human.
_KIND_LABELS = {
    "data_monitors": "data monitor",
    "job_builders": "job builder",
    "dispatchers": "dispatcher",
}

#: Kinds whose plugins are imported, to validate their settings. The others
#: are only looked up by name: importing some needs optional dependencies.
_IMPORTED_KINDS = frozenset({"dispatchers", "payloads"})


@dataclass
class PluginCheck:
    """What :func:`check_plugins` found beyond the service schema."""

    #: ``(location, message)`` pairs that stop the config from running.
    problems: list[tuple[str, str]] = field(default_factory=list)
    #: Not errors: payload template files that are not visible from here.
    notes: list[str] = field(default_factory=list)

    def report(self, where: str, exc: Exception) -> None:
        """Record *exc* at *where*; a pydantic error once per field, at that field.

        A template ``file`` that does not exist here is only a note: it may
        exist only where ``courier run`` starts (inside an image, say).
        """
        if not isinstance(exc, ValidationError):
            self.problems.append((where, str(exc)))
            return
        for error in exc.errors(include_url=False):
            location = ".".join([where, *map(str, error["loc"])])
            if error["loc"] == ("file",) and "File does not exist" in error["msg"]:
                self.notes.append(
                    f"{location}: {error['input']} is not visible from "
                    f"{Path.cwd()}, so its template was not checked",
                )
            else:
                self.problems.append((location, humanise_message(error["msg"])))


def _plugin_class(check: PluginCheck, kind: str, name: str, where: str) -> Any:
    """Return the *kind* plugin class *name*, or record why it cannot be used.

    Returns ``None`` for an installed plugin of a kind not in
    :data:`_IMPORTED_KINDS`, which is not imported.
    """
    registry = PLUGIN_REGISTRIES[kind]
    if kind not in _IMPORTED_KINDS and name in registry.names():
        return None
    try:
        return registry.get_plugin(name)
    except CourierError as exc:
        check.report(where, exc)
        return None


def _check_dispatcher(
    check: PluginCheck,
    entry: MicroserviceModel,
    dispatcher_class: Any,
) -> None:
    """Validate a dispatcher's settings as ``Dispatcher.__init__`` does."""
    try:
        # ``offline``: a ``log_dir`` is not created on this host.
        dispatcher_class.config_class.model_validate(
            entry.spec.config or {},
            context={"offline": True},
        )
    except Exception as exc:  # a third-party config model may raise anything
        check.report(f"{entry.identifier}.config", exc)


def _check_payload(check: PluginCheck, entry: MicroserviceModel) -> Any:
    """Build a job builder's payload as ``JobBuilder.__init__`` does.

    Returns
    -------
    type[Payload] or None
        The payload class, when the block names an installed one.
    """
    where = f"{entry.identifier}.config.{PAYLOAD_KEY}"
    try:
        block = parse_payload_block(entry.identifier, entry.spec.config)
    except CourierError as exc:
        check.report(where, exc)
        return None
    where = f"{where}.{block.identifier}"
    payload_class = _plugin_class(check, "payloads", block.spec.name, f"{where}.name")
    if payload_class is not None:
        # While it is built, a payload reads only ``service.config`` (logging).
        service = cast("Service", SimpleNamespace(config=None))
        try:
            payload_class(service, block.spec.config, block.identifier)
        except Exception as exc:  # a third-party payload may raise anything
            check.report(f"{where}.config", exc)
    return payload_class


def _check_compatibility(
    check: PluginCheck,
    config: ServiceConfigModel,
    dispatcher_classes: dict[str, Any],
    payload_classes: dict[str, Any],
) -> None:
    """Record each payload that a dispatcher its builder reaches cannot run.

    Called only once every step resolved, so every class is known.
    """
    try:
        topology = _collect_topology(config)
    except (AttributeError, TypeError) as exc:  # `courier run` fails here too
        check.problems.append(
            ("spec.run", f"cannot read the job builders' targets or routes: {exc}"),
        )
        return
    for dispatcher_id, dispatcher_class in dispatcher_classes.items():
        for builder_id in topology.builders_targeting(
            dispatcher_id,
            allow_implicit_target=config.spec.allow_implicit_target,
        ):
            payload_class = payload_classes[builder_id]
            try:
                _require_compatible(
                    builder_id,
                    payload_class,
                    payload_class.name,
                    dispatcher_id,
                    dispatcher_class,
                )
            except CourierError as exc:
                check.report(f"{builder_id}.config.{PAYLOAD_KEY}", exc)


def check_plugins(config: ServiceConfigModel) -> PluginCheck:
    """Check, without running anything, what ``courier run`` refuses at startup.

    Parameters
    ----------
    config : ServiceConfigModel
        A config that already passed schema validation.

    Returns
    -------
    PluginCheck
        Problems that would stop ``courier run``, plus non-fatal notes.
    """
    check = PluginCheck()
    dispatcher_classes: dict[str, Any] = {}
    payload_classes: dict[str, Any] = {}
    for entry in config.spec.run:
        kind = normalize_kind(entry.spec.kind)
        if kind not in RUN_KINDS:
            allowed = ", ".join(sorted(KIND_INFO[k][1] or k for k in RUN_KINDS))
            check.problems.append(
                (
                    f"{entry.identifier}.kind",
                    f"{entry.spec.kind!r} is not a pipeline step; use one of {allowed}",
                ),
            )
        elif not isinstance(entry.spec.config or {}, dict):
            check.problems.append(
                (f"{entry.identifier}.config", "should be a mapping of settings"),
            )
        else:
            where = f"{entry.identifier}.name"
            plugin_class = _plugin_class(check, kind, entry.spec.name, where)
            if kind == "dispatchers" and plugin_class is not None:
                _check_dispatcher(check, entry, plugin_class)
                dispatcher_classes[entry.identifier] = plugin_class
            elif kind == "job_builders":
                payload_classes[entry.identifier] = _check_payload(check, entry)
    try:
        _check_payload_identifiers(config)
    except CourierError as exc:
        check.report("spec.run", exc)
    if not check.problems:  # routing reads every job builder's config
        _check_compatibility(check, config, dispatcher_classes, payload_classes)
    return check


def _describe_pipeline(config: Any) -> list[str]:
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
        f"  {builder} runs payload {block.identifier} ({block.spec.name})"
        for builder, block in _payload_blocks(config).items()
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
    for line in _describe_pipeline(config):
        typer.echo(line)
    for note in check.notes:
        typer.echo(f"  note: {note}")
    typer.echo(f"\nRun it:  courier run {shell_quote(config_file)}")
