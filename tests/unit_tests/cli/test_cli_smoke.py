"""Smoke tests: every CLI command, driven through the real Typer app.

Until now the CLI was only exercised with mocked plugin registries, so the
commands operators actually type were never run end to end. That is where two
shipped bugs lived: ``courier dashboard`` crashed on a ``kind`` that ``courier
validate`` accepts, and two example configs named a job builder that does not
exist. Both were reachable simply by *running the command*.

Every command is run against the configs the project ships, with the real
registries — no mocking. ``run`` starts a service, so it is only run with
configs it must refuse while building one; its lifecycle is covered by
``tests/test_process_lifecycle.py``.
"""

# cspell:ignore geteuid summarises uids backticked usefixtures

from __future__ import annotations

import json
import os
import re
import signal
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import click
import click.testing
import pytest
import typer.main
from pydantic import BaseModel
from typer.testing import CliRunner

from courier.cli.app import app
from courier.interfaces.dispatchers import dispatchers
from courier.interfaces.payloads import (
    REMOVED_DISPATCHER_KEYS,
    DispatcherGroupConfig,
    PayloadConfig,
    payloads,
)

runner = CliRunner()

_REPO_ROOT = Path(__file__).resolve().parents[3]

#: Not a service config: the broker compose file sits alongside them.
_NOT_SERVICE_CONFIGS = {"docker-compose.rabbitmq-testing.yaml"}

_SHIPPED_CONFIGS = sorted(
    path
    for path in [_REPO_ROOT / "config.yaml", *(_REPO_ROOT / "tests").glob("*.yaml")]
    if path.name not in _NOT_SERVICE_CONFIGS
)
_CONFIG_IDS = [path.name for path in _SHIPPED_CONFIGS]


def test_shipped_configs_were_discovered() -> None:
    """Guard the guard: an empty glob makes every test below vacuous."""
    assert len(_SHIPPED_CONFIGS) >= 4, f"only found {_CONFIG_IDS}"


# ── help ────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "command",
    [[], ["init"], ["run"], ["validate"], ["plugins"], ["queues"]],
    ids=["root", "init", "run", "validate", "plugins", "queues"],
)
def test_help_renders(command: list[str]) -> None:
    """``--help`` must work for every command: it is the discovery path.

    Also catches import-time explosions in a subcommand module, which
    otherwise only surface when someone runs that command in anger.
    """
    result = runner.invoke(app, [*command, "--help"])
    assert result.exit_code == 0, result.output
    assert "Usage:" in result.output


# ── validate ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("config", _SHIPPED_CONFIGS, ids=_CONFIG_IDS)
def test_validate_accepts_every_shipped_config(config: Path) -> None:
    result = runner.invoke(app, ["validate", str(config)])
    assert result.exit_code == 0, result.output
    # Names the file it checked, summarises the pipeline, and offers the next
    # command -- a bare "valid" leaves the operator to guess all three.
    assert config.name in result.output
    assert "pipeline step" in result.output
    assert "courier run" in result.output
    # Every job builder's payload is named: it is what each job executes.
    if "job builder" in result.output:
        assert "runs payload" in result.output


def test_validate_rejects_a_malformed_config(tmp_path: Path) -> None:
    """A bad config must fail loudly with a non-zero exit, not pass silently."""
    bad = tmp_path / "bad.yaml"
    bad.write_text("apiVersion: runcourier.dev/v1alpha1\nkind: Service\n")

    result = runner.invoke(app, ["validate", str(bad)])

    assert result.exit_code != 0
    assert "is not valid" in result.output
    # pydantic internals must not be the operator-facing message
    assert "input_value=" not in result.output
    assert "errors.pydantic.dev" not in result.output


def test_validate_reports_a_missing_file(tmp_path: Path) -> None:
    result = runner.invoke(app, ["validate", str(tmp_path / "nope.yaml")])
    assert result.exit_code == 1
    assert "No config file at" in result.output
    assert "courier init" in result.output, "a dead end without a next step"


# ── validate: plugin checks ─────────────────────────────────────────────────
#
# The schema only checks the shape of `spec.run`. Everything below used to
# pass `courier validate` and then fail -- or, worse, be silently ignored -- at
# `courier run`: removed dispatcher keys, a builder with no payload, a typo in
# a payload setting, a template that does not compile.


def _service_yaml(builder_config: str, dispatcher_config: str = "") -> str:
    """A minimal builder + local dispatcher service, for plugin-check tests.

    Each argument is YAML indented to sit under the step's ``config:`` key.
    """
    return (
        "apiVersion: runcourier.dev/v1alpha1\n"
        "kind: Service\n"
        "metadata:\n"
        "  name: svc\n"
        "  namespace: svc\n"
        "  description: plugin checks\n"
        "spec:\n"
        "  run:\n"
        "    - identifier: build\n"
        "      spec:\n"
        "        kind: job_builder\n"
        "        name: DummyJobBuilder\n"
        "        config:\n"
        "          targets: [work]\n"
        f"{builder_config}"
        "    - identifier: work\n"
        "      spec:\n"
        "        kind: dispatcher\n"
        "        name: local_dispatcher\n"
        + (f"        config:\n{dispatcher_config}" if dispatcher_config else "")
    )


_ECHO_PAYLOAD = (
    "          payload:\n"
    "            identifier: echo\n"
    "            spec:\n"
    "              kind: payload\n"
    "              name: bash_payload\n"
    "              config:\n"
    "                script: echo {{ files | length }}\n"
)


def _validate(tmp_path: Path, text: str) -> click.testing.Result:
    path = tmp_path / "svc.yaml"
    path.write_text(text)
    return runner.invoke(app, ["validate", str(path)])


def test_validate_accepts_a_well_formed_payload(tmp_path: Path) -> None:
    result = _validate(tmp_path, _service_yaml(_ECHO_PAYLOAD))

    assert result.exit_code == 0, result.output
    assert "build runs payload echo (bash_payload)" in result.output


@pytest.mark.parametrize("key", sorted(REMOVED_DISPATCHER_KEYS))
def test_validate_rejects_removed_dispatcher_keys_with_migration_advice(
    tmp_path: Path,
    key: str,
) -> None:
    """A half-migrated serial_bash/parallel_bash config must not pass silently.

    Each removed key is reported at its own location, with the advice for
    what replaces it rather than a bare "not permitted".
    """
    result = _validate(
        tmp_path,
        _service_yaml(_ECHO_PAYLOAD, f"          {key}: x\n"),
    )

    assert result.exit_code == 1, result.output
    assert f"work.config.{key}" in result.output
    assert "no longer supported" in result.output
    assert REMOVED_DISPATCHER_KEYS[key] in result.output
    assert "courier validate" in result.output, "no next step offered"


def test_validate_reports_every_problem_in_one_run(tmp_path: Path) -> None:
    """A removed key must not hide the typo next to it until the next run."""
    result = _validate(
        tmp_path,
        _service_yaml(
            _ECHO_PAYLOAD,
            "          bash_script: echo hi\n          log_to_fil: true\n",
        ),
    )

    assert result.exit_code == 1, result.output
    assert "2 problems" in result.output
    assert "work.config.bash_script" in result.output
    assert "work.config.log_to_fil" in result.output
    assert "did you mean 'log_to_file'?" in result.output


def test_validate_reports_a_removed_key_in_a_payload_block_with_its_advice(
    tmp_path: Path,
) -> None:
    """A removed key in a payload block gets its advice and hides nothing."""
    result = _validate(
        tmp_path,
        _service_yaml(
            _ECHO_PAYLOAD
            + "                python_venv: /opt/venv\n"
            + "                scripts: echo typo\n",
        ),
    )

    assert result.exit_code == 1, result.output
    assert "2 problems" in result.output
    assert "build.config.payload.echo.config.python_venv" in result.output
    assert "no longer supported" in result.output
    assert "default_binary" in result.output
    assert "build.config.payload.echo.config.scripts" in result.output


def test_validate_points_a_payload_setting_on_a_dispatcher_at_the_payload(
    tmp_path: Path,
) -> None:
    result = _validate(
        tmp_path,
        _service_yaml(_ECHO_PAYLOAD, "          script: echo hi\n"),
    )

    assert result.exit_code == 1, result.output
    assert "work.config.script" in result.output
    assert "payload block" in result.output


def test_validate_points_a_dispatcher_setting_on_a_payload_at_the_dispatcher(
    tmp_path: Path,
) -> None:
    result = _validate(
        tmp_path,
        _service_yaml(_ECHO_PAYLOAD + "                timeout_seconds: 5\n"),
    )

    assert result.exit_code == 1, result.output
    assert "build.config.payload.echo.config.timeout_seconds" in result.output
    assert "dispatcher's config" in result.output


def _installed_fields(registry: Any, other: type[BaseModel]) -> list[Any]:
    """Every ``(plugin, field)`` of *registry*'s config models not in *other*."""
    return [
        pytest.param(name, field, id=f"{name}.{field}")
        for name in registry.names()
        for field in registry.get_plugin(name).config_class.model_fields
        if field not in other.model_fields
    ]


def _owners(registry: Any, base: type[BaseModel], field: str) -> list[str]:
    """Name the installed *registry* plugins that define *field*.

    ``[]`` for a field of *base*, which every such plugin accepts -- how the
    advice words it too.
    """
    if field in base.model_fields:
        return []
    return sorted(
        name
        for name in registry.names()
        if field in registry.get_plugin(name).config_class.model_fields
    )


@pytest.mark.parametrize(
    ("dispatcher", "field"),
    _installed_fields(dispatchers, PayloadConfig),
)
def test_validate_explains_every_dispatcher_setting_in_a_payload_block(
    tmp_path: Path,
    dispatcher: str,
    field: str,
) -> None:
    """Derived from the installed models: slurm's ``partition`` included."""
    result = _validate(
        tmp_path,
        _service_yaml(_ECHO_PAYLOAD + f"                {field}: x\n"),
    )

    owners = _owners(dispatchers, DispatcherGroupConfig, field)
    assert owners == [] or dispatcher in owners
    assert result.exit_code == 1, result.output
    assert "1 problem" in result.output
    assert f"build.config.payload.echo.config.{field}" in result.output
    assert (
        f"not a payload setting; it is a {' / '.join(owners) or 'dispatcher'} "
        "setting: put it in the dispatcher's config"
    ) in result.output


@pytest.mark.parametrize(
    ("payload", "field"),
    _installed_fields(payloads, DispatcherGroupConfig),
)
def test_validate_explains_every_payload_setting_in_a_dispatcher_block(
    tmp_path: Path,
    payload: str,
    field: str,
) -> None:
    result = _validate(
        tmp_path,
        _service_yaml(_ECHO_PAYLOAD, f"          {field}: x\n"),
    )

    owners = _owners(payloads, PayloadConfig, field)
    assert owners == [] or payload in owners
    assert result.exit_code == 1, result.output
    assert "1 problem" in result.output
    assert f"work.config.{field}" in result.output
    assert (
        f"not a dispatcher setting; it is a {' / '.join(owners) or 'payload'} "
        "setting: put it in the job builder's payload block"
    ) in result.output


# ── run: the same advice when the service is built ─────────────────────────
#
# `courier run` builds each plugin from its block, and the config models give
# the advice themselves. Only configs that must be refused while the service
# is being built are run here: nothing is started.


@pytest.fixture
def _signal_handlers() -> Iterator[None]:
    """Restore the handlers ``Service`` installs for SIGINT and SIGTERM."""
    saved = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    yield
    for sig, handler in saved.items():
        signal.signal(sig, handler)


#: What ``courier run`` logs, with the exception, when it cannot build the
#: service.
_RUN_FAILED = "Fatal error in run_service"


def _run_error(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    text: str,
) -> str:
    """``courier run`` *text* (a :func:`_service_yaml`) on the memory broker.

    Returns
    -------
    str
        The error that stopped it, read from the record ``courier run`` logs
        it with: the console output does not reliably capture log lines
        across ``CliRunner`` invocations.
    """
    path = tmp_path / "svc.yaml"
    broker = "\nspec:\n  broker:\n    transport: memory\n"
    path.write_text(text.replace("\nspec:\n", broker, 1))

    result = runner.invoke(app, ["run", str(path)])

    assert result.exit_code == 1, result.output
    [record] = [r for r in caplog.records if r.getMessage() == _RUN_FAILED]
    assert record.exc_info is not None
    return str(record.exc_info[1])


@pytest.mark.usefixtures("_signal_handlers")
@pytest.mark.timeout(60)
@pytest.mark.parametrize(
    ("dispatcher", "field"),
    _installed_fields(dispatchers, PayloadConfig),
)
def test_run_explains_every_dispatcher_setting_in_a_payload_block(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    dispatcher: str,
    field: str,
) -> None:
    """Building the payload gives the advice ``courier validate`` gives."""
    error = _run_error(
        tmp_path,
        caplog,
        _service_yaml(_ECHO_PAYLOAD + f"                {field}: x\n"),
    )

    owners = _owners(dispatchers, DispatcherGroupConfig, field)
    assert owners == [] or dispatcher in owners
    named = f"{field!r} ({' / '.join(owners)})" if owners else repr(field)
    assert (
        f"{named}: dispatcher option(s) set in a payload block; move them to "
        "the dispatcher's config"
    ) in error


@pytest.mark.usefixtures("_signal_handlers")
@pytest.mark.timeout(60)
@pytest.mark.parametrize(
    ("payload", "field"),
    _installed_fields(payloads, DispatcherGroupConfig),
)
def test_run_explains_every_payload_setting_in_a_dispatcher_block(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    payload: str,
    field: str,
) -> None:
    """Building the dispatcher gives the advice ``courier validate`` gives."""
    error = _run_error(
        tmp_path,
        caplog,
        _service_yaml(_ECHO_PAYLOAD, f"          {field}: x\n"),
    )

    owners = _owners(payloads, PayloadConfig, field)
    assert owners == [] or payload in owners
    named = f"{field!r} ({' / '.join(owners)})" if owners else repr(field)
    assert (
        f"{named}: payload setting(s) set in a dispatcher block; move them to "
        "the job builder's nested payload block"
    ) in error


def test_validate_rejects_an_unknown_payload_setting(tmp_path: Path) -> None:
    result = _validate(
        tmp_path,
        _service_yaml(_ECHO_PAYLOAD + "                suffix_arg: [x]\n"),
    )

    assert result.exit_code == 1, result.output
    assert "build.config.payload.echo.config.suffix_arg" in result.output
    assert "did you mean 'suffix_args'?" in result.output


def test_validate_rejects_a_builder_without_a_payload(tmp_path: Path) -> None:
    """Every builder needs one; `courier run` refuses the config without it."""
    result = _validate(tmp_path, _service_yaml(""))

    assert result.exit_code == 1, result.output
    assert "build.config.payload" in result.output
    assert "exactly one payload" in result.output
    # The same words as the error `courier run` raises (JobBuilder.__init__).
    assert "every job builder needs a payload block" in result.output


def test_validate_rejects_a_payload_block_naming_two_plugins(tmp_path: Path) -> None:
    two = (
        "          payload:\n"
        "            one: {kind: payload, name: bash_payload}\n"
        "            two: {kind: payload, name: shell_payload}\n"
    )
    result = _validate(tmp_path, _service_yaml(two))

    assert result.exit_code == 1, result.output
    assert "takes exactly one payload plugin; found 2" in result.output


def test_validate_rejects_the_wrong_kind_nested_as_a_payload(tmp_path: Path) -> None:
    wrong = (
        "          payload:\n"
        "            nested:\n"
        "              kind: dispatcher\n"
        "              name: local_dispatcher\n"
    )
    result = _validate(tmp_path, _service_yaml(wrong))

    assert result.exit_code == 1, result.output
    assert "build.config.payload.nested.kind" in result.output
    assert "takes a payload" in result.output


@pytest.mark.parametrize(
    ("block", "where"),
    [
        pytest.param(
            "          payload:\n"
            "            nested:\n"
            "              name: bash_payload\n",
            "build.config.payload.nested.kind",
            id="short-form",
        ),
        pytest.param(
            "          payload:\n"
            "            identifier: nested\n"
            "            spec:\n"
            "              name: bash_payload\n",
            "build.config.payload.spec.kind",
            id="canonical-form",
        ),
    ],
)
def test_validate_locates_a_payload_problem_by_the_keys_written(
    tmp_path: Path,
    block: str,
    where: str,
) -> None:
    """``spec`` is the model's name for the short form's ``<identifier>:``."""
    result = _validate(tmp_path, _service_yaml(block))

    assert result.exit_code == 1, result.output
    assert where in result.output
    assert "required, but missing" in result.output


def test_validate_rejects_an_unknown_payload_plugin(tmp_path: Path) -> None:
    result = _validate(
        tmp_path,
        _service_yaml(_ECHO_PAYLOAD.replace("bash_payload", "bash_falcon")),
    )

    assert result.exit_code == 1, result.output
    assert "no payload plugin named 'bash_falcon'" in result.output
    assert "bash_payload" in result.output, "the alternatives are listed"


@pytest.mark.parametrize("removed", ["serial_bash", "parallel_bash", "http_dispatcher"])
def test_validate_says_what_replaced_a_removed_dispatcher(
    tmp_path: Path,
    removed: str,
) -> None:
    """An upgrade leaves configs naming these; "unknown plugin" is a dead end."""
    text = _service_yaml(_ECHO_PAYLOAD).replace(
        "name: local_dispatcher",
        f"name: {removed}",
    )

    result = _validate(tmp_path, text)

    assert result.exit_code == 1, result.output
    assert "work.name" in result.output
    assert f"'{removed}' was removed:" in result.output
    if removed != "http_dispatcher":
        assert "use local_dispatcher" in result.output


def test_validate_rejects_a_payload_reusing_a_step_identifier(tmp_path: Path) -> None:
    result = _validate(
        tmp_path,
        _service_yaml(_ECHO_PAYLOAD.replace("identifier: echo", "identifier: work")),
    )

    assert result.exit_code == 1, result.output
    assert "identifier 'work' is already used" in result.output


def test_validate_reports_a_payload_template_that_does_not_compile(
    tmp_path: Path,
) -> None:
    """A Jinja syntax error used to pass validate and startup, then fail jobs."""
    result = _validate(
        tmp_path,
        _service_yaml(
            _ECHO_PAYLOAD.replace(
                "echo {{ files | length }}",
                "'echo {% if %}'",
            ),
        ),
    )

    assert result.exit_code == 1, result.output
    assert "build.config.payload.echo.config" in result.output
    assert "invalid Jinja template" in result.output
    assert "line 1" in result.output


def test_validate_reports_a_template_file_that_does_not_compile(
    tmp_path: Path,
) -> None:
    template = tmp_path / "job.sh.j2"
    template.write_text("#!/bin/bash\necho ok\necho {{ files[0] \n")
    payload = (
        "          payload:\n"
        "            echo:\n"
        "              kind: payload\n"
        "              name: bash_payload\n"
        "              config:\n"
        f"                file: {template}\n"
    )

    result = _validate(tmp_path, _service_yaml(payload))

    assert result.exit_code == 1, result.output
    assert "job.sh.j2" in result.output
    assert "line 3" in result.output


def test_validate_notes_a_template_file_it_cannot_see(tmp_path: Path) -> None:
    """A template may live only inside the image `courier run` starts in.

    That is worth a note, not a failure -- but everything else about the
    payload is still checked.
    """
    payload = (
        "          payload:\n"
        "            echo:\n"
        "              kind: payload\n"
        "              name: bash_payload\n"
        "              config:\n"
        "                file: /opt/image-only/job.sh\n"
    )

    result = _validate(tmp_path, _service_yaml(payload))

    assert result.exit_code == 0, result.output
    assert "note:" in result.output
    assert "/opt/image-only/job.sh" in result.output

    typo = _validate(tmp_path, _service_yaml(payload + "                binaries: x\n"))
    assert typo.exit_code == 1, typo.output
    assert "binaries" in typo.output


def test_validate_never_creates_a_log_dir(tmp_path: Path) -> None:
    """`validate` is offline: a log_dir for the target host is only a note.

    It used to create the directory on the validating host, and to reject a
    config whose log_dir only exists (or is only writable) in the image.
    """
    log_dir = tmp_path / "created" / "deep" / "logs"
    dispatcher = f"          log_to_file: true\n          log_dir: {log_dir}\n"

    result = _validate(tmp_path, _service_yaml(_ECHO_PAYLOAD, dispatcher))

    assert result.exit_code == 0, result.output
    assert not (tmp_path / "created").exists()
    assert "note:" in result.output
    assert "log_dir" in result.output


def _log_dir_note(tmp_path: Path, log_dir: Path) -> str:
    """Validate a config logging to *log_dir*; return its one log_dir note."""
    dispatcher = f"          log_to_file: true\n          log_dir: {log_dir}\n"
    result = _validate(tmp_path, _service_yaml(_ECHO_PAYLOAD, dispatcher))
    assert result.exit_code == 0, result.output
    notes = [
        line.strip()
        for line in result.output.splitlines()
        if line.strip().startswith("note: work.config.log_dir")
    ]
    assert len(notes) == 1, result.output
    return notes[0]


def test_validate_notes_a_missing_log_dir_courier_run_will_create(
    tmp_path: Path,
) -> None:
    log_dir = tmp_path / "not" / "yet"

    note = _log_dir_note(tmp_path, log_dir)

    assert note == (
        f"note: work.config.log_dir: {log_dir} does not exist here; `courier "
        "run` creates it when it builds the dispatcher at startup, and fails to "
        "start if it cannot create or write it where it runs"
    )
    assert not (tmp_path / "not").exists()


@pytest.mark.skipif(os.geteuid() == 0, reason="root can write any directory")
def test_validate_notes_a_read_only_log_dir_it_will_not_fix(
    tmp_path: Path,
) -> None:
    log_dir = tmp_path / "read-only"
    log_dir.mkdir()
    log_dir.chmod(0o500)
    try:
        note = _log_dir_note(tmp_path, log_dir)
    finally:
        log_dir.chmod(0o700)

    assert note == (
        f"note: work.config.log_dir: {log_dir} exists here but is not "
        "writable; `courier run` does not change its permissions, and fails to "
        "start unless it is writable where it runs"
    )
    assert "creates it" not in note


def test_validate_notes_a_log_dir_that_is_a_file(tmp_path: Path) -> None:
    log_dir = tmp_path / "a-file"
    log_dir.write_text("")

    note = _log_dir_note(tmp_path, log_dir)

    assert note == (
        f"note: work.config.log_dir: {log_dir} exists here but is not a "
        "directory; `courier run` fails to start unless it is a writable "
        "directory, or can be created as one, where it runs"
    )
    assert "does not exist" not in note


def test_validate_says_nothing_about_a_usable_log_dir(tmp_path: Path) -> None:
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    dispatcher = f"          log_to_file: true\n          log_dir: {log_dir}\n"

    result = _validate(tmp_path, _service_yaml(_ECHO_PAYLOAD, dispatcher))

    assert result.exit_code == 0, result.output
    assert "log_dir" not in result.output


def test_validate_uses_the_dispatchers_config_class(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Settings are judged by the model the dispatcher really validates with.

    A dispatcher with options of its own (``slurm_dispatcher``) declares them
    on a ``config_class`` subclass; validating against the base model would
    reject every one of them.
    """
    from courier.plugins.dispatchers.local_dispatcher import (
        LocalDispatcher,
        LocalDispatcherConfig,
    )

    class _Extended(LocalDispatcherConfig):
        queue_hint: str = "default"

    monkeypatch.setattr(LocalDispatcher, "config_class", _Extended)

    result = _validate(
        tmp_path,
        _service_yaml(_ECHO_PAYLOAD, "          queue_hint: fast\n"),
    )

    assert result.exit_code == 0, result.output


def test_validate_rejects_a_payload_its_dispatcher_cannot_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from courier.plugins.dispatchers.local_dispatcher import LocalDispatcher

    monkeypatch.setattr(LocalDispatcher, "representations", [])

    result = _validate(tmp_path, _service_yaml(_ECHO_PAYLOAD))

    assert result.exit_code == 1, result.output
    assert "cannot run on dispatcher 'work'" in result.output


def test_validate_checks_compatibility_through_implicit_routing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No `targets` and one dispatcher: preflight auto-wires, so check that pair."""
    from courier.plugins.dispatchers.local_dispatcher import LocalDispatcher

    monkeypatch.setattr(LocalDispatcher, "representations", [])
    text = _service_yaml(_ECHO_PAYLOAD).replace("          targets: [work]\n", "")

    result = _validate(tmp_path, text)

    assert result.exit_code == 1, result.output
    assert "cannot run on dispatcher 'work'" in result.output


# ── plugins ─────────────────────────────────────────────────────────────────


def test_plugins_list_names_the_builtin_plugins() -> None:
    """The registry must actually resolve, not just render an empty table."""
    result = runner.invoke(app, ["plugins", "list"])
    assert result.exit_code == 0, result.output
    for expected in ("filter_and_group", "cron_glob"):
        assert expected in result.output


def test_plugins_list_json_is_machine_readable() -> None:
    """``--json`` is documented as pipeable into jq; it must parse."""
    result = runner.invoke(app, ["plugins", "list", "--json"])
    assert result.exit_code == 0, result.output

    payload = json.loads(result.output)
    assert payload["plugins"], "no plugins reported"
    assert {"type", "name"} <= set(payload["plugins"][0])


@pytest.mark.parametrize("config", _SHIPPED_CONFIGS, ids=_CONFIG_IDS)
def test_plugins_list_filtered_by_config(config: Path) -> None:
    """Filtering by config must return the plugins that config references."""
    result = runner.invoke(
        app,
        ["plugins", "list", str(config), "--json"],
    )
    assert result.exit_code == 0, result.output

    reported = {entry["name"] for entry in json.loads(result.output)["plugins"]}
    assert reported, f"{config.name}: no plugins matched"


# ── queues ──────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("config", _SHIPPED_CONFIGS, ids=_CONFIG_IDS)
def test_queues_list_reports_namespaced_queues(config: Path) -> None:
    """Queue names must be namespaced, or two services collide on one broker."""
    result = runner.invoke(app, ["queues", "list", str(config)])
    assert result.exit_code == 0, result.output
    assert "namespace:" in result.output

    namespace = result.output.split("namespace:", 1)[1].split("\n", 1)[0].strip()
    queues = [
        line.strip()
        for line in result.output.splitlines()
        if line.strip() and not line.startswith("namespace:")
    ]
    assert queues, "no queues reported"
    assert all(q.startswith(f"{namespace}-") for q in queues), queues


def test_queues_prune_dry_run_deletes_nothing(tmp_path: Path) -> None:
    """The default must be a report, never a mutation."""
    config = _SHIPPED_CONFIGS[0]
    result = runner.invoke(
        app,
        ["queues", "prune", str(config), "--candidate", "ghost-queue"],
    )
    assert result.exit_code == 0, result.output
    assert "orphan:   ghost-queue" in result.output
    assert "dry-run" in result.output
    assert "deleted:" not in result.output


# ── dashboard ───────────────────────────────────────────────────────────────


@pytest.mark.parametrize("config", _SHIPPED_CONFIGS, ids=_CONFIG_IDS)
def test_dashboard_generates_valid_json(config: Path) -> None:
    """Regression guard: this used to crash on configs ``run`` accepts."""
    pytest.importorskip("grafanalib")

    result = runner.invoke(app, ["dashboard", str(config), "--only-metrics"])
    assert result.exit_code == 0, result.output

    dashboard = json.loads(result.output)
    assert dashboard["panels"], "dashboard has no panels"
    assert dashboard["uid"], "dashboard has no uid"


def test_dashboard_uid_is_stable_across_runs() -> None:
    """Grafana keys on uid; a changing one duplicates rather than updates."""
    pytest.importorskip("grafanalib")

    config = _SHIPPED_CONFIGS[0]
    uids = set()
    for _ in range(3):
        result = runner.invoke(app, ["dashboard", str(config), "--only-metrics"])
        assert result.exit_code == 0, result.output
        uids.add(json.loads(result.output)["uid"])

    assert len(uids) == 1, f"uid changed between runs: {uids}"


def test_dashboard_writes_to_a_file(tmp_path: Path) -> None:
    pytest.importorskip("grafanalib")

    target = tmp_path / "dash.json"
    result = runner.invoke(
        app,
        ["dashboard", str(_SHIPPED_CONFIGS[0]), "--only-metrics", "-o", str(target)],
    )

    assert result.exit_code == 0, result.output
    assert target.exists()
    assert json.loads(target.read_text())["panels"]


def test_dashboard_rejects_an_unknown_split_mode() -> None:
    pytest.importorskip("grafanalib")

    result = runner.invoke(
        app,
        ["dashboard", str(_SHIPPED_CONFIGS[0]), "--split-by", "nonsense"],
    )
    assert result.exit_code != 0
    assert "Invalid --split-by" in result.output


def test_dashboard_reports_a_missing_config(tmp_path: Path) -> None:
    pytest.importorskip("grafanalib")

    result = runner.invoke(app, ["dashboard", str(tmp_path / "nope.yaml")])
    # Same message and same exit code as `validate`: a missing config must not
    # read differently depending on which command noticed it.
    assert result.exit_code == 1
    assert "No config file at" in result.output


# ---------------------------------------------------------------------------
# The CLI as a contract with its own documentation
#
# Three bugs in this repo were all the same shape: an invocation written in the
# docs that nobody ever executed. `courier dashboard config.yaml --only-metrics`
# failed with "No such command"; the README's two headline examples,
# `courier run --config` and `courier validate --config`, both failed with
# "No such option". Each was correct prose about a CLI that did not exist.
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parents[3]

#: Tokens that stand in for a real value in prose. Never resolved.
_PLACEHOLDER = re.compile(r"^[<{\[]|[>}\]]$|^\$|^\.\.\.$")


def _iter_documented_invocations() -> list[tuple[str, int, str]]:
    """Yield ``(source, line number, command)`` for every documented `courier` call.

    Reads fenced code blocks and inline backtick spans from the README and every
    docs page. Shell noise (prompts, pipes, comments) is stripped; anything that
    is not a plain `courier ...` call is skipped.
    """
    sources = [_REPO_ROOT / "README.md"]
    sources += sorted((_REPO_ROOT / "sphinx").rglob("*.md"))
    sources += sorted((_REPO_ROOT / "examples").rglob("*.md"))

    found: list[tuple[str, int, str]] = []
    for path in sources:
        in_fence = False
        for number, raw in enumerate(path.read_text().splitlines(), start=1):
            if raw.lstrip().startswith("```"):
                in_fence = not in_fence
                continue

            # Inside a fence a line may start with the command; outside one,
            # only a backticked span counts. Prose such as "courier loads it,
            # rather than..." is a sentence, not an invocation.
            pattern = (
                r"(?:^|`|\$ )\s*(courier +[^`\n|>#]+)"
                if in_fence
                else (r"`\s*(courier +[^`\n]+)`")
            )
            for match in re.finditer(pattern, raw):
                command = match.group(1).strip().rstrip("\\").strip()
                found.append((str(path.relative_to(_REPO_ROOT)), number, command))
    return found


def _root_command() -> click.Command:
    """The real Click tree behind the Typer app."""
    return typer.main.get_command(app)


def _opts(command: click.Command) -> set[str]:
    """Every option string this command accepts."""
    return {opt for param in command.params for opt in getattr(param, "opts", [])}


def _resolve(root: click.Command, tokens: list[str]) -> tuple[click.Command, list[str]]:
    """Walk the Click tree to the command *tokens* names.

    Duck-typed on ``get_command`` rather than ``isinstance(.., click.Group)``:
    Typer's group class does not inherit from ``click.Group``, so an isinstance
    check silently resolves nothing and the guard passes on everything.
    """
    command = root
    index = 1  # skip "courier"
    while index < len(tokens) and hasattr(command, "get_command"):
        candidate = command.get_command(click.Context(command), tokens[index])
        if candidate is None:
            break
        command = candidate
        index += 1
    return command, tokens[index:]


def test_documented_invocations_were_found() -> None:
    """Guard the guard: a changed docs layout would make the check vacuous."""
    found = _iter_documented_invocations()
    assert len(found) >= 15, f"only found {found}"


def test_documented_invocations_parse() -> None:
    """Every `courier ...` line in the docs must work against the real CLI.

    Checks flags and subcommands only -- never that referenced files exist,
    since the docs are full of `my-service.yaml` and `{output_path}`. A guard
    that complained about those would be noise and would get switched off.
    """
    root = _root_command()
    problems: list[str] = []

    for source, line, command in _iter_documented_invocations():
        tokens = command.split()
        resolved, rest = _resolve(root, tokens)

        # `courier <command> CONFIG` in a reference page is a template, not an
        # invocation. Checking it would force docs to stop showing the shape.
        if len(tokens) > 1 and _PLACEHOLDER.match(tokens[1]):
            continue

        if resolved is root and len(tokens) > 1 and not tokens[1].startswith("-"):
            problems.append(f"{source}:{line}: unknown command {tokens[1]!r}")
            continue

        # Click attaches --help dynamically rather than as a declared param.
        known = _opts(resolved) | {"--help"}
        for token in rest:
            if not token.startswith("-") or _PLACEHOLDER.match(token):
                continue
            flag = token.split("=", 1)[0]
            if flag not in known:
                problems.append(
                    f"{source}:{line}: {' '.join(tokens[:2])} has no {flag!r}",
                )

    assert not problems, "documented commands that do not work:\n" + "\n".join(
        problems,
    )


def test_top_level_help_describes_the_tool() -> None:
    """`courier --help` must say what courier is, not how it is wired.

    Typer uses the ``@app.callback`` docstring as the program description, so
    this once read "Pre-command callback: validate --log-level" -- an
    implementation detail, to someone asking what the tool does.
    """
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "callback" not in result.output.lower()
    assert "courier init" in result.output, "should name a first command to run"


def test_version_flag_matches_the_package() -> None:
    """`--version` is the question every operator asks a new binary first."""
    import courier

    result = runner.invoke(app, ["--version"])

    assert result.exit_code == 0
    assert courier.__version__ in result.output


def test_config_is_positional_for_every_command() -> None:
    """One noun, one grammar.

    `courier queues list config.yaml` used to fail while
    `courier validate config.yaml` worked, because queues took `--config`.
    """
    root = _root_command()
    offenders: list[str] = []

    for name in ("run", "validate", "dashboard"):
        command = root.get_command(click.Context(root), name)
        if command and "--config" in _opts(command):
            offenders.append(name)

    for group_name, sub in (
        ("queues", "list"),
        ("queues", "prune"),
        ("plugins", "list"),
    ):
        group = root.get_command(click.Context(root), group_name)
        command = group.get_command(click.Context(group), sub)
        if "--config" in _opts(command):
            offenders.append(f"{group_name} {sub}")

    assert not offenders, f"these take --config instead of a positional: {offenders}"
