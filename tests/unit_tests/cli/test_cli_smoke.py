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

# cspell:ignore summarises uids backticked usefixtures

from __future__ import annotations

import json
import re
import signal
from collections.abc import Iterator
from pathlib import Path

import click
import click.testing
import pytest
import typer.main
from typer.testing import CliRunner

from courier.cli.app import app
from courier.interfaces.discovery import REMOVED_PLUGINS
from courier.interfaces.payloads import REMOVED_DISPATCHER_KEYS

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
# `courier run`. validate runs the checks `courier run` runs at startup, so
# each test here shows one of them is reached; the rules themselves are tested
# where they are defined.


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

_VALID = _service_yaml(_ECHO_PAYLOAD)


def _validate(tmp_path: Path, text: str) -> click.testing.Result:
    path = tmp_path / "svc.yaml"
    path.write_text(text)
    return runner.invoke(app, ["validate", str(path)])


def test_validate_accepts_a_well_formed_payload(tmp_path: Path) -> None:
    """The baseline every rejection below changes one thing in."""
    result = _validate(tmp_path, _VALID)

    assert result.exit_code == 0, result.output
    assert "build runs payload echo (bash_payload)" in result.output


@pytest.mark.parametrize(
    ("text", "where", "message"),
    [
        pytest.param(
            _VALID + "    - identifier: f\n      spec:\n        kind: falcon\n"
            "        name: bash_falcon\n",
            "f.kind",
            "'falcon' is not a pipeline step",
            id="kind-not-a-step",
        ),
        pytest.param(
            _VALID + "        config: hello\n",
            "work.config",
            "should be a mapping of settings",
            id="config-not-a-mapping",
        ),
        pytest.param(
            _VALID.replace("name: local_dispatcher", "name: serial_bash"),
            "work.name",
            "No dispatchers plugin named 'serial_bash'. It was removed: "
            + REMOVED_PLUGINS["dispatchers", "serial_bash"],
            id="removed-plugin",
        ),
        pytest.param(
            _service_yaml(""),
            "build.config.payload",
            "Job builder 'build' has no 'payload' block",
            id="no-payload-block",
        ),
        pytest.param(
            _VALID.replace("bash_payload", "bash_falcon"),
            "build.config.payload.echo.name",
            "No payloads plugin named 'bash_falcon'",
            id="unknown-payload-plugin",
        ),
        pytest.param(
            _service_yaml(_ECHO_PAYLOAD + "                suffix_arg: [x]\n"),
            "build.config.payload.echo.config.suffix_arg",
            "not a recognised setting",
            id="unknown-payload-setting",
        ),
        pytest.param(
            _VALID.replace("echo {{ files | length }}", "'echo {{ hostname | upper }}'"),
            "build.config.payload.echo.config",
            "Payload 'echo': unsupported template in inline script, line 1: "
            "dispatcher-only name 'hostname'",
            id="template-the-payload-rejects",
        ),
        pytest.param(
            _service_yaml(_ECHO_PAYLOAD, "          bash_script: x\n"),
            "work.config",
            "'bash_script' is no longer supported: "
            + REMOVED_DISPATCHER_KEYS["bash_script"],
            id="removed-dispatcher-key",
        ),
        pytest.param(
            _VALID.replace("identifier: echo", "identifier: work"),
            "spec.run",
            "'build': payload 'work' reuses the identifier of a run step",
            id="identifier-clash",
        ),
        pytest.param(
            _service_yaml(_ECHO_PAYLOAD + "          routes: [1]\n"),
            "spec.run",
            "cannot read the job builders' targets or routes",
            id="unreadable-routes",
        ),
    ],
)
def test_validate_rejects_what_courier_run_would(
    tmp_path: Path,
    text: str,
    where: str,
    message: str,
) -> None:
    result = _validate(tmp_path, text)
    output = " ".join(result.output.split())

    assert result.exit_code == 1, result.output
    assert "(1 problem)" in output
    assert f"{where} {message}" in output
    assert "Fix these, then re-run: courier validate" in output


@pytest.mark.parametrize("targets", ["          targets: [work]\n", ""])
def test_validate_rejects_a_payload_its_dispatcher_cannot_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    targets: str,
) -> None:
    """Also with no ``targets``: one dispatcher, so preflight wires it there."""
    from courier.plugins.dispatchers.local_dispatcher import LocalDispatcher

    monkeypatch.setattr(LocalDispatcher, "representations", [])
    text = _VALID.replace("          targets: [work]\n", targets)

    result = _validate(tmp_path, text)

    assert result.exit_code == 1, result.output
    assert "is not compatible with dispatcher 'work'" in result.output


def test_validate_accepts_a_dispatchers_own_settings(tmp_path: Path) -> None:
    """Settings are judged by the model the dispatcher validates with."""
    text = _service_yaml(
        _ECHO_PAYLOAD,
        "          partition: debug\n          slurm_output_dir: /scratch/out\n",
    ).replace("name: local_dispatcher", "name: slurm_dispatcher")

    result = _validate(tmp_path, text)

    assert result.exit_code == 0, result.output


def test_validate_notes_a_template_file_it_cannot_see(tmp_path: Path) -> None:
    """A template may live only inside the image `courier run` starts in.

    That is worth a note, not a failure -- but everything else about the
    payload is still checked.
    """
    payload = _ECHO_PAYLOAD.replace(
        "script: echo {{ files | length }}",
        "file: /opt/image-only/job.sh",
    )

    result = _validate(tmp_path, _service_yaml(payload))

    assert result.exit_code == 0, result.output
    assert "note:" in result.output
    assert "/opt/image-only/job.sh" in result.output

    typo = _validate(tmp_path, _service_yaml(payload + "                binaries: x\n"))
    assert typo.exit_code == 1, typo.output
    assert "binaries" in typo.output


def test_validate_never_creates_a_log_dir(tmp_path: Path) -> None:
    """`validate` is offline: a log_dir is for the host `courier run` runs on.

    It used to create the directory on the validating host, and to reject a
    config whose log_dir only exists (or is only writable) in the image.
    """
    log_dir = tmp_path / "created" / "deep" / "logs"
    dispatcher = f"          log_to_file: true\n          log_dir: {log_dir}\n"

    result = _validate(tmp_path, _service_yaml(_ECHO_PAYLOAD, dispatcher))

    assert result.exit_code == 0, result.output
    assert not (tmp_path / "created").exists()


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


@pytest.mark.usefixtures("_signal_handlers")
@pytest.mark.timeout(60)
@pytest.mark.parametrize(
    ("text", "advice"),
    [
        pytest.param(
            _service_yaml(_ECHO_PAYLOAD + "                timeout_seconds: 5\n"),
            "'timeout_seconds': dispatcher option(s) set in a payload block",
            id="dispatcher-option-in-a-payload",
        ),
        pytest.param(
            _service_yaml(_ECHO_PAYLOAD, "          script: echo hi\n"),
            "'script': payload setting(s) set in a dispatcher block",
            id="payload-setting-in-a-dispatcher",
        ),
    ],
)
def test_run_says_where_a_misplaced_setting_goes(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    text: str,
    advice: str,
) -> None:
    """The error is read from the record ``courier run`` logs it with.

    The console output does not reliably capture log lines across
    ``CliRunner`` invocations.
    """
    path = tmp_path / "svc.yaml"
    broker = "\nspec:\n  broker:\n    transport: memory\n"
    path.write_text(text.replace("\nspec:\n", broker, 1))

    result = runner.invoke(app, ["run", str(path)])

    assert result.exit_code == 1, result.output
    [record] = [r for r in caplog.records if r.getMessage() == _RUN_FAILED]
    assert record.exc_info is not None
    assert advice in str(record.exc_info[1])


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


def test_plugins_list_filtered_by_config_names_its_payloads(tmp_path: Path) -> None:
    """A payload is nested under its job builder, not a step of its own."""
    path = tmp_path / "svc.yaml"
    path.write_text(_VALID.replace("bash_payload", "shell_payload"))

    result = runner.invoke(app, ["plugins", "list", str(path), "--json"])

    assert result.exit_code == 0, result.output
    reported = {(e["type"], e["name"]) for e in json.loads(result.output)["plugins"]}
    assert ("payloads", "shell_payload") in reported
    assert ("payloads", "bash_payload") not in reported


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
