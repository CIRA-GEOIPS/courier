"""Smoke tests: every CLI command, driven through the real Typer app.

Until now the CLI was only exercised with mocked plugin registries, so the
commands operators actually type were never run end to end. That is where two
shipped bugs lived: ``courier dashboard`` crashed on a ``kind`` that ``courier
validate`` accepts, and two example configs named a job builder that does not
exist. Both were reachable simply by *running the command*.

Every command is run against the configs the project ships, with the real
registries — no mocking. ``run`` is excluded because it starts a service; its
lifecycle is covered by ``tests/test_process_lifecycle.py``.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import click
import pytest
import typer.main
from typer.testing import CliRunner

from courier.cli.app import app

runner = CliRunner()

_REPO_ROOT = Path(__file__).resolve().parents[3]

#: Not service configs: broker fixtures and compose files live alongside them.
_NOT_SERVICE_CONFIGS = {"rabbitmq.conf", "docker-compose.rabbitmq-testing.yaml"}

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


# ── plugins ─────────────────────────────────────────────────────────────────


def test_plugins_list_names_the_builtin_plugins() -> None:
    """The registry must actually resolve, not just render an empty table."""
    result = runner.invoke(app, ["plugins", "list"])
    assert result.exit_code == 0, result.output
    for expected in ("serial_bash", "filter_and_group", "cron_glob"):
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
        app, ["plugins", "list", str(config), "--json"],
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
            pattern = r"(?:^|`|\$ )\s*(courier +[^`\n|>#]+)" if in_fence else (
                r"`\s*(courier +[^`\n]+)`"
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

    for group_name, sub in (("queues", "list"), ("queues", "prune"), ("plugins", "list")):
        group = root.get_command(click.Context(root), group_name)
        command = group.get_command(click.Context(group), sub)
        if "--config" in _opts(command):
            offenders.append(f"{group_name} {sub}")

    assert not offenders, f"these take --config instead of a positional: {offenders}"
