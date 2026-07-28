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
from pathlib import Path

import pytest
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
    assert "Config valid" in result.output


def test_validate_rejects_a_malformed_config(tmp_path: Path) -> None:
    """A bad config must fail loudly with a non-zero exit, not pass silently."""
    bad = tmp_path / "bad.yaml"
    bad.write_text("apiVersion: runcourier.dev/v1alpha1\nkind: Service\n")

    result = runner.invoke(app, ["validate", str(bad)])

    assert result.exit_code != 0
    assert "Invalid config" in result.output


def test_validate_reports_a_missing_file(tmp_path: Path) -> None:
    result = runner.invoke(app, ["validate", str(tmp_path / "nope.yaml")])
    assert result.exit_code != 0
    assert "not found" in result.output


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
        app, ["plugins", "list", "--config", str(config), "--json"],
    )
    assert result.exit_code == 0, result.output

    reported = {entry["name"] for entry in json.loads(result.output)["plugins"]}
    assert reported, f"{config.name}: no plugins matched"


# ── queues ──────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("config", _SHIPPED_CONFIGS, ids=_CONFIG_IDS)
def test_queues_list_reports_namespaced_queues(config: Path) -> None:
    """Queue names must be namespaced, or two services collide on one broker."""
    result = runner.invoke(app, ["queues", "list", "--config", str(config)])
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
        ["queues", "prune", "--config", str(config), "--candidate", "ghost-queue"],
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
    assert result.exit_code != 0
    assert "not found" in result.output
