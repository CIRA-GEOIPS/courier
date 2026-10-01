"""Unit tests for the root Typer app — ``--log-level`` flag behaviour."""

from __future__ import annotations

import re
from unittest.mock import ANY, MagicMock, patch

import pytest
from typer.testing import CliRunner

from courier.cli.app import VALID_LOG_LEVELS, app

runner = CliRunner()

_ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")


def _plain(text: str) -> str:
    """Typer forces ANSI colour under GITHUB_ACTIONS; ignore it when matching."""
    return _ANSI_ESCAPE.sub("", text)


def test_help_shows_log_level_flag() -> None:
    """``courier --help`` includes ``--log-level`` and ``-l`` in its output."""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    output = _plain(result.output)
    assert "--log-level" in output
    assert "-l" in output


def test_log_level_valid_levels_accepted() -> None:
    """Every valid level accepted by ``--log-level`` produces exit code 0."""
    for level in VALID_LOG_LEVELS:
        result = runner.invoke(app, ["--log-level", level, "--help"])
        assert result.exit_code == 0, f"failed for level {level}: {result.output}"


def test_log_level_invalid_rejected() -> None:
    """Unrecognised ``--log-level`` value yields non-zero exit and error message."""
    result = runner.invoke(app, ["--log-level", "WRONG", "run"])
    assert result.exit_code != 0
    assert "not a valid log level" in result.output


def test_log_level_case_insensitive() -> None:
    """Lowercase ``--log-level debug`` is accepted (exit code 0)."""
    result = runner.invoke(app, ["--log-level", "debug", "--help"])
    assert result.exit_code == 0


def test_log_level_short_flag() -> None:
    """The short flag ``-l ERROR`` is accepted (exit code 0)."""
    result = runner.invoke(app, ["-l", "ERROR", "--help"])
    assert result.exit_code == 0


@pytest.mark.parametrize("flag", ["--log-level", "-l"])
def test_log_level_reaches_run(flag: str) -> None:
    """``courier -l info run CONFIG`` hands ``INFO`` to the service."""
    with (
        patch("courier.cli.run.load_config_or_exit", return_value=MagicMock()),
        patch("courier.cli.run.run_service") as run_service,
    ):
        result = runner.invoke(app, [flag, "info", "run", "config.yaml"])

    assert result.exit_code == 0, result.output
    run_service.assert_called_once_with(ANY, log_level="INFO", only_set=None)


def test_run_does_not_take_its_own_log_level() -> None:
    """The level is a global option, so it goes before ``run``."""
    result = runner.invoke(app, ["run", "config.yaml", "--log-level", "INFO"])

    assert result.exit_code == 2  # noqa: PLR2004
    assert "No such option" in _plain(result.output)
