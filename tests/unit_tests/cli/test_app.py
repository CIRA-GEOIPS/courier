"""Unit tests for the root Typer app — ``--log-level`` flag behaviour."""

from __future__ import annotations

from typer.testing import CliRunner

from courier.cli.app import VALID_LOG_LEVELS, app

runner = CliRunner()


def test_help_shows_log_level_flag() -> None:
    """``courier --help`` includes ``--log-level`` and ``-l`` in its output."""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "--log-level" in result.output
    assert "-l" in result.output


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
