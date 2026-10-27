"""``plugin_start_timeout`` bounds how long a plugin's ``start()`` may take."""

from __future__ import annotations

import pytest

from courier.config import ServiceConfig
from courier.errors import ConfigurationError


def test_defaults_to_sixty_seconds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PLUGIN_START_TIMEOUT", raising=False)
    assert ServiceConfig().plugin_start_timeout == 60


def test_reads_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PLUGIN_START_TIMEOUT", "15")
    assert ServiceConfig().plugin_start_timeout == 15


def test_accepts_fractional_seconds(monkeypatch: pytest.MonkeyPatch) -> None:
    """The timeout is a float, like the intervals it is compared with."""
    monkeypatch.setenv("PLUGIN_START_TIMEOUT", "2.5")
    assert ServiceConfig().plugin_start_timeout == 2.5  # noqa: PLR2004


@pytest.mark.parametrize("timeout", [0, -1])
def test_must_be_positive(timeout: float) -> None:
    """A zero or negative timeout would give up on every start at once."""
    with pytest.raises(ConfigurationError, match="plugin_start_timeout"):
        ServiceConfig(plugin_start_timeout=timeout)


def test_may_be_shorter_than_the_health_gate() -> None:
    """Only start() is timed, so the gate's length places no bound on it."""
    ServiceConfig(plugin_health_check_interval=10, plugin_start_timeout=5)
