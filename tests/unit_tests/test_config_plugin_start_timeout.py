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


def test_must_cover_the_health_gate() -> None:
    with pytest.raises(ConfigurationError, match="plugin_start_timeout"):
        ServiceConfig(plugin_health_check_interval=10, plugin_start_timeout=5)
    ServiceConfig(plugin_health_check_interval=10, plugin_start_timeout=10)
