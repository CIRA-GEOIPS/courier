"""Unit tests for the sftp_poller data monitor plugin."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from courier.plugins.classes.data_monitors.sftp_poller import (
    SftpPoller,
    SftpPollerConfig,
)
from courier.types.file import File


def _make_config(**overrides: Any) -> dict[str, Any]:
    defaults: dict[str, Any] = {
        "host": "sftp.example.com",
        "username": "u",
        "password": "p",
    }
    defaults.update(overrides)
    return defaults


# ─── Config Validation ──────────────────────────────────────────────────────


class TestSftpPollerConfig:
    def test_minimal_valid_with_password(self) -> None:
        cfg = SftpPollerConfig.model_validate(_make_config())
        assert cfg.host == "sftp.example.com"
        assert cfg.port == 22

    def test_minimal_valid_with_key(self) -> None:
        cfg = SftpPollerConfig.model_validate(
            {"host": "h", "username": "u", "private_key_path": "/id_rsa"},
        )
        assert cfg.private_key_path == "/id_rsa"

    def test_requires_exactly_one_auth(self) -> None:
        with pytest.raises(ValidationError, match="exactly one"):
            SftpPollerConfig.model_validate({"host": "h", "username": "u"})

    def test_rejects_both_auth(self) -> None:
        with pytest.raises(ValidationError, match="exactly one"):
            SftpPollerConfig.model_validate(
                {
                    "host": "h",
                    "username": "u",
                    "password": "p",
                    "private_key_path": "/k",
                },
            )

    def test_port_range(self) -> None:
        with pytest.raises(ValidationError):
            SftpPollerConfig.model_validate(_make_config(port=0))
        with pytest.raises(ValidationError):
            SftpPollerConfig.model_validate(_make_config(port=99999))


# ─── Constructor ────────────────────────────────────────────────────────────


class TestConstructor:
    def test_initializes_with_defaults(self, mock_service: MagicMock) -> None:
        plugin = SftpPoller(mock_service, _make_config())
        assert plugin.health is False
        assert plugin._client is None
        assert plugin._sftp is None

# ─── URIs and patterns ──────────────────────────────────────────────────────


class TestUriAndPattern:
    def test_uri_format(self, mock_service: MagicMock) -> None:
        plugin = SftpPoller(mock_service, _make_config(host="h", port=2222))
        assert plugin._uri_for("/data/x.nc") == "sftp://u@h:2222/data/x.nc"

    def test_hostname_label_defaults_to_host(self, mock_service: MagicMock) -> None:
        plugin = SftpPoller(mock_service, _make_config())
        assert plugin._hostname_label == "sftp.example.com"

    def test_hostname_label_override(self, mock_service: MagicMock) -> None:
        plugin = SftpPoller(mock_service, _make_config(hostname="alias"))
        assert plugin._hostname_label == "alias"

    def test_glob_matches(self, mock_service: MagicMock) -> None:
        plugin = SftpPoller(mock_service, _make_config(glob_pattern="*.nc"))
        assert plugin._matches_pattern("a.nc") is True
        assert plugin._matches_pattern("a.txt") is False


# ─── _scan_remote ───────────────────────────────────────────────────────────


class TestScanRemote:
    def test_no_sftp_returns_empty(self, mock_service: MagicMock) -> None:
        plugin = SftpPoller(mock_service, _make_config())
        assert list(plugin._scan_remote()) == []

    def test_yields_new_files_and_dedups(self, mock_service: MagicMock) -> None:
        plugin = SftpPoller(
            mock_service,
            _make_config(remote_path="/data", glob_pattern="*.nc"),
        )
        entry_a = MagicMock(filename="a.nc", st_mtime=1_700_000_000)
        entry_b = MagicMock(filename="b.nc", st_mtime=1_700_000_100)
        entry_c = MagicMock(filename="skip.txt")
        sftp = MagicMock()
        sftp.listdir_attr.return_value = [entry_a, entry_b, entry_c]
        plugin._sftp = sftp

        files = list(plugin._scan_remote())
        assert len(files) == 2
        assert all(isinstance(f, File) for f in files)
        # Dedup second scan
        sftp.listdir_attr.return_value = [entry_a, entry_b]
        assert list(plugin._scan_remote()) == []


# ─── _disconnect ────────────────────────────────────────────────────────────


class TestDisconnect:
    def test_clears_handles(self, mock_service: MagicMock) -> None:
        plugin = SftpPoller(mock_service, _make_config())
        plugin._sftp = MagicMock()
        plugin._client = MagicMock()
        plugin._disconnect()
        assert plugin._sftp is None
        assert plugin._client is None

    def test_swallows_errors(self, mock_service: MagicMock) -> None:
        plugin = SftpPoller(mock_service, _make_config())
        sftp = MagicMock()
        sftp.close.side_effect = OSError("boom")
        plugin._sftp = sftp
        plugin._client = MagicMock()
        plugin._disconnect()
        assert plugin._sftp is None


# ─── Lifecycle ──────────────────────────────────────────────────────────────


class TestLifecycle:
    def test_initially_unhealthy(self, mock_service: MagicMock) -> None:
        plugin = SftpPoller(mock_service, _make_config())
        assert plugin.is_healthy() is False

    def test_stop_sets_event(self, mock_service: MagicMock) -> None:
        plugin = SftpPoller(mock_service, _make_config())
        plugin.stop()
        assert plugin._stop_event.is_set()
