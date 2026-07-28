"""Unit tests for the file_system_poller_watchdog data monitor plugin."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pydantic
import pytest

from courier.plugins.classes.data_monitors.file_system_poller_watchdog import (
    FileSystemPoller,
)
from courier.types.file import File


# ─── Fixtures / Helpers ──────────────────────────────────────────────────────


def _make_config(tmp_path: Path) -> dict:
    return {"path": str(tmp_path)}


# ─── Constructor ─────────────────────────────────────────────────────────────


class TestConstructor:
    def test_stores_path_and_initial_health(self, mock_service: MagicMock, tmp_path: Path) -> None:
        plugin = FileSystemPoller(mock_service, _make_config(tmp_path))
        assert plugin.path_to_watch == str(tmp_path)
        assert plugin.health is False

    def test_module_init_short_circuits(self) -> None:
        """Plugin must not error when service is None (registration path)."""
        plugin = FileSystemPoller(None, None)
        # No path_to_watch set when service is None — registration mode
        assert not hasattr(plugin, "path_to_watch")

    def test_missing_path_in_config_raises(self, mock_service: MagicMock) -> None:
        with pytest.raises(pydantic.ValidationError):
            FileSystemPoller(mock_service, {})


# ─── is_healthy ──────────────────────────────────────────────────────────────


class TestIsHealthy:
    def test_initially_unhealthy(self, mock_service: MagicMock, tmp_path: Path) -> None:
        plugin = FileSystemPoller(mock_service, _make_config(tmp_path))
        assert plugin.is_healthy() is False

    def test_health_reflects_attribute(self, mock_service: MagicMock, tmp_path: Path) -> None:
        plugin = FileSystemPoller(mock_service, _make_config(tmp_path))
        plugin.health = True
        assert plugin.is_healthy() is True


# ─── find_file ───────────────────────────────────────────────────────────────


class TestFindFile:
    def test_yields_file_for_event(self, mock_service: MagicMock, tmp_path: Path, mocker) -> None:
        """A file path enqueued by the watchdog handler must be yielded as a File."""
        plugin = FileSystemPoller(mock_service, _make_config(tmp_path))

        observer_instance = MagicMock()
        mocker.patch(
            "courier.plugins.classes.data_monitors.file_system_poller_watchdog.Observer",
            return_value=observer_instance,
        )

        # Pre-load the queue used by find_file by patching queue.Queue's get
        sample_path = str(tmp_path / "sample.nc")
        sentinel = RuntimeError("stop-test")
        fake_queue = MagicMock()
        fake_queue.get.side_effect = [sample_path, sentinel]
        mocker.patch(
            "courier.plugins.classes.data_monitors.file_system_poller_watchdog.queue.Queue",
            return_value=fake_queue,
        )

        gen = plugin.find_file()
        first = next(gen)
        assert isinstance(first, File)
        assert first.file == Path(sample_path)
        assert first.hostname == "localhost"
        assert plugin.health is True

        # Next call raises sentinel; generator's finally must still run
        with pytest.raises(RuntimeError, match="stop-test"):
            next(gen)
        observer_instance.stop.assert_called_once()
        observer_instance.join.assert_called_once()

    def test_missing_directory_raises_runtime(
        self, mock_service: MagicMock, tmp_path: Path, mocker
    ) -> None:
        plugin = FileSystemPoller(mock_service, _make_config(tmp_path))
        observer_instance = MagicMock()
        observer_instance.start.side_effect = FileNotFoundError("nope")
        mocker.patch(
            "courier.plugins.classes.data_monitors.file_system_poller_watchdog.Observer",
            return_value=observer_instance,
        )
        with pytest.raises(RuntimeError, match="Cannot watch directory"):
            next(plugin.find_file())

    def test_missing_directory_raised_from_schedule(
        self, mock_service: MagicMock, tmp_path: Path, mocker
    ) -> None:
        """schedule(), not start(), is what raises on a missing directory.

        Regression guard: the try block used to wrap start() only, so an
        OSError from schedule() escaped as a bare watchdog error rather than
        the plugin's own message.
        """
        plugin = FileSystemPoller(mock_service, _make_config(tmp_path))
        observer_instance = MagicMock()
        observer_instance.schedule.side_effect = OSError("no such directory")
        mocker.patch(
            "courier.plugins.classes.data_monitors.file_system_poller_watchdog.Observer",
            return_value=observer_instance,
        )
        with pytest.raises(RuntimeError, match="Cannot watch directory"):
            next(plugin.find_file())

    def test_find_file_returns_when_stop_event_set(
        self, mock_service: MagicMock, tmp_path: Path, mocker
    ) -> None:
        """An idle watch directory must not block find_file() forever.

        Regression guard: the loop used to call a bare blocking
        ``file_queue.get()``, so a monitor watching a quiet directory never
        observed stop() and wedged interpreter shutdown.
        """
        plugin = FileSystemPoller(mock_service, _make_config(tmp_path))
        observer_instance = MagicMock()
        mocker.patch(
            "courier.plugins.classes.data_monitors.file_system_poller_watchdog.Observer",
            return_value=observer_instance,
        )
        plugin._stop_event.set()

        assert list(plugin.find_file()) == []
        observer_instance.stop.assert_called_once()
        observer_instance.join.assert_called_once()
