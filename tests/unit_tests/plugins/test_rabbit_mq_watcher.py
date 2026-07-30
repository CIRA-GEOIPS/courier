"""Unit tests for the rabbit_mq_watcher data monitor plugin."""

from __future__ import annotations

import queue
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from courier.plugins.data_monitors.rabbit_mq_watcher import (
    RabbitMQWatcher,
    _parse_hostname_only,
    _parse_regex,
    _parse_user_at_host_colon_path,
)
from courier.types.file import File


def _make_config(**overrides: Any) -> dict[str, Any]:
    cfg: dict[str, Any] = {}
    cfg.update(overrides)
    return cfg


# ─── Location Parsers ───────────────────────────────────────────────────────


class TestLocationParsers:
    def test_user_at_host_colon_path_parses(self) -> None:
        host, path = _parse_user_at_host_colon_path("admin@host1:/data")
        assert host == "host1"
        assert path == "/data"

    def test_user_at_host_missing_at_raises(self) -> None:
        with pytest.raises(ValueError, match="user@hostname"):
            _parse_user_at_host_colon_path("nohost")

    def test_user_at_host_missing_colon_raises(self) -> None:
        with pytest.raises(ValueError, match="separating hostname"):
            _parse_user_at_host_colon_path("user@host")

    def test_user_at_host_empty_hostname_raises(self) -> None:
        with pytest.raises(ValueError, match="Empty hostname"):
            _parse_user_at_host_colon_path("user@:/path")

    def test_hostname_only_parses(self) -> None:
        host, path = _parse_hostname_only("hostname1")
        assert host == "hostname1"
        assert path == "/"

    def test_hostname_only_empty_raises(self) -> None:
        with pytest.raises(ValueError, match="Empty location"):
            _parse_hostname_only("")

    def test_regex_with_path(self) -> None:
        host, path = _parse_regex(
            "host1@/data",
            pattern=r"(?P<hostname>[^@]+)@(?P<path>.+)",
        )
        assert host == "host1"
        assert path == "/data"

    def test_regex_no_match_raises(self) -> None:
        with pytest.raises(ValueError, match="did not match"):
            _parse_regex("xxx", pattern=r"(?P<hostname>\d+)")

    def test_regex_default_path_is_root(self) -> None:
        host, path = _parse_regex("host1", pattern=r"(?P<hostname>\w+)")
        assert host == "host1"
        assert path == "/"


# ─── Constructor ────────────────────────────────────────────────────────────


class TestConstructor:
    def test_defaults(self, mock_service: MagicMock) -> None:
        plugin = RabbitMQWatcher(mock_service, _make_config())
        assert plugin.rabbitmq_host == "localhost"
        assert plugin.rabbitmq_port == 5672
        assert plugin.rabbitmq_queue == "file_catalog"
        assert plugin.location_format == "user_at_host_colon_path"
        assert plugin.health is False

    def test_unknown_location_format_raises(self, mock_service: MagicMock) -> None:
        with pytest.raises(ValueError, match="Unknown location_format"):
            RabbitMQWatcher(mock_service, _make_config(location_format="bogus"))

    def test_regex_format_requires_pattern(self, mock_service: MagicMock) -> None:
        with pytest.raises(ValueError, match="location_format_regex"):
            RabbitMQWatcher(mock_service, _make_config(location_format="regex"))

    def test_custom_overrides(self, mock_service: MagicMock) -> None:
        plugin = RabbitMQWatcher(
            mock_service,
            _make_config(
                rabbitmq_host="rabbit.example.com",
                rabbitmq_port=15672,
                rabbitmq_queue="custom-queue",
                rabbitmq_username="me",
                rabbitmq_password="secret",
            ),
        )
        assert plugin.rabbitmq_host == "rabbit.example.com"
        assert plugin.rabbitmq_port == 15672
        assert plugin.rabbitmq_queue == "custom-queue"
        url = plugin._build_broker_url()
        assert "me:secret@rabbit.example.com:15672" in url

    def test_default_rate_limit_disabled(self, mock_service: MagicMock) -> None:
        plugin = RabbitMQWatcher(mock_service, _make_config())
        assert plugin.rate_limit_per_second == 0.0

    def test_custom_rate_limit(self, mock_service: MagicMock) -> None:
        plugin = RabbitMQWatcher(
            mock_service,
            _make_config(rate_limit_per_second=5.0),
        )
        assert plugin.rate_limit_per_second == 5.0


# ─── _parse_location dispatch ───────────────────────────────────────────────


class TestParseLocationDispatch:
    def test_dispatches_user_at_host(self, mock_service: MagicMock) -> None:
        plugin = RabbitMQWatcher(mock_service, _make_config())
        assert plugin._parse_location("u@h:/x") == ("h", "/x")

    def test_dispatches_regex(self, mock_service: MagicMock) -> None:
        plugin = RabbitMQWatcher(
            mock_service,
            _make_config(
                location_format="regex",
                location_format_regex=r"(?P<hostname>\w+)",
            ),
        )
        assert plugin._parse_location("server1") == ("server1", "/")


# ─── _extract_timestamp ─────────────────────────────────────────────────────


class TestExtractTimestamp:
    def test_default_uses_time_range_lower(self, mock_service: MagicMock) -> None:
        plugin = RabbitMQWatcher(
            mock_service,
            _make_config(
                field_map={
                    "time_range_key": "time_range",
                    "time_range_lower_key": "lower",
                    "time_range_start_key": "start",
                },
            ),
        )
        result = plugin._extract_timestamp(
            {"time_range": {"lower": "2026-01-01T00:00:00"}},
        )
        assert isinstance(result, datetime)

    def test_no_timestamp_returns_none(self, mock_service: MagicMock) -> None:
        plugin = RabbitMQWatcher(
            mock_service,
            _make_config(
                field_map={
                    "time_range_key": "time_range",
                    "time_range_lower_key": "lower",
                    "time_range_start_key": "start",
                },
            ),
        )
        assert plugin._extract_timestamp({}) is None

    def test_dotted_timestamp_field(self, mock_service: MagicMock) -> None:
        plugin = RabbitMQWatcher(
            mock_service,
            _make_config(timestamp_field="meta.created_at"),
        )
        result = plugin._extract_timestamp(
            {"meta": {"created_at": "2026-02-02T01:02:03"}},
        )
        assert isinstance(result, datetime)

    def test_postgres_array_string_uses_first(self, mock_service: MagicMock) -> None:
        plugin = RabbitMQWatcher(
            mock_service,
            _make_config(timestamp_field="ts"),
        )
        msg = {"ts": '["2026-03-03T00:00:00", "2026-03-03T01:00:00"]'}
        result = plugin._extract_timestamp(msg)
        assert isinstance(result, datetime)
        assert result.month == 3


# ─── is_healthy / stop ──────────────────────────────────────────────────────


class TestLifecycle:
    def test_initially_unhealthy(self, mock_service: MagicMock) -> None:
        plugin = RabbitMQWatcher(mock_service, _make_config())
        assert plugin.is_healthy() is False

    def test_stop_sets_event(self, mock_service: MagicMock) -> None:
        plugin = RabbitMQWatcher(mock_service, _make_config())
        plugin.stop()
        assert plugin._stop_event.is_set()


# ─── Rate Limiting ──────────────────────────────────────────────────────────


class TestRateLimit:
    """Tests for the optional rate limiting feature."""

    @staticmethod
    def _stop_after(plugin: RabbitMQWatcher, count: int):
        """Return a ``queue.get`` replacement that stops the plugin after *count*.

        Driving termination off the number of files *delivered* rather than a
        fixed-length ``is_set`` side_effect list keeps these tests independent
        of how many times ``find_file`` happens to poll the stop event -- the
        inner loop polls it once per empty read, so a fixed list breaks the
        moment that polling changes.
        """
        delivered = 0

        def _get(*_args: object, **_kwargs: object):
            nonlocal delivered
            if delivered >= count:
                plugin._stop_event.set()
                raise queue.Empty
            file = File(file=Path(f"/tmp/f{delivered + 1}.txt"), hostname="h1")
            delivered += 1
            return file

        return _get

    def test_rate_limit_throttles_yields(self, mock_service: MagicMock) -> None:
        """Verify that find_file() respects the configured rate limit."""
        plugin = RabbitMQWatcher(mock_service, _make_config(rate_limit_per_second=2.0))

        # Patch _listen_to_broker to a no-op so the background thread does
        # not race with the main thread on _stop_event.is_set().
        with (
            patch.object(plugin, "_listen_to_broker"),
            patch.object(queue.Queue, "get", self._stop_after(plugin, 3)),
            patch("time.sleep") as mock_sleep,
            patch("time.monotonic", side_effect=[0.0, 0.0, 0.5, 0.5, 1.0, 1.0]),
        ):
            result = list(plugin.find_file())

        # With rate_limit_per_second=2.0, interval=0.5s
        # First yield: elapsed=0.0, remaining=0.5 -> time.sleep(0.5)
        # Second yield: elapsed=0.5, remaining=0.0 -> no sleep (below 0.001)
        assert len(result) == 3
        mock_sleep.assert_called_once_with(0.5)

    def test_rate_limit_disabled_skips_sleep(self, mock_service: MagicMock) -> None:
        """Verify that disabled rate limit (0.0) does not call sleep."""
        plugin = RabbitMQWatcher(mock_service, _make_config(rate_limit_per_second=0.0))

        with (
            patch.object(plugin, "_listen_to_broker"),
            patch.object(queue.Queue, "get", self._stop_after(plugin, 2)),
            patch("time.sleep") as mock_sleep,
        ):
            result = list(plugin.find_file())

        assert len(result) == 2
        mock_sleep.assert_not_called()

    def test_find_file_returns_when_stop_event_set(
        self,
        mock_service: MagicMock,
    ) -> None:
        """An idle queue must not trap find_file() in its inner poll loop.

        Regression guard: the inner ``while`` around ``file_queue.get`` used to
        loop unconditionally, so a monitor with nothing to read never observed
        ``stop()`` and its non-daemon thread wedged interpreter shutdown.
        """
        plugin = RabbitMQWatcher(mock_service, _make_config())

        # find_file() clears the event on entry, so signal the stop from the
        # queue read -- i.e. exactly when a real idle monitor would see it.
        def _empty_then_stop(*_args: object, **_kwargs: object):
            plugin._stop_event.set()
            raise queue.Empty

        with (
            patch.object(plugin, "_listen_to_broker"),
            patch.object(queue.Queue, "get", _empty_then_stop),
        ):
            assert list(plugin.find_file()) == []
        assert plugin.health is False
