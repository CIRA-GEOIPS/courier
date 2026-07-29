"""Unit tests for the s3_poller data monitor plugin."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from courier.errors import InvalidPluginConfigError
from courier.plugins.classes.data_monitors.s3_poller import (
    S3Poller,
    S3PollerConfig,
)
from courier.types.file import File


def _make_config(**overrides: Any) -> dict[str, Any]:
    defaults: dict[str, Any] = {"bucket": "test-bucket"}
    defaults.update(overrides)
    return defaults


# ─── Config Validation ──────────────────────────────────────────────────────


class TestS3PollerConfig:
    def test_minimal_valid(self) -> None:
        cfg = S3PollerConfig.model_validate(_make_config())
        assert cfg.bucket == "test-bucket"
        assert cfg.region == "us-east-1"
        assert cfg.poll_interval_seconds == 60.0

    def test_missing_bucket_raises(self) -> None:
        with pytest.raises(ValidationError):
            S3PollerConfig.model_validate({})

    def test_suffix_normalization(self) -> None:
        cfg = S3PollerConfig.model_validate(
            _make_config(suffix_filter=["nc", ".TIF", "  jpg  "]),
        )
        assert cfg.suffix_filter == [".nc", ".tif", ".jpg"]

    def test_credentials_must_pair_key_only(self) -> None:
        with pytest.raises(ValidationError, match="must be supplied together"):
            S3PollerConfig.model_validate(_make_config(aws_access_key_id="k"))

    def test_credentials_must_pair_secret_only(self) -> None:
        with pytest.raises(ValidationError, match="must be supplied together"):
            S3PollerConfig.model_validate(_make_config(aws_secret_access_key="s"))

    def test_credentials_paired_ok(self) -> None:
        cfg = S3PollerConfig.model_validate(
            _make_config(aws_access_key_id="k", aws_secret_access_key="s"),
        )
        assert cfg.aws_access_key_id == "k"

    def test_poll_interval_min(self) -> None:
        with pytest.raises(ValidationError):
            S3PollerConfig.model_validate(_make_config(poll_interval_seconds=0.5))


# ─── Constructor ────────────────────────────────────────────────────────────


class TestConstructor:
    def test_initializes_with_defaults(self, mock_service: MagicMock) -> None:
        plugin = S3Poller(mock_service, _make_config())
        assert plugin.health is False
        assert plugin.validated.bucket == "test-bucket"

# ─── _matches_suffix ────────────────────────────────────────────────────────


class TestMatchesSuffix:
    def test_no_filter_matches_all(self, mock_service: MagicMock) -> None:
        plugin = S3Poller(mock_service, _make_config())
        assert plugin._matches_suffix("anything.foo") is True

    def test_filter_matches(self, mock_service: MagicMock) -> None:
        plugin = S3Poller(mock_service, _make_config(suffix_filter=["nc"]))
        assert plugin._matches_suffix("file.nc") is True
        assert plugin._matches_suffix("FILE.NC") is True

    def test_filter_rejects_non_matching(self, mock_service: MagicMock) -> None:
        plugin = S3Poller(mock_service, _make_config(suffix_filter=["nc"]))
        assert plugin._matches_suffix("file.txt") is False

    def test_multiple_suffixes(self, mock_service: MagicMock) -> None:
        plugin = S3Poller(mock_service, _make_config(suffix_filter=["nc", "tif"]))
        assert plugin._matches_suffix("a.nc") is True
        assert plugin._matches_suffix("a.tif") is True
        assert plugin._matches_suffix("a.zip") is False


# ─── _scan_bucket ───────────────────────────────────────────────────────────


def _make_paginator(pages: list[dict[str, Any]]) -> MagicMock:
    paginator = MagicMock()
    paginator.paginate.return_value = iter(pages)
    return paginator


class TestScanBucket:
    def test_yields_new_keys_only(self, mock_service: MagicMock) -> None:
        plugin = S3Poller(mock_service, _make_config(suffix_filter=["nc"]))
        client = MagicMock()
        pages = [{"Contents": [{"Key": "a.nc"}, {"Key": "b.nc"}, {"Key": "c.txt"}]}]
        client.get_paginator.return_value = _make_paginator(pages)

        files = list(plugin._scan_bucket(client))
        assert len(files) == 2
        assert all(isinstance(f, File) for f in files)
        # second scan returns 0 (dedup)
        client.get_paginator.return_value = _make_paginator(pages)
        assert list(plugin._scan_bucket(client)) == []

    def test_uri_format(self, mock_service: MagicMock) -> None:
        plugin = S3Poller(mock_service, _make_config(bucket="my-bucket"))
        assert plugin._key_to_uri("path/x.nc") == "s3://my-bucket/path/x.nc"


# ─── _handle_client_error ───────────────────────────────────────────────────


class TestHandleClientError:
    def _err(self, code: str) -> Exception:
        exc = Exception(f"sim {code}")
        exc.response = {"Error": {"Code": code}}  # type: ignore[attr-defined]
        return exc

    def test_fatal_no_such_bucket_raises(self, mock_service: MagicMock) -> None:
        plugin = S3Poller(mock_service, _make_config())
        with pytest.raises(InvalidPluginConfigError):
            plugin._handle_client_error(self._err("NoSuchBucket"))

    def test_fatal_access_denied_raises(self, mock_service: MagicMock) -> None:
        plugin = S3Poller(mock_service, _make_config())
        with pytest.raises(InvalidPluginConfigError):
            plugin._handle_client_error(self._err("AccessDenied"))

    def test_transient_returns_true(self, mock_service: MagicMock) -> None:
        plugin = S3Poller(mock_service, _make_config())
        assert plugin._handle_client_error(self._err("Throttling")) is True


# ─── Lifecycle ──────────────────────────────────────────────────────────────


class TestLifecycle:
    def test_initially_unhealthy(self, mock_service: MagicMock) -> None:
        plugin = S3Poller(mock_service, _make_config())
        assert plugin.is_healthy() is False

    def test_stop_sets_event(self, mock_service: MagicMock) -> None:
        plugin = S3Poller(mock_service, _make_config())
        plugin.stop()
        assert plugin._stop_event.is_set()
