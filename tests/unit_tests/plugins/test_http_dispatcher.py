"""Unit tests for the http_dispatcher plugin."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
from pydantic import ValidationError

from courier.plugins.classes.dispatchers.http_dispatcher import (
    HttpDispatcher,
    HttpDispatcherConfig,
)
from courier.types.execution_log import ExecutionLog


def _make_config(**overrides: Any) -> dict[str, Any]:
    cfg: dict[str, Any] = {"url": "https://example.com/ingest"}
    cfg.update(overrides)
    return cfg


# ─── Config Validation ──────────────────────────────────────────────────────


class TestHttpDispatcherConfig:
    def test_minimal_valid(self) -> None:
        cfg = HttpDispatcherConfig.model_validate(_make_config())
        assert cfg.method == "POST"
        assert cfg.auth_type == "none"

    def test_url_scheme_required(self) -> None:
        with pytest.raises(ValidationError, match="must begin with"):
            HttpDispatcherConfig.model_validate({"url": "example.com"})

    def test_bearer_requires_token(self) -> None:
        with pytest.raises(ValidationError, match="bearer"):
            HttpDispatcherConfig.model_validate(_make_config(auth_type="bearer"))

    def test_basic_requires_credentials(self) -> None:
        with pytest.raises(ValidationError, match="basic"):
            HttpDispatcherConfig.model_validate(_make_config(auth_type="basic"))

    def test_invalid_template_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid payload_template"):
            HttpDispatcherConfig.model_validate(
                _make_config(payload_template="{{ unclosed"),
            )


# ─── Constructor ────────────────────────────────────────────────────────────


class TestConstructor:
    def test_initializes(self, mock_service: MagicMock) -> None:
        plugin = HttpDispatcher(mock_service, _make_config(), identifier="test-disp")
        assert plugin.is_healthy() is True


# ─── _build_context ─────────────────────────────────────────────────────────


class TestBuildContext:
    def test_context_has_job_and_first_file(
        self, mock_service: MagicMock, make_frozen_file, make_job
    ) -> None:
        plugin = HttpDispatcher(mock_service, _make_config(), identifier="test-disp")
        f = make_frozen_file(source="goes16")
        job = make_job(files=(f,))
        ctx = plugin._build_context(job)
        assert ctx["job_id"] == "job-1"
        assert ctx["file_count"] == 1
        assert ctx["source"] == "goes16"

    def test_empty_job_context(self, mock_service: MagicMock, make_job) -> None:
        plugin = HttpDispatcher(mock_service, _make_config(), identifier="test-disp")
        ctx = plugin._build_context(make_job())
        assert ctx["file_count"] == 0
        assert ctx["source"] is None


# ─── _send_with_retries ─────────────────────────────────────────────────────


class TestSendWithRetries:
    def test_success_on_first_try(self, mock_service: MagicMock) -> None:
        plugin = HttpDispatcher(mock_service, _make_config(), identifier="test-disp")
        client = MagicMock()
        resp = MagicMock()
        resp.status_code = 200
        resp.text = "ok"
        client.request.return_value = resp
        status, body, err = plugin._send_with_retries(client, "{}")
        assert status == 200
        assert body == "ok"
        assert err is None

    def test_transport_error_retries_then_fails(
        self, mock_service: MagicMock, mocker
    ) -> None:
        mocker.patch(
            "courier.plugins.classes.dispatchers.http_dispatcher.time.sleep",
        )
        plugin = HttpDispatcher(
            mock_service,
            _make_config(retry_count=1, retry_delay_seconds=0.01),
            identifier="test-disp",
        )
        client = MagicMock()
        client.request.side_effect = httpx.TransportError("boom")
        status, _, err = plugin._send_with_retries(client, "{}")
        assert status is None
        assert err is not None and "TransportError" in err

    def test_non_success_status_returned(self, mock_service: MagicMock) -> None:
        plugin = HttpDispatcher(mock_service, _make_config(retry_count=0), identifier="test-disp")
        client = MagicMock()
        resp = MagicMock()
        resp.status_code = 400
        resp.text = "bad"
        client.request.return_value = resp
        status, body, err = plugin._send_with_retries(client, "{}")
        assert status == 400
        assert err == "HTTP 400"


# ─── get_execution_log ──────────────────────────────────────────────────────


class TestGetExecutionLog:
    def test_success_path(
        self, mock_service: MagicMock, make_job, mocker
    ) -> None:
        plugin = HttpDispatcher(mock_service, _make_config(), identifier="test-disp")
        client = MagicMock()
        resp = MagicMock()
        resp.status_code = 200
        resp.text = "body"
        client.request.return_value = resp
        mocker.patch.object(plugin, "_build_client", return_value=client)

        logs = plugin.get_execution_log(make_job())
        assert len(logs) == 1
        assert isinstance(logs[0], ExecutionLog)
        assert logs[0].return_code == 200
        client.close.assert_called_once()

    def test_template_error_returns_failure_log(
        self, mock_service: MagicMock, make_job, mocker
    ) -> None:
        plugin = HttpDispatcher(mock_service, _make_config(), identifier="test-disp")
        mocker.patch.object(
            plugin._template, "render", side_effect=Exception("render")
        )
        # The plugin catches jinja2.TemplateError only; force a real one:
        import jinja2

        mocker.patch.object(
            plugin._template,
            "render",
            side_effect=jinja2.TemplateError("render-fail"),
        )
        logs = plugin.get_execution_log(make_job())
        assert logs[0].return_code == -1
        assert "render-fail" in (logs[0].stderr or "")
