"""Unit tests for the file_count_builder plugin."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from courier.plugins.classes.job_builders.file_count_builder import (
    FileCountBuilder,
    FileCountBuilderConfig,
    FileCountJobGroup,
    _matches_filters,
    _render_context,
)


def _make_config(**overrides: Any) -> dict[str, Any]:
    cfg: dict[str, Any] = {}
    cfg.update(overrides)
    return cfg


# ─── Config Validation ──────────────────────────────────────────────────────


class TestFileCountBuilderConfig:
    def test_defaults(self) -> None:
        cfg = FileCountBuilderConfig.model_validate({})
        assert cfg.files_per_job == 1

    def test_files_per_job_ge_one(self) -> None:
        with pytest.raises(ValidationError):
            FileCountBuilderConfig.model_validate(_make_config(files_per_job=0))

    def test_invalid_template_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid job_name_template"):
            FileCountBuilderConfig.model_validate(
                _make_config(job_name_template="{{ unclosed"),
            )


# ─── _matches_filters / _render_context ─────────────────────────────────────


class TestHelpers:
    def test_matches_filters_true(self, make_frozen_file) -> None:
        f = make_frozen_file(source="goes16")
        assert _matches_filters(f, {"source": "goes16"}) is True

    def test_matches_filters_false(self, make_frozen_file) -> None:
        f = make_frozen_file(source="goes16")
        assert _matches_filters(f, {"source": "himawari"}) is False

    def test_render_context_timestamp_iso(self, make_frozen_file) -> None:
        ts = datetime(2026, 1, 1, 12, 0, 0)
        ctx = _render_context(make_frozen_file(timestamp=ts))
        assert ctx["timestamp"] == ts.isoformat()
        assert ctx["source"] == "goes16"

    def test_render_context_none_becomes_empty(self, make_frozen_file) -> None:
        ctx = _render_context(make_frozen_file(domain=None))
        assert ctx["domain"] == ""


# ─── FileCountJobGroup ──────────────────────────────────────────────────────


class TestJobGroup:
    def test_file_is_relevant_filter_match(self, make_frozen_file) -> None:
        cfg = FileCountBuilderConfig.model_validate({"filters": {"source": "goes16"}})
        group = FileCountJobGroup(cfg)
        assert group.file_is_relevant(make_frozen_file(source="goes16")) is True
        assert group.file_is_relevant(make_frozen_file(source="other")) is False

    def test_get_job_ids_renders_template(self, make_frozen_file) -> None:
        cfg = FileCountBuilderConfig.model_validate(
            {"job_name_template": "{{ source }}-bucket"},
        )
        group = FileCountJobGroup(cfg)
        assert group.get_job_ids_from_file(
            make_frozen_file(source="goes16"),
        ) == ["goes16-bucket"]

    def test_add_file_creates_job(self, make_frozen_file) -> None:
        cfg = FileCountBuilderConfig.model_validate(
            {"files_per_job": 2, "job_name_template": "one-bucket"},
        )
        group = FileCountJobGroup(cfg)
        assert group.add_file(make_frozen_file()) is True
        assert group.add_file(make_frozen_file(source="b")) is True
        assert len(group.jobs["one-bucket"].files) == 2


# ─── Constructor ────────────────────────────────────────────────────────────


class TestConstructor:
    def test_initializes(self, mock_service: MagicMock) -> None:
        builder = FileCountBuilder(mock_service, {})
        assert len(builder.job_groups) == 1

    def test_module_init_short_circuits(self) -> None:
        builder = FileCountBuilder(None, None)
        assert not hasattr(builder, "job_groups")
