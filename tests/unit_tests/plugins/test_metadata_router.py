"""Unit tests for the metadata_router job builder."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from courier.plugins.job_builders.metadata_router import (
    MetadataRouterBuilder,
    MetadataRouterConfig,
    RouteConfig,
)


def _route(**overrides: Any) -> dict[str, Any]:
    cfg: dict[str, Any] = {"name": "r1", "filters": {"source": "goes16"}}
    cfg.update(overrides)
    return cfg


# ─── RouteConfig / MetadataRouterConfig ─────────────────────────────────────


class TestRouteConfig:
    def test_min_files_le_files_per_job(self) -> None:
        with pytest.raises(ValidationError, match="min_files"):
            RouteConfig.model_validate(_route(files_per_job=1, min_files=2))

    def test_to_filter_and_group_config_roundtrips(self) -> None:
        rc = RouteConfig.model_validate(
            _route(files_per_job=3, min_files=2, window_timeout_seconds=10.0),
        )
        fag = rc.to_filter_and_group_config()
        assert fag.files_per_job == 3
        assert fag.min_files == 2
        assert fag.window_timeout_seconds == 10.0
        assert fag.filters == {"source": "goes16"}


class TestMetadataRouterConfig:
    def test_requires_at_least_one_route(self) -> None:
        with pytest.raises(ValidationError):
            MetadataRouterConfig.model_validate({"routes": []})

    def test_duplicate_names_rejected(self) -> None:
        with pytest.raises(ValidationError, match="unique"):
            MetadataRouterConfig.model_validate(
                {
                    "routes": [
                        _route(name="same"),
                        _route(name="same", filters={"source": "other"}),
                    ],
                },
            )


# ─── MetadataRouterBuilder ──────────────────────────────────────────────────


class TestBuilder:
    def test_initializes_one_group_per_route(self, mock_service: MagicMock) -> None:
        builder = MetadataRouterBuilder(
            mock_service,
            {
                "routes": [
                    _route(name="a"),
                    _route(name="b", filters={"source": "himawari"}),
                ],
            },
        )
        assert len(builder.job_groups) == 2
        assert not builder._has_timeout

    def test_has_timeout_flag(self, mock_service: MagicMock) -> None:
        builder = MetadataRouterBuilder(
            mock_service,
            {"routes": [_route(window_timeout_seconds=1.0)]},
        )
        assert builder._has_timeout is True

    def test_reap_group_emits_ready(
        self, mock_service: MagicMock, make_frozen_file, mocker
    ) -> None:
        builder = MetadataRouterBuilder(
            mock_service,
            {"routes": [_route(files_per_job=1)]},
        )
        group = builder.job_groups[0]
        JobCls = group.job
        job = JobCls(name=group.name, identifier="jid", config={})
        job.add_file(make_frozen_file(source="goes16"))
        group.jobs["jid"] = job

        emit = mocker.patch.object(builder, "emit")
        builder._reap_group(group)
        emit.assert_called_once()
        assert "jid" not in group.jobs

    def test_is_healthy_without_running(self, mock_service: MagicMock) -> None:
        builder = MetadataRouterBuilder(mock_service, {"routes": [_route()]})
        assert builder.is_healthy() is False
