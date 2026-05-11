"""Tests for run.py kind value fix."""

from __future__ import annotations

from unittest.mock import MagicMock

from courier.cli.run import _collect_builder_targets


class TestCollectBuilderTargets:
    """Verify _collect_builder_targets matches singular kind values."""

    def test_finds_singular_job_builder(self):
        """Should find entries with kind='job_builder' (singular)."""
        entry = MagicMock()
        entry.spec.kind = "job_builder"
        entry.spec.config = {"targets": ["dispatcher-1"]}
        entry.identifier = "my-builder"

        config = MagicMock()
        config.spec.run = [entry]

        result = _collect_builder_targets(config)
        assert "my-builder" in result
        assert result["my-builder"] == ("dispatcher-1",)

    def test_skips_non_builder(self):
        """Should skip entries that are not job_builder."""
        entry = MagicMock()
        entry.spec.kind = "data_monitor"

        config = MagicMock()
        config.spec.run = [entry]

        result = _collect_builder_targets(config)
        assert len(result) == 0
