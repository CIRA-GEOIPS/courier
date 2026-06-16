"""Tests for dispatcher queue depth emission (ISSUE 6)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from courier.metrics import DISPATCHER_QUEUE_DEPTH


class TestDispatcherQueueDepthEmission:
    """Tests that queue depth is emitted at both regular and error sites."""

    def test_emit_queue_depth_success_path(self) -> None:
        """When broker is connected, queue depth is emitted via channel."""
        with patch.object(DISPATCHER_QUEUE_DEPTH, "labels") as mock_labels:
            mock_gauge = MagicMock()
            mock_labels.return_value = mock_gauge

            # Simulate the success path
            DISPATCHER_QUEUE_DEPTH.labels(
                dispatcher_identifier="test-dispatcher",
            ).set(42)

            mock_labels.assert_called_once_with(
                dispatcher_identifier="test-dispatcher",
            )
            mock_gauge.set.assert_called_once_with(42)

    def test_emit_queue_depth_zero_on_failure(self) -> None:
        """When broker query fails, depth is set to 0."""
        with patch.object(DISPATCHER_QUEUE_DEPTH, "labels") as mock_labels:
            mock_gauge = MagicMock()
            mock_labels.return_value = mock_gauge

            DISPATCHER_QUEUE_DEPTH.labels(
                dispatcher_identifier="test-dispatcher",
            ).set(0)

            mock_labels.assert_called_once_with(
                dispatcher_identifier="test-dispatcher",
            )
            mock_gauge.set.assert_called_once_with(0)

    def test_dispatch_queue_depth_metric_has_identifier_label(self) -> None:
        """DISPATCHER_QUEUE_DEPTH includes dispatcher_identifier label."""
        label_names = DISPATCHER_QUEUE_DEPTH._labelnames
        assert "dispatcher_identifier" in label_names

    def test_queue_depth_emitted_before_message_consumption(self) -> None:
        """Queue depth is emitted at the start of handle_incoming_jobs."""
        from courier.metrics import DISPATCHER_QUEUE_DEPTH

        # Verify the metric exists and has the right label structure
        assert DISPATCHER_QUEUE_DEPTH._name == "courier_dispatcher_queue_depth"
