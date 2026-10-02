"""``Service.start`` reports the real failure and always cleans up.

The post-startup health check used to run in ``finally``. Every exit path
reaches ``finally`` -- a startup failure, Ctrl-C, a clean shutdown -- and by
then the managers are stopping, so the check failed and its ``RuntimeError``
replaced the exception in flight and skipped ``_cleanup()``.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

from courier.config import ServiceConfig
from courier.service import Service

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator


@pytest.fixture
def service() -> Service:
    """Return a service on the in-memory broker with nothing started."""
    return Service(
        ServiceConfig(
            broker_url="memory://",
            prometheus_port=0,
            namespace=f"start-{uuid.uuid4().hex[:8]}",
            tracing_enabled=False,
        ),
    )


@pytest.fixture
def lifecycle(service: Service) -> Iterator[dict[str, MagicMock]]:
    """Patch the steps ``start`` drives, healthy and returning by default."""
    with (
        patch.object(service, "preflight_check") as preflight,
        patch.object(service, "_start_managers") as start_managers,
        patch.object(service, "_health_check", return_value=True) as health,
        patch.object(service, "_run_heartbeat_loop") as heartbeat,
        patch.object(service, "_cleanup") as cleanup,
    ):
        yield {
            "preflight": preflight,
            "start_managers": start_managers,
            "health": health,
            "heartbeat": heartbeat,
            "cleanup": cleanup,
        }


def _managers_stop(
    lifecycle: dict[str, MagicMock],
    then_raise: type[BaseException] | None = None,
) -> Callable[[], None]:
    """Return a heartbeat loop that exits with the managers stopping.

    From then on the health check fails, as it does in a real shutdown.
    """

    def _loop() -> None:
        lifecycle["health"].return_value = False
        if then_raise is not None:
            raise then_raise

    return _loop


class TestServiceStart:
    """Exceptions from ``start`` are the real ones, and cleanup always runs."""

    def test_a_clean_shutdown_does_not_raise(
        self,
        service: Service,
        lifecycle: dict[str, MagicMock],
    ) -> None:
        """Managers report unhealthy once stopping; that is not a failure."""
        lifecycle["heartbeat"].side_effect = _managers_stop(lifecycle)

        service.start()

        lifecycle["heartbeat"].assert_called_once()
        lifecycle["cleanup"].assert_called_once()

    def test_a_startup_failure_keeps_its_own_exception(
        self,
        service: Service,
        lifecycle: dict[str, MagicMock],
    ) -> None:
        """The original error propagates, not a health-check RuntimeError."""
        lifecycle["start_managers"].side_effect = ValueError("broker refused")
        lifecycle["health"].return_value = False

        with pytest.raises(ValueError, match="broker refused"):
            service.start()

        lifecycle["cleanup"].assert_called_once()

    def test_keyboard_interrupt_propagates_unchanged(
        self,
        service: Service,
        lifecycle: dict[str, MagicMock],
    ) -> None:
        """Ctrl-C in the heartbeat loop is not turned into a RuntimeError."""
        lifecycle["heartbeat"].side_effect = _managers_stop(
            lifecycle,
            then_raise=KeyboardInterrupt,
        )

        with pytest.raises(KeyboardInterrupt):
            service.start()

        lifecycle["cleanup"].assert_called_once()

    def test_unhealthy_after_startup_raises_and_cleans_up(
        self,
        service: Service,
        lifecycle: dict[str, MagicMock],
    ) -> None:
        """A failed post-startup check stops the service before the loop."""
        lifecycle["health"].return_value = False

        with pytest.raises(RuntimeError, match="health check failed"):
            service.start()

        lifecycle["heartbeat"].assert_not_called()
        lifecycle["cleanup"].assert_called_once()
