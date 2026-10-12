"""``Service.start`` reports the real failure and always cleans up.

The post-startup health check used to run in ``finally``. Every exit path
reaches ``finally`` -- a startup failure, Ctrl-C, a clean shutdown -- and by
then the managers are stopping, so the check failed and its ``RuntimeError``
replaced the exception in flight and skipped ``_cleanup()``.
"""

from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

from courier.config import ServiceConfig
from courier.errors import PluginFailedError
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


class TestFailedPluginsEndTheService:
    """A plugin the manager has given up on takes the process down with it."""

    def test_an_exhausted_plugin_raises(self, service: Service) -> None:
        with patch.object(
            service._plugin_manager,
            "exhausted_plugins",
            return_value=["watcher"],
        ):
            with pytest.raises(PluginFailedError, match="watcher"):
                service._require_no_failed_plugins()

    def test_no_exhausted_plugins_is_quiet(self, service: Service) -> None:
        with patch.object(
            service._plugin_manager,
            "exhausted_plugins",
            return_value=[],
        ):
            service._require_no_failed_plugins()

    def test_the_heartbeat_loop_itself_raises(self, service: Service) -> None:
        """The real loop, not the helper: the check must be on its path."""
        service._start_time = time.time()
        with (
            patch("courier.service.time.sleep"),
            patch.object(service, "_prometheus_manager"),
            patch.object(service, "_health_check", return_value=True),
            patch.object(
                service._plugin_manager,
                "exhausted_plugins",
                return_value=["watcher"],
            ),
            pytest.raises(PluginFailedError, match="watcher"),
        ):
            service._run_heartbeat_loop()

    def test_start_propagates_it_and_cleans_up(self, service: Service) -> None:
        with (
            patch.object(service, "preflight_check"),
            patch.object(service, "_start_managers"),
            patch.object(service, "_health_check", return_value=True),
            patch.object(service, "_cleanup") as cleanup,
            patch("courier.service.time.sleep"),
            patch.object(service, "_prometheus_manager"),
            patch.object(
                service._plugin_manager,
                "exhausted_plugins",
                return_value=["watcher"],
            ),
            pytest.raises(PluginFailedError, match="watcher"),
        ):
            service.start()

        cleanup.assert_called_once()
