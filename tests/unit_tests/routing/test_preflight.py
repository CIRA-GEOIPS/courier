"""Tests for Service.preflight_check routing validation."""

from __future__ import annotations

import logging

import pytest

from courier.config import ServiceConfig
from courier.errors import (
    AmbiguousImplicitTargetError,
    ConfigurationError,
    DuplicateTargetError,
    UnknownTargetError,
)
from courier.service import Service


def _service() -> Service:
    return Service(ServiceConfig(broker_url="memory://", namespace="t"))


def test_unknown_target_rejected() -> None:
    svc = _service()
    svc.configure_routing(
        dispatcher_identifiers=["runner-a"],
        builder_targets={"builder": ("runner-b",)},
    )
    with pytest.raises(UnknownTargetError):
        svc.preflight_check()


def test_duplicate_target_rejected() -> None:
    svc = _service()
    svc.configure_routing(
        dispatcher_identifiers=["runner-a"],
        builder_targets={"builder": ("runner-a", "runner-a")},
    )
    with pytest.raises(DuplicateTargetError):
        svc.preflight_check()


def test_implicit_routing_resolves_to_sole_dispatcher(
    caplog: pytest.LogCaptureFixture,
) -> None:
    svc = _service()
    svc.configure_routing(
        dispatcher_identifiers=["only"],
        builder_targets={"builder": ()},
        allow_implicit_target=True,
    )
    # Courier loggers don't propagate to root; attach caplog's handler to the
    # actual service logger instance.
    logger = svc._logger.logger  # underlying Logger behind the ContextAdapter
    logger.addHandler(caplog.handler)
    prev_level = logger.level
    logger.setLevel(logging.WARNING)
    try:
        svc.preflight_check()
    finally:
        logger.removeHandler(caplog.handler)
        logger.setLevel(prev_level)
    assert any("auto-wired" in r.getMessage() for r in caplog.records)
    assert svc._builder_targets["builder"] == ("only",)


def test_implicit_routing_fails_with_multiple_dispatchers() -> None:
    svc = _service()
    svc.configure_routing(
        dispatcher_identifiers=["a", "b"],
        builder_targets={"builder": ()},
        allow_implicit_target=True,
    )
    with pytest.raises(AmbiguousImplicitTargetError):
        svc.preflight_check()


def test_implicit_routing_disabled_fails_hard() -> None:
    svc = _service()
    svc.configure_routing(
        dispatcher_identifiers=["only"],
        builder_targets={"builder": ()},
        allow_implicit_target=False,
    )
    with pytest.raises(AmbiguousImplicitTargetError):
        svc.preflight_check()


def test_oversized_queue_name_rejected() -> None:
    svc = Service(ServiceConfig(broker_url="memory://", namespace="n" * 250))
    svc.configure_routing(
        dispatcher_identifiers=["runner"],
        builder_targets={"builder": ("runner",)},
    )
    with pytest.raises(ConfigurationError):
        svc.preflight_check()
