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


# Durable per-builder file-found queues (issue #44).


def test_oversized_file_found_queue_name_rejected() -> None:
    """A namespace that overflows the AMQP limit fails preflight.

    The check runs against the namespaced name, which is what the broker sees.
    Validating the base name alone let an over-long namespace through to a
    broker error at publish time.
    """
    svc = Service(ServiceConfig(broker_url="memory://", namespace="n" * 240))
    svc.configure_routing(
        dispatcher_identifiers=["r"],
        builder_targets={},
        builder_identifiers=["b" * 20],
    )
    with pytest.raises(ConfigurationError):
        svc.preflight_check()


def test_malformed_builder_identifier_rejected_at_preflight() -> None:
    """A builder identifier that cannot be a queue name fails fast."""
    svc = _service()
    svc.configure_routing(
        dispatcher_identifiers=["only"],
        builder_targets={},
        builder_identifiers=["bad id"],
    )
    with pytest.raises(ConfigurationError):
        svc.preflight_check()


def test_builder_identifiers_backfilled_from_builder_targets() -> None:
    """Harnesses that only pass builder targets still get their queues.

    Tests and embedded harnesses that omit ``configure_routing``'s new
    ``builder_identifiers`` argument depend on this backfill.
    """
    svc = _service()
    svc.configure_routing(
        dispatcher_identifiers=["only"],
        builder_targets={"builder": ()},
    )
    svc.preflight_check()

    assert "builder" in svc._builder_identifiers  # noqa: SLF001
    assert "t-FilesFound-builder" in svc._broker_manager._queues  # noqa: SLF001


# ── builder payload binding and compatibility ───────────────────────────────


def _register_payload_builder(svc: Service) -> None:
    from courier.plugins.job_builders.dummy_job_builder import DummyJobBuilder
    from courier.plugins.payloads.bash_payload import BashPayload

    svc.register_plugin(BashPayload, {"script": "echo {{ files[0].file }}"}, identifier="p1")
    svc.register_plugin(
        DummyJobBuilder,
        {
            "payload": {
                "p1": {
                    "kind": "payload",
                    "name": "bash_payload",
                    "config": {"script": "echo {{ files[0].file }}"},
                },
            },
        },
        identifier="b1",
    )


def test_builder_payload_binding_resolves_singleton_form() -> None:
    """The ``payload: {id: {...}}`` form binds a functional payload instance.

    Proving the binding is real means rendering through it: the bound payload
    must turn the job's files into a concrete script, not merely be non-None.
    """
    from pathlib import Path

    from courier.plugins.dispatchers.local_dispatcher import LocalDispatcher
    from courier.types.file import File
    from courier.types.job import Job

    svc = _service()
    _register_payload_builder(svc)
    svc.register_plugin(LocalDispatcher, {}, identifier="d1")
    svc.configure_routing(
        dispatcher_identifiers=["d1"],
        builder_targets={"b1": ("d1",)},
    )

    svc.preflight_check()

    builder = svc._plugin_manager.get_plugins()["b1"].plugin  # noqa: SLF001
    job = Job("n", "job-1", {}, files=[File(file=Path("/d/a.nc")).freeze()])
    spec = builder.payload.to_job_spec(job, builder=builder)

    assert spec.script == "echo /d/a.nc"
    assert spec.identifier == "p1"


def test_incompatible_payload_fails_preflight() -> None:
    """A target dispatcher that cannot run the payload's representation fails."""
    from courier.interfaces.dispatchers import Dispatcher

    class _EmptyDispatcher(Dispatcher):
        name = "empty_dispatcher"
        representations: list = []

    svc = _service()
    _register_payload_builder(svc)
    svc.register_plugin(_EmptyDispatcher, {}, identifier="d1")
    svc.configure_routing(
        dispatcher_identifiers=["d1"],
        builder_targets={"b1": ("d1",)},
    )

    with pytest.raises(ConfigurationError, match="not compatible"):
        svc.preflight_check()
