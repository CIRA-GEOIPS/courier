"""Tests for Service.preflight_check routing validation."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

from courier.config import ServiceConfig
from courier.errors import (
    AmbiguousImplicitTargetError,
    ConfigurationError,
    DuplicateTargetError,
    InvalidPluginConfigError,
    UnknownTargetError,
)
from courier.service import PipelineTopology, Service
from tests._helpers import payload_block


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

    svc.register_plugin(
        BashPayload, {"script": "echo {{ files[0].file }}"}, identifier="p1"
    )
    svc.register_plugin(
        DummyJobBuilder,
        {"payload": payload_block("p1", "echo {{ files[0].file }}")},
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


# ── payload binding error paths ─────────────────────────────────────────────


def _register_local_dispatcher(svc: Service, identifier: str = "d1") -> None:
    from courier.plugins.dispatchers.local_dispatcher import LocalDispatcher

    svc.register_plugin(LocalDispatcher, {}, identifier=identifier)


def _route_b1_to_d1(svc: Service) -> None:
    svc.configure_routing(
        dispatcher_identifiers=["d1"],
        builder_targets={"b1": ("d1",)},
    )


def test_builder_without_a_payload_block_cannot_be_registered() -> None:
    """An absent block says so, and the builder is never constructed.

    It used to be caught only at preflight, so a harness that skipped
    preflight ran a builder whose jobs carried no payload.
    """
    from courier.plugins.job_builders.dummy_job_builder import DummyJobBuilder

    svc = _service()

    with pytest.raises(InvalidPluginConfigError, match="'b1' has no 'payload' block"):
        svc.register_plugin(DummyJobBuilder, {"targets": ["d1"]}, identifier="b1")

    assert "b1" not in svc._plugin_manager.get_plugins()  # noqa: SLF001


def test_malformed_payload_block_cannot_be_registered() -> None:
    """Two payloads under one builder is not a payload block."""
    from courier.plugins.job_builders.dummy_job_builder import DummyJobBuilder

    svc = _service()
    spec = {"kind": "payload", "name": "bash_payload", "config": {"script": "x"}}

    with pytest.raises(ConfigurationError, match="malformed 'payload' block"):
        svc.register_plugin(
            DummyJobBuilder,
            {"payload": {"p1": spec, "p2": spec}},
            identifier="b1",
        )


def test_preflight_binds_the_payload_the_builder_needs_to_start() -> None:
    """Until preflight binds its payload, a registered builder cannot start."""
    from courier.plugins.dispatchers.local_dispatcher import LocalDispatcher

    svc = _service()
    _register_payload_builder(svc)
    svc.register_plugin(LocalDispatcher, {}, identifier="d1")
    _route_b1_to_d1(svc)
    builder = svc._plugin_manager.get_plugins()["b1"].plugin  # noqa: SLF001

    assert builder.has_payload is False
    with pytest.raises(ConfigurationError, match="'b1' has no payload bound"):
        builder.start()

    svc.preflight_check()

    assert builder.has_payload is True
    assert builder.payload is svc._plugin_manager.get_plugins()["p1"].plugin  # noqa: SLF001


@pytest.mark.parametrize(
    "payload_id",
    [
        pytest.param("p-unknown", id="unregistered"),
        pytest.param("d1", id="not-a-payload"),
    ],
)
def test_payload_identifier_must_name_a_registered_payload(payload_id: str) -> None:
    from courier.plugins.job_builders.dummy_job_builder import DummyJobBuilder

    svc = _service()
    svc.register_plugin(
        DummyJobBuilder,
        {"payload": payload_block(payload_id)},
        identifier="b1",
    )
    _register_local_dispatcher(svc)
    _route_b1_to_d1(svc)

    with pytest.raises(
        ConfigurationError,
        match=f"Job builder 'b1' nests payload '{payload_id}', which is not "
        "registered as a payload",
    ):
        svc.preflight_check()


# ── static compatibility across split (--only) deployments ──────────────────


def _empty_dispatcher_cls() -> type:
    from courier.interfaces.dispatchers import Dispatcher

    class _Empty(Dispatcher):
        name = "empty_dispatcher"
        representations: list = []

    return _Empty


def _topology(
    builder_payload: str = "bash_payload",
    targets: tuple[str, ...] = ("d1",),
    dispatcher_plugin: str = "local_dispatcher",
    dispatcher_id: str = "d1",
) -> PipelineTopology:
    return PipelineTopology(
        dispatcher_plugins={dispatcher_id: dispatcher_plugin},
        builder_payloads={"b-remote": builder_payload},
        builder_targets={"b-remote": targets},
    )


class TestBuilderOnlyProcess:
    """A builder container checks the dispatcher class its YAML names."""

    @staticmethod
    def _builder_only(dispatcher_plugin: str) -> Service:
        svc = _service()
        _register_payload_builder(svc)
        svc.configure_routing(
            dispatcher_identifiers=["d-remote"],
            builder_targets={"b1": ("d-remote",)},
        )
        svc.configure_topology(
            PipelineTopology(
                dispatcher_plugins={"d-remote": dispatcher_plugin},
                builder_payloads={"b1": "bash_payload"},
                builder_targets={"b1": ("d-remote",)},
            ),
        )
        return svc

    def test_incompatible_remote_dispatcher_fails_preflight(self) -> None:
        registry = MagicMock()
        registry.get_plugin.return_value = _empty_dispatcher_cls()
        svc = self._builder_only("empty_dispatcher")

        with (
            patch("courier.interfaces.dispatchers.dispatchers", registry),
            pytest.raises(ConfigurationError, match="not compatible with dispatcher"),
        ):
            svc.preflight_check()
        registry.get_plugin.assert_called_once_with("empty_dispatcher")

    def test_compatible_remote_dispatcher_passes(self) -> None:
        self._builder_only("local_dispatcher").preflight_check()

    def test_remote_dispatcher_not_installed_here_is_left_to_its_process(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A site dispatcher missing from the builder image is not an error.

        The dispatcher's own process checks the payloads it receives.
        """
        svc = self._builder_only("site_dispatcher")
        logger = svc._logger.logger
        logger.addHandler(caplog.handler)
        try:
            svc.preflight_check()
        finally:
            logger.removeHandler(caplog.handler)

        assert any(
            "Cannot check payload compatibility" in r.getMessage()
            for r in caplog.records
        )


class TestDispatcherOnlyProcess:
    """A dispatcher container checks every builder in the YAML targeting it."""

    @staticmethod
    def _dispatcher_only(
        topology: PipelineTopology,
        dispatcher_cls: type | None = None,
        *,
        allow_implicit_target: bool = True,
    ) -> Service:
        svc = _service()
        if dispatcher_cls is None:
            _register_local_dispatcher(svc)
        else:
            svc.register_plugin(dispatcher_cls, {}, identifier="d1")
        svc.configure_routing(
            dispatcher_identifiers=["d1"],
            builder_targets={},
            allow_implicit_target=allow_implicit_target,
            builder_identifiers=["b-remote"],
        )
        svc.configure_topology(topology)
        return svc

    def test_payload_not_installed_here_fails_preflight(self) -> None:
        """Otherwise every job from that builder fails on arrival."""
        svc = self._dispatcher_only(_topology(builder_payload="site_payload"))

        with pytest.raises(ConfigurationError, match="'site_payload'.*'b-remote'"):
            svc.preflight_check()

    def test_incompatible_payload_fails_preflight(self) -> None:
        svc = self._dispatcher_only(_topology(), _empty_dispatcher_cls())

        with pytest.raises(ConfigurationError, match="not compatible"):
            svc.preflight_check()

    def test_compatible_payload_passes(self) -> None:
        self._dispatcher_only(_topology()).preflight_check()

    def test_builders_targeting_someone_else_are_ignored(self) -> None:
        topology = PipelineTopology(
            dispatcher_plugins={"d1": "local_dispatcher", "d2": "local_dispatcher"},
            builder_payloads={"b-remote": "site_payload"},
            builder_targets={"b-remote": ("d2",)},
        )

        self._dispatcher_only(topology).preflight_check()

    def test_implicitly_routed_builder_is_checked(self) -> None:
        """A builder with no targets reaches the sole dispatcher, so it counts."""
        svc = self._dispatcher_only(
            _topology(builder_payload="site_payload", targets=()),
        )

        with pytest.raises(ConfigurationError, match="site_payload"):
            svc.preflight_check()

    def test_no_implicit_route_when_implicit_targets_are_disabled(self) -> None:
        svc = self._dispatcher_only(
            _topology(builder_payload="site_payload", targets=()),
            allow_implicit_target=False,
        )

        svc.preflight_check()
