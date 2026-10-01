"""A job builder's ``payload`` block in ``courier run``, against the real registries.

``test_run_only.py`` stubs the registries to test ``--only`` filtering; these
tests use the real ones. The builder constructs its payload from the block,
so ``courier run`` registers run steps only, and reads the block for the
YAML-wide identifier check and the topology preflight uses.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from courier.cli.run import _collect_topology, get_registered_plugin, run_service
from courier.config import ServiceConfig
from courier.errors import InvalidPluginConfigError
from courier.plugins.dispatchers.local_dispatcher import LocalDispatcher
from courier.plugins.job_builders.dummy_job_builder import DummyJobBuilder
from courier.schema.v1alpha1.service_config import MicroserviceModel
from courier.service import PipelineTopology
from tests._helpers import payload_block


def _payload(
    identifier: str = "echo",
    kind: str = "payload",
    name: str = "bash_payload",
) -> dict[str, Any]:
    return payload_block(identifier, "echo {{ files[0].file }}", name=name, kind=kind)


def _builder(
    identifier: str = "build",
    payload: dict | None = None,
    targets: tuple[str, ...] = ("work",),
) -> MicroserviceModel:
    config: dict[str, Any] = {"targets": list(targets)}
    if payload is not None:
        config["payload"] = payload
    return MicroserviceModel.model_validate(
        {
            identifier: {
                "kind": "job_builder",
                "name": "DummyJobBuilder",
                "config": config,
            },
        },
    )


def _dispatcher(identifier: str = "work") -> MicroserviceModel:
    return MicroserviceModel.model_validate(
        {identifier: {"kind": "dispatcher", "name": "local_dispatcher"}},
    )


def _config(*entries: MicroserviceModel) -> MagicMock:
    config = MagicMock()
    config.spec.run = list(entries)
    config.spec.broker.to_url.return_value = "memory://"
    config.spec.allow_implicit_target = True
    config.spec.service_config = ServiceConfig(
        heartbeat_interval=30,
        tracing_enabled=False,
    )
    config.metadata.namespace = "test"
    config.metadata.name = "test-service"
    return config


class TestRegistration:
    """Run steps are registered; the payload is the builder's own."""

    def test_the_payload_is_not_registered_beside_its_builder(self) -> None:
        registrations: list[tuple[Any, dict, str | None]] = []
        for entry in (_builder(payload=_payload()), _dispatcher()):
            get_registered_plugin(registrations, entry)

        assert [(cls, ident) for cls, _cfg, ident in registrations] == [
            (DummyJobBuilder, "build"),
            (LocalDispatcher, "work"),
        ]

    def test_top_level_steps_still_need_a_runnable_kind(self) -> None:
        entry = MicroserviceModel.model_validate(
            {"p": {"kind": "payload", "name": "bash_payload"}},
        )

        with pytest.raises(ValueError, match="not a runnable kind"):
            get_registered_plugin([], entry)

    def test_run_refuses_a_builder_without_a_payload_block(self) -> None:
        """Constructing the builder rejects it, before the service starts."""
        with (
            patch("courier.service.Service.start") as start,
            pytest.raises(InvalidPluginConfigError, match="'build' has no 'payload'"),
        ):
            run_service(_config(_builder(), _dispatcher()))

        start.assert_not_called()


class TestPayloadIdentifierCollisions:
    """A payload identifier must be unique across the whole YAML."""

    @pytest.mark.parametrize(
        ("entries", "only", "match"),
        [
            pytest.param(
                lambda: [
                    _builder("b1", payload=_payload("shared")),
                    _builder("b2", payload=_payload("shared")),
                    _dispatcher(),
                ],
                {"b1"},
                "'b2': payload 'shared'.*the payload of 'b1'",
                id="two-builders-split-apart",
            ),
            pytest.param(
                lambda: [_builder(payload=_payload("work")), _dispatcher("work")],
                {"work"},
                "'build': payload 'work'.*a run step",
                id="payload-named-like-a-remote-step",
            ),
        ],
    )
    @patch("courier.cli.run.create_service_with_plugins")
    def test_collisions_fail_whatever_only_selects(
        self,
        create_service: MagicMock,
        entries: Any,
        only: set[str],
        match: str,
    ) -> None:
        """The same YAML fails in every container, not just one hosting both."""
        with pytest.raises(InvalidPluginConfigError, match=match):
            run_service(_config(*entries()), only_set=only)

        create_service.assert_not_called()


class TestTopology:
    """Preflight gets the whole YAML's plugin names, whatever ``--only`` runs."""

    def test_topology_names_every_dispatcher_and_builder_payload(self) -> None:
        config = _config(
            _builder("b1", payload=_payload("p1", name="python_payload")),
            _builder("b2", payload=_payload("p2"), targets=()),
            _dispatcher("work"),
        )

        assert _collect_topology(config) == PipelineTopology(
            dispatcher_plugins={"work": "local_dispatcher"},
            builder_payloads={"b1": "python_payload", "b2": "bash_payload"},
            builder_targets={"b1": ("work",), "b2": ()},
        )

    def test_builders_with_an_unusable_payload_block_are_left_out(self) -> None:
        """Their own process reports the block; every other one skips it.

        A wrong kind in particular is not misreported by a dispatcher process
        as a missing payload plugin.
        """
        config = _config(
            _builder("b1"),
            _builder("b2", payload={"a": {}, "b": {}}),
            _builder(
                "b3", payload=_payload(kind="dispatcher", name="local_dispatcher")
            ),
            _dispatcher("work"),
        )

        assert _collect_topology(config).builder_payloads == {}

    @pytest.mark.parametrize("only", [{"work"}, {"build"}])
    @patch("courier.cli.run.create_service_with_plugins")
    def test_run_service_hands_over_the_unfiltered_topology(
        self,
        create_service: MagicMock,
        only: set[str],
    ) -> None:
        config = _config(_builder(payload=_payload()), _dispatcher())

        run_service(config, only_set=only)

        service = create_service.return_value
        service.configure_topology.assert_called_once_with(
            PipelineTopology(
                dispatcher_plugins={"work": "local_dispatcher"},
                builder_payloads={"build": "bash_payload"},
                builder_targets={"build": ("work",)},
            ),
        )
        # Only the selected step is registered here.
        registrations = create_service.call_args[0][1]
        assert [ident for _cls, _cfg, ident in registrations] == list(only)
        service.start.assert_called_once()
