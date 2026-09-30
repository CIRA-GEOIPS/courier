"""Nested sub-plugin registration in ``courier run``, against the real registries.

``test_run_only.py`` stubs the registries (and their ``nested_values``) to
test ``--only`` filtering; these tests use the real ones, so the nested
``payload`` block under a job builder is actually walked.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from courier.cli.plugins import PLUGIN_REGISTRIES
from courier.cli.run import _collect_topology, get_registered_plugin, run_service
from courier.config import ServiceConfig
from courier.errors import InvalidPluginConfigError
from courier.plugins.dispatchers.local_dispatcher import LocalDispatcher
from courier.plugins.job_builders.dummy_job_builder import DummyJobBuilder
from courier.plugins.payloads.bash_payload import BashPayload
from courier.schema.v1alpha1.service_config import MicroserviceModel
from courier.service import PipelineTopology


def _payload(identifier: str = "echo", kind: str = "payload", **spec: Any) -> dict:
    return {
        identifier: {
            "kind": kind,
            "name": spec.pop("name", "bash_payload"),
            "config": spec.pop("config", {"script": "echo {{ files[0].file }}"}),
        },
    }


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


def _register(*entries: MicroserviceModel) -> list[tuple[Any, dict, str | None]]:
    registrations: list[tuple[Any, dict, str | None]] = []
    for entry in entries:
        get_registered_plugin(registrations, entry)
    return registrations


class TestNestedPayloadRegistration:
    """The ``payload`` block registers a payload plugin beside its builder."""

    def test_builder_and_its_payload_are_both_registered(self) -> None:
        registrations = _register(_builder(payload=_payload()), _dispatcher())

        assert [(cls, ident) for cls, _cfg, ident in registrations] == [
            (DummyJobBuilder, "build"),
            (BashPayload, "echo"),
            (LocalDispatcher, "work"),
        ]

    def test_plural_kind_is_accepted_for_the_payload(self) -> None:
        registrations = _register(_builder(payload=_payload(kind="payloads")))

        assert registrations[1][0] is BashPayload

    def test_missing_payload_block_is_reported(self) -> None:
        with pytest.raises(InvalidPluginConfigError, match="'build' has no 'payload'"):
            _register(_builder())

    @pytest.mark.parametrize(
        "payload",
        [
            pytest.param(None, id="missing"),
            pytest.param({"a": {}, "b": {}}, id="malformed"),
            pytest.param(
                _payload(kind="dispatcher", name="local_dispatcher"),
                id="wrong-kind",
            ),
        ],
    )
    def test_run_reports_a_bad_block_as_constructing_the_builder_does(
        self,
        payload: dict | None,
    ) -> None:
        """One check, one wording, whichever path meets the block first."""
        entry = _builder(payload=payload)
        service = MagicMock()
        service.config = ServiceConfig(heartbeat_interval=30)

        with pytest.raises(InvalidPluginConfigError) as from_run:
            _register(entry)
        with pytest.raises(InvalidPluginConfigError) as from_builder:
            DummyJobBuilder(service, entry.spec.config, identifier=entry.identifier)

        assert str(from_run.value) == str(from_builder.value)
        assert "Every job builder needs a payload block" in str(from_run.value)

    @pytest.mark.parametrize(
        "payload",
        [
            pytest.param("bash_payload", id="not-a-mapping"),
            pytest.param({"a": {}, "b": {}}, id="two-sub-plugins"),
            pytest.param({"p": {"kind": "payload"}}, id="no-name"),
        ],
    )
    def test_unusable_payload_block_names_its_builder(self, payload: Any) -> None:
        """A clear config error instead of a raw pydantic traceback."""
        entry = MicroserviceModel.model_validate(
            {
                "build": {
                    "kind": "job_builder",
                    "name": "DummyJobBuilder",
                    "config": {"payload": payload},
                },
            },
        )

        with pytest.raises(InvalidPluginConfigError, match="'build'.*'payload'"):
            _register(entry)

    @pytest.mark.parametrize(
        ("kind", "name"),
        [
            pytest.param("dispatcher", "local_dispatcher", id="runnable-kind"),
            pytest.param("falcon", "bash_falcon", id="removed-kind"),
            pytest.param("data_monitor_configs", "goes18_abi", id="config-kind"),
        ],
    )
    def test_a_sub_plugin_of_the_wrong_kind_is_rejected(
        self,
        kind: str,
        name: str,
    ) -> None:
        """Clear error instead of a KeyError, a TypeError, or a stray dispatcher."""
        entry = _builder(payload=_payload(kind=kind, name=name, config={}))

        with pytest.raises(InvalidPluginConfigError) as caught:
            _register(entry)

        message = str(caught.value)
        assert "'build'" in message
        assert "'echo'" in message
        assert f"{kind!r}" in message
        assert "'payload'" in message

    def test_top_level_steps_still_need_a_runnable_kind(self) -> None:
        entry = MicroserviceModel.model_validate(
            {"p": {"kind": "payload", "name": "bash_payload"}},
        )

        with pytest.raises(ValueError, match="not a runnable kind"):
            _register(entry)


class TestOtherNestedSections:
    """A registry other than job builders' parses its sections generically."""

    @pytest.fixture(autouse=True)
    def _nesting_dispatchers(self, monkeypatch: pytest.MonkeyPatch) -> None:
        registry = MagicMock(nested_values=["helper"])
        registry.get_plugin.return_value = LocalDispatcher
        monkeypatch.setitem(PLUGIN_REGISTRIES, "dispatchers", registry)

    @staticmethod
    def _entry(config: dict | None) -> MicroserviceModel:
        return MicroserviceModel.model_validate(
            {"work": {"kind": "dispatcher", "name": "x", "config": config}},
        )

    def test_a_missing_section_is_reported(self) -> None:
        """Named by its key, not described as a payload block."""
        with pytest.raises(
            InvalidPluginConfigError,
            match="'work' is missing required config section 'helper'",
        ):
            _register(self._entry(None))

    @pytest.mark.parametrize(
        ("section", "problem"),
        [
            pytest.param("x", "must be a mapping", id="not-a-mapping"),
            pytest.param({"a": {}, "b": {}}, "must map one identifier", id="two"),
        ],
    )
    def test_an_unusable_section_is_reported(self, section: Any, problem: str) -> None:
        """The same checks the payload block gets, in generic words."""
        with pytest.raises(InvalidPluginConfigError, match=problem):
            _register(self._entry({"helper": section}))


class TestNestedIdentifierCollisions:
    """A nested identifier shares the plugin manager's keyspace."""

    def test_two_builders_cannot_share_a_payload_identifier(self) -> None:
        with pytest.raises(InvalidPluginConfigError, match="'shared'.*'b2'"):
            _register(
                _builder("b1", payload=_payload("shared")),
                _builder("b2", payload=_payload("shared")),
            )

    def test_a_payload_cannot_reuse_a_run_step_identifier(self) -> None:
        with pytest.raises(InvalidPluginConfigError, match="'work'"):
            _register(_builder(payload=_payload("work")), _dispatcher("work"))

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
                "'b2': nested sub-plugin 'shared'.*nested under 'b1'",
                id="two-builders-split-apart",
            ),
            pytest.param(
                lambda: [_builder(payload=_payload("work")), _dispatcher("work")],
                {"work"},
                "'build': nested sub-plugin 'work'.*a run step",
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


def _config(*entries: MicroserviceModel) -> MagicMock:
    config = MagicMock()
    config.spec.run = list(entries)
    config.spec.broker.to_url.return_value = "memory://"
    config.spec.allow_implicit_target = True
    config.spec.service_config = ServiceConfig(heartbeat_interval=30)
    config.metadata.namespace = "test"
    config.metadata.name = "test-service"
    return config


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
        """Their own process reports the block; every other one skips it."""
        config = _config(
            _builder("b1"),
            _builder("b2", payload={"a": {}, "b": {}}),
            _dispatcher("work"),
        )

        assert _collect_topology(config).builder_payloads == {}

    def test_a_payload_block_of_the_wrong_kind_is_left_out(self) -> None:
        """Not misreported by a dispatcher process as a missing payload plugin."""
        config = _config(
            _builder(payload=_payload(kind="dispatcher", name="local_dispatcher")),
            _dispatcher("work"),
        )

        assert _collect_topology(config).builder_payloads == {}

    @patch("courier.cli.run.create_service_with_plugins")
    def test_only_a_builder_registers_its_payload_too(
        self,
        create_service: MagicMock,
    ) -> None:
        config = _config(_builder(payload=_payload()), _dispatcher())

        run_service(config, only_set={"build"})

        registrations = create_service.call_args[0][1]
        assert [(cls, ident) for cls, _cfg, ident in registrations] == [
            (DummyJobBuilder, "build"),
            (BashPayload, "echo"),
        ]

    @patch("courier.cli.run.create_service_with_plugins")
    def test_run_service_hands_over_the_unfiltered_topology(
        self,
        create_service: MagicMock,
    ) -> None:
        config = _config(_builder(payload=_payload()), _dispatcher())

        run_service(config, only_set={"work"})

        service = create_service.return_value
        service.configure_topology.assert_called_once_with(
            PipelineTopology(
                dispatcher_plugins={"work": "local_dispatcher"},
                builder_payloads={"build": "bash_payload"},
                builder_targets={"build": ("work",)},
            ),
        )
        # Only the dispatcher runs here; the builder and payload do not.
        registrations = create_service.call_args[0][1]
        assert [ident for _cls, _cfg, ident in registrations] == ["work"]
        service.start.assert_called_once()
