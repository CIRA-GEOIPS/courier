"""Every job builder requires a payload.

A job is only executable with the payload its builder renders onto it, so the
requirement is enforced in the :class:`JobBuilder` base class, where it covers
every builder, including a third-party one that calls ``super().__init__``:

* construction rejects a config without a valid ``payload`` block, and
  otherwise constructs the payload plugin the block names;
* ``emit()`` always attaches it, so a job without one is never published.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import MappingProxyType
from typing import Any, ClassVar, cast
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from courier.errors import InvalidPluginConfigError, PluginNotFoundError
from courier.interfaces.job_builders import JobBuilder, job_builders
from courier.plugins.payloads.bash_payload import BashPayload
from courier.types.file import FrozenFile
from courier.types.job import Job
from tests._helpers import (
    DEFAULT_PAYLOAD_ID,
    IN_TREE_BUILDER_SETTINGS,
    payload_block,
    with_payload,
)

BUILDER_NAMES = job_builders.names()

#: What each in-tree builder is, so a newly registered one cannot be skipped.
IN_TREE_BUILDERS = list(IN_TREE_BUILDER_SETTINGS)

_UNUSABLE_CONFIGS = [
    pytest.param(None, "has no 'payload' block", id="no-config"),
    pytest.param({}, "has no 'payload' block", id="no-block"),
    pytest.param(
        {"payload": "bash_payload"},
        "(should be a mapping, not str)",
        id="not-a-mapping",
    ),
    pytest.param(
        {"payload": {"a": {"kind": "payload", "name": "bash_payload"}, "b": {}}},
        "(it takes exactly one payload plugin; found 2)",
        id="two-plugins",
    ),
    pytest.param(
        {"payload": {}},
        "(it takes exactly one payload plugin; found 0)",
        id="no-plugin",
    ),
    # Located by the keys written (``p.name``), not the model's ``spec.name``.
    pytest.param({"payload": {"p": {"kind": "payload"}}}, "(p.name: ", id="no-name"),
    pytest.param(
        {"payload": {"identifier": "p", "spec": {"kind": "payload"}}},
        "(spec.name: ",
        id="canonical-form-no-name",
    ),
    pytest.param(
        {"payload": {"p": {"kind": "dispatcher", "name": "local_dispatcher"}}},
        "of kind 'dispatcher'",
        id="wrong-kind",
    ),
    # Settings the payload plugin could never be constructed from.
    pytest.param(
        {"payload": {"p": {"kind": "payload", "name": "bash_payload", "config": "x"}}},
        "(p.config: should be a mapping of settings, not str)",
        id="settings-not-a-mapping",
    ),
]


@pytest.fixture
def service() -> MagicMock:
    """Service stub whose ``emit`` records every publish."""
    svc = MagicMock()
    svc.config = MagicMock(log_level="DEBUG", loki_enabled=False, namespace="ns")
    svc.target_resolver.resolve.side_effect = lambda ident: f"JobReady-{ident}"
    return svc


def _builder_class(name: str) -> type[JobBuilder]:
    """Return the job builder class registered under *name*."""
    return cast("type[JobBuilder]", job_builders.get_plugin(name))


def _config(name: str, **settings: Any) -> dict[str, Any]:
    """Return a builder config for *name* with ``targets`` but no payload.

    A third-party builder gets no settings of its own: the payload check runs
    before a subclass validates anything.
    """
    return {"targets": ["dp-1"], **IN_TREE_BUILDER_SETTINGS.get(name, {}), **settings}


def _file(name: str = "a") -> FrozenFile:
    return FrozenFile(
        file=Path(f"/data/{name}.nc"),
        hostname="h",
        source="goes16",
        instrument="abi",
    )


class _SiteBuilder(JobBuilder):
    """A third-party builder: its own config, and ``super().__init__``."""

    name: ClassVar[str] = "site_builder"
    version: ClassVar[str] = "1"

    def __init__(
        self,
        service: Any,
        config: dict | None = None,
        identifier: str | None = None,
    ) -> None:
        super().__init__(service, config, identifier=identifier)
        self.site_setting = self.config["site_setting"]


def test_every_in_tree_builder_is_registered() -> None:
    """Guards the parametrization below against silently losing a builder."""
    assert set(IN_TREE_BUILDERS) <= set(BUILDER_NAMES)


# ── construction ────────────────────────────────────────────────────────────


class TestConstruction:
    """A builder is constructed with its payload, or not at all."""

    @pytest.mark.parametrize("name", BUILDER_NAMES)
    def test_a_config_without_a_payload_block_is_rejected(
        self,
        service: MagicMock,
        name: str,
    ) -> None:
        """The error names the builder and shows a block to copy."""
        with pytest.raises(InvalidPluginConfigError) as caught:
            _builder_class(name)(service, _config(name), identifier="jb-1")

        message = str(caught.value)
        assert "Job builder 'jb-1' has no 'payload' block" in message
        assert "Every job builder needs a payload block" in message
        # A minimal block to copy from, not just a complaint.
        assert "kind: payload" in message
        assert "name: bash_payload" in message

    @pytest.mark.parametrize(("config", "problem"), _UNUSABLE_CONFIGS)
    def test_an_unusable_payload_block_is_rejected(
        self,
        service: MagicMock,
        config: Any,
        problem: str,
    ) -> None:
        """Each way a block can be unusable is reported, not raised raw."""
        with pytest.raises(InvalidPluginConfigError) as caught:
            JobBuilder(service, config, identifier="jb-1")

        message = str(caught.value)
        assert "Job builder 'jb-1'" in message
        assert problem in message
        assert "Every job builder needs a payload block" in message

    @pytest.mark.parametrize("name", BUILDER_NAMES)
    def test_a_valid_block_constructs_the_payload_it_names(
        self,
        service: MagicMock,
        name: str,
    ) -> None:
        builder = _builder_class(name)(
            service,
            _config(name, payload=payload_block("p-7", "echo hi")),
            identifier="jb-1",
        )

        assert isinstance(builder.payload, BashPayload)
        assert builder.payload.identifier == "p-7"
        assert builder.payload.config.script == "echo hi"

    @pytest.mark.parametrize(
        ("block", "error"),
        [
            pytest.param(
                payload_block(name="no_such_payload"),
                PluginNotFoundError,
                id="not-installed",
            ),
            pytest.param(payload_block(settings={}), ValidationError, id="bad-config"),
        ],
    )
    def test_a_payload_that_cannot_be_constructed_fails_the_builder(
        self,
        service: MagicMock,
        block: dict[str, Any],
        error: type[Exception],
    ) -> None:
        """At startup, when the builder is built, not when a job arrives."""
        with pytest.raises(error):
            JobBuilder(service, {"payload": block}, identifier="jb-1")

    @pytest.mark.parametrize("kind", ["payload", "payloads", "Payload"])
    def test_every_spelling_of_the_payload_kind_is_accepted(
        self,
        service: MagicMock,
        kind: str,
    ) -> None:
        """The same normalization ``courier run`` applies to ``kind``."""
        builder = JobBuilder(
            service,
            {"payload": payload_block(kind=kind)},
            identifier="jb-1",
        )

        assert builder.payload.identifier == DEFAULT_PAYLOAD_ID

    def test_a_read_only_mapping_is_a_payload_block(self, service: MagicMock) -> None:
        """Any mapping will do, not only the dict a YAML file loads as."""
        builder = JobBuilder(
            service,
            {"payload": MappingProxyType(payload_block("p-7"))},
            identifier="jb-1",
        )

        assert builder.payload.identifier == "p-7"

    def test_a_third_party_builder_gets_the_check_from_the_base_class(
        self,
        service: MagicMock,
    ) -> None:
        """Rejected before its own ``__init__`` reads a single setting."""
        with pytest.raises(InvalidPluginConfigError, match="'site' has no 'payload'"):
            _SiteBuilder(service, {"site_setting": 1}, identifier="site")

        builder = _SiteBuilder(
            service,
            with_payload({"site_setting": 1}),
            identifier="site",
        )
        assert builder.site_setting == 1
        assert builder.payload.identifier == DEFAULT_PAYLOAD_ID


# ── emit() ──────────────────────────────────────────────────────────────────


class TestEmit:
    """A job without a payload is never published."""

    def test_every_published_job_carries_the_payload(
        self,
        service: MagicMock,
    ) -> None:
        """Every target gets the rendered payload with the job."""
        builder = JobBuilder(service, with_payload({"targets": ["dp-1"]}))

        builder.emit(Job("n", "job-1", {}, files=[_file()]), ["dp-a", "dp-b"])

        published = [
            Job.from_string(call.kwargs["message"])
            for call in service.emit.call_args_list
        ]
        assert len(published) == 2  # noqa: PLR2004
        for job in published:
            assert job.payload is not None
            assert job.payload.identifier == DEFAULT_PAYLOAD_ID
            assert job.payload.script == "echo 1"

    def test_a_payload_that_renders_no_spec_publishes_nothing(
        self,
        service: MagicMock,
    ) -> None:
        """A ``to_job_spec`` override returning ``None`` is a render failure."""

        class _NoSpecPayload(BashPayload):
            def to_job_spec(self, job: Job, builder: Any | None = None) -> Any:
                del job, builder

        builder = JobBuilder(service, with_payload({"targets": ["dp-1"]}))
        builder.payload = _NoSpecPayload(
            service,
            {"script": "echo 1"},
            identifier=DEFAULT_PAYLOAD_ID,
        )
        job = Job("n", "job-1", {}, files=[_file()])

        with patch.object(builder._logger, "exception") as logged:
            assert builder.emit(job) is True

        service.emit.assert_not_called()
        assert job.payload is None
        [call] = logged.call_args_list
        assert "failed to render" in call.args[0]


# ── the payload block stays out of job.config ───────────────────────────────


#: A string only the payload block contains, so finding it anywhere in a job's
#: config means the block (or its template) was copied there.
_MARKER = "only-in-the-payload-block"


@pytest.mark.parametrize("name", IN_TREE_BUILDERS)
def test_a_published_job_has_no_copy_of_the_payload_block_in_its_config(
    service: MagicMock,
    name: str,
) -> None:
    """The block reaches the dispatcher once, as ``job.payload``.

    Pass one also sees ``config`` without it: the template's own context
    cannot find a ``payload`` key there.
    """
    script = f"echo {_MARKER} {{{{ 'payload' in config }}}}"
    builder = _builder_class(name)(
        service,
        with_payload(_config(name), script=script),
        identifier="jb-1",
    )

    builder._dispatch_file(_file())

    [call] = service.emit.call_args_list
    body = json.loads(call.kwargs["message"])
    config_text = json.dumps(body["config"])
    assert _MARKER not in config_text
    assert DEFAULT_PAYLOAD_ID not in config_text
    assert "payload" not in body["config"]
    job = Job.from_string(call.kwargs["message"])
    assert job.payload is not None
    assert job.payload.script == f"echo {_MARKER} False"


@pytest.mark.parametrize("name", IN_TREE_BUILDERS)
def test_a_published_job_carries_no_state_sync_settings(
    service: MagicMock,
    name: str,
) -> None:
    """Like the payload block, ``state_sync`` is the builder's own config.

    It holds the Redis connection settings, a password among them, which must
    not travel to every dispatcher in every job message.
    """
    secret = "redis-password-not-for-the-wire"
    config = with_payload(
        _config(name, state_sync={"host": "redis", "password": secret}),
    )
    # No Redis here: construct as a synced builder, then emit unsynced.
    with patch.object(JobBuilder, "_init_sync", return_value=MagicMock()):
        builder = _builder_class(name)(service, config, identifier="jb-1")
    builder._sync = None

    builder._dispatch_file(_file())

    [call] = service.emit.call_args_list
    assert secret not in call.kwargs["message"]
    assert "state_sync" not in json.loads(call.kwargs["message"])["config"]
