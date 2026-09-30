"""Every job builder requires a payload.

A job is only executable with the payload its builder renders onto it, so the
requirement is enforced in the :class:`JobBuilder` base class, where it covers
every builder, including a third-party one that calls ``super().__init__``:

* construction rejects a config without a valid ``payload`` block;
* the bound payload is read through a property that refuses to hand back
  nothing, and only the payload the block names can be bound;
* ``start()`` refuses to run without it, before consuming anything;
* ``emit()`` always attaches it, so a job without one is never published.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, ClassVar, cast
from unittest.mock import MagicMock, patch

import pytest

from courier.constants import PluginRunState
from courier.errors import ConfigurationError, InvalidPluginConfigError
from courier.interfaces.job_builders import JobBuilder, job_builders
from courier.interfaces.payloads import Payload
from courier.plugins.payloads.bash_payload import BashPayload
from courier.types.file import FrozenFile
from courier.types.job import Job
from tests._helpers import (
    DEFAULT_PAYLOAD_ID,
    bind_payload,
    payload_block,
    with_payload,
)

#: Settings besides ``payload`` that an in-tree builder needs, so the payload
#: block is the only thing wrong with the configs these tests build.  A builder
#: missing from this table (a third-party one) is given ``{}``: the payload
#: check runs before a subclass validates anything of its own.
_OWN_SETTINGS: dict[str, dict[str, Any]] = {
    "metadata_router": {"routes": [{"name": "all", "files_per_job": 1}]},
    "filter_and_group": {"files_per_job": 1},
    "file_count_builder": {"files_per_job": 1},
}

BUILDER_NAMES = job_builders.names()

#: What each in-tree builder is, so a newly registered one cannot be skipped.
IN_TREE_BUILDERS = [
    "DummyJobBuilder",
    "file_count_builder",
    "filter_and_group",
    "metadata_router",
]

_MALFORMED_BLOCKS = [
    pytest.param("bash_payload", "(a str, not a mapping)", id="not-a-mapping"),
    pytest.param(
        {"a": {"kind": "payload", "name": "bash_payload"}, "b": {}},
        "exactly one identifier mapping",
        id="two-plugins",
    ),
    # Located by the keys written (``p.name``), not the model's ``spec.name``.
    pytest.param({"p": {"kind": "payload"}}, "(p.name: ", id="no-plugin-name"),
    pytest.param(
        {"identifier": "p", "spec": {"kind": "payload"}},
        "(spec.name: ",
        id="canonical-form-no-plugin-name",
    ),
    pytest.param(
        {"p": {"kind": "dispatcher", "name": "local_dispatcher"}},
        "of kind 'dispatcher'",
        id="wrong-kind",
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
    """Return a builder config for *name* with ``targets`` but no payload."""
    return {"targets": ["dp-1"], **_OWN_SETTINGS.get(name, {}), **settings}


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
    """A builder cannot be constructed without a valid payload block."""

    @pytest.mark.parametrize("name", BUILDER_NAMES)
    def test_a_config_without_a_payload_block_is_rejected(
        self,
        service: MagicMock,
        name: str,
    ) -> None:
        """The error names the builder and shows a block to copy."""
        builder_cls = _builder_class(name)

        with pytest.raises(InvalidPluginConfigError) as caught:
            builder_cls(service, _config(name), identifier="jb-1")

        message = str(caught.value)
        assert "Job builder 'jb-1' has no 'payload' block" in message
        assert "Every job builder needs a payload block" in message
        # A minimal block to copy from, not just a complaint.
        assert "kind: payload" in message
        assert "name: bash_payload" in message

    @pytest.mark.parametrize("name", BUILDER_NAMES)
    def test_no_config_at_all_is_rejected(
        self,
        service: MagicMock,
        name: str,
    ) -> None:
        """``None`` is the default config, and it has no payload either."""
        with pytest.raises(InvalidPluginConfigError, match="has no 'payload' block"):
            _builder_class(name)(service, None, identifier="jb-1")

    @pytest.mark.parametrize("name", BUILDER_NAMES)
    @pytest.mark.parametrize(("block", "problem"), _MALFORMED_BLOCKS)
    def test_a_malformed_payload_block_is_rejected(
        self,
        service: MagicMock,
        name: str,
        block: Any,
        problem: str,
    ) -> None:
        """Each way a block can be unusable is reported, not raised raw."""
        builder_cls = _builder_class(name)

        with pytest.raises(InvalidPluginConfigError) as caught:
            builder_cls(service, _config(name, payload=block), identifier="jb-1")

        message = str(caught.value)
        assert "Job builder 'jb-1'" in message
        assert problem in message
        assert "Every job builder needs a payload block" in message

    @pytest.mark.parametrize("name", BUILDER_NAMES)
    def test_a_valid_block_is_kept_for_binding(
        self,
        service: MagicMock,
        name: str,
    ) -> None:
        """The validated block is what preflight binds the payload by."""
        builder = _builder_class(name)(
            service,
            _config(name, payload=payload_block("p-7")),
            identifier="jb-1",
        )

        assert builder.payload_identifier == "p-7"
        assert builder.payload_block.spec.name == "bash_payload"
        assert builder.has_payload is False

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

        assert builder.payload_identifier == DEFAULT_PAYLOAD_ID

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
        assert builder.payload_identifier == DEFAULT_PAYLOAD_ID


# ── binding ─────────────────────────────────────────────────────────────────


class TestBinding:
    """Only the payload the block names can be bound, and reading needs one."""

    @staticmethod
    def _builder(service: MagicMock) -> JobBuilder:
        return JobBuilder(service, with_payload({"targets": ["dp-1"]}), identifier="jb")

    def test_reading_an_unbound_payload_raises(self, service: MagicMock) -> None:
        """Nothing is handed back that emit could quietly skip."""
        builder = self._builder(service)

        with pytest.raises(ConfigurationError) as caught:
            _ = builder.payload

        message = str(caught.value)
        assert "'jb' has no payload bound" in message
        assert repr(DEFAULT_PAYLOAD_ID) in message

    def test_has_payload_asks_without_raising(self, service: MagicMock) -> None:
        """The cheap question code can ask before anything is bound."""
        builder = self._builder(service)
        assert builder.has_payload is False

        bind_payload(builder)

        assert builder.has_payload is True

    def test_the_named_payload_binds(self, service: MagicMock) -> None:
        """The payload registered under the block's identifier is accepted."""
        builder = self._builder(service)
        payload = BashPayload(
            service,
            {"script": "true"},
            identifier=DEFAULT_PAYLOAD_ID,
        )

        builder.payload = payload

        assert builder.payload is payload

    def test_a_payload_with_another_identifier_is_refused(
        self,
        service: MagicMock,
    ) -> None:
        """Binding the wrong payload would run the wrong script."""
        builder = self._builder(service)
        other = BashPayload(service, {"script": "true"}, identifier="someone-else")

        with pytest.raises(ConfigurationError) as caught:
            builder.payload = other

        message = str(caught.value)
        assert repr(DEFAULT_PAYLOAD_ID) in message
        assert "'someone-else'" in message
        assert builder.has_payload is False

    def test_something_that_is_not_a_payload_is_refused(
        self,
        service: MagicMock,
    ) -> None:
        """Only a Payload can render a job spec."""
        builder = self._builder(service)

        with pytest.raises(ConfigurationError, match="only bind a Payload"):
            builder.payload = MagicMock(identifier=DEFAULT_PAYLOAD_ID)

        assert builder.has_payload is False

    def test_a_rebind_must_name_the_same_payload(self, service: MagicMock) -> None:
        """A refused rebind leaves the bound payload in place."""
        builder = self._builder(service)
        first = bind_payload(builder)

        with pytest.raises(ConfigurationError):
            builder.payload = BashPayload(service, {"script": "true"}, identifier="x")

        assert builder.payload is first


# ── start() ─────────────────────────────────────────────────────────────────


class TestStart:
    """A builder with no payload bound refuses to start."""

    @pytest.mark.parametrize("name", BUILDER_NAMES)
    def test_start_without_a_payload_raises_before_consuming(
        self,
        service: MagicMock,
        name: str,
    ) -> None:
        """Nothing is consumed, connected to or published first."""
        builder = _builder_class(name)(
            service,
            with_payload(_config(name)),
            identifier="jb-1",
        )
        builder._sync = MagicMock()
        service.consume.return_value = iter([(str(_file()), None)])

        with pytest.raises(ConfigurationError, match="'jb-1' has no payload bound"):
            builder.start()

        assert builder._state is PluginRunState.STOPPED
        assert builder._main_thread is None
        service.consume.assert_not_called()
        builder._sync.connect.assert_not_called()
        service.emit.assert_not_called()

    def test_start_with_a_payload_runs(self, service: MagicMock) -> None:
        """The check does not get in the way of a wired builder."""
        builder = JobBuilder(service, with_payload({"targets": ["dp-1"]}))
        bind_payload(builder)
        service.consume.return_value = iter(())

        builder.start()
        try:
            assert builder.is_healthy() is True
        finally:
            builder.stop()


# ── emit() ──────────────────────────────────────────────────────────────────


class TestEmit:
    """A job without a payload is never published."""

    def test_emit_without_a_payload_raises_and_publishes_nothing(
        self,
        service: MagicMock,
    ) -> None:
        """A wiring error, not a per-job render failure to log and move past."""
        builder = JobBuilder(service, with_payload({"targets": ["dp-1"]}))
        job = Job("n", "job-1", {}, files=[_file()])

        with pytest.raises(ConfigurationError, match="has no payload bound"):
            builder.emit(job, ["dp-1"])

        service.emit.assert_not_called()
        assert job.payload is None
        assert job.emit_time is None

    def test_every_published_job_carries_the_bound_payload(
        self,
        service: MagicMock,
    ) -> None:
        """Every target gets the rendered payload with the job."""
        builder = JobBuilder(service, with_payload({"targets": ["dp-1"]}))
        bind_payload(builder)

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

    def test_a_payload_that_fails_to_render_publishes_nothing(
        self,
        service: MagicMock,
    ) -> None:
        """The one way a bound payload yields no spec: the job is dropped."""
        builder = JobBuilder(service, with_payload({"targets": ["dp-1"]}))
        payload = MagicMock(spec=Payload, identifier=DEFAULT_PAYLOAD_ID)
        payload.to_job_spec.side_effect = RuntimeError("no")
        builder.payload = payload

        assert builder.emit(Job("n", "job-1", {}, files=[_file()])) is True

        service.emit.assert_not_called()


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
    bind_payload(builder)

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
    bind_payload(builder)

    builder._dispatch_file(_file())

    [call] = service.emit.call_args_list
    assert secret not in call.kwargs["message"]
    assert "state_sync" not in json.loads(call.kwargs["message"])["config"]
