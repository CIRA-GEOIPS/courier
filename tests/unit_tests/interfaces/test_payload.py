from pathlib import Path
from unittest.mock import MagicMock

import pytest

from courier.interfaces.payloads import Payload
from courier.plugins.payloads.shell_payload import ShellPayload
from courier.types.file import File
from courier.types.job import Job


@pytest.fixture
def service() -> MagicMock:
    svc = MagicMock()
    svc.config = MagicMock(log_level="DEBUG", loki_enabled=False, namespace="ns")
    svc._broker_manager._connection = None
    return svc


class TestRepresentationHierarchy:
    def test_child_hierarchy(self) -> None:
        res = ShellPayload.get_representation_hierarchy()

        assert len(res) == 1
        assert res == [ShellPayload]

    def test_child_of_child_hierarchy(self) -> None:
        class GrandchildPayload(ShellPayload):
            pass

        res = GrandchildPayload.get_representation_hierarchy()
        assert len(res) == 2
        assert res == [ShellPayload, GrandchildPayload]

    def test_base_hierarchy(self) -> None:
        res = Payload.get_representation_hierarchy()

        assert len(res) == 0
        assert res == []


# ── two-pass render and deferred-expression security ────────────────────────


def _bash_payload(service, tmp_path, template: str):
    from courier.plugins.payloads.bash_payload import BashPayload

    script = tmp_path / "template.sh"
    script.write_text(template)
    return BashPayload(service, {"file": script}, "payload-1")


def _job_with_file(path: str) -> Job:
    return Job("n", "job-1", {}, files=[File(file=Path(path)).freeze()])


def test_pass_one_defers_dispatcher_values(service, tmp_path) -> None:
    from courier.interfaces.payloads import _DEFER_PREFIX

    payload = _bash_payload(
        service,
        tmp_path,
        'echo "f={{ files[0].file }}"\necho "d={{ dispatcher.identifier }}"',
    )
    job = _job_with_file("/data/a.nc")

    spec = payload.to_job_spec(job)

    assert "/data/a.nc" in spec.script
    assert _DEFER_PREFIX in spec.script
    assert spec.defer_nonce
    # The deferred value round-trips: the dispatcher's pass two replaces the
    # marker with the value the builder could not know.
    resolved = payload.resolve_deferred_expressions(
        spec.script,
        job,
        {"dispatcher": {"identifier": "ld"}},
        defer_nonce=spec.defer_nonce,
    )
    assert "d=ld" in resolved
    assert _DEFER_PREFIX not in resolved


def test_pass_two_resolves_only_authenticated_markers(service, tmp_path) -> None:
    payload = _bash_payload(
        service,
        tmp_path,
        'echo "f={{ files[0].file }}"\necho "d={{ dispatcher.identifier }}"'
        '\necho "p={{ script_path }}"',
    )
    job = _job_with_file("/data/a.nc")
    spec = payload.to_job_spec(job)

    resolved = payload.resolve_deferred_expressions(
        spec.script,
        job,
        {"dispatcher": {"identifier": "ld"}, "script_path": "/tmp/x.sh"},
        defer_nonce=spec.defer_nonce,
    )

    assert "/data/a.nc" in resolved
    assert "d=ld" in resolved
    assert "p=/tmp/x.sh" in resolved


def test_untrusted_job_data_is_never_evaluated(service, tmp_path) -> None:
    payload = _bash_payload(service, tmp_path, "echo {{ files[0].file }}")
    job = _job_with_file(
        "/data/{{ 6*7 }}-{{ ''.__class__.__mro__[1].__subclasses__() }}.nc",
    )
    spec = payload.to_job_spec(job)

    resolved = payload.resolve_deferred_expressions(
        spec.script,
        job,
        {"dispatcher": {"identifier": "ld"}},
        defer_nonce=spec.defer_nonce,
    )

    assert "{{ 6*7 }}" in resolved
    assert "__subclasses__" in resolved
    assert "42" not in resolved


def test_conditional_over_dispatcher_value_raises(service, tmp_path) -> None:
    from courier.errors import CourierError

    payload = _bash_payload(
        service,
        tmp_path,
        "{% if dispatcher.name %}yes{% else %}no{% endif %}",
    )
    job = _job_with_file("/data/a.nc")

    with pytest.raises(CourierError, match="conditional"):
        payload.to_job_spec(job)


def test_filter_over_dispatcher_value_raises(service, tmp_path) -> None:
    """A filter over a dispatcher-only value fails at the builder (pass one).

    It used to slip through pass one and fail -- or silently render the
    marker's text -- only on the dispatcher.
    """
    from courier.errors import CourierError

    payload = _bash_payload(service, tmp_path, "echo {{ script_path | lower }}")
    job = _job_with_file("/data/a.nc")

    with pytest.raises(CourierError, match="'lower' filter"):
        payload.to_job_spec(job)


def test_forged_deferred_marker_is_refused(service, tmp_path) -> None:
    from courier.errors import CourierError
    from courier.interfaces.payloads import _deferred_marker

    payload = _bash_payload(service, tmp_path, "echo x")
    job = _job_with_file("/data/a.nc")
    spec = payload.to_job_spec(job)
    spec.script = "echo " + _deferred_marker("deadbeef", "dispatcher.identifier")

    with pytest.raises(CourierError, match="authentication"):
        payload.resolve_deferred_expressions(
            spec.script,
            job,
            {"dispatcher": {"identifier": "ld"}},
            defer_nonce=spec.defer_nonce,
        )


def test_from_job_spec_hydrates_without_local_template(service, tmp_path) -> None:
    """A spec whose template file is absent here must still execute.

    The builder-rendered ``script`` is authoritative; ``config.file`` is path
    metadata only, so hydration must not require the file to exist on the
    dispatcher host — and the result must be a *usable* payload, not merely the
    right class.
    """
    from courier.interfaces.payloads import PayloadSpec
    from courier.plugins.dispatchers.local_dispatcher import LocalDispatcher
    from courier.types.job import Job

    spec = PayloadSpec(
        name="bash_payload",
        identifier="p",
        config={"file": str(tmp_path / "does-not-exist.sh")},
        script="echo hydrated-and-runnable",
        defer_nonce="abc",
    )
    job = Job("n", "job-1", {}, payload=spec)

    dispatcher = LocalDispatcher(service, {}, identifier="ld")
    resolved = dispatcher._resolve_job_payload(job)
    env = dispatcher.initialize_environment(job, resolved)

    assert env.file is not None
    assert env.file.read_text() == "echo hydrated-and-runnable"


def test_from_job_spec_rejects_empty_config(service) -> None:
    from pydantic import ValidationError

    from courier.interfaces.payloads import PayloadSpec
    from courier.plugins.payloads.bash_payload import BashPayload

    with pytest.raises(ValidationError):
        BashPayload.from_job_spec(
            PayloadSpec(name="bash_payload", identifier="p", config={}),
            service,
        )


def test_from_job_spec_attaches_base_config(service) -> None:
    """Passing a dispatcher config attaches it, instead of a separate assignment."""
    from courier.interfaces.payloads import DispatcherGroupConfig, PayloadSpec
    from courier.plugins.payloads.bash_payload import BashPayload

    base = DispatcherGroupConfig.model_validate({})
    payload = BashPayload.from_job_spec(
        PayloadSpec(
            name="bash_payload",
            identifier="p",
            config={"script": "echo hi"},
            script="echo hi",
        ),
        service,
        base,
    )

    assert payload.base_config is base


def test_arithmetic_over_dispatcher_value_raises(service, tmp_path) -> None:
    from courier.errors import CourierError

    payload = _bash_payload(service, tmp_path, "echo {{ dispatcher.x + 1 }}")
    job = _job_with_file("/data/a.nc")

    with pytest.raises(CourierError):
        payload.to_job_spec(job)


def test_comparison_over_dispatcher_value_raises(service, tmp_path) -> None:
    from courier.errors import CourierError

    payload = _bash_payload(service, tmp_path, "echo {{ dispatcher.x == 1 }}")
    job = _job_with_file("/data/a.nc")

    with pytest.raises(CourierError):
        payload.to_job_spec(job)


def test_default_filter_over_dispatcher_value_raises(service, tmp_path) -> None:
    from courier.errors import CourierError

    payload = _bash_payload(
        service,
        tmp_path,
        "echo {{ dispatcher.name | default('FALLBACK') }}",
    )
    job = _job_with_file("/data/a.nc")

    with pytest.raises(CourierError, match="default"):
        payload.to_job_spec(job)


def test_attr_filter_with_untrusted_name_is_refused(service, tmp_path) -> None:
    from courier.errors import CourierError

    payload = _bash_payload(
        service,
        tmp_path,
        "echo {{ dispatcher|attr(files[0].file) }}",
    )
    # A file name is attacker-influenced; it must never be spliced into the
    # pass-two expression.
    job = _job_with_file("/data/nope or 'INJECTED-VALUE'")

    with pytest.raises(CourierError):
        payload.to_job_spec(job)


def test_literal_defer_prefix_is_not_treated_as_a_marker() -> None:
    from courier.interfaces.payloads import _resolve_deferred_expressions

    text = "echo COURIER-DEFER: this is just script text"

    assert _resolve_deferred_expressions(text, {}, "aabbccdd") == text


def test_malformed_marker_with_correct_nonce_is_refused(service) -> None:
    from courier.errors import CourierError
    from courier.interfaces.payloads import _resolve_deferred_expressions

    bad = "\x00COURIER-DEFER:aabbccdd:A\x00"

    with pytest.raises(CourierError, match="Could not resolve"):
        _resolve_deferred_expressions(bad, {}, "aabbccdd")
