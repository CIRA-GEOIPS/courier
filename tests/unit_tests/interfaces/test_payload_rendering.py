"""Two-pass payload rendering: strict builder names, deferred dispatcher names.

Pass one (the job builder) must behave like a strict single-pass render for
every name the builder owns, and defer only the names in
``DISPATCHER_CONTEXT_NAMES``.  Pass two (the dispatcher) must evaluate nothing
but authenticated access paths rooted in those names.  These tests pin both
halves, and in particular the injection that the old "defer every undefined
name" design allowed: a data-keyed lookup miss was signed on the builder and
evaluated on the dispatcher.
"""

# cspell:ignore confg dirr binop binops endset

from __future__ import annotations

import base64
import importlib
import json
import re
import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import jinja2
import pytest
from pydantic import BaseModel

from courier.interfaces.payloads import (
    _DEFER_MARKER_RE,
    _DEFER_PREFIX,
    DISPATCHER_CONTEXT_NAMES,
    DeferredExpressionError,
    _deferred_marker,
    _DeferredValue,
    _resolve_deferred_expressions,
)
from courier.plugins.payloads.bash_payload import BashPayload
from courier.types.file import File
from courier.types.job import Job
from courier.types.payload import PayloadSpec

NONCE = "0123456789abcdef0123456789abcdef"
SCRIPT_PATH = "/scratch/courier-abc.sh"
FILTERS_AND_TESTS = "filters and tests over dispatcher-only values are not supported"


@pytest.fixture
def service() -> MagicMock:
    svc = MagicMock()
    svc.config = MagicMock(log_level="DEBUG", loki_enabled=False, namespace="ns")
    svc._broker_manager._connection = None
    return svc


def _payload(service: MagicMock, template: str) -> BashPayload:
    return BashPayload(service, {"script": template}, "payload-1")


def _job(
    metadata: dict[str, Any] | None = None,
    config: Any = None,
    files: tuple[str, ...] = ("/data/a.nc",),
) -> Job:
    return Job(
        "n",
        "job-1",
        config if config is not None else {},
        files=[File(file=Path(f), metadata=dict(metadata or {})) for f in files],
        targets=("ld",),
    )


def _local_context(**overrides: Any) -> dict[str, Any]:
    """What LocalDispatcher exposes in pass two: no ``output_dir``."""
    context: dict[str, Any] = {
        "dispatcher": {
            "name": "local_dispatcher",
            "identifier": "ld",
            "config": {
                "log_dir": "/var/log/courier",
                "hostname": "cfg-host",
                "weird key": "spaced",
                "args": ["--x", "--y"],
                "empty": [],
                "nothing": None,
            },
        },
        "script_path": SCRIPT_PATH,
        "hostname": "node-1",
    }
    context.update(overrides)
    return context


def _spec(payload: BashPayload, job: Job, builder: Any = None) -> PayloadSpec:
    return payload.to_job_spec(job, builder)


def _pass_two(
    service: MagicMock,
    spec: PayloadSpec,
    job: Job,
    context: dict[str, Any] | None = None,
) -> str:
    """Resolve *spec* the way a dispatcher does: after the wire, hydrated."""
    wire = Job.from_string(str(job))
    hydrated = BashPayload.from_job_spec(spec, service)
    assert spec.script is not None
    return hydrated.resolve_deferred_expressions(
        spec.script,
        wire,
        context if context is not None else _local_context(),
        defer_nonce=spec.defer_nonce,
    )


def _render(
    service: MagicMock,
    template: str,
    job: Job | None = None,
    context: dict[str, Any] | None = None,
) -> str:
    job = job or _job()
    return _pass_two(service, _spec(_payload(service, template), job), job, context)


def _expressions(script: str) -> list[str]:
    """Decode the deferred expressions carried by *script*'s markers."""
    return [
        base64.b64decode(match.group(2)).decode()
        for match in _DEFER_MARKER_RE.finditer(script)
    ]


# ── builder-side names: strict, exactly as a single-pass render ─────────────


class TestBuilderNamesAreStrict:
    @pytest.mark.parametrize(
        "template",
        [
            # A typo must not render '' (``rm -rf /``) or be deferred.
            "rm -rf {{ output_dirr }}/",
            "echo {{ files[0].metadata.product }}",
            "echo {{ config.missing }}",
            "echo {{ job.config.missing }}",
            "echo {{ job.nope }}",
            "echo {{ files[1].file }}",
            "echo {{ files[0].metadata.a.b }}",
            # Leaf names that collide with dispatcher names stay builder-side.
            "echo {{ config.hostname }}",
            "echo {{ config.output_dir }}",
            "echo {{ config.script_path }}",
            "echo {{ files[0].metadata.dispatcher }}",
        ],
    )
    def test_missing_builder_value_raises_undefined_at_the_builder(
        self,
        service: MagicMock,
        template: str,
    ) -> None:
        payload = _payload(service, template)

        with pytest.raises(jinja2.UndefinedError):
            payload.to_job_spec(_job())

    def test_missing_builder_value_is_not_a_deferred_error(
        self,
        service: MagicMock,
    ) -> None:
        payload = _payload(service, "echo {{ config.hostname }}")

        with pytest.raises(jinja2.UndefinedError) as info:
            payload.to_job_spec(_job())

        assert not isinstance(info.value, DeferredExpressionError)

    def test_builder_value_named_like_a_dispatcher_value_renders_its_own(
        self,
        service: MagicMock,
    ) -> None:
        rendered = _render(
            service,
            "{{ config.hostname }}|{{ hostname }}",
            _job(config={"hostname": "builder-cfg"}),
        )

        assert rendered == "builder-cfg|node-1"

    def test_single_pass_render_without_a_nonce_defers_nothing(
        self,
        service: MagicMock,
    ) -> None:
        payload = _payload(service, "echo x")

        with pytest.raises(jinja2.UndefinedError):
            payload.render_script(_job(), "echo {{ script_path }}")

    def test_invalid_nonce_is_rejected(self, service: MagicMock) -> None:
        payload = _payload(service, "echo x")

        with pytest.raises(ValueError, match="defer_nonce"):
            payload.render_script(_job(), "echo x", defer_nonce="NOT-HEX")


class TestBuilderValuesUseStockJinja:
    @pytest.mark.parametrize(
        ("template", "metadata", "config", "expected"),
        [
            ("[{{ files[0].metadata.product | default('none') }}]", {}, {}, "[none]"),
            ("[{{ files[0].metadata.product | d('none') }}]", {}, {}, "[none]"),
            (
                "[{{ files[0].metadata.product | default('x') }}]",
                {"product": "p"},
                {},
                "[p]",
            ),
            ("[{{ config.threads | default(4) }}]", {}, {}, "[4]"),
            ("[{{ config.threads | default(4) }}]", {}, {"threads": 8}, "[8]"),
            ("[{{ config.empty | default('e', true) }}]", {}, {"empty": ""}, "[e]"),
            (
                "{% if files[0].metadata.x is defined %}yes{% else %}no{% endif %}",
                {},
                {},
                "no",
            ),
            (
                "{% if files[0].metadata.x is defined %}yes{% else %}no{% endif %}",
                {"x": 1},
                {},
                "yes",
            ),
            ("{{ config.missing is undefined }}", {}, {}, "True"),
            (
                "{% if config.flag %}on{% else %}off{% endif %}",
                {},
                {"flag": True},
                "on",
            ),
            ("{{ config.get('threads', 2) }}", {}, {}, "2"),
            (
                "{{ config.modes[files[0].metadata.band] }}",
                {"band": "a"},
                {"modes": {"a": "b"}},
                "b",
            ),
            (
                "{{ config.modes.get(files[0].metadata.band, 'none') }}",
                {"band": "zz"},
                {"modes": {"a": "b"}},
                "none",
            ),
            ("{{ files | map(attribute='file') | join(' ') }}", {}, {}, "/data/a.nc"),
            ("{{ files | length }}", {}, {}, "1"),
        ],
    )
    def test_builder_side_template(
        self,
        service: MagicMock,
        template: str,
        metadata: dict[str, Any],
        config: dict[str, Any],
        expected: str,
    ) -> None:
        job = _job(metadata, config)
        spec = _spec(_payload(service, template), job)

        assert spec.script == expected
        assert _DEFER_PREFIX not in spec.script
        assert _pass_two(service, spec, job) == expected


# ── data-keyed lookups: a miss fails at the builder, nothing is signed ──────


class TestDataKeyedLookups:
    TEMPLATE = "echo {{ config.modes[files[0].metadata.band] }}"

    def test_injection_repro_fails_at_the_builder_and_runs_nothing(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        """The exact review repro: the lookup miss used to be signed and run."""
        target = tmp_path / "pwned"
        job = _job({"band": f"'$(touch {target})'"}, {"modes": {"a": "b"}})
        payload = _payload(service, self.TEMPLATE)

        with pytest.raises(jinja2.UndefinedError):
            payload.to_job_spec(job)

        assert not target.exists()

    @pytest.mark.parametrize(
        "band",
        [
            "dispatcher.config",
            "hostname",
            "('x'*300000000)|length",
            "{{ 7*7 }}",
            "script_path",
        ],
    )
    def test_any_miss_raises_undefined_instead_of_deferring(
        self,
        service: MagicMock,
        band: str,
    ) -> None:
        payload = _payload(service, self.TEMPLATE)

        with pytest.raises(jinja2.UndefinedError):
            payload.to_job_spec(_job({"band": band}, {"modes": {"a": "b"}}))

    def test_unsafe_key_is_refused_by_the_sandbox_at_the_builder(
        self,
        service: MagicMock,
    ) -> None:
        payload = _payload(service, self.TEMPLATE)

        with pytest.raises(jinja2.exceptions.SecurityError):
            payload.to_job_spec(_job({"band": "__class__"}, {"modes": {"a": "b"}}))

    def test_default_over_a_miss_renders_the_fallback_and_signs_nothing(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        target = tmp_path / "pwned"
        job = _job({"band": f"'$(touch {target})'"}, {"modes": {"a": "b"}})
        payload = _payload(
            service,
            "echo {{ config.modes[files[0].metadata.band] | default('none') }}",
        )

        spec = payload.to_job_spec(job)
        resolved = _pass_two(service, spec, job)
        subprocess.run(["bash", "-c", resolved], check=True, capture_output=True)  # noqa: S603, S607

        assert spec.script == "echo none"
        assert _DEFER_PREFIX not in spec.script
        assert resolved == "echo none"
        assert not target.exists()

    def test_data_rendered_as_a_leaf_stays_literal_text(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        target = tmp_path / "pwned"
        band = f"{{{{ hostname }}}} $(touch {target})"
        job = _job({"band": band})

        resolved = _render(service, "echo '{{ files[0].metadata.band }}'", job)
        subprocess.run(["bash", "-c", resolved], check=True, capture_output=True)  # noqa: S603, S607

        assert resolved == f"echo '{band}'"
        assert not target.exists()


# ── dispatcher-only values: leaf interpolation ──────────────────────────────


class TestDispatcherOnlyLeafInterpolation:
    @pytest.mark.parametrize(
        ("template", "expected"),
        [
            ("{{ dispatcher.identifier }}", "ld"),
            ("{{ dispatcher.config.log_dir }}", "/var/log/courier"),
            ("{{ dispatcher.config['log_dir'] }}", "/var/log/courier"),
            ('{{ dispatcher["config"]["log_dir"] }}', "/var/log/courier"),
            ("{{ dispatcher.config['weird key'] }}", "spaced"),
            ("{{ dispatcher.config.args[0] }}", "--x"),
            ("{{ dispatcher.config.args.1 }}", "--y"),
            ("{{ dispatcher.config.args[-1] }}", "--y"),
            ("{{ script_path }}", SCRIPT_PATH),
            ("{{ hostname ~ '-' ~ script_path }}", f"node-1-{SCRIPT_PATH}"),
            ("{{ files[0].file ~ '@' ~ hostname }}", "/data/a.nc@node-1"),
            ("{{ [script_path, 'x'] | join(' ') }}", f"{SCRIPT_PATH} x"),
            ("{% set p = script_path %}{{ p }}", SCRIPT_PATH),
            (
                "{% for f in files %}{{ f.file }}@{{ hostname }};{% endfor %}",
                "/data/a.nc@node-1;",
            ),
            (
                "{% macro at(x) %}[{{ x }}]{% endmacro %}{{ at(script_path) }}",
                f"[{SCRIPT_PATH}]",
            ),
            ("{{ script_path if files else 'none' }}", SCRIPT_PATH),
            ("{{ '%s' % hostname }}", "node-1"),
            ("{{ '%s/%s' % (hostname, 'x') }}", "node-1/x"),
            ("{{ '%(h)s' % {'h': hostname} }}", "node-1"),
            ("{{ '{}@{}'.format(hostname, files[0].file) }}", "node-1@/data/a.nc"),
            ("{{ '{h}'.format_map({'h': hostname}) }}", "node-1"),
            (
                "{% macro at(x) %}[{{ x }}]{% endmacro %}{{ at(hostname ~ '/x') }}",
                "[node-1/x]",
            ),
            ("{{ [hostname ~ '/a', 'b'] | join(' ') }}", "node-1/a b"),
            ("{% for c in (hostname ~ '') %}{{ c }}{% endfor %}", "node-1"),
            # The full access path is carried, so leaf names never collide.
            ("{{ dispatcher.config.hostname }}|{{ hostname }}", "cfg-host|node-1"),
        ],
    )
    def test_leaf_interpolation_resolves_on_the_dispatcher(
        self,
        service: MagicMock,
        template: str,
        expected: str,
    ) -> None:
        assert _render(service, template) == expected

    @pytest.mark.parametrize(
        ("template", "expression"),
        [
            ("{{ dispatcher.config.log_dir }}", "dispatcher.config.log_dir"),
            ("{{ dispatcher.config['k'] }}", "dispatcher.config['k']"),
            ("{{ dispatcher.config.args[-2] }}", "dispatcher.config.args[-2]"),
            ("{{ dispatcher['a']['b'].c }}", "dispatcher['a']['b'].c"),
            ("{{ output_dir }}", "output_dir"),
        ],
    )
    def test_marker_carries_the_full_access_path(
        self,
        service: MagicMock,
        template: str,
        expression: str,
    ) -> None:
        spec = _spec(_payload(service, template), _job())

        assert spec.script is not None
        assert _expressions(spec.script) == [expression]

    def test_data_driven_key_is_spliced_as_a_literal(
        self,
        service: MagicMock,
    ) -> None:
        key = "a'b\"c ~ hostname ~ \\"
        context = _local_context()
        context["dispatcher"]["config"][key] = "found"
        job = _job({"k": key})

        rendered = _render(
            service, "{{ dispatcher.config[files[0].metadata.k] }}", job, context
        )

        assert rendered == "found"

    def test_data_driven_key_miss_is_not_evaluated(
        self,
        service: MagicMock,
    ) -> None:
        job = _job({"k": "' ~ hostname ~ '"})

        with pytest.raises(DeferredExpressionError, match="not defined by this"):
            _render(service, "{{ dispatcher.config[files[0].metadata.k] }}", job)

    def test_every_dispatcher_name_is_deferred(self, service: MagicMock) -> None:
        template = " ".join(
            f"{{{{ {name} }}}}" for name in sorted(DISPATCHER_CONTEXT_NAMES)
        )
        spec = _spec(_payload(service, template), _job())

        assert spec.script is not None
        assert _expressions(spec.script) == sorted(DISPATCHER_CONTEXT_NAMES)


# ── dispatcher-only values: everything else raises at the builder ───────────


class TestUnsupportedUseOfDispatcherValues:
    @pytest.mark.parametrize(
        "template",
        [
            "{{ script_path | lower }}",
            "{{ script_path | upper }}",
            "{{ output_dir | replace('/scratch', '/archive') }}",
            "{{ dispatcher.config.log_dir | e }}",
            "{{ dispatcher.config.log_dir | escape }}",
            "{{ dispatcher.name | default('FALLBACK') }}",
            "{{ dispatcher.config.nope | d('x') }}",
            "{{ script_path | string }}",
            "{{ script_path | length }}",
            "{{ script_path | trim }}",
            "{{ script_path | center(40) }}",
            "{{ script_path | wordcount }}",
            "{{ script_path | pprint }}",
            "{{ script_path | tojson }}",
            "{{ script_path | attr('x') }}",
            "{{ script_path | first }}",
            "{{ dispatcher | attr(files[0].file) }}",
            "{{ script_path is defined }}",
            "{{ script_path is undefined }}",
            "{{ script_path is string }}",
            "{{ script_path is none }}",
            "{% if script_path is defined %}a{% else %}b{% endif %}",
            "{% if dispatcher.config.x is defined %}a{% endif %}",
            "{{ [script_path] | map('upper') | join }}",
            "{{ [script_path] | select('defined') | join }}",
        ],
    )
    def test_filters_and_tests_raise(
        self,
        service: MagicMock,
        template: str,
    ) -> None:
        payload = _payload(service, template)

        with pytest.raises(DeferredExpressionError, match=FILTERS_AND_TESTS):
            payload.to_job_spec(_job())

    @pytest.mark.parametrize(
        ("template", "message"),
        [
            ("{% if dispatcher.name %}y{% endif %}", "conditional"),
            ("{{ script_path or 'x' }}", "conditional"),
            ("{{ not script_path }}", "conditional"),
            ("{% for x in dispatcher.config %}{% endfor %}", "loop"),
            ("{{ script_path + 'x' }}", "arithmetic"),
            ("{{ dispatcher.x * 2 }}", "arithmetic"),
            ("{{ dispatcher.x == 1 }}", "comparison"),
            ("{{ dispatcher.x < 1 }}", "comparison"),
            ("{{ 'x' in script_path }}", "'in' test"),
            ("{{ dispatcher.items() }}", "called"),
            ("{{ dispatcher.config.get('k') }}", "called"),
            ("{{ range(script_path) | list }}", "converted to a number"),
            ("{{ lipsum(n=script_path) }}", "converted to a number"),
            ("{{ 'abc'.replace('b', script_path) }}", "string method 'replace'"),
            ("{{ 'a/b'.split(hostname) }}", "string method 'split'"),
            ("{{ config.table.get(script_path, 'x') }}", "lookup key"),
            ("{{ config.table.pop(hostname, 'x') }}", "lookup key"),
            ("{{ config.table.setdefault(hostname, 'x') }}", "lookup key"),
            ("{{ '%80s' % hostname }}", "'%' conversion"),
            ("{{ '%(h)9s' % {'h': hostname} }}", "'%' conversion"),
            # Python matches nested parentheses in a mapping key, so each of
            # these pads the value; none may pass as a plain %s.
            ("{{ '%(a(b)s)200s' % {'a(b)s': hostname} }}", "'%' conversion"),
            ("{{ '%(a(b))5s' % {'a(b)': hostname} }}", "'%' conversion"),
            ("{{ '%*s' % (80, hostname) }}", "'%' conversion"),
            ("{{ '{!s:>80}'.format(hostname) }}", "format specification"),
            ("{{ '{h:>9}'.format_map({'h': hostname}) }}", "format specification"),
            ("{{ script_path[1:] }}", "string or integer key"),
            ("{{ dispatcher[none] }}", "string or integer key"),
            ("{{ dispatcher[true] }}", "string or integer key"),
            ("{{ dispatcher[1.5] }}", "string or integer key"),
            ("{{ dispatcher[script_path] }}", "string or integer key"),
            ("{{ [script_path] }}", "repr"),
            ("{{ {'a': hostname} }}", "repr"),
            ("{{ config.table[script_path] }}", "lookup key"),
            ("{{ '{:>9}'.format(script_path) }}", "formatted"),
        ],
    )
    def test_other_constructs_raise(
        self,
        service: MagicMock,
        template: str,
        message: str,
    ) -> None:
        payload = _payload(service, template)

        with pytest.raises(DeferredExpressionError, match=message):
            payload.to_job_spec(_job(config={"table": {}}))


# ── calls that only pass a dispatcher-only value through ────────────────────


class TestPassThroughCalls:
    """A call that stores or returns a value it was given computes nothing.

    These rendered at the builder before the call guard was added; each must
    render end to end (pass one, then pass two on a hydrated payload).
    """

    @pytest.mark.parametrize(
        ("template", "expected"),
        [
            ("{{ namespace(p=hostname).p }}", "node-1"),
            ("{% set ns = namespace(p=script_path) %}{{ ns.p }}", SCRIPT_PATH),
            (
                "{% set ns = namespace(p='') %}{% set ns.p = hostname %}{{ ns.p }}",
                "node-1",
            ),
            ("{{ dict(p=hostname).p }}", "node-1"),
            ("{{ dict(p=hostname ~ '/x')['p'] }}", "node-1/x"),
            (
                "{% set c = cycler(hostname, 'x') %}"
                "{{ c.next() }}-{{ c.next() }}-{{ c.next() }}",
                "node-1-x-node-1",
            ),
            (
                "{% for f in files %}{{ loop.cycle(hostname, 'b') }};{% endfor %}",
                "node-1;b;",
            ),
            ("{{ config.get('missing', hostname) }}", "node-1"),
            (
                "{{ config.get('missing', dispatcher.config.log_dir ~ '/x') }}",
                "/var/log/courier/x",
            ),
            (
                "{% set l = [] %}{% set _ = l.append(hostname) %}"
                "{% set _ = l.append(script_path ~ '!') %}{{ l | join(' ') }}",
                f"node-1 {SCRIPT_PATH}!",
            ),
            ("{% set j = joiner(hostname) %}a {{ j() }}b {{ j() }}c", "a b node-1c"),
            ("{{ ' '.join([hostname ~ '', 'x']) }}", "node-1 x"),
            (
                "{% macro m(x) %}<{{ x }}>{% endmacro %}{{ m(dict(p=hostname).p) }}",
                "<node-1>",
            ),
        ],
    )
    def test_pass_through_renders_end_to_end(
        self,
        service: MagicMock,
        template: str,
        expected: str,
    ) -> None:
        job = _job(files=("/data/a.nc", "/data/b.nc"))

        assert _render(service, template, job) == expected

    def test_data_keyed_get_with_a_dispatcher_default_runs_nothing(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        """The default is passed through; the data-chosen key stays data."""
        target = tmp_path / "pwned"
        job = _job({"band": f"'$(touch {target})'"}, {"modes": {"a": "b"}})
        template = "echo {{ config.modes.get(files[0].metadata.band, hostname) }}"

        resolved = _render(service, template, job)
        subprocess.run(["bash", "-c", resolved], check=True, capture_output=True)  # noqa: S603, S607

        assert resolved == "echo node-1"
        assert not target.exists()

    @pytest.mark.parametrize(
        ("template", "error"),
        [
            # The protections the guard exists for still hold.
            ("echo {{ config.modes[files[0].metadata.band] }}", jinja2.UndefinedError),
            ("echo {{ output_dirr }}", jinja2.UndefinedError),
            ("echo {{ namespace(p=hostname).q }}", jinja2.UndefinedError),
            ("echo {{ namespace(p=hostname).p | upper }}", DeferredExpressionError),
            ("echo {{ dict(p=hostname).p is defined }}", DeferredExpressionError),
        ],
    )
    def test_original_protections_still_hold(
        self,
        service: MagicMock,
        tmp_path: Path,
        template: str,
        error: type[Exception],
    ) -> None:
        job = _job({"band": f"'$(touch {tmp_path / 'x'})'"}, {"modes": {"a": "b"}})

        with pytest.raises(error):
            _payload(service, template).to_job_spec(job)

        assert not (tmp_path / "x").exists()

    def test_default_over_builder_data_still_works(
        self,
        service: MagicMock,
    ) -> None:
        template = "{{ files[0].metadata.p | default(namespace(v='none').v) }}"

        assert _render(service, template) == "none"


class TestIsPlainPrintf:
    @pytest.mark.parametrize(
        ("template", "plain"),
        [
            ("%s", True),
            ("%s/%s %%", True),
            ("%(h)s", True),
            ("%%(h)5s", True),
            ("%5s", False),
            ("%-s", False),
            ("%r", False),
            ("%*s", False),
            ("%(h)5s", False),
            ("%(a(b))5s", False),
            ("%(a(b)s)200s", False),
            ("%(a(b)s", False),
        ],
    )
    def test_conversions(self, template: str, *, plain: bool) -> None:
        from courier.interfaces.payloads import _is_plain_printf  # noqa: PLC0415

        assert _is_plain_printf(template) is plain


# ── strings built from dispatcher-only values with ``~`` ───────────────────

STRING_FROM_DISPATCHER_VALUE = "a string built from a dispatcher-only value"


class TestStringsBuiltFromDispatcherValues:
    """A ``~`` concatenation holds a marker: it is as unknown as the value."""

    @pytest.mark.parametrize(
        "template",
        [
            "{% filter upper %}{{ script_path }}{% endfilter %}",
            "{% filter lower %}{{ script_path }}{% endfilter %}",
            "{% set s | upper %}{{ hostname }}{% endset %}{{ s }}",
            "{{ (script_path ~ '') | upper }}",
            "{{ (output_dir ~ '') | e }}",
            "{{ (hostname ~ ' a') | replace(' ', '_') }}",
            "{{ ('/scratch' ~ output_dir) | replace('/scratch', '/archive') }}",
            "{{ (hostname ~ '') | replace('\\x00', '') }}",
            "{{ (hostname ~ '') | replace('COURIER', 'X') }}",
            "{{ (hostname ~ '') | length }}",
            "{{ (hostname ~ '') | center(80) }}",
            "{{ (hostname ~ '') | default('x') }}",
            "{{ (script_path ~ '') | truncate(12, true, '') }}",
            "{{ (hostname ~ '') is defined }}",
            "{{ (hostname ~ '') is string }}",
            "{{ files | map(attribute='file') | join(' ' ~ hostname) }}",
            "{{ [hostname ~ ''] | map('upper') | join }}",
        ],
    )
    def test_filters_and_tests_raise(
        self,
        service: MagicMock,
        template: str,
    ) -> None:
        payload = _payload(service, template)

        with pytest.raises(DeferredExpressionError, match=FILTERS_AND_TESTS):
            payload.to_job_spec(_job())

    @pytest.mark.parametrize(
        ("template", "message"),
        [
            ("{{ (script_path ~ '').replace('c', 'C') }}", "attribute 'replace'"),
            ("{{ (script_path ~ '').upper() }}", "attribute 'upper'"),
            ("{{ (hostname ~ '').startswith('n') }}", "attribute 'startswith'"),
            ("{{ (hostname ~ '')[0] }}", "subscripted"),
            ("{{ '%80s' % (hostname ~ '') }}", "'%' conversion"),
            ("{{ '%(a(b)s)200s' % {'a(b)s': hostname ~ ''} }}", "'%' conversion"),
            ("{{ '{:>80}'.format(hostname ~ '') }}", "format specification"),
            ("{{ 'a/b'.split(hostname ~ '') }}", "string method 'split'"),
            ("{{ 'a/b'.startswith(hostname ~ '') }}", "string method 'startswith'"),
            ("{{ ' '.join(hostname ~ '') }}", "string method 'join'"),
            ("{{ 'x'.replace('x', hostname ~ '') }}", "string method 'replace'"),
            ("{{ config.get(hostname ~ '', 'x') }}", "lookup key"),
        ],
    )
    def test_methods_subscripts_and_calls_raise(
        self,
        service: MagicMock,
        template: str,
        message: str,
    ) -> None:
        payload = _payload(service, template)

        with pytest.raises(DeferredExpressionError, match=STRING_FROM_DISPATCHER_VALUE):
            payload.to_job_spec(_job())
        with pytest.raises(DeferredExpressionError, match=message):
            payload.to_job_spec(_job())

    @pytest.mark.parametrize(
        ("template", "message"),
        [
            # Escaped into another form: the nonce survives outside a marker.
            (
                "{{ {'log': dispatcher.config.log_dir ~ '/run.log'} | tojson }}",
                "altered or escaped",
            ),
            ("{{ {'a': hostname ~ ' b'} | urlencode }}", "altered or escaped"),
            ("{{ [hostname ~ ''] | pprint }}", "altered or escaped"),
            ("{{ [hostname ~ ''] | string }}", "altered or escaped"),
            # A pass-through call does not launder a marker: what is done to
            # its result is checked like anything else.
            ("{{ dict(p=hostname ~ '/x') | tojson }}", "altered or escaped"),
            ("{{ namespace(p=hostname ~ '') | string }}", "altered or escaped"),
            ("{{ dict(p=script_path ~ '')['p'][:10] }}", "NUL character"),
            (
                "{% for c in (hostname ~ '') %}"
                "{{ c if c != '\\x00' else '' }}{% endfor %}",
                "altered or escaped",
            ),
            # Rewritten inside the base64 path: intact, but not what was emitted.
            (
                "{% for c in (hostname ~ '') %}"
                "{{ 'H' if c == 'G' else c }}{% endfor %}",
                "altered or escaped",
            ),
            # Truncated: every NUL must belong to an intact marker.
            ("{{ (script_path ~ '')[:10] }}", "NUL character"),
            (
                "{% for c in (hostname ~ '') %}{{ c if c != 'C' else '' }}{% endfor %}",
                "NUL character",
            ),
        ],
    )
    def test_altered_or_escaped_markers_are_refused_at_the_builder(
        self,
        service: MagicMock,
        template: str,
        message: str,
    ) -> None:
        payload = _payload(service, template)

        with pytest.raises(DeferredExpressionError, match=message):
            payload.to_job_spec(_job())


class TestDeferredValue:
    @pytest.mark.parametrize(
        ("key", "expression"),
        [
            ("k", "dispatcher['k']"),
            ("it's", 'dispatcher["it\'s"]'),
            (3, "dispatcher[3]"),
            (-1, "dispatcher[-1]"),
        ],
    )
    def test_string_and_integer_keys_are_spliced_as_repr(
        self,
        key: object,
        expression: str,
    ) -> None:
        value = _DeferredValue(NONCE, "dispatcher")[key]

        assert _expressions(str(value)) == [expression]

    @pytest.mark.parametrize(
        "key",
        [True, False, None, 1.5, slice(1, None), ("a",), b"k"],
        ids=repr,
    )
    def test_other_keys_are_refused(self, key: object) -> None:
        with pytest.raises(DeferredExpressionError, match="string or integer key"):
            _DeferredValue(NONCE, "dispatcher")[key]

    def test_deferred_key_is_refused(self) -> None:
        with pytest.raises(DeferredExpressionError, match="string or integer key"):
            _DeferredValue(NONCE, "dispatcher")[_DeferredValue(NONCE, "hostname")]

    @pytest.mark.parametrize(
        "name",
        [
            "__html__",
            "__class_getitem__",
            "_nonce_probe",
            "jinja_pass_arg",
            "alters_data",
        ],
    )
    def test_protocol_and_private_attributes_are_missing(self, name: str) -> None:
        assert not hasattr(_DeferredValue(NONCE, "dispatcher"), name)

    def test_non_identifier_attribute_is_refused(self) -> None:
        with pytest.raises(DeferredExpressionError, match="attribute name"):
            getattr(_DeferredValue(NONCE, "dispatcher"), "a' ~ hostname ~ 'b")

    def test_attribute_access_extends_the_path(self) -> None:
        value = _DeferredValue(NONCE, "dispatcher").config.log_dir

        assert _expressions(str(value)) == ["dispatcher.config.log_dir"]
        assert str(value).startswith("\x00" + _DEFER_PREFIX + NONCE + ":")


# ── pass two: only authenticated, allow-listed access paths ─────────────────


def _full_context() -> dict[str, Any]:
    """A pass-two context that also holds builder names, to prove they stay out."""
    return {
        **_local_context(),
        "files": [{"file": "/data/a.nc"}],
        "config": {"modes": {"a": "b"}},
        "job": {"identifier": "job-1"},
        "builder": {"identifier": "b"},
    }


class TestPassTwo:
    @pytest.mark.parametrize(
        "expression",
        [
            "files[0].file",
            "config.modes",
            "job.identifier",
            "builder.identifier",
            "lipsum",
            "''.__class__",
            "dispatcher.config ~ 'x'",
            "dispatcher.items()",
            "dispatcher[hostname]",
            "dispatcher.config.log_dir | upper",
            "dispatcher.config[1:2]",
            "('x' * 300000000) | length",
            "dispatcher.identifier == 'ld'",
        ],
    )
    def test_only_access_paths_rooted_in_dispatcher_names_are_evaluated(
        self,
        expression: str,
    ) -> None:
        text = "echo " + _deferred_marker(NONCE, expression)

        with pytest.raises(DeferredExpressionError, match="refusing to evaluate"):
            _resolve_deferred_expressions(text, _full_context(), NONCE)

    @pytest.mark.parametrize(
        ("expression", "expected"),
        [
            ("dispatcher.identifier", "ld"),
            ("dispatcher['config']['log_dir']", "/var/log/courier"),
            ("dispatcher.config.args[-1]", "--y"),
            ("hostname", "node-1"),
            ("script_path", SCRIPT_PATH),
        ],
    )
    def test_allowed_access_paths_are_evaluated(
        self,
        expression: str,
        expected: str,
    ) -> None:
        text = "echo " + _deferred_marker(NONCE, expression)

        assert _resolve_deferred_expressions(text, _full_context(), NONCE) == (
            f"echo {expected}"
        )

    @pytest.mark.parametrize(
        ("template", "expression"),
        [
            ("{{ output_dir }}", "output_dir"),
            ("{{ dispatcher.confg }}", "dispatcher.confg"),
            ("{{ dispatcher.config.args[5] }}", "dispatcher.config.args[5]"),
            ("{{ dispatcher.config['nope'] }}", "dispatcher.config['nope']"),
            ("{{ dispatcher.confg.deeper }}", "dispatcher.confg.deeper"),
        ],
    )
    def test_value_this_dispatcher_does_not_define_raises(
        self,
        service: MagicMock,
        template: str,
        expression: str,
    ) -> None:
        with pytest.raises(
            DeferredExpressionError, match="not defined by this"
        ) as info:
            _render(service, template)

        assert repr(expression) in str(info.value)

    def test_output_dir_resolves_where_the_dispatcher_defines_it(
        self,
        service: MagicMock,
    ) -> None:
        context = _local_context(output_dir="/shared/slurm")

        assert _render(service, "cd {{ output_dir }}", context=context) == (
            "cd /shared/slurm"
        )

    @pytest.mark.parametrize(
        ("value", "expected"),
        [(None, ""), ([], ""), ([1], "[1]"), ("", ""), (0, "0"), (False, "False")],
        ids=repr,
    )
    def test_finalize_is_the_same_in_both_passes(
        self,
        service: MagicMock,
        value: object,
        expected: str,
    ) -> None:
        context = _local_context()
        context["dispatcher"]["config"]["v"] = value
        job = _job(config={"v": value})

        rendered = _render(
            service, "[{{ config.v }}|{{ dispatcher.config.v }}]", job, context
        )

        assert rendered == f"[{expected}|{expected}]"

    def test_marker_with_another_nonce_fails_authentication(self) -> None:
        text = "echo " + _deferred_marker("deadbeef", "dispatcher.identifier")

        with pytest.raises(DeferredExpressionError, match="authentication"):
            _resolve_deferred_expressions(text, _local_context(), NONCE)

    def test_empty_nonce_authenticates_nothing(self) -> None:
        text = "echo " + _deferred_marker(NONCE, "dispatcher.identifier")

        with pytest.raises(DeferredExpressionError, match="authentication"):
            _resolve_deferred_expressions(text, _local_context(), "")

    def test_altered_marker_is_reported_as_altered(self) -> None:
        text = "echo " + _deferred_marker(NONCE, "dispatcher.identifier").upper()

        with pytest.raises(DeferredExpressionError, match="altered"):
            _resolve_deferred_expressions(text, _local_context(), NONCE)

    def test_each_distinct_marker_is_evaluated_once(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        payloads_module = importlib.import_module("courier.interfaces.payloads")

        calls: list[str] = []
        evaluate = payloads_module._evaluate_deferred

        def counting(expression: str, context: Any) -> str:
            calls.append(expression)
            return evaluate(expression, context)

        monkeypatch.setattr(payloads_module, "_evaluate_deferred", counting)
        host = _deferred_marker(NONCE, "hostname")
        path = _deferred_marker(NONCE, "script_path")
        text = f"{host} {path}\n" * 500

        resolved = _resolve_deferred_expressions(text, _local_context(), NONCE)

        assert resolved == f"node-1 {SCRIPT_PATH}\n" * 500
        assert sorted(calls) == ["hostname", "script_path"]

    def test_text_outside_markers_is_never_parsed(self) -> None:
        text = "echo {{ 6*7 }} " + _deferred_marker(NONCE, "hostname")

        assert _resolve_deferred_expressions(text, _local_context(), NONCE) == (
            "echo {{ 6*7 }} node-1"
        )


# ── nonce, builder context and wire normalization ───────────────────────────


class TestNonceAndContext:
    def test_every_spec_gets_a_fresh_random_nonce(self, service: MagicMock) -> None:
        payload = _payload(service, "echo {{ hostname }}")

        first = payload.to_job_spec(_job())
        second = payload.to_job_spec(_job())

        assert re.fullmatch(r"[0-9a-f]{32}", first.defer_nonce)
        assert re.fullmatch(r"[0-9a-f]{32}", second.defer_nonce)
        assert first.defer_nonce != second.defer_nonce
        assert first.script is not None
        assert [m.group(1) for m in _DEFER_MARKER_RE.finditer(first.script)] == [
            first.defer_nonce,
        ]

    def test_builder_context_is_rendered(self, service: MagicMock) -> None:
        builder = MagicMock()
        builder.name = "filter_and_group"
        builder.identifier = "just-pass"
        payload = _payload(
            service,
            "{{ builder.name }} {{ builder.identifier }} {{ builder.targets | join(',') }}",
        )

        spec = payload.to_job_spec(_job(), builder)

        assert spec.script == "filter_and_group just-pass ld"

    def test_marker_shaped_job_data_is_refused_at_the_builder(
        self,
        service: MagicMock,
    ) -> None:
        forged = _deferred_marker("deadbeef", "dispatcher.config")
        payload = _payload(service, "echo {{ files[0].metadata.title }}")

        with pytest.raises(DeferredExpressionError, match="did not emit"):
            payload.to_job_spec(_job({"title": forged}))

    def test_nul_in_job_data_is_kept_when_no_marker_was_emitted(
        self,
        service: MagicMock,
    ) -> None:
        """With no dispatcher-only value there is no marker to truncate."""
        job = _job({"title": "a\x00b"})
        spec = _spec(_payload(service, "echo {{ files[0].metadata.title }}"), job)

        assert spec.script == "echo a\x00b"
        assert _pass_two(service, spec, job) == "echo a\x00b"

    def test_nul_in_job_data_is_refused_next_to_a_dispatcher_value(
        self,
        service: MagicMock,
    ) -> None:
        """A NUL outside a marker is how a truncated marker shows."""
        payload = _payload(
            service,
            "echo {{ files[0].metadata.title }} {{ hostname }}",
        )

        with pytest.raises(DeferredExpressionError, match="NUL character"):
            payload.to_job_spec(_job({"title": "a\x00b"}))

    def test_nonce_outside_a_marker_is_refused_even_without_markers(
        self,
        service: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Only the NUL check depends on markers; the nonce check never does."""
        monkeypatch.setattr(
            importlib.import_module("courier.interfaces.payloads").secrets,
            "token_hex",
            lambda _n: NONCE,
        )
        payload = _payload(service, "echo {{ files[0].metadata.title }}")

        with pytest.raises(DeferredExpressionError, match="altered or escaped"):
            payload.to_job_spec(_job({"title": f"x\x00{NONCE}"}))

    def test_literal_prefix_without_a_marker_is_plain_data(
        self,
        service: MagicMock,
    ) -> None:
        job = _job({"title": "COURIER-DEFER:not-a-marker"})

        rendered = _render(service, "echo {{ files[0].metadata.title }}", job)

        assert rendered == "echo COURIER-DEFER:not-a-marker"


class _GroupConfig(BaseModel):
    threads: int = 4
    modes: dict[str, str] = {"a": "b"}  # noqa: RUF012
    tags: tuple[str, ...] = ("x", "y")


class TestConfigIsTheWireForm:
    @pytest.mark.parametrize(
        "template",
        [
            "{{ config | tojson }}",
            "{{ job.config | tojson }}",
            "{{ config.items() | list | length }}",
            "{{ config.tags }}",
            "{{ config.tags | join(',') }}",
            "{{ config.modes.a }} {{ config.threads }}",
            "{{ config is mapping }}",
        ],
    )
    def test_pass_one_sees_the_dict_a_dispatcher_sees(
        self,
        service: MagicMock,
        template: str,
    ) -> None:
        payload = _payload(service, template)
        model_job = _job(config=_GroupConfig())
        wire_job = Job.from_string(str(model_job))

        assert isinstance(wire_job.config, dict)
        assert payload.to_job_spec(model_job).script == (
            payload.to_job_spec(wire_job).script
        )

    def test_tojson_renders_the_model_as_json(self, service: MagicMock) -> None:
        model = _GroupConfig()
        spec = _payload(service, "{{ config | tojson }}").to_job_spec(
            _job(config=model),
        )

        assert spec.script is not None
        assert json.loads(spec.script) == {
            "modes": model.modes,
            "tags": list(model.tags),
            "threads": model.threads,
        }


# ── environments and single-pass renders ────────────────────────────────────


class TestEnvironments:
    def test_wrapped_filters_and_tests_keep_their_jinja_pass_markers(self) -> None:
        from jinja2.sandbox import SandboxedEnvironment  # noqa: PLC0415
        from jinja2.utils import _PassArg  # noqa: PLC0415

        from courier.interfaces.payloads import _PASS_ONE_ENV  # noqa: PLC0415

        stock = SandboxedEnvironment()

        assert set(_PASS_ONE_ENV.filters) == set(stock.filters)
        assert set(_PASS_ONE_ENV.tests) == set(stock.tests)
        for table, stock_table in (
            (_PASS_ONE_ENV.filters, stock.filters),
            (_PASS_ONE_ENV.tests, stock.tests),
        ):
            for name, func in stock_table.items():
                assert _PassArg.from_obj(table[name]) is _PassArg.from_obj(func), name

    def test_the_percent_operator_is_intercepted(self) -> None:
        """Jinja routes ``%`` through ``call_binop`` only when it is listed.

        Without it a padded ``'%80s' % hostname`` would bypass the guard.
        """
        from courier.interfaces.payloads import _PASS_ONE_ENV  # noqa: PLC0415

        assert "%" in _PASS_ONE_ENV.intercepted_binops

    def test_single_pass_render_uses_the_callers_context(
        self,
        service: MagicMock,
    ) -> None:
        payload = _payload(service, "echo x")

        rendered = payload.render_script(
            _job(),
            "{{ files[0].file }} {{ script_path }} {{ dispatcher.identifier }}",
            {"script_path": SCRIPT_PATH, "dispatcher": {"identifier": "ld"}},
        )

        assert rendered == f"/data/a.nc {SCRIPT_PATH} ld"

    def test_finalize_never_compares_values(self, service: MagicMock) -> None:
        class NoEquality:
            def __eq__(self, other: object) -> bool:
                raise AssertionError("finalize compared a value")

            __hash__ = object.__hash__

            def __str__(self) -> str:
                return "no-equality"

        payload = _payload(service, "echo x")

        assert payload.render_script(_job(), "{{ v }}", {"v": NoEquality()}) == (
            "no-equality"
        )
