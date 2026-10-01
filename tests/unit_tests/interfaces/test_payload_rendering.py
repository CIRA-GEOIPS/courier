"""Two-pass payload rendering: strict builder names, bare dispatcher values.

Pass one (the job builder) must behave like a strict single-pass render for
every name the builder owns, and defer only the names in
``DISPATCHER_CONTEXT_NAMES``.  A template may use those only as bare values;
anything else is rejected when the template is checked -- when the payload is
built, and whenever ``render_template`` renders pass one.  Pass two (the
dispatcher) must evaluate nothing: it looks up the access path of each marker
carrying the job's nonce in its own context and leaves every other character
alone.  These tests pin both halves, and in particular the injection that the
old "defer every undefined name" design allowed: a data-keyed lookup miss was
signed on the builder and evaluated on the dispatcher.
"""

# cspell:ignore confg dirr cycler joiner endblock binops eostname hostnames

from __future__ import annotations

import base64
import json
import re
import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import jinja2
import pytest
from jinja2.sandbox import SandboxedEnvironment
from pydantic import BaseModel

from courier.interfaces.payloads import (
    _DEFER_PREFIX,
    _ENV,
    DISPATCHER_CONTEXT_NAMES,
    DeferredExpressionError,
    _deferred_marker,
    _DeferredValue,
    _resolve_deferred_expressions,
)
from courier.plugins.payloads.bash_payload import BashPayload
from courier.types.datum import Datum
from courier.types.job import Job
from courier.types.payload import PayloadSpec

NONCE = "0123456789abcdef0123456789abcdef"
SCRIPT_PATH = "/scratch/courier-abc.sh"
#: Any marker, whatever its nonce: ``(nonce, encoded path)``.
MARKER_RE = re.compile(r"\x00COURIER-DEFER:([0-9a-f]+):([^\x00]*)\x00")


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
        files=[Datum(file=Path(f), metadata=dict(metadata or {})) for f in files],
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
    job.payload = spec
    wire = Job.from_string(str(job))
    wire_spec = wire.payload
    assert wire_spec is not None
    assert wire_spec.script is not None
    hydrated = BashPayload.from_job_spec(wire_spec, service)
    return hydrated.resolve_deferred_expressions(
        wire_spec.script,
        wire,
        context if context is not None else _local_context(),
        defer_nonce=wire_spec.defer_nonce,
    )


def _render(
    service: MagicMock,
    template: str,
    job: Job | None = None,
    context: dict[str, Any] | None = None,
) -> str:
    job = job or _job()
    return _pass_two(service, _spec(_payload(service, template), job), job, context)


def _paths(script: str) -> list[list[Any]]:
    """Decode the access paths carried by *script*'s markers."""
    return [
        json.loads(base64.b64decode(match.group(2)))
        for match in MARKER_RE.finditer(script)
    ]


def _raw_marker(nonce: str, encoded: str) -> str:
    """A marker carrying *encoded* verbatim, well-formed or not."""
    return f"\x00{_DEFER_PREFIX}{nonce}:{encoded}\x00"


def _b64(text: bytes) -> str:
    return base64.b64encode(text).decode()


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

    @pytest.mark.parametrize(
        ("template", "expected"),
        [
            # Attribute, key and keyword names are not dispatcher-only names.
            ("{{ config.hostname | default('none') }}", "none"),
            ("{{ files[0].metadata.dispatcher | default('-') | upper }}", "-"),
            ("{% for f in files %}{{ f.output_dir | default('x') }}{% endfor %}", "x"),
            ("{{ {'hostname': 'h'}.hostname ~ '!' }}", "h!"),
            ("{{ namespace(script_path='p').script_path | upper }}", "P"),
            ("{{ dict(dispatcher=1) | length }}", "1"),
        ],
    )
    def test_names_that_are_not_variables_are_builder_side(
        self,
        service: MagicMock,
        template: str,
        expected: str,
    ) -> None:
        assert _render(service, template) == expected

    def test_single_pass_render_without_a_nonce_defers_nothing(
        self,
        service: MagicMock,
    ) -> None:
        payload = _payload(service, "echo x")

        with pytest.raises(jinja2.UndefinedError):
            payload.render_template(_job(), "echo {{ script_path }}")

    def test_invalid_nonce_is_rejected(self, service: MagicMock) -> None:
        payload = _payload(service, "echo x")

        with pytest.raises(ValueError, match="defer_nonce"):
            payload.render_template(_job(), "echo x", defer_nonce="NOT-HEX")


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
            (
                "{% macro q(x) %}'{{ x }}'{% endmacro %}{{ q(files[0].file) }}",
                {},
                {},
                "'/data/a.nc'",
            ),
            ("{% set p = files[0].file %}{{ p ~ '.out' }}", {}, {}, "/data/a.nc.out"),
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

    def test_a_data_chosen_dispatcher_key_is_rejected_before_any_job(
        self,
        service: MagicMock,
    ) -> None:
        """A subscript built from job data cannot pick a dispatcher value."""
        with pytest.raises(ValueError, match="dispatcher-only name 'dispatcher'"):
            _payload(service, "{{ dispatcher.config[files[0].metadata.k] }}")


# ── dispatcher-only names as bare values ────────────────────────────────────


class TestBareValues:
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
            ("{{ dispatcher.config.args }}", "['--x', '--y']"),
            ("{{ script_path }}", SCRIPT_PATH),
            ("{{ hostname }}", "node-1"),
            ("{{- hostname -}}", "node-1"),
            # Literal text next to a bare value.
            ("{{ script_path }}.log", f"{SCRIPT_PATH}.log"),
            ("--out={{ hostname }}/x", "--out=node-1/x"),
            ("{{ hostname }}-{{ script_path }}", f"node-1-{SCRIPT_PATH}"),
            ("{{ files[0].file }}@{{ hostname }}", "/data/a.nc@node-1"),
            # The full access path is carried, so leaf names never collide.
            ("{{ dispatcher.config.hostname }}|{{ hostname }}", "cfg-host|node-1"),
            # Inside {% if %} (elif and else too) and non-recursive {% for %}.
            ("{% if files %}{{ hostname }}{% endif %}", "node-1"),
            (
                "{% if not files %}x{% elif config is mapping %}"
                "{{ script_path }}{% endif %}",
                SCRIPT_PATH,
            ),
            (
                "{% if not files %}x{% else %}[{{ dispatcher.identifier }}]{% endif %}",
                "[ld]",
            ),
            (
                "{% for f in files %}{{ f.file }}@{{ hostname }};{% endfor %}",
                "/data/a.nc@node-1;",
            ),
            ("{% for f in [] %}x{% else %}{{ hostname }}{% endfor %}", "node-1"),
            (
                "{% for f in files %}{% if f.file %}"
                "{{ dispatcher.config['log_dir'] }}/{{ loop.index }}"
                "{% endif %}{% endfor %}",
                "/var/log/courier/1",
            ),
        ],
    )
    def test_bare_value_renders_end_to_end(
        self,
        service: MagicMock,
        template: str,
        expected: str,
    ) -> None:
        """Pass one, the JSON wire round trip, then pass two on a hydrated payload."""
        assert _render(service, template) == expected

    @pytest.mark.parametrize(
        ("template", "path"),
        [
            ("{{ dispatcher.config.log_dir }}", ["dispatcher", "config", "log_dir"]),
            ("{{ dispatcher.config['k'] }}", ["dispatcher", "config", "k"]),
            ("{{ dispatcher.config.args[-2] }}", ["dispatcher", "config", "args", -2]),
            ("{{ dispatcher['a']['b'].c }}", ["dispatcher", "a", "b", "c"]),
            ("{{ dispatcher.args.0 }}", ["dispatcher", "args", 0]),
            ("{{ dispatcher['_k'] }}", ["dispatcher", "_k"]),
        ],
    )
    def test_marker_carries_the_full_access_path(
        self,
        service: MagicMock,
        template: str,
        path: list[Any],
    ) -> None:
        spec = _spec(_payload(service, template), _job())

        assert spec.script is not None
        assert _paths(spec.script) == [path]

    def test_every_dispatcher_name_is_deferred(self, service: MagicMock) -> None:
        template = " ".join(
            f"{{{{ {name} }}}}" for name in sorted(DISPATCHER_CONTEXT_NAMES)
        )
        spec = _spec(_payload(service, template), _job())

        assert spec.script is not None
        assert _paths(spec.script) == [[n] for n in sorted(DISPATCHER_CONTEXT_NAMES)]

    def test_output_dir_resolves_where_the_dispatcher_defines_it(
        self,
        service: MagicMock,
    ) -> None:
        context = _local_context(output_dir="/shared/slurm")

        assert _render(service, "cd {{ output_dir }}", context=context) == (
            "cd /shared/slurm"
        )

    def test_render_template_pass_one_renders_bare_values(
        self,
        service: MagicMock,
    ) -> None:
        payload = _payload(service, "echo x")

        rendered = payload.render_template(
            _job(),
            "{% for f in files %}{{ f.file }} {{ hostname }}{% endfor %}",
            defer_nonce=NONCE,
        )

        assert _resolve_deferred_expressions(rendered, _local_context(), NONCE) == (
            "/data/a.nc node-1"
        )

    def test_a_name_the_caller_supplies_is_not_deferred(
        self,
        service: MagicMock,
    ) -> None:
        payload = _payload(service, "echo x")

        rendered = payload.render_template(
            _job(),
            "{{ hostname }} {{ script_path }}",
            {"hostname": "given"},
            defer_nonce=NONCE,
        )

        assert rendered.startswith("given ")
        assert _paths(rendered) == [["script_path"]]


# ── everything else involving a dispatcher-only name is rejected ────────────

#: ``(form, the dispatcher-only name reported)``: uses other than bare values.
REJECTED_USES = [
    # Filters, including {% filter %} blocks.
    ("{{ script_path | lower }}", "script_path"),
    ("{{ output_dir | replace('/scratch', '/archive') }}", "output_dir"),
    ("{{ dispatcher.config.log_dir | e }}", "dispatcher"),
    ("{{ dispatcher.name | default('FALLBACK') }}", "dispatcher"),
    ("{{ script_path | tojson }}", "script_path"),
    ("{{ dispatcher | attr(files[0].file) }}", "dispatcher"),
    ("{{ [script_path] | join(' ') }}", "script_path"),
    ("{{ files | map(attribute='file') | join(' ' ~ hostname) }}", "hostname"),
    ("{% filter upper %}{{ script_path | e }}{% endfilter %}", "script_path"),
    # Tests.
    ("{{ script_path is defined }}", "script_path"),
    ("{{ dispatcher.config.x is string }}", "dispatcher"),
    ("{% if script_path is defined %}a{% endif %}", "script_path"),
    ("{{ [hostname] | select('defined') | list }}", "hostname"),
    # Calls, including those that only store or return a value.
    ("{{ script_path() }}", "script_path"),
    ("{{ namespace(p=hostname).p }}", "hostname"),
    ("{{ dict(p=hostname).p }}", "hostname"),
    ("{% set c = cycler(hostname, 'x') %}{{ c.next() }}", "hostname"),
    ("{% for f in files %}{{ loop.cycle(hostname, 'b') }}{% endfor %}", "hostname"),
    ("{{ config.get('missing', hostname) }}", "hostname"),
    ("{% set l = [] %}{{ l.append(script_path) }}", "script_path"),
    ("{% set j = joiner(hostname) %}{{ j() }}", "hostname"),
    ("{{ range(script_path) | list }}", "script_path"),
    ("{{ lipsum(n=script_path) }}", "script_path"),
    # Method calls, string methods included.
    ("{{ dispatcher.items() }}", "dispatcher"),
    ("{{ dispatcher.config.get('k') }}", "dispatcher"),
    ("{{ hostname.upper() }}", "hostname"),
    ("{{ 'a/b'.split(hostname) }}", "hostname"),
    ("{{ ' '.join([hostname, 'x']) }}", "hostname"),
    ("{{ '{}'.format(hostname) }}", "hostname"),
    ("{{ '{h}'.format_map({'h': hostname}) }}", "hostname"),
    # Operators: ~, +, %, arithmetic, comparisons, in, not, and/or, if/else.
    ("{{ hostname ~ '-' ~ script_path }}", "hostname"),
    ("{{ script_path ~ '.log' }}", "script_path"),
    ("{{ script_path + 'x' }}", "script_path"),
    ("{{ '%s' % hostname }}", "hostname"),
    ("{{ '%80s' % hostname }}", "hostname"),
    ("{{ dispatcher.x * 2 }}", "dispatcher"),
    ("{{ -dispatcher.x }}", "dispatcher"),
    ("{{ dispatcher.x == 1 }}", "dispatcher"),
    ("{{ dispatcher.x < 1 }}", "dispatcher"),
    ("{{ 'x' in script_path }}", "script_path"),
    ("{{ script_path not in 'x' }}", "script_path"),
    ("{{ not script_path }}", "script_path"),
    ("{{ script_path or 'x' }}", "script_path"),
    ("{{ script_path and 'x' }}", "script_path"),
    ("{{ script_path if files else 'none' }}", "script_path"),
    ("{{ 'x' if hostname else 'y' }}", "hostname"),
    # Slices, non-constant and non-str/int subscripts, private attributes.
    ("{{ script_path[1:] }}", "script_path"),
    ("{{ dispatcher.config.args[0:1] }}", "dispatcher"),
    ("{{ dispatcher[none] }}", "dispatcher"),
    ("{{ dispatcher[true] }}", "dispatcher"),
    ("{{ dispatcher[1.5] }}", "dispatcher"),
    ("{{ dispatcher[-1.5] }}", "dispatcher"),
    ("{{ dispatcher[+1] }}", "dispatcher"),
    ("{{ dispatcher[files[0].file] }}", "dispatcher"),
    ("{{ dispatcher.config[files[0].metadata.k] }}", "dispatcher"),
    ("{{ dispatcher['a' ~ 'b'] }}", "dispatcher"),
    ("{{ dispatcher[script_path] }}", "dispatcher"),
    ("{{ config.table[script_path] }}", "script_path"),
    ("{{ dispatcher._private }}", "dispatcher"),
    ("{{ dispatcher.config.__class__ }}", "dispatcher"),
    # Containers.
    ("{{ [script_path] }}", "script_path"),
    ("{{ {'a': hostname} }}", "hostname"),
    ("{{ (hostname, 'x') }}", "hostname"),
    ("{{ hostname, script_path }}", "hostname"),
    # {% if %} / {% for %} conditions and iterables.
    ("{% if dispatcher.name %}y{% endif %}", "dispatcher"),
    ("{% if files %}a{% elif hostname %}b{% endif %}", "hostname"),
    ("{% for x in dispatcher.config %}{% endfor %}", "dispatcher"),
    ("{% for c in script_path %}{{ c }}{% endfor %}", "script_path"),
    ("{% for f in files if hostname %}{% endfor %}", "hostname"),
    # {% set %} and {% with %} over one, macro arguments and defaults.
    ("{% set p = script_path %}{{ p }}", "script_path"),
    ("{% set ns = namespace(p='') %}{% set ns.p = hostname %}", "hostname"),
    ("{% with p = script_path %}{{ p }}{% endwith %}", "script_path"),
    ("{% macro at(x) %}[{{ x }}]{% endmacro %}{{ at(script_path) }}", "script_path"),
    ("{% macro at(x=hostname) %}{{ x }}{% endmacro %}", "hostname"),
]

#: ``(form, the dispatcher-only name reported)``: a bare value in a tag that
#: cannot hold one.
REJECTED_PLACES = [
    ("{% filter upper %}{{ script_path }}{% endfilter %}", "script_path"),
    ("{% set p %}{{ hostname }}{% endset %}{{ p }}", "hostname"),
    ("{% set s | upper %}{{ hostname }}{% endset %}", "hostname"),
    ("{% with %}{{ hostname }}{% endwith %}", "hostname"),
    ("{% macro at() %}{{ script_path }}{% endmacro %}{{ at() }}", "script_path"),
    (
        "{% macro m() %}{{ caller() }}{% endmacro %}"
        "{% call m() %}{{ hostname }}{% endcall %}",
        "hostname",
    ),
    ("{% for f in files recursive %}{{ hostname }}{% endfor %}", "hostname"),
    (
        "{% for f in files %}{% for g in files recursive %}{{ hostname }}"
        "{% endfor %}{% endfor %}",
        "hostname",
    ),
    (
        "{% if files %}{% for f in files recursive %}{{ dispatcher.name }}"
        "{% endfor %}{% endif %}",
        "dispatcher",
    ),
    ("{% autoescape true %}{{ hostname }}{% endautoescape %}", "hostname"),
    ("{% block b %}{{ hostname }}{% endblock %}", "hostname"),
]

#: ``(form, the dispatcher-only name reported)``: a path step after one whose
#: value is a string.
REJECTED_STRING_PATHS = [
    ("{{ hostname[0] }}", "hostname"),
    ("{{ hostname[-1] }}", "hostname"),
    ("{{ hostname.upper }}", "hostname"),
    ("{{ script_path.name }}", "script_path"),
    ("{{ script_path['x'] }}", "script_path"),
    ("{% if files %}{{ output_dir.parent }}{% endif %}", "output_dir"),
    ("{% for f in files %}{{ output_dir[0] }}{% endfor %}", "output_dir"),
]

#: ``(form, the dispatcher-only name reported)``: bindings and shadowing.
REJECTED_BINDINGS = [
    ("{% set hostname = 'x' %}", "hostname"),
    ("{% set hostname = 'x' %}{{ hostname }}", "hostname"),
    ("{% set a, dispatcher = 1, 2 %}", "dispatcher"),
    ("{% set hostname %}x{% endset %}", "hostname"),
    ("{% set hostname.x = 1 %}", "hostname"),
    ("{% for hostname in files %}{% endfor %}", "hostname"),
    ("{% for f, output_dir in [] %}{% endfor %}", "output_dir"),
    ("{% macro m(script_path) %}{% endmacro %}", "script_path"),
    ("{% macro hostname() %}{% endmacro %}", "hostname"),
    ("{% call(hostname) m() %}{% endcall %}", "hostname"),
    ("{% with dispatcher = 1 %}{% endwith %}", "dispatcher"),
    ("{% import 'lib' as hostname %}", "hostname"),
    ("{% from 'lib' import y as script_path %}", "script_path"),
    ("{% from 'lib' import output_dir %}", "output_dir"),
]

#: Look-alike spellings of ``hostname``: fullwidth letters, and U+210E (the
#: Planck constant, which reads as an italic h).  Python NFKC-normalizes the
#: identifiers Jinja compiles them into, so each would be ``hostname`` itself.
FULLWIDTH_HOSTNAME = "".join(chr(ord(c) + 0xFEE0) for c in "hostname")
PLANCK_HOSTNAME = "\u210eostname"

#: ``(form, the spelling reported, problem)``: look-alike spellings, as a
#: value and as a binding.
REJECTED_LOOKALIKES = [
    (f"{{{{ {spelling} }}}}", spelling, "is spelled with look-alike non-ASCII")
    for spelling in (FULLWIDTH_HOSTNAME, PLANCK_HOSTNAME)
] + [
    (form.format(h=spelling), spelling, problem)
    for spelling in (FULLWIDTH_HOSTNAME, PLANCK_HOSTNAME)
    for form, problem in [
        ("{{{{ {h} | upper }}}}{{{{ hostname }}}}", "is spelled with look-alike"),
        ("{{{{ {h} | string | list | join }}}}", "is spelled with look-alike"),
        ("{{{{ hostname }}}}{{{{ {h}.x }}}}", "is spelled with look-alike"),
        ("{{% set {h} = 'SHADOW' %}}{{{{ hostname }}}}", "is assigned or rebound"),
        (
            "{{% if files %}}{{% set {h} = 'S' %}}{{{{ hostname }}}}{{% endif %}}",
            "is assigned or rebound",
        ),
        ("{{% macro {h}() %}}{{% endmacro %}}{{{{ hostname }}}}", "is assigned"),
        ("{{% macro m({h}) %}}{{% endmacro %}}", "is assigned or rebound"),
        ("{{% for {h} in files %}}{{% endfor %}}", "is assigned or rebound"),
        ("{{% with {h} = 1 %}}{{% endwith %}}", "is assigned or rebound"),
        ("{{% set {h}.x = 1 %}}", "is assigned or rebound"),
        ("{{% import 'lib' as {h} %}}", "is assigned or rebound"),
        ("{{% from 'lib' import y as {h} %}}", "is assigned or rebound"),
    ]
]

_REJECTED = (
    [
        pytest.param(form, repr(name), "is used other than as a bare value", id=form)
        for form, name in REJECTED_USES
    ]
    + [
        pytest.param(form, repr(name), "is assigned or rebound", id=form)
        for form, name in REJECTED_BINDINGS
    ]
    + [
        pytest.param(
            form,
            repr(name),
            "is a bare value inside a tag that cannot hold one",
            id=form,
        )
        for form, name in REJECTED_PLACES
    ]
    + [
        pytest.param(
            form,
            repr(name),
            "is a string, so no attribute or item can follow it",
            id=form,
        )
        for form, name in REJECTED_STRING_PATHS
    ]
    + [
        pytest.param(
            form,
            f"{spelling!r} (reads as 'hostname')",
            problem,
            id=form.encode("unicode_escape").decode(),
        )
        for form, spelling, problem in REJECTED_LOOKALIKES
    ]
)


class TestRejectedForms:
    @pytest.mark.parametrize(("form", "reported", "problem"), _REJECTED)
    def test_rejected_when_the_payload_is_built(
        self,
        service: MagicMock,
        form: str,
        reported: str,
        problem: str,
    ) -> None:
        """Startup and ``courier validate`` build the payload, so both refuse it."""
        with pytest.raises(ValueError, match="unsupported template") as info:
            _payload(service, f"#!/bin/sh\n{form}\n")

        message = str(info.value)
        assert isinstance(info.value.__cause__, DeferredExpressionError)
        assert "inline script" in message
        assert f"line 2: dispatcher-only name {reported} {problem}" in message
        assert "bare value" in message

    def test_rejected_by_render_template_in_pass_one(self, service: MagicMock) -> None:
        """The same check as at construction; the table above covers its rules."""
        payload = _payload(service, "echo ok")
        expected = "line 2: dispatcher-only name 'hostname' is used other than"

        with pytest.raises(DeferredExpressionError, match=re.escape(expected)):
            payload.render_template(
                _job(), "#!/bin/sh\n{{ hostname | upper }}\n", defer_nonce=NONCE
            )

    def test_message_says_how_to_write_it(self, service: MagicMock) -> None:
        with pytest.raises(ValueError, match="unsupported template") as info:
            _payload(service, "echo a\necho b\n\necho {{ hostname | upper }}\n")

        message = str(info.value)
        assert "line 4: dispatcher-only name 'hostname'" in message
        for advice in (
            "{{ script_path }}.log",
            "{{ dispatcher.config['log_dir'] }}",
            "non-recursive {% for %}",
            "Not supported: filters, tests, calls",
            "{% block %}, {% autoescape %}",
            "recursive loops holding it",
            "look-alike non-ASCII letters",
        ):
            assert advice in message

    @pytest.mark.parametrize("spelling", [FULLWIDTH_HOSTNAME, PLANCK_HOSTNAME])
    def test_a_look_alike_cannot_let_job_data_pick_the_resolved_path(
        self,
        service: MagicMock,
        spelling: str,
    ) -> None:
        """The review repro: a filter on a look-alike rewrote the marker's path.

        Without NFKC comparison this template passed the check, and job
        metadata replaced the encoded ``["hostname"]`` with a path of its
        choosing, which pass two then resolved.
        """
        encoded = _b64(b'["hostname"]')
        template = (
            f"{{{{ {spelling} | string | replace({encoded!r}, "
            f"files[0].metadata.p) }}}}\n{{{{ hostname }}}}"
        )

        with pytest.raises(ValueError, match="reads as 'hostname'") as info:
            _payload(service, template)

        assert isinstance(info.value.__cause__, DeferredExpressionError)

    def test_a_keyword_spelled_like_a_reserved_name_is_builder_side(
        self,
        service: MagicMock,
    ) -> None:
        """A keyword argument is not a variable, however it is spelled."""
        template = f"{{{{ namespace({PLANCK_HOSTNAME}='x').hostname }}}}"

        assert _render(service, template) == "x"

    @pytest.mark.parametrize(
        "name",
        ["hostnames", "my_hostname", "Hostname", "HOSTNAME", "host_name"],
    )
    def test_a_name_that_only_resembles_one_is_builder_side(
        self,
        service: MagicMock,
        name: str,
    ) -> None:
        """Only the NFKC form of a reserved name is reserved; case is kept."""
        payload = _payload(service, f"{{{{ {name} | default('d') }}}}")

        assert payload.to_job_spec(_job()).script == "d"

    def test_a_template_file_is_named_in_the_error(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        template = tmp_path / "job.sh"
        template.write_text("echo {{ files[0].file }}\necho {{ script_path ~ '' }}\n")

        with pytest.raises(ValueError, match=r"job\.sh'?, line 2: dispatcher"):
            BashPayload(service, {"file": template}, "p")

    def test_nothing_is_checked_in_a_strict_single_pass(
        self,
        service: MagicMock,
    ) -> None:
        """Without a nonce every name is the caller's, so stock Jinja applies."""
        payload = _payload(service, "echo x")

        rendered = payload.render_template(
            _job(),
            "{{ hostname | upper }} {{ script_path ~ '.log' }}",
            {"hostname": "node-1", "script_path": "/s"},
        )

        assert rendered == "NODE-1 /s.log"

    def test_argument_templates_are_not_checked(self, service: MagicMock) -> None:
        """``binary``/``prefix_args``/``suffix_args`` render on the dispatcher."""
        payload = BashPayload(
            service,
            {"script": "echo x", "prefix_args": ["--log={{ hostname | upper }}"]},
            "p",
        )

        rendered = payload.with_rendered_arguments(_job(), _local_context())

        assert rendered.config.prefix_args == ["--log=NODE-1"]


# ── pass two: look the path up, evaluate nothing ────────────────────────────


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
        ("path", "expected"),
        [
            (["dispatcher", "identifier"], "ld"),
            (["dispatcher", "config", "log_dir"], "/var/log/courier"),
            (["dispatcher", "config", "weird key"], "spaced"),
            (["dispatcher", "config", "args", 0], "--x"),
            (["dispatcher", "config", "args", -1], "--y"),
            (["hostname"], "node-1"),
            (["script_path"], SCRIPT_PATH),
        ],
    )
    def test_access_path_is_looked_up(self, path: list[Any], expected: str) -> None:
        text = "echo " + _deferred_marker(NONCE, path)

        assert _resolve_deferred_expressions(text, _full_context(), NONCE) == (
            f"echo {expected}"
        )

    def test_a_tuple_is_indexed_like_a_list(self) -> None:
        context = {"dispatcher": {"pair": ("a", "b")}}
        text = _deferred_marker(NONCE, ["dispatcher", "pair", 1])

        assert _resolve_deferred_expressions(text, context, NONCE) == "b"

    def test_empty_nonce_resolves_nothing(self) -> None:
        text = "echo " + _deferred_marker(NONCE, ["dispatcher", "identifier"])

        assert _resolve_deferred_expressions(text, _local_context(), "") == text

    def test_only_this_nonce_is_resolved_next_to_another(self) -> None:
        other = _deferred_marker("deadbeef", ["hostname"])
        text = f"{other} {_deferred_marker(NONCE, ['hostname'])}"

        assert _resolve_deferred_expressions(text, _local_context(), NONCE) == (
            f"{other} node-1"
        )

    @pytest.mark.parametrize(
        "encoded",
        [
            "",
            "A",
            "!!!!",
            _b64(b"dispatcher"),
            _b64(b"\xff\xfe"),
            _b64(b'{"a": 1}'),
            _b64(b'"hostname"'),
            _b64(b"[]"),
            _b64(b"[1]"),
            _b64(b'[["hostname"]]'),
            _b64(b'["files", 0, "file"]'),
            _b64(b'["config", "modes"]'),
            _b64(b'["job", "identifier"]'),
            _b64(b'["builder", "identifier"]'),
            _b64(b'["lipsum"]'),
            _b64(b'["dispatcher", true]'),
            _b64(b'["dispatcher", 1.5]'),
            _b64(b'["dispatcher", null]'),
            _b64(b'["dispatcher", ["config"]]'),
        ],
    )
    def test_malformed_marker_with_this_nonce_raises(self, encoded: str) -> None:
        text = "echo " + _raw_marker(NONCE, encoded)

        with pytest.raises(DeferredExpressionError, match="malformed"):
            _resolve_deferred_expressions(text, _full_context(), NONCE)

    @pytest.mark.parametrize(
        ("template", "path", "missing"),
        [
            ("{{ output_dir }}", "output_dir", "output_dir"),
            ("{{ dispatcher.confg }}", "dispatcher.confg", "dispatcher.confg"),
            (
                "{{ dispatcher.confg.deeper }}",
                "dispatcher.confg.deeper",
                "dispatcher.confg",
            ),
            (
                "{{ dispatcher.config.args[5] }}",
                "dispatcher.config.args[5]",
                "dispatcher.config.args[5]",
            ),
            (
                "{{ dispatcher.config['no pe'] }}",
                "dispatcher.config['no pe']",
                "dispatcher.config['no pe']",
            ),
            # Pass two indexes mappings and lists only: no attribute of a value.
            ("{{ dispatcher.items }}", "dispatcher.items", "dispatcher.items"),
            (
                "{{ dispatcher.config.args.x }}",
                "dispatcher.config.args.x",
                "dispatcher.config.args.x",
            ),
            (
                "{{ dispatcher.config[0] }}",
                "dispatcher.config[0]",
                "dispatcher.config[0]",
            ),
        ],
    )
    def test_value_this_dispatcher_does_not_define_raises(
        self,
        service: MagicMock,
        template: str,
        path: str,
        missing: str,
    ) -> None:
        with pytest.raises(
            DeferredExpressionError, match="not defined by this"
        ) as info:
            _render(service, template)

        assert repr(path) in str(info.value)
        assert f"it has no {missing!r}" in str(info.value)

    @pytest.mark.parametrize(
        ("template", "path", "parent", "kind"),
        [
            ("{{ dispatcher.name.x }}", "dispatcher.name.x", "dispatcher.name", "str"),
            (
                "{{ dispatcher.name[0] }}",
                "dispatcher.name[0]",
                "dispatcher.name",
                "str",
            ),
            (
                "{{ dispatcher.config.log_dir.parent }}",
                "dispatcher.config.log_dir.parent",
                "dispatcher.config.log_dir",
                "str",
            ),
        ],
    )
    def test_a_step_below_a_plain_value_says_so(
        self,
        service: MagicMock,
        template: str,
        path: str,
        parent: str,
        kind: str,
    ) -> None:
        """Not "not defined": the value is there, but has no keys or indexes."""
        with pytest.raises(
            DeferredExpressionError, match="cannot be looked up"
        ) as info:
            _render(service, template)

        message = str(info.value)
        assert repr(path) in message
        assert f"{parent!r} is a {kind}" in message
        assert "only a dictionary key or a list index" in message
        assert "not defined" not in message

    def test_a_step_below_a_null_value_is_not_defined(
        self,
        service: MagicMock,
    ) -> None:
        with pytest.raises(DeferredExpressionError) as info:
            _render(service, "{{ dispatcher.config.nothing.x }}")

        assert str(info.value) == (
            "dispatcher-only value 'dispatcher.config.nothing.x' is not defined "
            "by this dispatcher ('dispatcher.config.nothing' is null)"
        )

    def test_a_step_below_a_string_root_raises_in_pass_two(self) -> None:
        """The template check rejects these; pass two still refuses a marker."""
        text = _deferred_marker(NONCE, ["hostname", 0])

        with pytest.raises(DeferredExpressionError, match="'hostname' is a str"):
            _resolve_deferred_expressions(text, _local_context(), NONCE)

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

    def test_text_outside_markers_is_never_evaluated(self) -> None:
        text = "echo {{ 6*7 }} {% if 1 %}x{% endif %} " + _deferred_marker(
            NONCE,
            ["hostname"],
        )

        assert _resolve_deferred_expressions(text, _local_context(), NONCE) == (
            "echo {{ 6*7 }} {% if 1 %}x{% endif %} node-1"
        )

    def test_a_value_holding_template_syntax_is_not_evaluated(self) -> None:
        context = _local_context(hostname="{{ 6*7 }}")
        text = _deferred_marker(NONCE, ["hostname"])

        assert _resolve_deferred_expressions(text, context, NONCE) == "{{ 6*7 }}"

    def test_every_occurrence_is_resolved(self) -> None:
        host = _deferred_marker(NONCE, ["hostname"])
        path = _deferred_marker(NONCE, ["script_path"])
        text = f"{host} {path}\n" * 500

        resolved = _resolve_deferred_expressions(text, _local_context(), NONCE)

        assert resolved == f"node-1 {SCRIPT_PATH}\n" * 500

    def test_the_job_is_not_consulted(self, service: MagicMock) -> None:
        """Values come from the dispatcher's context alone."""
        payload = _payload(service, "echo x")
        text = _deferred_marker(NONCE, ["hostname"])
        job = _job(config={"hostname": "from-the-job"})

        assert payload.resolve_deferred_expressions(
            text,
            job,
            {"hostname": "from-the-dispatcher"},
            defer_nonce=NONCE,
        ) == ("from-the-dispatcher")

    def test_no_context_defines_nothing(self, service: MagicMock) -> None:
        payload = _payload(service, "echo x")
        text = _deferred_marker(NONCE, ["hostname"])

        with pytest.raises(DeferredExpressionError, match="not defined by this"):
            payload.resolve_deferred_expressions(text, _job(), defer_nonce=NONCE)


# ── job data passes through both passes verbatim ────────────────────────────


class TestDataIsPassedThroughVerbatim:
    def test_nul_in_job_data_is_kept_next_to_a_dispatcher_value(
        self,
        service: MagicMock,
    ) -> None:
        job = _job({"title": "a\x00b\x00"})

        rendered = _render(
            service,
            "echo {{ files[0].metadata.title }}{{ hostname }}",
            job,
        )

        assert rendered == "echo a\x00b\x00node-1"

    @pytest.mark.parametrize(
        "title",
        [
            _deferred_marker("deadbeef", ["dispatcher", "config"]),
            _raw_marker("deadbeef", "not base64!"),
            "\x00COURIER-DEFER:abcd:xyz",
            "COURIER-DEFER:not-a-marker",
            "{{ hostname }}",
        ],
        ids=["foreign-marker", "foreign-junk", "unterminated", "prefix", "jinja"],
    )
    def test_marker_shaped_job_data_stays_verbatim(
        self,
        service: MagicMock,
        title: str,
    ) -> None:
        """Nothing in job data can carry the job's nonce, so none is resolved.

        A run of data right before a real marker cannot swallow it either.
        """
        job = _job({"title": title})
        spec = _spec(
            _payload(service, "echo {{ files[0].metadata.title }}{{ hostname }}"),
            job,
        )

        assert spec.script is not None
        assert spec.script.startswith(f"echo {title}")
        assert _pass_two(service, spec, job) == f"echo {title}node-1"


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
        assert [m.group(1) for m in MARKER_RE.finditer(first.script)] == [
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


# ── the environment, the placeholder and single-pass renders ────────────────


class TestEnvironment:
    def test_the_environment_is_a_stock_strict_sandbox(self) -> None:
        stock = SandboxedEnvironment()

        assert type(_ENV) is SandboxedEnvironment
        assert _ENV.undefined is jinja2.StrictUndefined
        assert _ENV.autoescape is False
        assert _ENV.filters == stock.filters
        assert _ENV.tests == stock.tests
        assert _ENV.intercepted_binops == stock.intercepted_binops

    def test_single_pass_render_uses_the_callers_context(
        self,
        service: MagicMock,
    ) -> None:
        payload = _payload(service, "echo x")

        rendered = payload.render_template(
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

        assert payload.render_template(_job(), "{{ v }}", {"v": NoEquality()}) == (
            "no-equality"
        )


class TestDeferredValue:
    def test_attribute_and_item_access_extend_the_path(self) -> None:
        value = _DeferredValue(NONCE, ("dispatcher",)).config["k"][-1].x

        assert _paths(str(value)) == [["dispatcher", "config", "k", -1, "x"]]

    def test_marker_format(self) -> None:
        marker = str(_DeferredValue(NONCE, ("hostname",)))
        encoded = base64.b64encode(b'["hostname"]').decode()

        assert marker == f"\x00{_DEFER_PREFIX}{NONCE}:{encoded}\x00"

    @pytest.mark.parametrize(
        "name",
        ["__html__", "__class_getitem__", "_nonce_probe", "_path_x"],
    )
    def test_underscore_attributes_are_missing(self, name: str) -> None:
        assert not hasattr(_DeferredValue(NONCE, ("dispatcher",)), name)
