"""The dashboard must describe payloads, which now decide what a job runs.

Before payloads, a dispatcher's own config named the script it ran, so the
topology inventory showed it. Now the script lives in a ``payload`` block
nested under the job builder, and the ``courier_payload_*`` metrics -- the only
ones that count a job by its script's exit code -- had no panel at all.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("grafanalib")

from grafanalib._gen import DashboardEncoder  # noqa: E402

from courier.dashboard.config_parser import (  # noqa: E402
    DashboardModel,
    PayloadInfo,
    parse_config,
)
from courier.dashboard.generator import generate_dashboard  # noqa: E402
from courier.dashboard.prometheus_panels import (  # noqa: E402
    build_prometheus_panels,
    build_prometheus_templates,
)
from courier.dashboard.topology import build_topology_panels  # noqa: E402

_TESTS_DIR = Path(__file__).resolve().parents[2]
_DEMO = _TESTS_DIR / "demo.yaml"


def _write(tmp_path: Path, run: str) -> Path:
    path = tmp_path / "svc.yaml"
    path.write_text(
        "apiVersion: runcourier.dev/v1alpha1\n"
        "kind: Service\n"
        "metadata:\n"
        "  name: svc\n"
        "  namespace: svc\n"
        "  description: payload dashboard\n"
        "spec:\n"
        "  run:\n" + run,
    )
    return path


#: Two builders, two dispatchers: ``fast`` goes to ``local``, ``batch`` to
#: ``hpc``, so every payload reaches exactly one dispatcher.
_TWO_ROUTES = """\
    - watch:
        kind: data_monitor
        name: file_system_poller_watchdog
        config: {path: /tmp}
    - fast:
        kind: job_builder
        name: DummyJobBuilder
        config:
          targets: [local]
          payload:
            fast-payload:
              kind: payload
              name: bash_payload
              config: {script: echo fast}
    - batch:
        kind: job_builder
        name: DummyJobBuilder
        config:
          targets: [hpc]
          payload:
            identifier: batch-payload
            spec:
              kind: payload
              name: python_payload
              config: {script: "print('batch')"}
    - local:
        kind: dispatcher
        name: local_dispatcher
    - hpc:
        kind: dispatcher
        name: local_dispatcher
"""


def _exprs(panels: list[Any]) -> list[str]:
    """Every PromQL expression in *panels* (rows and their children)."""
    found: list[str] = []
    for panel in panels:
        for child in [panel, *(getattr(panel, "panels", []) or [])]:
            found.extend(t.expr for t in getattr(child, "targets", []) or [])
    return found


def _dashboard_json(dashboard: Any) -> str:
    return json.dumps(dashboard.to_json_data(), cls=DashboardEncoder)


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------


def test_parse_config_records_each_builders_payload(tmp_path: Path) -> None:
    model = parse_config(_write(tmp_path, _TWO_ROUTES))

    assert model.payloads == [
        PayloadInfo(
            identifier="fast-payload",
            plugin_name="bash_payload",
            builder="fast",
            config={"script": "echo fast"},
            targets=["local"],
        ),
        PayloadInfo(
            identifier="batch-payload",
            plugin_name="python_payload",
            builder="batch",
            config={"script": "print('batch')"},
            targets=["hpc"],
        ),
    ]
    builders = {jb.identifier: jb.payload for jb in model.job_builders}
    assert builders["fast"] is model.payloads[0]
    assert all(p.payload is None for p in model.dispatchers + model.data_monitors)


def test_a_malformed_payload_block_is_skipped_not_raised(tmp_path: Path) -> None:
    """The dashboard describes a config; `courier validate` reports this one."""
    run = """\
    - build:
        kind: job_builder
        name: DummyJobBuilder
        config:
          payload:
            one: {kind: payload, name: bash_payload}
            two: {kind: payload, name: bash_payload}
    - work:
        kind: dispatcher
        name: local_dispatcher
"""
    model = parse_config(_write(tmp_path, run))

    assert model.payloads == []
    assert model.job_builders[0].payload is None


# ---------------------------------------------------------------------------
# Prometheus panels
# ---------------------------------------------------------------------------


def _payload_row(model: DashboardModel) -> Any:
    rows = [r for r in build_prometheus_panels(model) if r.title == "Payloads"]
    assert len(rows) <= 1
    return rows[0] if rows else None


def test_payload_row_charts_the_configured_payloads(tmp_path: Path) -> None:
    model = parse_config(_write(tmp_path, _TWO_ROUTES))

    row = _payload_row(model)

    assert row is not None, "no Payloads row for a config with payloads"
    exprs = _exprs([row])
    assert any("courier_payload_jobs_processed_total" in e for e in exprs)
    assert any(
        "courier_payload_job_execution_duration_seconds_bucket" in e for e in exprs
    )
    for expr in exprs:
        assert 'payload_identifier=~"fast-payload|batch-payload"' in expr
        assert 'payload_name=~"$pl_plugin"' in expr
    assert any('status="success"' in e for e in exprs), "no success ratio"


def test_payload_template_offers_each_payload_plugin_once(tmp_path: Path) -> None:
    text = _TWO_ROUTES.replace("python_payload", "bash_payload")
    model = parse_config(_write(tmp_path, text))

    (template,) = [
        t for t in build_prometheus_templates(model) if t.name == "pl_plugin"
    ]

    assert template.query == "bash_payload"


def test_no_payload_row_without_payloads() -> None:
    model = parse_config(_TESTS_DIR / "cira-data-inventory-example.yaml")

    assert model.payloads == []
    assert _payload_row(model) is None
    assert "pl_plugin" not in {t.name for t in build_prometheus_templates(model)}


def test_no_panel_queries_the_removed_parallel_workers_gauge() -> None:
    """parallel_bash was its only producer; the column could only be empty."""
    model = parse_config(_DEMO)

    assert not [
        e for e in _exprs(build_prometheus_panels(model)) if "parallel_workers" in e
    ]


# ---------------------------------------------------------------------------
# Topology
# ---------------------------------------------------------------------------


def _inventory_rows(model: DashboardModel) -> dict[str, str]:
    """Map identifier -> its rendered HTML row in the Pipeline Topology table."""
    topology = build_topology_panels(model)[0]
    assert topology.title == "Pipeline Topology"
    html = topology.panels[0].content
    rows: dict[str, str] = {}
    for row in re.findall(r"<tr style=.*?</tr>", html):
        for plugin in model.plugins:
            if f">{plugin.identifier}</td>" in row:
                rows[plugin.identifier] = row
    return rows


def test_inventory_shows_what_each_builder_and_dispatcher_runs(
    tmp_path: Path,
) -> None:
    model = parse_config(_write(tmp_path, _TWO_ROUTES))

    rows = _inventory_rows(model)

    assert "fast-payload" in rows["fast"]
    assert "(bash_payload)" in rows["fast"]
    # A dispatcher runs the payloads routed to it -- and only those.
    assert "fast-payload" in rows["local"]
    assert "batch-payload" not in rows["local"]
    assert "batch-payload" in rows["hpc"]
    assert "(python_payload)" in rows["hpc"]
    assert "payload" not in rows["watch"]


def test_implicit_routing_reaches_the_sole_dispatcher(tmp_path: Path) -> None:
    run = """\
    - build:
        kind: job_builder
        name: DummyJobBuilder
        config:
          payload:
            only: {kind: payload, name: bash_payload, config: {script: echo}}
    - work:
        kind: dispatcher
        name: local_dispatcher
"""
    model = parse_config(_write(tmp_path, run))

    assert "only" in _inventory_rows(model)["work"]


def test_builder_config_summary_leaves_the_payload_to_its_column(
    tmp_path: Path,
) -> None:
    """The nested block used to crowd the summary out as a truncated dict."""
    model = parse_config(_write(tmp_path, _TWO_ROUTES))

    row = _inventory_rows(model)["fast"]

    assert "{&#x27;" not in row, "the payload dict was rendered as config"
    assert "targets" in row


def test_flow_rates_select_builders_by_identifier() -> None:
    """``job_builder_name`` is the plugin type; the routing table is keyed by
    YAML identifier, so selecting the name matched nothing."""
    model = parse_config(_DEMO)

    flow = [r for r in build_topology_panels(model) if r.title == "Pipeline Flow Rates"]
    (expr, *_rest) = _exprs(flow)

    assert 'job_builder_identifier=~"job-builder"' in expr
    assert "job_builder_name" not in expr


def test_dependency_health_selects_plugins_by_identifier() -> None:
    model = parse_config(_DEMO, run_identifiers={"run-preprocessing-suite"})

    health = [r for r in build_topology_panels(model) if r.title == "Dependency Health"]
    exprs = _exprs(health)

    assert exprs, "no dependency health queries for a sub-section"
    for expr in exprs:
        assert "plugin_identifier=~" in expr
        assert "plugin_name" not in expr


# ---------------------------------------------------------------------------
# Split dashboards
# ---------------------------------------------------------------------------


def test_split_by_plugin_charts_payloads_where_they_run(tmp_path: Path) -> None:
    model = parse_config(_write(tmp_path, _TWO_ROUTES))

    # Titled "<service> - <identifier>" when split by plugin.
    dashboards = {
        d.title.rsplit(" - ", 1)[-1]: _dashboard_json(d)
        for d in generate_dashboard(model, mode="SPLIT_BY_PLUGIN", only_metrics=True)
    }

    assert set(dashboards) == {"watch", "fast", "batch", "local", "hpc"}
    assert "fast-payload" in dashboards["local"]
    assert "batch-payload" not in dashboards["local"]
    assert "batch-payload" in dashboards["hpc"]
    assert "fast-payload" in dashboards["fast"], "a builder's own payload is charted"
    assert "courier_payload_jobs_processed_total" not in dashboards["watch"]


def test_split_by_kind_charts_payloads_with_builders_and_dispatchers(
    tmp_path: Path,
) -> None:
    model = parse_config(_write(tmp_path, _TWO_ROUTES))

    # Titled "<service> - <kind>" when split by kind.
    dashboards = {
        d.title.rsplit(" - ", 1)[-1]: _dashboard_json(d)
        for d in generate_dashboard(model, mode="SPLIT_BY_KIND", only_metrics=True)
    }
    charted = {
        kind
        for kind, text in dashboards.items()
        if "courier_payload_jobs_processed_total" in text
    }

    assert charted == {"job_builder", "dispatcher"}
