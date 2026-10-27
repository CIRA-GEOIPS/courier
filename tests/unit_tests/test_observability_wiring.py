"""Guards against drift between emitted metrics/logs and what reads them.

Every bug pinned here was the same shape: one side of an observability contract
changed and the other silently kept querying something that no longer existed.
Nothing failed loudly — panels rendered empty and counters read zero.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from prometheus_client.metrics import MetricWrapperBase

import courier.metrics as metrics_module
from courier.dashboard.config_parser import PluginKind, _plugin_kind_or_none
from courier.dashboard.live_detector import (
    _extract_plugin_states,
    detect_active_plugins,
)
from courier.metrics import (
    DATA_MONITOR_FILES_PROCESSED,
    DATA_MONITOR_LAST_SCAN_TIMESTAMP,
    DATA_MONITOR_SCAN_DURATION,
    collect_labeled,
)


class TestCollectLabeled:
    """``collect_labeled`` must work for every metric type, not just Gauges."""

    def test_counter_samples_are_returned(self) -> None:
        """Counters export ``<name>_total`` while ``_name`` drops the suffix.

        Matching sample names for equality against ``_name`` therefore returned
        nothing, so every plugin's ``get_metrics()`` silently omitted all of
        its counters.
        """
        DATA_MONITOR_FILES_PROCESSED.labels(
            monitor_name="cl_counter",
            monitor_identifier="dm-1",
            status="success",
        ).inc(3)
        result = collect_labeled(
            DATA_MONITOR_FILES_PROCESSED,
            "monitor_name",
            "cl_counter",
        )
        assert result, "counter samples missing"
        # Keys are "<sample_name>_{labels}"; check the sample-name portion.
        sample_names = {key.split("_{")[0] for key in result}
        assert any(name.endswith("_total") for name in sample_names), sample_names

    def test_histogram_sum_and_count_are_returned(self) -> None:
        DATA_MONITOR_SCAN_DURATION.labels(
            monitor_name="cl_hist",
            monitor_identifier="dm-1",
        ).observe(0.25)
        result = collect_labeled(
            DATA_MONITOR_SCAN_DURATION,
            "monitor_name",
            "cl_hist",
        )
        suffixes = {name.split("_{")[0].rsplit("_", 1)[-1] for name in result}
        assert {"sum", "count"} <= suffixes

    def test_gauge_still_works(self) -> None:
        DATA_MONITOR_LAST_SCAN_TIMESTAMP.labels(
            monitor_name="cl_gauge",
            monitor_identifier="dm-1",
        ).set(5)
        result = collect_labeled(
            DATA_MONITOR_LAST_SCAN_TIMESTAMP,
            "monitor_name",
            "cl_gauge",
        )
        assert [entry["value"] for entry in result.values()] == [5.0]

    def test_bookkeeping_samples_are_excluded(self) -> None:
        """``_created`` and ``_bucket`` are not measurements."""
        DATA_MONITOR_SCAN_DURATION.labels(
            monitor_name="cl_excl",
            monitor_identifier="dm-1",
        ).observe(0.5)
        result = collect_labeled(
            DATA_MONITOR_SCAN_DURATION,
            "monitor_name",
            "cl_excl",
        )
        assert not any("_created" in n or "_bucket" in n for n in result)

    def test_other_label_values_are_filtered_out(self) -> None:
        DATA_MONITOR_FILES_PROCESSED.labels(
            monitor_name="cl_keep",
            monitor_identifier="dm-1",
            status="success",
        ).inc()
        DATA_MONITOR_FILES_PROCESSED.labels(
            monitor_name="cl_drop",
            monitor_identifier="dm-1",
            status="success",
        ).inc()
        result = collect_labeled(
            DATA_MONITOR_FILES_PROCESSED,
            "monitor_name",
            "cl_keep",
        )
        assert all(e["labels"]["monitor_name"] == "cl_keep" for e in result.values())


class TestDashboardSelectorsMatchMetrics:
    """Dashboard PromQL may only reference labels the metrics actually carry."""

    @staticmethod
    def _known_labels() -> dict[str, set[str]]:
        known: dict[str, set[str]] = {}
        for value in vars(metrics_module).values():
            if not isinstance(value, MetricWrapperBase):
                continue
            base = value._name
            labels = set(value._labelnames)
            for suffix in ("", "_total", "_created", "_bucket", "_sum", "_count"):
                known[base + suffix] = labels
        return known

    def test_no_selector_uses_an_unknown_label(self) -> None:
        """Catches the identifier-vs-name confusion that broke cluster panels."""
        known = self._known_labels()
        dashboard_dir = Path(metrics_module.__file__).parent / "dashboard"
        offenders: list[str] = []

        for path in sorted(dashboard_dir.glob("*.py")):
            for lineno, line in enumerate(path.read_text().splitlines(), 1):
                for match in re.finditer(r"(courier_[a-z0-9_]+)\{([^}]*)\}", line):
                    name, selector = match.group(1), match.group(2)
                    if name not in known:  # prefix-built at runtime
                        continue
                    for label in re.finditer(r"(\w+)\s*[=!]~?", selector):
                        if label.group(1) not in known[name]:
                            offenders.append(
                                f"{path.name}:{lineno} {name}{{{label.group(1)}=...}} "
                                f"valid={sorted(known[name])}",
                            )
        assert not offenders, "\n".join(offenders)


_DEMO_CONFIG = Path(__file__).resolve().parents[1] / "demo.yaml"


class TestLokiSelectorsMatchHandlerTags:
    """LogQL stream selectors must use the labels and JSON fields
    ``loki-logger-handler`` actually emits.
    """

    _LOG_VIEWING_PANEL_TITLES = frozenset(
        {
            "Error Log Details",
            "Data Monitor: Recent Logs",
            "Job Builder: Recent Logs",
            "Dispatcher: Recent Logs",
            "Log Search Results",
            "Logs by Correlation ID",
        },
    )

    def test_queries_use_the_tags_the_handler_sets(self) -> None:
        """``_create_loki_handler`` tags streams with ``service``.

        The panels queried ``service_name``, which no stream ever carried, so
        every Loki panel came back empty.
        """
        from courier.dashboard import loki_panels

        source = Path(loki_panels.__file__).read_text()
        assert 'service_name="' not in source
        assert 'service="$service"' in source

    def test_no_logfmt_parser(self) -> None:
        """``loki-logger-handler`` ships JSON lines, never logfmt pairs."""
        from courier.dashboard import loki_panels

        source = Path(loki_panels.__file__).read_text()
        assert "logfmt" not in source

    def test_log_viewing_panels_parse_json(self) -> None:
        """Panels that render raw log lines must ``| json`` before formatting.

        Every Loki line is a JSON object (``{"message": ..., "level": ...}``).
        A panel that applied ``line_format`` without first parsing it would
        format against the raw JSON blob instead of the extracted message
        text, so every log-viewing panel must carry both.
        """
        from courier.dashboard.config_parser import parse_config
        from courier.dashboard.loki_panels import build_loki_panels

        model = parse_config(str(_DEMO_CONFIG))
        rows = build_loki_panels(model, datasource="Loki")

        found_titles: set[str] = set()
        for row in rows:
            for panel in getattr(row, "panels", []) or []:
                if panel.title not in self._LOG_VIEWING_PANEL_TITLES:
                    continue
                found_titles.add(panel.title)
                for target in panel.targets:
                    expr = target["expr"]
                    assert "| json" in expr, expr
                    assert 'line_format "{{.message}}"' in expr, expr

        missing = self._LOG_VIEWING_PANEL_TITLES - found_titles
        assert not missing, f"expected log-viewing panels not generated: {missing}"

    def test_correlation_id_template_exists_and_is_referenced(self) -> None:
        """``$correlation_id`` must be both declared and used by a panel query.

        A template variable nobody queries is a dead dropdown; a query
        referencing a variable nobody declared renders literally as the
        string ``$correlation_id`` instead of being substituted.
        """
        from courier.dashboard.config_parser import parse_config
        from courier.dashboard.loki_panels import (
            build_loki_panels,
            build_loki_templates,
        )

        model = parse_config(str(_DEMO_CONFIG))
        template_names = {
            getattr(tmpl, "name", None) for tmpl in build_loki_templates(model)
        }
        assert "correlation_id" in template_names

        rows = build_loki_panels(model, datasource="Loki")
        exprs = [
            target["expr"]
            for row in rows
            for panel in getattr(row, "panels", []) or []
            for target in panel.targets
        ]
        assert any("$correlation_id" in expr for expr in exprs)

    @pytest.mark.parametrize("mode", ["UNIFIED", "SPLIT_BY_KIND", "SPLIT_BY_PLUGIN"])
    def test_trace_and_log_dashboards_share_one_correlation_id(self, mode: str) -> None:
        """Trace and log builders both declare ``$correlation_id``.

        Grafana rejects a dashboard with two variables of the same name, so
        a dashboard carrying both panel kinds must declare it exactly once.
        """
        from courier.dashboard.config_parser import parse_config
        from courier.dashboard.generator import generate_dashboard

        model = parse_config(str(_DEMO_CONFIG))
        for dashboard in generate_dashboard(model, mode=mode, loki_datasource="Loki"):
            names = [tmpl.name for tmpl in dashboard.templating.list]
            assert len(names) == len(set(names)), names

    def test_correlation_id_field_matches_dispatcher_extra_key(self) -> None:
        """The JSON field Loki panels query must match the ``extra=`` key dispatchers log with.

        ``src/courier/interfaces/dispatchers.py`` attaches the correlation id
        via ``extra={"correlation_id": ...}``, which ``loki-logger-handler``
        promotes to a top-level JSON field of the same name.  If that key
        were ever renamed on one side only, the dashboard's correlation-ID
        panel would silently come back empty.
        """
        import importlib.util

        from courier.dashboard import loki_panels

        spec = importlib.util.find_spec("courier.interfaces.dispatchers")
        assert spec is not None and spec.origin is not None
        dispatchers_source = Path(spec.origin).read_text()
        match = re.search(
            r'extra=\{"(\w+)":\s*job\.correlation_id\}', dispatchers_source
        )
        assert match, "could not find the correlation_id extra= key in dispatchers.py"
        extra_key = match.group(1)

        loki_source = Path(loki_panels.__file__).read_text()
        assert f"| {extra_key}" in loki_source


class TestLiveDetection:
    """``--live`` resolves identifiers, which is what parse_config matches on."""

    _METRICS = (
        'courier_plugin_state{plugin_name="serial_bash",'
        'plugin_identifier="my-dispatcher"} 3.0\n'
    )

    def test_identifier_label_is_preferred(self) -> None:
        states = _extract_plugin_states(self._METRICS)
        assert "my-dispatcher" in states
        assert states["my-dispatcher"] == 3

    def test_falls_back_to_plugin_name_when_identifier_absent(self) -> None:
        """Tolerates metrics scraped from a courier predating the label."""
        legacy = 'courier_plugin_state{plugin_name="serial_bash"} 3.0\n'
        assert _extract_plugin_states(legacy) == {"serial_bash": 3}

    def test_detect_active_plugins_never_raises_on_a_bad_sample(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``1e999`` parses as infinity, which no ``int`` holds: skip the sample."""
        import io
        import urllib.request

        from courier.constants import PluginRunState

        running = PluginRunState.RUNNING.value
        body = (
            'courier_plugin_state{plugin_identifier="broken"} 1e999\n'
            f'courier_plugin_state{{plugin_identifier="running"}} {running}\n'
        ).encode()

        class _Response(io.BytesIO):
            status = 200

        monkeypatch.setattr(
            urllib.request,
            "urlopen",
            lambda *_args, **_kwargs: _Response(body),
        )

        result = detect_active_plugins("metrics.invalid", 1)

        assert result.is_reachable
        assert result.plugin_states == {"running": running}
        assert result.identifiers == {"running"}


class TestDashboardKindMapping:
    """The dashboard must accept every config ``courier run`` accepts."""

    @pytest.mark.parametrize(
        ("kind", "expected"),
        [
            ("data_monitor", PluginKind.DATA_MONITOR),
            ("data_monitors", PluginKind.DATA_MONITOR),
            ("job_builder", PluginKind.JOB_BUILDER),
            ("job_builders", PluginKind.JOB_BUILDER),
            ("dispatcher", PluginKind.DISPATCHER),
            ("dispatchers", PluginKind.DISPATCHER),
            # normalize_kind tolerates case and whitespace, so the runtime
            # runs these; the dashboard used to drop them.
            ("Dispatcher", PluginKind.DISPATCHER),
            (" dispatcher ", PluginKind.DISPATCHER),
            ("DATA_MONITOR", PluginKind.DATA_MONITOR),
            ("Job_Builder", PluginKind.JOB_BUILDER),
            # A payload is configured under a builder, never a run step.
            ("payload", None),
            # Valid in a run list but not a runnable plugin -- skipped, not fatal.
            ("data_monitor_configs", None),
            ("nonsense", None),
        ],
    )
    def test_kind_mapping(self, kind: str, expected: PluginKind | None) -> None:
        assert _plugin_kind_or_none(kind) is expected
