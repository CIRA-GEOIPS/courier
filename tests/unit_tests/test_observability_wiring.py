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
from courier.dashboard.live_detector import _extract_plugin_states
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
            DATA_MONITOR_FILES_PROCESSED, "monitor_name", "cl_counter",
        )
        assert result, "counter samples missing"
        # Keys are "<sample_name>_{labels}"; check the sample-name portion.
        sample_names = {key.split("_{")[0] for key in result}
        assert any(name.endswith("_total") for name in sample_names), sample_names

    def test_histogram_sum_and_count_are_returned(self) -> None:
        DATA_MONITOR_SCAN_DURATION.labels(
            monitor_name="cl_hist", monitor_identifier="dm-1",
        ).observe(0.25)
        result = collect_labeled(
            DATA_MONITOR_SCAN_DURATION, "monitor_name", "cl_hist",
        )
        suffixes = {name.split("_{")[0].rsplit("_", 1)[-1] for name in result}
        assert {"sum", "count"} <= suffixes

    def test_gauge_still_works(self) -> None:
        DATA_MONITOR_LAST_SCAN_TIMESTAMP.labels(
            monitor_name="cl_gauge", monitor_identifier="dm-1",
        ).set(5)
        result = collect_labeled(
            DATA_MONITOR_LAST_SCAN_TIMESTAMP, "monitor_name", "cl_gauge",
        )
        assert [entry["value"] for entry in result.values()] == [5.0]

    def test_bookkeeping_samples_are_excluded(self) -> None:
        """``_created`` and ``_bucket`` are not measurements."""
        DATA_MONITOR_SCAN_DURATION.labels(
            monitor_name="cl_excl", monitor_identifier="dm-1",
        ).observe(0.5)
        result = collect_labeled(
            DATA_MONITOR_SCAN_DURATION, "monitor_name", "cl_excl",
        )
        assert not any("_created" in n or "_bucket" in n for n in result)

    def test_other_label_values_are_filtered_out(self) -> None:
        DATA_MONITOR_FILES_PROCESSED.labels(
            monitor_name="cl_keep", monitor_identifier="dm-1", status="success",
        ).inc()
        DATA_MONITOR_FILES_PROCESSED.labels(
            monitor_name="cl_drop", monitor_identifier="dm-1", status="success",
        ).inc()
        result = collect_labeled(
            DATA_MONITOR_FILES_PROCESSED, "monitor_name", "cl_keep",
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


class TestLokiSelectorsMatchHandlerTags:
    """LogQL stream selectors must use the labels the Loki handler emits."""

    def test_queries_use_the_tags_the_handler_sets(self) -> None:
        """``_create_loki_handler`` tags streams with ``service``.

        The panels queried ``service_name``, which no stream ever carried, so
        every Loki panel came back empty.
        """
        from courier.dashboard import loki_panels

        source = Path(loki_panels.__file__).read_text()
        assert 'service_name="' not in source
        assert 'service="$service"' in source

    def test_no_logfmt_parser_on_plain_text_lines(self) -> None:
        """python-logging-loki ships the formatted message, not logfmt pairs."""
        from courier.dashboard import loki_panels

        source = Path(loki_panels.__file__).read_text()
        assert "logfmt" not in source
        assert "{{.message}}" not in source


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
            # Valid in a run list but not a runnable plugin -- skipped, not fatal.
            ("data_monitor_configs", None),
            ("nonsense", None),
        ],
    )
    def test_kind_mapping(self, kind: str, expected: PluginKind | None) -> None:
        assert _plugin_kind_or_none(kind) is expected
