"""CI guard: validate dashboard generator against the Prometheus metric registry.

Ensures the dashboard generator never references a metric name or label
that does not exist in ``courier/metrics.py``.  Also verifies that any
literal ``PluginRunState`` enum-value comparisons in PromQL match the
actual enum definitions.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from prometheus_client import REGISTRY

import courier.metrics  # noqa: F401 — registers all metric singletons

from courier.constants import PluginRunState
from courier.dashboard.config_parser import parse_config
from courier.dashboard.prometheus_panels import (
    build_prometheus_panels,
    build_prometheus_templates,
)

if TYPE_CHECKING:
    from collections.abc import Iterator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TEST_CONFIG_DIR = Path(__file__).resolve().parents[2]
_METRIC_PATTERN = re.compile(r"courier_[a-zA-Z_][a-zA-Z0-9_]*")


def _iter_target_expressions() -> Iterator[str]:
    """Yield every PromQL expression from panels generated for demo.yaml."""
    path = _TEST_CONFIG_DIR / "demo.yaml"
    model = parse_config(str(path))
    for row in build_prometheus_panels(model):
        for panel in getattr(row, "panels", []) or []:
            for target in getattr(panel, "targets", []) or []:
                yield target.expr


def _emitted_metric_families() -> dict[str, frozenset[str]]:
    """Collect every registered metric family with its label names.

    Also adds the suffixes that Prometheus appends for
    Counter (``_total``, ``_created``) and Histogram
    (``_sum``, ``_count``, ``_bucket``) families so that
    PromQL expressions referencing those derived names are not
    flagged as phantom.
    """
    families: dict[str, frozenset[str]] = {}
    for collector, names in REGISTRY._collector_to_names.items():
        base_labels = frozenset(getattr(collector, "_labelnames", ()) or ())
        for name in names:
            families[name] = base_labels
            for suffix in ("_total", "_created", "_sum", "_count", "_bucket"):
                families[f"{name}{suffix}"] = base_labels
    return families


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestGeneratorVsRegistry:
    """Dashboard generator must only reference metrics in the registry."""

    def test_no_phantom_metric_names(self) -> None:
        """Every ``courier_*`` name in generated PromQL must exist in the registry."""
        families = _emitted_metric_families()
        phantoms: set[str] = set()

        for expr in _iter_target_expressions():
            for name in _METRIC_PATTERN.findall(expr):
                # Strip trailing underscore (common with ``_total`` etc.
                # when the regex captures a partial token)
                clean = name.rstrip("_")
                if clean not in families:
                    phantoms.add(clean)

        assert not phantoms, (
            f"Generator references {len(phantoms)} metric(s) not in the registry: "
            f"{sorted(phantoms)}. "
            "Either add the metric to courier/metrics.py or update "
            "prometheus_panels.py to use an existing name."
        )

    def test_no_phantom_label_selectors(self) -> None:
        """Every ``{label=…}`` selector in generated PromQL must match the metric.

        When a single PromQL expression references multiple metrics (e.g.
        an ``or`` chain), a label selector is accepted if it matches
        *any* metric named in the same expression — it may belong to an
        adjacent operand.
        """
        families = _emitted_metric_families()
        phantoms: list[str] = []

        label_re = re.compile(r"\{([^}]+)\}")
        selector_re = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)\s*[=!~]')

        for expr in _iter_target_expressions():
            all_metrics_in_expr = set(_METRIC_PATTERN.findall(expr))
            # Collect valid labels across ALL metrics in this expression.
            valid_anywhere: set[str] = set()
            for m in all_metrics_in_expr:
                valid_anywhere.update(families.get(m.rstrip("_"), set()))

            for name_str in all_metrics_in_expr:
                clean = name_str.rstrip("_")
                expected = families.get(clean)
                if expected is None:
                    continue  # already caught by test_no_phantom_metric_names

                for block in label_re.findall(expr):
                    for selector in selector_re.findall(block):
                        if selector in expected:
                            continue
                        if selector in valid_anywhere:
                            continue
                        # Template variables like $plugin_filter are not labels
                        value_part = block.split(selector, 1)[1] if selector in block else ""
                        if value_part.lstrip("=!~\"'").startswith("$"):
                            continue
                        phantoms.append(
                            f"{clean}{{{selector}}} (valid: {sorted(expected)})"
                        )

        assert not phantoms, (
            f"Generator references {len(phantoms)} label(s) not on the metric:\n  "
            + "\n  ".join(sorted(set(phantoms)))
        )


class TestPluginRunStatePromQL:
    """PromQL literals that compare against PluginRunState must match the enum."""

    _STATE_VALUE_RE = re.compile(
        r"courier_plugin_state\b[^}]*\}\s*==\s*(\d+)"
    )

    def test_state_values_match_enum(self) -> None:
        """Every ``courier_plugin_state{…} == N`` must have N in PluginRunState."""
        valid_values = {s.value for s in PluginRunState}

        for expr in _iter_target_expressions():
            for match in self._STATE_VALUE_RE.finditer(expr):
                value = int(match.group(1))
                assert value in valid_values, (
                    f"PromQL references PluginRunState value {value}, "
                    f"but valid values are {sorted(valid_values)}. "
                    f"Expression: {expr.strip()}"
                )


class TestGeneratorTemplates:
    """Template variables used by dashboards must be internally consistent."""

    def test_template_domains_agree_with_labels(self) -> None:
        """All *plugin filters must use the plugin type-name domain.

        The metrics emitted at runtime label with ``self.name`` (the
        plugin's type ``ClassVar``, e.g. ``"serial_bash"``), not with
        the configuration identifier.  Template variables must
        therefore be populated from the same domain so the
        ``{…=~"$filter"}`` selectors actually match.
        """
        path = _TEST_CONFIG_DIR / "demo.yaml"
        model = parse_config(str(path))
        templates = build_prometheus_templates(model)

        # Collect all query-backed template variables that filter plugins.
        filters = {
            "dm_plugin": {dm.plugin_name for dm in model.data_monitors},
            "jb_plugin": {jb.plugin_name for jb in model.job_builders},
            "dp_plugin": {dp.plugin_name for dp in model.dispatchers},
        }

        type_names = {p.plugin_name for p in model.plugins}

        for tmpl in templates:
            name = getattr(tmpl, "name", "")
            if name not in ("plugin_filter",):
                continue

            query_values = set(getattr(tmpl, "query", "").split(","))
            extra = query_values - type_names
            assert not extra, (
                f"Template $plugin_filter contains values not in the type-name "
                f"domain: {sorted(extra)}. "
                "Use plugin_name (class name) instead of config identifier."
            )
