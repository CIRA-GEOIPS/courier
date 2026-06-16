"""Pipeline topology visualization panels for Grafana.

Generates Grafana panel objects that visualize the Courier pipeline
topology — showing the data flow from Data Monitors through Job
Builders to Dispatchers.  Uses grafanalib to create HTML table panels,
flow-rate indicators, and dependency-health views.
"""

from __future__ import annotations

import html as _html
import re as _re
from typing import TYPE_CHECKING

from grafanalib.core import (
    TABLE_TARGET_FORMAT,
    TEXT_MODE_HTML,
    Pixels,
    Row,
    Table,
    Target,
    Text,
)

if TYPE_CHECKING:
    from courier.dashboard.config_parser import DashboardModel


# ---------------------------------------------------------------------------
# RE2-safe regex escaping
# ---------------------------------------------------------------------------

# Characters that MUST be backslash-escaped in RE2/PromQL regex patterns.
# RE2 does NOT support escape sequences for characters like '-' and '#'
# that Python's re.escape() adds in Python >= 3.7.
_RE2_ESCAPE_RE = _re.compile(r"([.^$*+?{}\[\]()\\|])")


def _re2_escape(text: str) -> str:
    """Escape *text* for use as a literal inside a PromQL ``=~""`` regex.

    Escapes only the characters that are RE2 metacharacters.  Does **not**
    escape hyphens, exclamation marks, or hashes — those are literal in
    RE2 and ``\\-`` / ``\\!`` / ``\\#`` are invalid escape sequences.
    """
    return _RE2_ESCAPE_RE.sub(r"\\\1", text)


# ---------------------------------------------------------------------------
# Color palette for topology visualization
# ---------------------------------------------------------------------------

COLORS: dict[str, str] = {
    "DATA_MONITOR": "#33C7FF",      # cyan — data ingest
    "JOB_BUILDER": "#FFB357",       # amber — transformation
    "DISPATCHER": "#C85EFA",        # purple — execution
    "MUTED": "#8A9BB5",             # muted blue-gray for deps/de-emphasis
    "HEALTHY": "#73BF69",           # green — healthy/running
    "UNHEALTHY": "#F2495C",         # red — unhealthy/failed
    "LOCAL_HIGHLIGHT": "#F5E050",   # gold — highlights local plugins
    "ROW_BG": "#1E1E2E",            # dark row background
    "HEADER_BG": "#181825",         # slightly darker header
    "BORDER": "#45475A",            # subtle border
    "TEXT": "#CDD6F4",              # primary text (Catppuccin Text)
    "TEXT_MUTED": "#6C7086",        # muted text (Catppuccin Overlay0)
}

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

_CONFIG_VALUE_MAX_LEN: int = 32
"""Maximum length of a config value before truncation in summary tables."""

# ---------------------------------------------------------------------------
# Shared style fragments (embedded in HTML panels)
# ---------------------------------------------------------------------------

_CSS_RESET: str = (
    "margin:0;padding:0;border-collapse:collapse;"
    "font-family:'Fira Code', 'JetBrains Mono', monospace;"
    f"font-size:13px;color:{COLORS['TEXT']};"
)

_TABLE_STYLE: str = f"width:100%;{_CSS_RESET}"

_TH_STYLE: str = (
    "text-align:left;padding:8px 10px;"
    f"background:{COLORS['HEADER_BG']};"
    f"border-bottom:2px solid {COLORS['BORDER']};"
    "font-weight:600;text-transform:uppercase;font-size:11px;"
    f"letter-spacing:0.5px;color:{COLORS['TEXT_MUTED']};"
)

_TD_STYLE: str = (
    "padding:7px 10px;"
    f"border-bottom:1px solid {COLORS['BORDER']};"
    "vertical-align:middle;"
)

_BADGE_BASE: str = (
    "display:inline-block;padding:2px 8px;border-radius:4px;"
    "font-size:11px;font-weight:700;text-transform:uppercase;"
)

_FLOW_ARROW: str = (
    f'<span style="color:{COLORS["MUTED"]};margin:0 6px;">\u2192</span>'
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_topology_panels(
    model: DashboardModel,
    *,
    datasource: str = "Prometheus",
) -> list[Row]:
    """Generate pipeline topology visualization panels.

    Returns a list of :class:`~grafanalib.core.Row` objects containing
    :class:`~grafanalib.core.Text` and :class:`~grafanalib.core.Table`
    panels that visualize the pipeline structure, flow rates between
    stages, and upstream / downstream dependency health.

    Parameters
    ----------
    model : DashboardModel
        Parsed dashboard model describing the courier pipeline.
    datasource : str
        Grafana datasource name for Prometheus queries
        (default: ``"Prometheus"``).

    Returns
    -------
    list[Row]
        One Row per section (topology, flow rates, dependency health,
        summary).  Some rows are conditionally omitted when the model
        has no routing edges or is not a sub-section.
    """
    panels: list[Row] = []

    # 1. Pipeline topology HTML table (always)
    panels.append(_build_topology_row(model))

    # 2. Flow rate table (only when routing edges exist)
    if model.routing:
        panels.append(_build_flow_rate_row(model, datasource))

    # 3. Dependency health (only for sub-sections with dependencies)
    if model.is_sub_section and (
        model.upstream_dependencies or model.downstream_dependencies
    ):
        panels.append(_build_dependency_health_row(model, datasource))

    # 4. Pipeline summary (always)
    panels.append(_build_summary_row(model))

    return panels


# ---------------------------------------------------------------------------
# Row builders
# ---------------------------------------------------------------------------


def _build_topology_row(model: DashboardModel) -> Row:
    """Build the Pipeline Topology row — an HTML table showing every plugin."""
    if not model.plugins:
        return Row(
            title="Pipeline Topology",
            height=Pixels(80),
            panels=[
                Text(
                    title="",
                    mode=TEXT_MODE_HTML,
                    content=(
                        f"<div style='padding:16px;"
                        f"color:{COLORS['TEXT_MUTED']};"
                        f"font-style:italic;'>"
                        "No plugins configured in this pipeline."
                        "</div>"
                    ),
                ),
            ],
        )

    html_rows: list[str] = []
    for i, plugin in enumerate(model.plugins, 1):
        kind_name = plugin.kind.name
        kind_color = COLORS.get(kind_name, COLORS["MUTED"])
        is_local = (
            model.local_identifiers is not None
            and plugin.identifier in model.local_identifiers
        )

        # Highlight local plugins in sub-section mode
        row_style = ""
        id_style = ""
        if is_local:
            row_style = (
                f"background:{COLORS['ROW_BG']};"
                f"border-left:3px solid {COLORS['LOCAL_HIGHLIGHT']};"
            )
            id_style = (
                f"color:{COLORS['LOCAL_HIGHLIGHT']};font-weight:700;"
            )

        # Target display — show flow arrows when routing downstream
        if plugin.targets:
            target_spans = _FLOW_ARROW.join(
                (
                    f'<code style="font-size:12px;'
                    f'color:{COLORS["TEXT"]};">'
                    f'{_html.escape(t)}</code>'
                )
                for t in plugin.targets
            )
            target_cell = f"{_FLOW_ARROW}{target_spans}"
        else:
            target_cell = (
                f'<span style="color:{COLORS["TEXT_MUTED"]};">'
                "\u2014</span>"
            )

        config_summary = _build_config_summary(plugin.config)
        badge = _build_kind_badge(kind_name, kind_color)

        # Build the row HTML with f-string for the wrapper, using
        # pre-built fragments to stay under the line-length limit.
        code_open = (
            f'<code style="font-size:12px;color:{COLORS["TEXT"]};">'
        )
        code_close = "</code>"
        td_meta = (
            f"font-size:11px;color:{COLORS['TEXT_MUTED']};"
            "max-width:200px;overflow:hidden;"
            "text-overflow:ellipsis;white-space:nowrap;"
        )

        html_rows.append(
            f"<tr style='{row_style}'>"
            f"<td style='{_TD_STYLE}color:{COLORS['TEXT_MUTED']};"
            f"width:30px;'>{i}</td>"
            f"<td style='{_TD_STYLE}width:120px;'>{badge}</td>"
            f"<td style='{_TD_STYLE}{id_style}'>"
            f"{_html.escape(plugin.identifier)}</td>"
            f"<td style='{_TD_STYLE}'>"
            f"{code_open}{_html.escape(plugin.plugin_name)}{code_close}</td>"
            f"<td style='{_TD_STYLE}{td_meta}'>"
            f"{config_summary if config_summary else '\u2014'}</td>"
            f"<td style='{_TD_STYLE}'>{target_cell}</td>"
            f"</tr>",
        )

    header = (
        "<tr>"
        f"<th style='{_TH_STYLE}width:30px;'>#</th>"
        f"<th style='{_TH_STYLE}width:120px;'>Kind</th>"
        f"<th style='{_TH_STYLE}'>Identifier</th>"
        f"<th style='{_TH_STYLE}'>Plugin</th>"
        f"<th style='{_TH_STYLE}'>Config</th>"
        f"<th style='{_TH_STYLE}'>Targets</th>"
        "</tr>"
    )

    content = (
        f"<table style='{_TABLE_STYLE}'>"
        f"{header}"
        f"{''.join(html_rows)}"
        f"</table>"
    )

    # Height: 50px header + ~34px per plugin row
    row_height = max(120, 50 + len(model.plugins) * 34)

    return Row(
        title="Pipeline Topology",
        height=Pixels(row_height),
        panels=[
            Text(
                title="",
                mode=TEXT_MODE_HTML,
                content=content,
            ),
        ],
    )


def _build_flow_rate_row(model: DashboardModel, datasource: str) -> Row:
    """Build the Flow Rates row — PromQL table of jobs/sec per routing edge."""
    edges = _collect_routing_edges(model)
    if not edges:
        return Row(
            title="Pipeline Flow Rates",
            height=Pixels(80),
            panels=[
                Text(
                    title="",
                    mode=TEXT_MODE_HTML,
                    content=(
                        f"<div style='padding:16px;"
                        f"color:{COLORS['TEXT_MUTED']};"
                        f"font-style:italic;'>"
                        "No routing edges defined (builders have no targets)."
                        "</div>"
                    ),
                ),
            ],
        )

    # Build a regex matching all builder identifiers in the model so a single
    # PromQL query returns every active edge at once.
    builder_pattern = "|".join(
        _re2_escape(builder_id) for builder_id in model.routing
    )

    targets = [
        Target(
            expr=(
                "rate(courier_job_builder_jobs_emitted_total{"
                f'job_builder_name=~"{builder_pattern}"'
                "}[5m])"
            ),
            format=TABLE_TARGET_FORMAT,
            instant=True,
            refId="A",
            legendFormat="{{job_builder_name}} → {{target}}",
        ),
    ]

    # Include dispatcher throughput as a second target when dispatchers exist
    if model.dispatchers:
        dispatcher_pattern = "|".join(
            _re2_escape(d.identifier) for d in model.dispatchers
        )
        targets.append(
            Target(
                expr=(
                    "rate(courier_dispatcher_jobs_consumed_total{"
                    f'dispatcher_identifier=~"{dispatcher_pattern}"'
                    "}[5m])"
                ),
                format=TABLE_TARGET_FORMAT,
                instant=True,
                refId="B",
                legendFormat="{{dispatcher_identifier}} consumed",
            ),
        )

    return Row(
        title="Pipeline Flow Rates",
        height=Pixels(250),
        panels=[
            Table(
                title="Jobs / second by routing edge",
                dataSource=datasource,
                targets=targets,
                showHeader=True,
            ),
        ],
    )


def _build_dependency_health_row(
    model: DashboardModel,
    datasource: str,
) -> Row:
    """Build the Dependency Health row for sub-section dashboards."""
    panels: list = []
    up_count = len(model.upstream_dependencies)
    down_count = len(model.downstream_dependencies)
    total_deps = up_count + down_count

    # Upstream dependencies table
    if model.upstream_dependencies:
        panels.append(
            _build_dep_table(
                title="Upstream Dependencies",
                identifiers=sorted(model.upstream_dependencies),
                datasource=datasource,
            ),
        )
    else:
        panels.append(
            Text(
                title="Upstream Dependencies",
                mode=TEXT_MODE_HTML,
                content=(
                    f"<div style='padding:16px;"
                    f"color:{COLORS['TEXT_MUTED']};"
                    f"font-style:italic;'>"
                    "No upstream dependencies."
                    "</div>"
                ),
            ),
        )

    # Downstream dependencies table
    if model.downstream_dependencies:
        panels.append(
            _build_dep_table(
                title="Downstream Dependencies",
                identifiers=sorted(model.downstream_dependencies),
                datasource=datasource,
            ),
        )
    else:
        panels.append(
            Text(
                title="Downstream Dependencies",
                mode=TEXT_MODE_HTML,
                content=(
                    f"<div style='padding:16px;"
                    f"color:{COLORS['TEXT_MUTED']};"
                    f"font-style:italic;'>"
                    "No downstream dependencies."
                    "</div>"
                ),
            ),
        )

    return Row(
        title="Dependency Health",
        height=Pixels(200 + max(total_deps, 1) * 20),
        panels=panels,
    )


def _build_summary_row(model: DashboardModel) -> Row:
    """Build the Pipeline Summary row — comprehensive table with config hints."""
    if not model.plugins:
        return Row(
            title="Pipeline Summary",
            height=Pixels(80),
            panels=[
                Text(
                    title="",
                    mode=TEXT_MODE_HTML,
                    content=(
                        f"<div style='padding:16px;"
                        f"color:{COLORS['TEXT_MUTED']};"
                        f"font-style:italic;'>"
                        "No pipeline stages configured."
                        "</div>"
                    ),
                ),
            ],
        )

    html_rows: list[str] = []
    for i, plugin in enumerate(model.plugins, 1):
        kind_name = plugin.kind.name
        kind_color = COLORS.get(kind_name, COLORS["MUTED"])

        config_keys = (
            ", ".join(sorted(plugin.config.keys()))
            if plugin.config
            else "\u2014"
        )

        target_list = ", ".join(plugin.targets) if plugin.targets else "\u2014"

        badge = _build_kind_badge(kind_name, kind_color)

        code_open = (
            f'<code style="font-size:12px;color:{COLORS["TEXT"]};">'
        )
        code_close = "</code>"
        td_meta = (
            f"font-size:11px;color:{COLORS['TEXT_MUTED']};"
            "max-width:200px;overflow:hidden;"
            "text-overflow:ellipsis;white-space:nowrap;"
        )

        html_rows.append(
            f"<tr>"
            f"<td style='{_TD_STYLE}color:{COLORS['TEXT_MUTED']};"
            f"width:30px;'>{i}</td>"
            f"<td style='{_TD_STYLE}'>{badge}</td>"
            f"<td style='{_TD_STYLE}'>"
            f"{code_open}{_html.escape(plugin.identifier)}{code_close}</td>"
            f"<td style='{_TD_STYLE}'>"
            f"{code_open}{_html.escape(plugin.plugin_name)}{code_close}</td>"
            f"<td style='{_TD_STYLE}{td_meta}'>{config_keys}</td>"
            f"<td style='{_TD_STYLE}font-size:12px;'>"
            f"{_html.escape(target_list)}</td>"
            f"</tr>",
        )

    header = (
        "<tr>"
        f"<th style='{_TH_STYLE}width:30px;'>#</th>"
        f"<th style='{_TH_STYLE}width:100px;'>Kind</th>"
        f"<th style='{_TH_STYLE}'>Identifier</th>"
        f"<th style='{_TH_STYLE}'>Plugin</th>"
        f"<th style='{_TH_STYLE}'>Config Keys</th>"
        f"<th style='{_TH_STYLE}'>Targets</th>"
        "</tr>"
    )

    content = (
        f"<table style='{_TABLE_STYLE}'>"
        f"{header}"
        f"{''.join(html_rows)}"
        f"</table>"
    )

    # Height: header + rows + sub-section note if applicable
    row_height = 50 + len(model.plugins) * 34
    if model.is_sub_section:
        row_height += 30

    return Row(
        title="Pipeline Summary",
        height=Pixels(row_height),
        panels=[
            Text(
                title="",
                mode=TEXT_MODE_HTML,
                content=content,
            ),
        ],
    )


# ---------------------------------------------------------------------------
# HTML building helpers
# ---------------------------------------------------------------------------


def _build_kind_badge(kind_name: str, color: str) -> str:
    """Render a small coloured badge for a plugin kind."""
    return (
        f'<span style="{_BADGE_BASE}'
        f"background:{color};color:#fff;\">"
        f"{_html.escape(kind_name)}</span>"
    )


def _build_config_summary(config: dict, max_keys: int = 3) -> str:
    """Summarise a plugin config dict as a compact string.

    Shows the first *max_keys* keys with their values.  Values longer
    than *_CONFIG_VALUE_MAX_LEN* characters are truncated.
    """
    if not config:
        return ""

    items: list[str] = []
    for key, value in list(config.items())[:max_keys]:
        val_str = str(value)
        if len(val_str) > _CONFIG_VALUE_MAX_LEN:
            val_str = val_str[:_CONFIG_VALUE_MAX_LEN - 3] + "..."
        items.append(
            f"<span style='color:{COLORS['TEXT']};'>"
            f"{_html.escape(key)}</span>"
            f"<span style='color:{COLORS['TEXT_MUTED']};'>"
            f"={_html.escape(val_str)}</span>",
        )

    return ", ".join(items)


# ---------------------------------------------------------------------------
# Dependency helpers
# ---------------------------------------------------------------------------


def _collect_routing_edges(
    model: DashboardModel,
) -> list[tuple[str, str]]:
    """Collect every (builder_id, dispatcher_id) routing edge from the model."""
    edges: list[tuple[str, str]] = []
    for builder_id, targets in model.routing.items():
        for target_id in targets:
            edges.append((builder_id, target_id))
    return edges


def _build_dep_table(
    title: str,
    identifiers: list[str],
    datasource: str,
) -> Table | Text:
    """Build a Table (PromQL) or Text (static) panel for dependencies.

    When *identifiers* is non-empty, creates a Prometheus Table panel
    querying ``courier_plugin_state`` for each dependency so the user
    can see live health status.  Otherwise returns an empty-state Text
    panel.
    """
    if not identifiers:
        return Text(
            title=title,
            mode=TEXT_MODE_HTML,
            content=(
                f"<div style='padding:16px;"
                f"color:{COLORS['TEXT_MUTED']};"
                f"font-style:italic;'>"
                "None"
                "</div>"
            ),
        )

    identifier_pattern = "|".join(_re2_escape(i) for i in identifiers)

    return Table(
        title=title,
        dataSource=datasource,
        targets=[
            Target(
                expr=(
                    "courier_plugin_state{"
                    f'plugin_name=~"{identifier_pattern}"'
                    "}"
                ),
                format=TABLE_TARGET_FORMAT,
                instant=True,
                refId="A",
                legendFormat="{{plugin_name}}",
            ),
        ],
        showHeader=True,
    )


# ---------------------------------------------------------------------------
# Sub-section header (exported for use by the dashboard generator)
# ---------------------------------------------------------------------------


def build_subsection_header(
    model: DashboardModel,
    *,
    datasource: str = "Prometheus",  # noqa: ARG001 — reserved for future use
) -> Row | None:
    """Build a sub-section header row.

    Returns a Row with a Text panel describing the sub-section scope,
    or ``None`` when *model* is not a sub-section dashboard.

    Parameters
    ----------
    model : DashboardModel
        Parsed dashboard model.
    datasource : str
        Grafana datasource name (unused here — reserved for future use).

    Returns
    -------
    Row or None
    """
    if not model.is_sub_section:
        return None

    local_ids = model.local_identifiers or set()
    if not local_ids:
        return None

    id_spans: list[str] = []
    code_fmt = (
        f"<code style='color:{COLORS['LOCAL_HIGHLIGHT']};"
        f"font-weight:700;'>"
    )
    for i in sorted(local_ids):
        id_spans.append(f"{code_fmt}{_html.escape(i)}</code>")
    id_list = ", ".join(id_spans)

    content = (
        "<div style='padding:12px 16px;font-size:14px;'>"
        f"<span style='color:{COLORS['TEXT']};'>"
        "Sub-section dashboard for: </span>"
        f"{id_list}"
        "</div>"
    )

    return Row(
        title="Sub-section Scope",
        height=Pixels(60),
        panels=[
            Text(
                title="",
                mode=TEXT_MODE_HTML,
                content=content,
            ),
        ],
    )
