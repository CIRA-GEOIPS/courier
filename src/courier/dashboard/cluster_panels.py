"""Cluster sub-section support panels for Grafana dashboards.

When a Courier service is deployed across a cluster (multiple nodes/copies
of the service config, each running a subset of the pipeline), this module
generates panels that show cross-node boundary metrics, peer plugin health,
and sub-section identification.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from grafanalib.core import (
    GAUGE_CALC_LAST,
    SECONDS_FORMAT,
    GridPos,
    RowPanel,
    Stat,
    StatValueMappingItem,
    StatValueMappings,
    Table,
    Target,
    Text,
    Threshold,
)

if TYPE_CHECKING:
    from courier.dashboard.config_parser import DashboardModel

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_COURIER_PREFIX: str = "courier"

# Grid layout dimensions (Grafana grid is 24 units wide)
_GRID_FULL: int = 24
_ROW_H: int = 1
_STAT_H: int = 4
_STAT_W: int = 6

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _classify(identifier: str, local_ids: set[str]) -> str:
    """Return ``"local"`` or ``"remote"`` for a plugin identifier."""
    return "local" if identifier in local_ids else "remote"


def _make_target(
    expr: str,
    *,
    ref: str = "A",
    instant: bool = False,
    legend: str = "",
    datasource: str = "$datasource",
) -> Target:
    """Create a grafanalib Prometheus Target with the given expression."""
    return Target(
        expr=expr,
        legendFormat=legend,
        refId=ref,
        instant=instant,
        datasource=datasource,
    )


def _html_escape(text: str) -> str:
    """Minimal HTML escaping for inline text-panel content."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _format_plugin_list(identifiers: set[str]) -> str:
    """Return a comma-separated, sorted string of identifiers.

    Returns ``"(none)"`` when *identifiers* is empty.
    """
    if not identifiers:
        return "(none)"
    return ", ".join(sorted(identifiers))


def _state_mappings() -> StatValueMappings:
    """Return value-to-colour mappings for discrete PluginRunState values.

    Green  for RUNNING (3), STARTING (2), RESTARTING (6).
    Amber  for STOPPING (4).
    Red    for STOPPED (1), FAILED (5).
    """
    return StatValueMappings(
        StatValueMappingItem("STOPPED", "1", "red"),
        StatValueMappingItem("STARTING", "2", "green"),
        StatValueMappingItem("RUNNING", "3", "green"),
        StatValueMappingItem("STOPPING", "4", "#EAB308"),
        StatValueMappingItem("FAILED", "5", "red"),
        StatValueMappingItem("RESTARTING", "6", "green"),
    )


def _find_boundary_edges(
    model: DashboardModel,
) -> list[dict[str, str]]:
    """Find routing edges that cross the local/remote boundary.

    Returns
    -------
    list[dict]
        Each dict has keys ``source``, ``source_loc``, ``target``,
        ``target_loc``, and ``direction``.
    """
    local_ids = model.local_identifiers or set()
    if not local_ids:
        return []

    edges: list[dict[str, str]] = []

    for builder in model.job_builders:
        builder_loc = _classify(builder.identifier, local_ids)
        for target in builder.targets:
            target_loc = _classify(target, local_ids)

            # Skip edges that are entirely internal or entirely remote.
            if builder_loc == target_loc:
                continue

            direction = "outbound" if builder_loc == "local" else "inbound"
            edges.append(
                {
                    "source": builder.identifier,
                    "source_loc": builder_loc,
                    "target": target,
                    "target_loc": target_loc,
                    "direction": direction,
                },
            )

    return edges


# ---------------------------------------------------------------------------
# Row builders
# ---------------------------------------------------------------------------


def _build_node_identity(
    model: DashboardModel,
    datasource: str,  # noqa: ARG001
    y: int,
) -> list:
    """Build the Node Identity row — a single Text panel with HTML content.

    Shows which plugins run locally and which are remote dependencies.
    """
    local_ids = model.local_identifiers or set()
    upstream = model.upstream_dependencies
    downstream = model.downstream_dependencies

    content_lines: list[str] = [
        '<div style="font-family: monospace; font-size: 14px;">',
        "<p>",
        "<b>This node runs:</b> ",
        _html_escape(_format_plugin_list(local_ids)),
        "</p>",
        "<p>",
        "<b>Upstream dependencies</b> (feeding into this node): ",
        _html_escape(_format_plugin_list(upstream)),
        "</p>",
        "<p>",
        "<b>Downstream dependencies</b> (consuming from this node): ",
        _html_escape(_format_plugin_list(downstream)),
        "</p>",
        "</div>",
    ]

    text_panel = Text(
        title="Node Identity",
        content="".join(content_lines),
        mode="html",
        gridPos=GridPos(h=6, w=_GRID_FULL, x=0, y=y + _ROW_H),
    )

    return [
        RowPanel(
            title="Cluster — Node Identity",
            gridPos=GridPos(h=_ROW_H, w=_GRID_FULL, x=0, y=y),
            panels=[text_panel],
        ),
    ]


def _build_boundary_metrics(
    model: DashboardModel,
    datasource: str,
    y: int,
) -> list:
    """Build the Boundary Metrics row.

    Contains broker message rate Stat panels and a boundary-flow
    Text panel showing routing edges that cross node boundaries.
    """
    edges = _find_boundary_edges(model)
    inner_y = y + _ROW_H

    # ---- Broker message rate panels ---------------------------------------
    msg_received = Stat(
        title="Broker Msg Received /s",
        datasource=datasource,
        targets=[
            _make_target(
                f"rate({_COURIER_PREFIX}_broker_messages_received_total[5m])",
                legend="{{queue_name}}",
                datasource=datasource,
            ),
        ],
        reduceCalc=GAUGE_CALC_LAST,
        gridPos=GridPos(h=_STAT_H, w=6, x=0, y=inner_y),
    )

    msg_sent = Stat(
        title="Broker Msg Sent /s",
        datasource=datasource,
        targets=[
            _make_target(
                f"rate({_COURIER_PREFIX}_broker_messages_sent_total[5m])",
                legend="{{queue_name}}",
                datasource=datasource,
            ),
        ],
        reduceCalc=GAUGE_CALC_LAST,
        gridPos=GridPos(h=_STAT_H, w=6, x=6, y=inner_y),
    )

    # ---- Boundary flow summary — Text panel with HTML table ----------------
    if not edges:
        flow_content = "<p>No cross-node routing edges detected.</p>"
    else:
        rows_html: list[str] = []
        for edge in edges:
            rows_html.append(
                "<tr>"
                f"<td>{_html_escape(edge['source'])} ({edge['source_loc']})</td>"
                f"<td>{_html_escape(edge['target'])} ({edge['target_loc']})</td>"
                f"<td>{edge['direction']}</td>"
                "</tr>",
            )

        flow_content = (
            '<div style="font-family: monospace; font-size: 13px;">'
            "<table>"
            "<tr>"
            "<th>Source</th><th>Target</th><th>Direction</th>"
            "</tr>"
            + "".join(rows_html)
            + "</table>"
            "</div>"
        )

    flow_panel = Text(
        title="Boundary Flow Edges",
        content=flow_content,
        mode="html",
        gridPos=GridPos(h=6, w=12, x=12, y=inner_y),
    )

    return [
        RowPanel(
            title="Cluster — Boundary Metrics",
            gridPos=GridPos(h=_ROW_H, w=_GRID_FULL, x=0, y=y),
            panels=[msg_received, msg_sent, flow_panel],
        ),
    ]


def _build_peer_health(
    model: DashboardModel,
    datasource: str,
    y: int,
) -> list | None:
    """Build the Peer Health row — Stat panels for each dependency.

    Returns ``None`` when there are no upstream or downstream dependencies.

    Each Stat panel queries ``courier_plugin_state`` for the dependency
    and uses colour thresholds to indicate health.
    """
    upstream = sorted(model.upstream_dependencies)
    downstream = sorted(model.downstream_dependencies)

    if not upstream and not downstream:
        return None

    inner_y = y + _ROW_H
    stats: list[Stat] = []
    peers_per_row = 4
    state_mappings = _state_mappings()

    def _next_position(index: int) -> tuple[int, int]:
        """Return (x, y_offset) for the panel at *index* (0-based)."""
        row_idx = index // peers_per_row
        col = (index % peers_per_row) * _STAT_W
        return col, inner_y + row_idx * _STAT_H

    for i, dep_id in enumerate(upstream):
        x, py = _next_position(i)
        stats.append(
            Stat(
                title=f"Upstream: {dep_id}",
                datasource=datasource,
                targets=[
                    _make_target(
                        f'{_COURIER_PREFIX}_plugin_state{{plugin_identifier="{dep_id}"}}',
                        instant=True,
                        datasource=datasource,
                    ),
                ],
                reduceCalc=GAUGE_CALC_LAST,
                colorMode="background",
                graphMode="none",
                mappings=[state_mappings],
                gridPos=GridPos(h=_STAT_H, w=_STAT_W, x=x, y=py),
            ),
        )

    offset = len(upstream)
    for i, dep_id in enumerate(downstream):
        x, py = _next_position(offset + i)
        stats.append(
            Stat(
                title=f"Downstream: {dep_id}",
                datasource=datasource,
                targets=[
                    _make_target(
                        f'{_COURIER_PREFIX}_plugin_state{{plugin_identifier="{dep_id}"}}',
                        instant=True,
                        datasource=datasource,
                    ),
                ],
                reduceCalc=GAUGE_CALC_LAST,
                colorMode="background",
                graphMode="none",
                mappings=[state_mappings],
                gridPos=GridPos(h=_STAT_H, w=_STAT_W, x=x, y=py),
            ),
        )

    return [
        RowPanel(
            title="Cluster — Peer Health",
            gridPos=GridPos(h=_ROW_H, w=_GRID_FULL, x=0, y=y),
            panels=stats,
        ),
    ]


def _build_host_state_table(
    model: DashboardModel,
    datasource: str,
    y: int,
) -> list:
    """Build the Plugin State by Host row.

    Queries ``courier_plugin_state`` for all known plugins and displays
    the result as a Table.  If the metric lacks a ``hostname`` label,
    operators can use Prometheus ``instance`` labels to distinguish hosts.
    """
    # Collect all plugin identifiers involved in this sub-section.
    local_ids = model.local_identifiers or set()
    all_relevant = sorted(
        local_ids | model.upstream_dependencies | model.downstream_dependencies,
    )

    if not all_relevant:
        # No plugins to query — show a note.
        note = Text(
            title="Plugin State by Host",
            content="<p>No plugins in this sub-section.</p>",
            mode="html",
            gridPos=GridPos(h=4, w=_GRID_FULL, x=0, y=y + _ROW_H),
        )
        return [
            RowPanel(
                title="Cluster — Plugin State by Host",
                gridPos=GridPos(h=_ROW_H, w=_GRID_FULL, x=0, y=y),
                panels=[note],
            ),
        ]

    # all_relevant holds YAML run identifiers, so the selector must match on
    # plugin_identifier -- plugin_name carries the plugin class name.
    plugin_pattern = "|".join(all_relevant)

    state_table = Table(
        title="Plugin State by Host",
        datasource=datasource,
        targets=[
            _make_target(
                f'{_COURIER_PREFIX}_plugin_state{{plugin_identifier=~"{plugin_pattern}"}}',
                instant=True,
                datasource=datasource,
            ),
        ],
        gridPos=GridPos(h=8, w=_GRID_FULL, x=0, y=y + _ROW_H),
    )

    return [
        RowPanel(
            title="Cluster — Plugin State by Host",
            gridPos=GridPos(h=_ROW_H, w=_GRID_FULL, x=0, y=y),
            panels=[state_table],
        ),
    ]


def _build_peer_latency(
    model: DashboardModel,
    datasource: str,
    y: int,
) -> list | None:
    """Build the Peer Latency row.

    For each upstream dependency, shows a Stat panel computing
    ``time() - last_processed_timestamp`` so operators can see how long
    it has been since the upstream plugin last produced data.

    Returns ``None`` when there are no upstream dependencies.
    """
    upstream = sorted(model.upstream_dependencies)

    if not upstream:
        return None

    inner_y = y + _ROW_H
    stats: list[Stat] = []
    peers_per_row = 4

    for i, dep_id in enumerate(upstream):
        row_idx = i // peers_per_row
        x = (i % peers_per_row) * _STAT_W
        py = inner_y + row_idx * _STAT_H

        stats.append(
            Stat(
                title=f"Up Latency: {dep_id}",
                datasource=datasource,
                targets=[
                    _make_target(
                        (
                            "time() - "
                            f'{_COURIER_PREFIX}_data_monitor_last_processed_timestamp_seconds'
                            f'{{monitor_identifier="{dep_id}"}}'
                        ),
                        instant=True,
                        datasource=datasource,
                    ),
                ],
                reduceCalc=GAUGE_CALC_LAST,
                format=SECONDS_FORMAT,
                colorMode="background",
                graphMode="none",
                thresholds=[
                    Threshold("green", 0, 0.0),
                    Threshold("green", 1, 30.0),
                    Threshold("#EAB308", 31, 120.0),
                    Threshold("red", 121, 1_000_000.0),
                ],
                gridPos=GridPos(h=_STAT_H, w=_STAT_W, x=x, y=py),
            ),
        )

    return [
        RowPanel(
            title="Cluster — Peer Latency",
            gridPos=GridPos(h=_ROW_H, w=_GRID_FULL, x=0, y=y),
            panels=stats,
        ),
    ]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_cluster_panels(
    model: DashboardModel,
    *,
    datasource: str = "$datasource",
) -> list | None:
    """Generate cross-node boundary and peer-health panels.

    Only generates panels when the model represents a sub-section
    (i.e., ``model.is_sub_section`` is ``True``).

    Parameters
    ----------
    model : DashboardModel
        Parsed dashboard model from
        :func:`~courier.dashboard.config_parser.parse_config`.
    datasource : str
        Grafana datasource name or UID (default: ``"$datasource"``).

    Returns
    -------
    list or None
        Flat list of :class:`~grafanalib.core.RowPanel` and child panel
        objects ready to merge into a dashboard, or ``None`` when the
        model is for a full pipeline (no sub-section context).
    """
    # ------------------------------------------------------------------
    # Law 1 (Early Exit): full pipeline — no cluster context to render.
    # ------------------------------------------------------------------
    if not model.is_sub_section:
        return None

    # Law 5 (Intentional Naming): y cursor tracks absolute grid row.
    panels: list = []
    y_cursor = 0

    # 1. Node Identity — which plugins are local vs remote.
    panels.extend(_build_node_identity(model, datasource, y_cursor))
    y_cursor += _ROW_H + 6

    # 2. Boundary Metrics — broker rates and cross-node flow table.
    panels.extend(_build_boundary_metrics(model, datasource, y_cursor))
    y_cursor += _ROW_H + _STAT_H

    # 3. Peer Health — Stat panels for upstream/downstream dependencies.
    peer = _build_peer_health(model, datasource, y_cursor)
    if peer is not None:
        panels.extend(peer)
        n_peers = len(model.upstream_dependencies) + len(model.downstream_dependencies)
        peer_rows = max(1, (n_peers + 3) // 4)  # ceil division by 4
        y_cursor += _ROW_H + peer_rows * _STAT_H

    # 4. Plugin State by Host — table of plugin states across hosts.
    panels.extend(_build_host_state_table(model, datasource, y_cursor))
    y_cursor += _ROW_H + 8

    # 5. Peer Latency — time since upstream last produced data.
    latency = _build_peer_latency(model, datasource, y_cursor)
    if latency is not None:
        panels.extend(latency)

    return panels
