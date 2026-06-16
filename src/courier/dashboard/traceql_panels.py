"""TraceQL query panel generation for Grafana Tempo dashboards.

Generates Grafana panel objects (``Row`` containers with nested ``Table`` panels)
that query the Tempo datasource using the TraceQL query language. These panels
complement the Prometheus metrics panels by providing distributed tracing
visibility — latency analysis, error spans, gap detection, and data lineage
queries.

Every query is expressed as an inline ``dict`` target (not a grafanalib Tempo
wrapper class), since grafanalib's Tempo integration varies by version.
"""

# NOTE: All TraceQL queries in this module use `by(...)` aggregation groupings
# and return multi-row results. No panels can be converted to Stat/Gauge without
# changing the query semantics. If single-value aggregation panels are desired,
# add new queries without `by()` clauses (e.g., `{ } | avg(duration)`).

from __future__ import annotations

from typing import TYPE_CHECKING

from grafanalib.core import GridPos, Row, Table

from courier.dashboard.config_parser import SPAN_ATTRS, PluginKind

if TYPE_CHECKING:
    from courier.dashboard.config_parser import DashboardModel


# ---------------------------------------------------------------------------
# Convenience aliases for span attribute names (dotted OpenTelemetry form)
# ---------------------------------------------------------------------------

_A = SPAN_ATTRS  # brevity inside query strings

# ---------------------------------------------------------------------------
# Helper — inline Tempo datasource target dict
# ---------------------------------------------------------------------------


def _target(
    query: str,
    *,
    ref_id: str = "A",
    table_type: str = "spans",
    limit: int = 200,
    datasource_uid: str,
) -> dict:
    """Return a grafanalib-compatible inline target dict for a TraceQL query.

    Parameters
    ----------
    query : str
        The TraceQL query string, e.g. ``"{ duration > 5s }"``.
    ref_id : str
        Grafana target reference identifier (``"A"``, ``"B"``, ...).
    table_type : str
        ``"spans"`` for span-level results, ``"traces"`` for trace-level.
    limit : int
        Maximum number of results returned by Tempo (default 200).
    datasource_uid : str
        The ``uid`` of the Tempo datasource (e.g. ``"Tempo"``).

    Returns
    -------
    dict
        Inline target dict suitable for passing inside a ``Table`` panel's
        ``targets`` list.
    """
    return {
        "refId": ref_id,
        "datasource": {"type": "tempo", "uid": datasource_uid},
        "queryType": "traceql",
        "query": query,
        "tableType": table_type,
        "limit": limit,
    }


# ---------------------------------------------------------------------------
# Row builders — each returns (panels: list, next_y: int)
# ---------------------------------------------------------------------------

# Panel heights used in layout calculations.
_H_ROW_HEADER = 1
_H_TALL = 12
_H_MEDIUM = 10
_H_STANDARD = 8
_W_FULL = 24


def _build_trace_overview_row(
    datasource: str,
    y: int,
) -> tuple[list, int]:
    """Panels for the Trace Overview row (always generated).

    Includes slowest spans, error spans, and span count by kind.
    """
    uid = datasource

    panels: list = []
    inner_y = y + _H_ROW_HEADER  # below the row header

    # --- Slowest Spans -------------------------------------------------------
    panels.append(
        Table(
            title="Slowest Spans (last 1h)",
            description="All spans exceeding 5-second duration in the last hour.",
            dataSource=uid,
            targets=[_target("{ duration > 5s }", datasource_uid=uid)],
            gridPos=GridPos(h=_H_TALL, w=_W_FULL, x=0, y=inner_y),
        ),
    )
    inner_y += _H_TALL

    # --- Error Spans ---------------------------------------------------------
    panels.append(
        Table(
            title="Error Spans",
            description=(
                "Any span whose OTel status code indicates "
                "an error (status.code = 2)."
            ),
            dataSource=uid,
            targets=[_target("{ status.code = 2 }", datasource_uid=uid)],
            gridPos=GridPos(h=_H_MEDIUM, w=_W_FULL, x=0, y=inner_y),
        ),
    )
    inner_y += _H_MEDIUM

    # --- Span Count by Kind --------------------------------------------------
    # Note: Grafana 10.2+ supports `by()`; on older versions `count()` may
    # need `group by` syntax.  This uses the pipe-aggregation style.
    panels.append(
        Table(
            title="Span Count by Kind",
            description="Total span count grouped by OpenTelemetry span kind.",
            dataSource=uid,
            targets=[
                _target(
                    "{ } | count() by(kind)",
                    datasource_uid=uid,
                    table_type="traces",
                ),
            ],
            gridPos=GridPos(h=_H_STANDARD, w=_W_FULL, x=0, y=inner_y),
        ),
    )
    inner_y += _H_STANDARD

    row = Row(
        title="Trace Overview",
        panels=panels,
    )
    return [row], inner_y


def _build_data_monitor_traces_row(
    datasource: str,
    y: int,
) -> tuple[list, int]:
    """Panels for Data Monitor trace queries.

    Only generated when *model.data_monitors* is non-empty.
    """
    uid = datasource
    attr_source = _A["file_source"]
    attr_path = _A["file_path"]

    panels: list = []
    inner_y = y + _H_ROW_HEADER

    # ---- DM Processing Timeline ---------------------------------------------
    # avg(duration) per file path for spans attributed to the selected DM
    panels.append(
        Table(
            title="DM Processing Timeline",
            description=(
                "Average span duration per file path for the selected data monitor "
                "source.  Use the ``dm_source`` template variable to pick a plugin."
            ),
            dataSource=uid,
            targets=[
                _target(
                    f'{{ name =~ "{PluginKind.DATA_MONITOR.value}.*"'
                    f' && {attr_source} = "$dm_source" }}'
                    f" | avg(duration) by({attr_path})",
                    datasource_uid=uid,
                ),
            ],
            gridPos=GridPos(h=_H_TALL, w=_W_FULL, x=0, y=inner_y),
        ),
    )
    inner_y += _H_TALL

    # ---- DM Files by Source -------------------------------------------------
    panels.append(
        Table(
            title="DM Files by Source",
            description=(
                "Table of file paths with their span durations, filtered to the "
                "selected data monitor.  Each row represents one file emitted."
            ),
            dataSource=uid,
            targets=[
                _target(
                    f'{{ name =~ "{PluginKind.DATA_MONITOR.value}.*"'
                    f' && {attr_source} = "$dm_source" }}',
                    datasource_uid=uid,
                ),
            ],
            gridPos=GridPos(h=_H_TALL, w=_W_FULL, x=0, y=inner_y),
        ),
    )
    inner_y += _H_TALL

    row = Row(
        title="Data Monitor Traces",
        panels=panels,
    )
    return [row], inner_y


def _build_job_builder_traces_row(
    datasource: str,
    y: int,
) -> tuple[list, int]:
    """Panels for Job Builder trace queries.

    Only generated when *model.job_builders* is non-empty.
    """
    uid = datasource
    attr_group = _A["job_group_name"]
    attr_file_count = _A["job_file_count"]
    attr_plugin = _A["plugin_name"]

    panels: list = []
    inner_y = y + _H_ROW_HEADER

    # ---- Job Build Duration by Builder --------------------------------------
    panels.append(
        Table(
            title="Job Build Duration by Builder",
            description=(
                "Average span duration per plugin for the selected job builder.  "
                "Use the ``jb_trace_plugin`` template variable to filter."
            ),
            dataSource=uid,
            targets=[
                _target(
                    f'{{ name =~ "{PluginKind.JOB_BUILDER.value}.*"'
                    f' && {attr_plugin} = "$jb_trace_plugin" }}'
                    f" | avg(duration) by({attr_plugin})",
                    datasource_uid=uid,
                ),
            ],
            gridPos=GridPos(h=_H_TALL, w=_W_FULL, x=0, y=inner_y),
        ),
    )
    inner_y += _H_TALL

    # ---- Jobs Per Group -----------------------------------------------------
    panels.append(
        Table(
            title="Jobs Per Group",
            description=(
                "Count of spans that carry a non-empty ``courier.job_group.name`` "
                "attribute, grouped by group name."
            ),
            dataSource=uid,
            targets=[
                _target(
                    f'{{ {attr_group} != "" }}'
                    f" | count() by({attr_group})",
                    datasource_uid=uid,
                    table_type="traces",
                ),
            ],
            gridPos=GridPos(h=_H_STANDARD, w=_W_FULL, x=0, y=inner_y),
        ),
    )
    inner_y += _H_STANDARD

    # ---- Job File Counts ----------------------------------------------------
    panels.append(
        Table(
            title="Job File Counts",
            description=(
                "Table of job IDs whose file count is > 0, showing file counts "
                "and dispatch targets."
            ),
            dataSource=uid,
            targets=[
                _target(
                    f"{{ {attr_file_count} > 0 }}",
                    datasource_uid=uid,
                ),
            ],
            gridPos=GridPos(h=_H_TALL, w=_W_FULL, x=0, y=inner_y),
        ),
    )
    inner_y += _H_TALL

    row = Row(
        title="Job Builder Traces",
        panels=panels,
    )
    return [row], inner_y


def _build_metadata_router_traces_row(
    datasource: str,
    y: int,
) -> tuple[list, int]:
    """Panels for Metadata Router trace queries.

    Only generated when *model.has_metadata_router* is ``True``.
    """
    uid = datasource
    attr_target = _A["target"]

    panels: list = []
    inner_y = y + _H_ROW_HEADER

    # ---- Route Distribution -------------------------------------------------
    panels.append(
        Table(
            title="Route Distribution",
            description=(
                "Count of ``metadata_router.route_file`` spans grouped by the "
                "courier dispatch target they route to."
            ),
            dataSource=uid,
            targets=[
                _target(
                    f'{{ span.name = "metadata_router.route_file"'
                    f' && {attr_target} != "" }}'
                    f" | count() by({attr_target})",
                    datasource_uid=uid,
                    table_type="traces",
                ),
            ],
            gridPos=GridPos(h=_H_STANDARD, w=_W_FULL, x=0, y=inner_y),
        ),
    )
    inner_y += _H_STANDARD

    # ---- Router Latency -----------------------------------------------------
    panels.append(
        Table(
            title="Router Latency",
            description=(
                "Average duration of ``metadata_router.route_file`` spans."
            ),
            dataSource=uid,
            targets=[
                _target(
                    '{ span.name = "metadata_router.route_file" }'
                    " | avg(duration)",
                    datasource_uid=uid,
                    table_type="traces",
                ),
            ],
            gridPos=GridPos(h=_H_STANDARD, w=_W_FULL, x=0, y=inner_y),
        ),
    )
    inner_y += _H_STANDARD

    row = Row(
        title="Metadata Router Traces",
        panels=panels,
    )
    return [row], inner_y


def _build_dispatcher_traces_row(
    datasource: str,
    y: int,
) -> tuple[list, int]:
    """Panels for Dispatcher trace queries.

    Only generated when *model.dispatchers* is non-empty.
    """
    uid = datasource
    attr_rc = _A["execution_return_code"]
    attr_host = _A["execution_hostname"]

    panels: list = []
    inner_y = y + _H_ROW_HEADER

    # ---- Execution Return Codes ---------------------------------------------
    panels.append(
        Table(
            title="Execution Return Codes",
            description=(
                "Count of dispatcher spans with a return code >= 0, grouped "
                "by return code value."
            ),
            dataSource=uid,
            targets=[
                _target(
                    f'{{ name =~ "{PluginKind.DISPATCHER.value}.*"'
                    f" && {attr_rc} >= 0 }}"
                    f" | count() by({attr_rc})",
                    datasource_uid=uid,
                    table_type="traces",
                ),
            ],
            gridPos=GridPos(h=_H_STANDARD, w=_W_FULL, x=0, y=inner_y),
        ),
    )
    inner_y += _H_STANDARD

    # ---- Execution Duration by Host -----------------------------------------
    panels.append(
        Table(
            title="Execution Duration by Host",
            description=(
                "Average dispatcher span duration grouped by execution hostname."
            ),
            dataSource=uid,
            targets=[
                _target(
                    f'{{ name =~ "{PluginKind.DISPATCHER.value}.*" }}'
                    f" | avg(duration) by({attr_host})",
                    datasource_uid=uid,
                    table_type="traces",
                ),
            ],
            gridPos=GridPos(h=_H_STANDARD, w=_W_FULL, x=0, y=inner_y),
        ),
    )
    inner_y += _H_STANDARD

    # ---- Failed Executions --------------------------------------------------
    panels.append(
        Table(
            title="Failed Executions",
            description=(
                "Table of dispatcher spans where the execution return code > 0 "
                "(non-zero / failure)."
            ),
            dataSource=uid,
            targets=[
                _target(
                    f'{{ {attr_rc} > 0 }}',
                    datasource_uid=uid,
                ),
            ],
            gridPos=GridPos(h=_H_TALL, w=_W_FULL, x=0, y=inner_y),
        ),
    )
    inner_y += _H_TALL

    row = Row(
        title="Dispatcher Traces",
        panels=panels,
    )
    return [row], inner_y


def _build_trace_gap_detection_row(
    datasource: str,
    y: int,
) -> tuple[list, int]:
    """Panels for Trace Gap Detection row (always generated).

    Helps identify pipeline gaps — data monitors with no downstream successors
    and other pipeline-routing anomalies.
    """
    uid = datasource

    panels: list = []
    inner_y = y + _H_ROW_HEADER

    # ---- Data Monitor Span Count (benchmark) --------------------------------
    panels.append(
        Table(
            title="Data Monitor Span Count",
            description=(
                "Total span count for ``data_monitor``-kind spans.  Compare "
                "with Prometheus ``courier_data_monitor_files_processed_total`` "
                "to detect spans that never made it to tracing."
            ),
            dataSource=uid,
            targets=[
                _target(
                    f'{{ name =~ "{PluginKind.DATA_MONITOR.value}.*" }}'
                    " | count()",
                    datasource_uid=uid,
                    table_type="traces",
                ),
            ],
            gridPos=GridPos(h=_H_STANDARD, w=12, x=0, y=inner_y),
        ),
    )

    # ---- Pipeline Span Count Benchmarks -------------------------------------
    panels.append(
        Table(
            title="Pipeline Span Counts",
            description=(
                "Count of spans for each pipeline stage.  Sudden drops between "
                "stages can indicate routing gaps or failed downstream plugins."
            ),
            dataSource=uid,
            targets=[
                _target(
                    "{ } | count() by(name)",
                    datasource_uid=uid,
                    table_type="traces",
                ),
            ],
            gridPos=GridPos(h=_H_STANDARD, w=12, x=12, y=inner_y),
        ),
    )
    inner_y += _H_STANDARD

    # ---- Gap Detection Guidance ---------------------------------------------
    # A note panel directing operators to the Tempo search UI for manual
    # correlation-id-based gap tracing.
    panels.append(
        Table(
            title="Gap Detection Guide",
            description=(
                "For deep gap detection, use the Tempo Search UI directly:  "
                "find a data_monitor span, copy its correlation_id, then "
                "search for all spans with that id.  Missing downstream "
                "spans indicate a pipeline gap."
            ),
            dataSource=uid,
            targets=[
                _target(
                    f'{{ name =~ "{PluginKind.DATA_MONITOR.value}.*"'
                    f' && {_A["correlation_id"]} != "" }}',
                    datasource_uid=uid,
                ),
            ],
            gridPos=GridPos(h=_H_STANDARD, w=_W_FULL, x=0, y=inner_y),
        ),
    )
    inner_y += _H_STANDARD

    row = Row(
        title="Trace Gap Detection",
        panels=panels,
    )
    return [row], inner_y


def _build_correlated_trace_view_row(
    datasource: str,
    y: int,
) -> tuple[list, int]:
    """Panels for the Correlated Trace View row (always generated).

    Allows operators to view the full trace for a specific correlation ID
    selected via a dashboard template variable.
    """
    uid = datasource
    attr_corr = _A["correlation_id"]

    panels: list = []
    inner_y = y + _H_ROW_HEADER

    # ---- Full Trace by Correlation ID ---------------------------------------
    panels.append(
        Table(
            title="Full Trace by Correlation ID",
            description=(
                "All spans that share the selected ``correlation_id`` template "
                "variable.  Enter a correlation_id in the dashboard dropdown "
                "to view the complete end-to-end trace."
            ),
            dataSource=uid,
            targets=[
                _target(
                    f"{{ {attr_corr} = \"$correlation_id\" }}",
                    datasource_uid=uid,
                ),
            ],
            gridPos=GridPos(h=_H_TALL, w=_W_FULL, x=0, y=inner_y),
        ),
    )

    row = Row(
        title="Correlated Trace View",
        panels=panels,
    )
    return [row], inner_y + _H_TALL


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_traceql_panels(
    model: DashboardModel,
    *,
    datasource: str = "Tempo",
) -> list:
    """Generate config-aware TraceQL panels for a Tempo datasource.

    Returns a list of grafanalib ``Row`` panel objects, each containing one or
    more ``Table`` panels that query the Tempo datasource with TraceQL.  The
    panels complement Prometheus metrics by providing distributed tracing
    visibility — latency analysis, error spans, gap detection, and data
    lineage queries.

    Panel rows are only emitted when the model contains relevant plugins:

    * **Trace Overview** — always generated; slow spans, errors, span counts.
    * **Data Monitor Traces** — emitted only when ``model.data_monitors`` is
      non-empty.
    * **Job Builder Traces** — emitted only when ``model.job_builders`` is
      non-empty.
    * **Metadata Router Traces** — emitted only when
      ``model.has_metadata_router`` is ``True``.
    * **Dispatcher Traces** — emitted only when ``model.dispatchers`` is
      non-empty.
    * **Trace Gap Detection** — always generated; span counts and gap guidance.
    * **Correlated Trace View** — always generated; full-trace lookup by
      correlation ID.

    The queries embed template variables (``$dm_source``, ``$jb_trace_plugin``,
    ``$correlation_id``).  Callers must create matching Grafana ``Template``
    objects in the dashboard template list for these variables to function.

    Parameters
    ----------
    model : DashboardModel
        Parsed dashboard model built from a courier service config.
    datasource : str
        Grafana datasource name or UID for Tempo (default ``"Tempo"``).

    Returns
    -------
    list
        Ordered list of grafanalib panel objects (``Row`` instances).
    """
    rows: list = []
    y = 0  # vertical grid position cursor

    # ---- 1. Trace Overview --------------------------------------------------
    overview_rows, y = _build_trace_overview_row(datasource, y)
    rows.extend(overview_rows)

    # ---- 2. Data Monitor Traces ---------------------------------------------
    if model.data_monitors:
        dm_rows, y = _build_data_monitor_traces_row(datasource, y)
        rows.extend(dm_rows)

    # ---- 3. Job Builder Traces ----------------------------------------------
    if model.job_builders:
        jb_rows, y = _build_job_builder_traces_row(datasource, y)
        rows.extend(jb_rows)

    # ---- 4. Metadata Router Traces ------------------------------------------
    if model.has_metadata_router:
        mr_rows, y = _build_metadata_router_traces_row(datasource, y)
        rows.extend(mr_rows)

    # ---- 5. Dispatcher Traces -----------------------------------------------
    if model.dispatchers:
        disp_rows, y = _build_dispatcher_traces_row(datasource, y)
        rows.extend(disp_rows)

    # ---- 6. Trace Gap Detection ---------------------------------------------
    gap_rows, y = _build_trace_gap_detection_row(datasource, y)
    rows.extend(gap_rows)

    # ---- 7. Correlated Trace View -------------------------------------------
    corr_rows, y = _build_correlated_trace_view_row(datasource, y)
    rows.extend(corr_rows)

    return rows
