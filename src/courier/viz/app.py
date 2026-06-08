"""Textual TUI dashboard for ``courier viz`` — live Prometheus metrics visualizer.

Displays real-time courier service metrics fetched from the /metrics
endpoint and rendered as a keyboard-driven terminal dashboard.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar

import httpx
from rich.text import Text
from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.reactive import reactive
from textual.widgets import DataTable, Footer, Header, Static

from courier.constants import PluginRunState
from courier.viz.design import (
    COLOR_HEALTHY,
    COLOR_HIGHLIGHT,
    COLOR_SECONDARY,
    COLOR_UNHEALTHY,
    COLOR_WARNING,
    DEFAULT_REFRESH,
    REFRESH_RATES,
    STEEL_BLUE,
)
from courier.viz.fetcher import MetricsFetcher

if TYPE_CHECKING:
    from courier.viz.models import MetricsSnapshot

# ---------------------------------------------------------------------------
# Threshold constants (Law 1: Early Exit — named constants prevent magic values)
# ---------------------------------------------------------------------------

_HEALTH_THRESHOLD: float = 0.5
_SUCCESS_GOOD: float = 0.95
_SUCCESS_WARN: float = 0.80
_SECONDS_PER_MINUTE: int = 60
_SECONDS_PER_HOUR: int = 3600


class CourierViz(App):
    """Live Prometheus metrics visualizer for courier."""

    # ------------------------------------------------------------------
    # Reactive state
    # ------------------------------------------------------------------

    refresh_interval: reactive[int] = reactive(DEFAULT_REFRESH, init=False)

    # ------------------------------------------------------------------
    # TCSS Stylesheet
    # ------------------------------------------------------------------

    CSS = f"""
    /* === Palette Variables === */
    $steel-blue: {STEEL_BLUE};
    $frosted-blue: {COLOR_SECONDARY};
    $yellow: {COLOR_HIGHLIGHT};
    $powder-blush: {COLOR_WARNING};
    $healthy: {COLOR_HEALTHY};
    $unhealthy: {COLOR_UNHEALTHY};

    /* === Base Screen === */
    Screen {{
        background: #0a0a1a;
    }}

    /* === Header & Footer === */
    Header {{
        background: $steel-blue;
        color: #ffffff;
        text-style: bold;
    }}

    Footer {{
        background: $steel-blue;
        color: #ffffff;
    }}

    /* === Section Header Labels === */
    .section-header {{
        background: $steel-blue;
        color: #ffffff;
        text-style: bold;
        padding: 0 2;
        margin: 1 0 0 0;
    }}

    /* === KPI Row === */
    #kpi-row {{
        height: 5;
        align: center middle;
        margin: 1 1 1 1;
    }}

    #kpi-row Static {{
        width: 1fr;
        height: 5;
        content-align: center middle;
        border: solid $frosted-blue;
        margin: 0 1;
        padding: 0 1;
    }}

    /* === DataTables === */
    DataTable {{
        height: auto;
        max-height: 10;
        margin: 0 2;
    }}

    DataTable > .datatable--header {{
        background: $frosted-blue;
        color: #0a0a1a;
        text-style: bold;
    }}

    /* === Status Statics === */
    #broker-status,
    #state-sync-status,
    #routing-emit-failures {{
        padding: 1 2;
        margin: 0 2;
    }}

    /* === Pipeline Bars === */
    #pipeline-bars {{
        height: 5;
        padding: 0 2;
        margin: 0 2;
    }}

    /* === Scrollable container for the whole body === */
    #body-scroll {{
        overflow-y: auto;
    }}
    """

    # ------------------------------------------------------------------
    # Key Bindings
    # ------------------------------------------------------------------

    BINDINGS: ClassVar[list[Binding | tuple[str, str] | tuple[str, str, str]]] = [
        Binding("q", "quit", "Quit"),
        Binding("r", "refresh", "Force Refresh"),
        Binding("p", "toggle_pause", "Pause/Resume"),
        Binding("f", "freeze", "Freeze View"),
        Binding("plus,equals", "faster", "Faster Rate"),
        Binding("minus,underscore", "slower", "Slower Rate"),
        Binding("0", "reset_refresh", "Reset Rate"),
    ]

    # ------------------------------------------------------------------
    # Constructor
    # ------------------------------------------------------------------

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8000,
        refresh_interval: int = DEFAULT_REFRESH,
    ) -> None:
        super().__init__()
        self._host: str = host
        self._port: int = port
        self.refresh_interval = refresh_interval

        # State tracking
        self._paused: bool = False
        self._frozen: bool = False
        self._error_count: int = 0
        self._last_refresh: str = "never"
        self._connected: bool = False
        self._snapshot: MetricsSnapshot | None = None

        # Initialized in on_mount
        self._client: httpx.AsyncClient | None = None
        self._fetcher: MetricsFetcher | None = None
        self._refresh_timer: Any | None = None

    # ------------------------------------------------------------------
    # Widget Tree
    # ------------------------------------------------------------------

    def compose(self) -> ComposeResult:
        """Build the complete widget hierarchy."""
        yield Header(show_clock=True)

        with VerticalScroll(id="body-scroll"):
            # --- KPI Row ---
            with Horizontal(id="kpi-row"):
                yield Static(id="kpi-health")
                yield Static(id="kpi-uptime")
                yield Static(id="kpi-heartbeat")
                yield Static(id="kpi-files")

            # --- Data Monitors ---
            with Vertical(id="section-monitors"):
                yield Static("── Data Monitors ──", classes="section-header")
                yield DataTable(id="table-monitors")

            # --- Job Builders ---
            with Vertical(id="section-builders"):
                yield Static("── Job Builders ──", classes="section-header")
                yield DataTable(id="table-builders")

            # --- Dispatchers ---
            with Vertical(id="section-dispatchers"):
                yield Static("── Dispatchers ──", classes="section-header")
                yield DataTable(id="table-dispatchers")

            # --- Plugins ---
            with Vertical(id="section-plugins"):
                yield Static("── Plugins ──", classes="section-header")
                yield DataTable(id="table-plugins")

            # --- Broker ---
            with Vertical(id="section-broker"):
                yield Static("── Broker ──", classes="section-header")
                yield Static(id="broker-status")

            # --- Routing ---
            with Vertical(id="section-routing"):
                yield Static("── Routing ──", classes="section-header")
                yield DataTable(id="table-routing")
                yield Static(id="routing-emit-failures")

            # --- State Sync ---
            with Vertical(id="section-state-sync"):
                yield Static("── State Sync / HA ──", classes="section-header")
                yield Static(id="state-sync-status")

            # --- Pipeline Summary ---
            with Vertical(id="section-pipeline"):
                yield Static("── Pipeline ──", classes="section-header")
                yield Static(id="pipeline-bars")

        yield Footer()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def on_mount(self) -> None:
        """Initialize HTTP client, fetcher, tables, and auto-refresh timer."""
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(5.0))
        self._fetcher = MetricsFetcher(self._client, self._host, self._port)

        self._setup_tables()
        self._set_header_title()
        self._update_footer_text()

        # Start auto-refresh
        self._refresh_timer = self.set_interval(
            self.refresh_interval,
            self._do_refresh,
        )

        # Immediate initial fetch
        self._do_refresh()

    # ------------------------------------------------------------------
    # Auto-refresh worker
    # ------------------------------------------------------------------

    @work(exclusive=True)
    async def _do_refresh(self) -> None:
        """Fetch metrics and update the display.

        Guarded by @work(exclusive=True) so only one fetch runs at a time.
        """
        if self._paused:
            return

        if self._fetcher is None:
            return

        try:
            self._snapshot = await self._fetcher.fetch()
            self._connected = True
            self._last_refresh = datetime.now().strftime("%H:%M:%S")

            if not self._frozen:
                self._update_all_widgets()

        except Exception as exc:
            self._connected = False
            self._error_count += 1
            self._last_refresh = datetime.now().strftime("%H:%M:%S")
            self.sub_title = (
                f"Refresh: {self.refresh_interval}s │ "
                f"Last: {self._last_refresh} │ "
                f"[bold #f8ad9d]Error: {exc}[/]"
            )

    # ------------------------------------------------------------------
    # Display update — one sub-method per section
    # ------------------------------------------------------------------

    def _update_all_widgets(self) -> None:
        """Populate every widget with current snapshot data."""
        snap = self._snapshot
        if snap is None:
            return

        self._update_kpi_row(snap)
        self._update_monitors_table(snap)
        self._update_builders_table(snap)
        self._update_dispatchers_table(snap)
        self._update_plugins_table(snap)
        self._update_broker_status(snap)
        self._update_routing_table(snap)
        self._update_state_sync(snap)
        self._update_pipeline_bars(snap)
        self._set_header_title()
        self._update_footer_text()

    # ------------------------------------------------------------------
    # KPI Row
    # ------------------------------------------------------------------

    def _update_kpi_row(self, snap: MetricsSnapshot) -> None:
        """Render the four KPI statics."""
        svc = snap.service

        if svc.health >= _HEALTH_THRESHOLD:
            health_md = "[#00ff00]● Healthy[/]"
        else:
            health_md = "[#ff3333]● Unhealthy[/]"

        uptime_str = self._format_duration(svc.uptime_seconds)
        heartbeat_str = f"{svc.heartbeat_age_seconds:.1f}s"
        files_str = f"{svc.total_files_processed:,.0f}"

        self.query_one("#kpi-health", Static).update(
            f"{health_md}\n[yellow]Health[/]",
        )
        self.query_one("#kpi-uptime", Static).update(
            f"[yellow]{uptime_str}[/]\nUptime",
        )
        self.query_one("#kpi-heartbeat", Static).update(
            f"[yellow]{heartbeat_str}[/]\nHeartbeat Age",
        )
        self.query_one("#kpi-files", Static).update(
            f"[yellow]{files_str}[/]\nFiles Processed",
        )

    # ------------------------------------------------------------------
    # Data Monitors Table
    # ------------------------------------------------------------------

    def _update_monitors_table(self, snap: MetricsSnapshot) -> None:
        """Populate the data monitors DataTable."""
        table = self.query_one("#table-monitors", DataTable)
        table.clear()

        for mon in snap.data_monitors.monitors:
            fail_cell = "0"
            if mon.failure_count > 0:
                fail_cell = f"[#ff3333]{mon.failure_count:,.0f}[/]"

            table.add_row(
                mon.name,
                f"{mon.files_processed:,.0f}",
                f"{mon.success_count:,.0f}",
                fail_cell,
                f"{mon.last_scan_age_seconds:.1f}s",
                f"{mon.scan_duration_p50:.3f}s",
            )

    # ------------------------------------------------------------------
    # Job Builders Table
    # ------------------------------------------------------------------

    def _update_builders_table(self, snap: MetricsSnapshot) -> None:
        """Populate the job builders DataTable."""
        table = self.query_one("#table-builders", DataTable)
        table.clear()

        for bld in snap.job_builders.builders:
            if bld.success_rate >= _SUCCESS_GOOD:
                succ_color = "#00ff00"
            elif bld.success_rate >= _SUCCESS_WARN:
                succ_color = "#ffff39"
            else:
                succ_color = "#ff3333"

            table.add_row(
                bld.name,
                f"{bld.files_received_rate:,.0f}",
                f"{bld.jobs_built_rate:,.0f}",
                f"[{succ_color}]{bld.success_rate:.0%}[/]",
                f"{bld.active_groups:.0f}",
                f"{bld.jobs_discarded_rate:,.0f}",
                f"{bld.processing_duration_p50:.3f}s",
                f"{bld.files_per_job_p50:.1f}",
            )

    # ------------------------------------------------------------------
    # Dispatchers Table
    # ------------------------------------------------------------------

    def _update_dispatchers_table(self, snap: MetricsSnapshot) -> None:
        """Populate the dispatchers DataTable."""
        table = self.query_one("#table-dispatchers", DataTable)
        table.clear()

        for dsp in snap.dispatchers.dispatchers:
            if dsp.success_ratio >= _SUCCESS_GOOD:
                succ_color = "#00ff00"
            elif dsp.success_ratio >= _SUCCESS_WARN:
                succ_color = "#ffff39"
            else:
                succ_color = "#ff3333"

            table.add_row(
                dsp.name,
                f"{dsp.jobs_processed_rate:,.0f}",
                f"[{succ_color}]{dsp.success_ratio:.0%}[/]",
                f"{dsp.active_jobs:.0f}",
                f"{dsp.execution_duration_p50:.3f}s",
                f"{dsp.logs_emitted_rate:,.0f}",
                f"{dsp.queue_wait_p50:.3f}s",
            )

    # ------------------------------------------------------------------
    # Plugins Table
    # ------------------------------------------------------------------

    _PLUGIN_STATE_NAMES: ClassVar[dict[int, str]] = {
        PluginRunState.STOPPED.value: "STOPPED",
        PluginRunState.STARTING.value: "STARTING",
        PluginRunState.RUNNING.value: "RUNNING",
        PluginRunState.STOPPING.value: "STOPPING",
        PluginRunState.FAILED.value: "FAILED",
        PluginRunState.RESTARTING.value: "RESTARTING",
    }

    def _update_plugins_table(self, snap: MetricsSnapshot) -> None:
        """Populate the plugins DataTable."""
        table = self.query_one("#table-plugins", DataTable)
        table.clear()

        for plg in snap.plugins.plugins:
            health_cell = (
                "[#00ff00]●[/]" if plg.health >= _HEALTH_THRESHOLD else "[#ff3333]●[/]"
            )

            state_idx = int(plg.state)
            state_name = self._PLUGIN_STATE_NAMES.get(
                state_idx,
                f"STATE_{state_idx}",
            )

            restart_text = f"{plg.restart_rate:,.0f}"
            if plg.restart_rate > 0:
                restart_text = f"[#f8ad9d]{restart_text}[/]"

            table.add_row(plg.name, state_name, health_cell, restart_text)

    # ------------------------------------------------------------------
    # Broker Status
    # ------------------------------------------------------------------

    def _update_broker_status(self, snap: MetricsSnapshot) -> None:
        """Render broker connectivity and throughput as a status line."""
        broker = snap.broker

        if broker.connected >= _HEALTH_THRESHOLD:
            conn_str = "[#00ff00]● Connected[/]"
        else:
            conn_str = "[#ff3333]● Disconnected[/]"

        status = (
            f"  {conn_str}  │  "
            f"Attempts: [#ffff39]{broker.connection_attempts_rate:,.0f}[/]  │  "
            f"Sent: [#ffff39]{broker.messages_sent_rate:,.0f}[/]/s  │  "
            f"Recv: [#ffff39]{broker.messages_received_rate:,.0f}[/]/s"
        )
        self.query_one("#broker-status", Static).update(status)

    # ------------------------------------------------------------------
    # Routing Table
    # ------------------------------------------------------------------

    def _update_routing_table(self, snap: MetricsSnapshot) -> None:
        """Populate the routing DataTable and emit failures status."""
        table = self.query_one("#table-routing", DataTable)
        table.clear()

        for rt in snap.routing.routes:
            table.add_row(
                rt.dispatcher_identifier,
                f"{rt.jobs_consumed_rate:,.0f}",
                f"{rt.dispatch_latency_p50:.3f}s",
                f"{rt.queue_depth:.0f}",
            )

        # Emit failures
        failures = snap.routing.emit_failures_rate
        if failures > 0:
            msg = f"  Emit Failures: [#f8ad9d]{failures:,.0f}[/]"
        else:
            msg = f"  Emit Failures: {failures:,.0f}"
        self.query_one("#routing-emit-failures", Static).update(msg)

    # ------------------------------------------------------------------
    # State Sync / HA
    # ------------------------------------------------------------------

    def _update_state_sync(self, snap: MetricsSnapshot) -> None:
        """Render state sync / HA metrics as a status line."""
        ss = snap.state_sync

        errors_str = f"[#f8ad9d]{ss.errors_rate:,.0f}[/]" if ss.errors_rate > 0 else "0"

        status = (
            f"  Pushes: [#ffff39]{ss.pushes_rate:,.0f}[/]/s  │  "
            f"Applies: [#ffff39]{ss.applies_rate:,.0f}[/]/s  │  "
            f"Claims: [#ffff39]{ss.emit_claims_rate:,.0f}[/]/s  │  "
            f"Errors: {errors_str}"
        )
        self.query_one("#state-sync-status", Static).update(status)

    # ------------------------------------------------------------------
    # Pipeline Summary (bar chart)
    # ------------------------------------------------------------------

    @staticmethod
    def _render_bar(
        label: str,
        value: float,
        max_value: float,
        width: int,
        color: str,
    ) -> Text:
        """Render a single horizontal bar as a Rich Text object."""
        bar_width = int((value / max_value) * width) if max_value > 0 else 0
        text = Text()
        text.append(f"  {label}: ", style="bold")
        text.append("▓" * bar_width, style=color)
        text.append(f" {value:,.0f}/s")
        return text

    def _update_pipeline_bars(self, snap: MetricsSnapshot) -> None:
        """Render the pipeline throughput bar chart."""
        p = snap.pipeline_summary
        max_val = max(
            p.files_detected_rate,
            p.jobs_built_rate,
            p.jobs_dispatched_rate,
            1.0,
        )
        bar_width = 40

        lines = Text()
        lines.append(
            self._render_bar(
                "Files Detected",
                p.files_detected_rate,
                max_val,
                bar_width,
                "#2081c3",
            ),
        )
        lines.append("\n")
        lines.append(
            self._render_bar(
                "Jobs Built   ",
                p.jobs_built_rate,
                max_val,
                bar_width,
                "#84e6f8",
            ),
        )
        lines.append("\n")
        lines.append(
            self._render_bar(
                "Jobs Dispatch",
                p.jobs_dispatched_rate,
                max_val,
                bar_width,
                "#ffff39",
            ),
        )

        self.query_one("#pipeline-bars", Static).update(lines)

    # ------------------------------------------------------------------
    # Header & Footer helpers
    # ------------------------------------------------------------------

    def _set_header_title(self) -> None:
        """Update Header title and sub-title."""
        self.title = f"Courier Viz ▶ http://{self._host}:{self._port}"

    def _update_footer_text(self) -> None:
        """Recompute the sub-title line shown in the header area."""
        if self._connected:
            conn = "[#00ff00]✓ Connected[/]"
        else:
            conn = "[#ff3333]✗ Disconnected[/]"

        err_part = f" │ Errors: {self._error_count}" if self._error_count > 0 else ""

        self.sub_title = (
            f"Refresh: {self.refresh_interval}s │ "
            f"Last: {self._last_refresh} │ "
            f"{conn}"
            f"{err_part}"
        )

    # ------------------------------------------------------------------
    # Keyboard action handlers
    # ------------------------------------------------------------------

    def action_refresh(self) -> None:
        """Force an immediate metrics refresh."""
        self._do_refresh()

    def action_toggle_pause(self) -> None:
        """Toggle auto-refresh on/off."""
        self._paused = not self._paused
        state = "Paused" if self._paused else "Resumed"
        self.notify(state)

    def action_freeze(self) -> None:
        """Toggle display freeze (keeps fetching in background)."""
        self._frozen = not self._frozen
        state = "Frozen" if self._frozen else "Live"
        self.notify(state)

    def action_faster(self) -> None:
        """Increase refresh rate (shorter interval)."""
        current = self.refresh_interval
        for rate in REFRESH_RATES:
            if rate < current:
                self.refresh_interval = rate
                self.notify(f"Refresh: {rate}s")
                self._reset_timer()
                return
        self.notify("Already at max rate (1s)")

    def action_slower(self) -> None:
        """Decrease refresh rate (longer interval)."""
        current = self.refresh_interval
        for rate in reversed(REFRESH_RATES):
            if rate > current:
                self.refresh_interval = rate
                self.notify(f"Refresh: {rate}s")
                self._reset_timer()
                return
        self.notify("Already at min rate (30s)")

    def action_reset_refresh(self) -> None:
        """Reset refresh rate to default."""
        self.refresh_interval = DEFAULT_REFRESH
        self.notify(f"Refresh: {DEFAULT_REFRESH}s (default)")
        self._reset_timer()

    # ------------------------------------------------------------------
    # Reactive watcher
    # ------------------------------------------------------------------

    def watch_refresh_interval(self, _new_interval: int) -> None:
        """When refresh_interval changes, update the footer text."""
        self._update_footer_text()

    # ------------------------------------------------------------------
    # Timer management
    # ------------------------------------------------------------------

    def _reset_timer(self) -> None:
        """Stop the current auto-refresh timer and create a new one.

        This ensures the new refresh_interval takes effect immediately.
        """
        if self._refresh_timer is not None:
            self._refresh_timer.stop()
        self._refresh_timer = self.set_interval(
            self.refresh_interval,
            self._do_refresh,
        )

    # ------------------------------------------------------------------
    # Static helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _format_duration(seconds: float) -> str:
        """Format seconds into a human-readable duration string."""
        if seconds < 0:
            return "0s"
        if seconds < _SECONDS_PER_MINUTE:
            return f"{seconds:.0f}s"
        if seconds < _SECONDS_PER_HOUR:
            minutes = int(seconds // _SECONDS_PER_MINUTE)
            secs = int(seconds % _SECONDS_PER_MINUTE)
            return f"{minutes}m {secs}s"
        hours = int(seconds // _SECONDS_PER_HOUR)
        minutes = int((seconds % _SECONDS_PER_HOUR) // _SECONDS_PER_MINUTE)
        return f"{hours}h {minutes}m"

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    async def on_unmount(self) -> None:
        """Clean up the HTTP client on app exit."""
        if self._client is not None:
            await self._client.aclose()

    # ------------------------------------------------------------------
    # Table setup (called once in on_mount)
    # ------------------------------------------------------------------

    def _setup_tables(self) -> None:
        """Initialize column headers for all DataTables."""
        self.query_one("#table-monitors", DataTable).add_columns(
            "Name",
            "Files",
            "Success",
            "Fail",
            "Scan Age",
            "Avg Dur",
        )
        self.query_one("#table-builders", DataTable).add_columns(
            "Name",
            "Files",
            "Jobs",
            "Succ%",
            "Active",
            "Disc",
            "Dur",
            "FPJ",
        )
        self.query_one("#table-dispatchers", DataTable).add_columns(
            "Name",
            "Jobs",
            "Succ%",
            "Active",
            "Dur",
            "Logs",
            "Q Wait",
        )
        self.query_one("#table-plugins", DataTable).add_columns(
            "Name",
            "State",
            "Health",
            "Restarts",
        )
        self.query_one("#table-routing", DataTable).add_columns(
            "Identifier",
            "Consumed",
            "Latency",
            "Q Depth",
        )
