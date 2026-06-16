"""CLI entry point for ``courier dashboard``."""
from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

dashboard_app = typer.Typer()


@dashboard_app.callback(invoke_without_command=True)
def dashboard(  # noqa: PLR0912, PLR0913, PLR0915
    config: Annotated[
        Path | None,
        typer.Argument(
            help=(
                "Path to courier service config YAML/JSON file. "
                "Defaults to 'courier.yaml' in the current directory."
            ),
        ),
    ] = None,
    # Output
    output: Annotated[
        Path | None,
        typer.Option(
            "--output", "-o",
            help=(
                "Output path. A file path writes a single .json file; "
                "a directory path writes one file per dashboard. "
                "Defaults to stdout for unified mode."
            ),
        ),
    ] = None,
    # Generation mode
    split_by: Annotated[
        str,
        typer.Option(
            "--split-by",
            help=(
                "Split dashboard generation: 'kind' = one per plugin kind, "
                "'plugin' = one per plugin instance."
            ),
        ),
    ] = "unified",
    # Cluster / sub-section
    run_identifiers: Annotated[
        str | None,
        typer.Option(
            "--run-identifiers",
            help="Comma-separated plugin identifiers to filter "
                 "(cluster sub-section).",
        ),
    ] = None,
    run_kinds: Annotated[
        str | None,
        typer.Option(
            "--run-kinds",
            help="Comma-separated plugin kinds to filter: "
                 "data_monitor, job_builder, dispatcher.",
        ),
    ] = None,
    # Live detection
    live: Annotated[
        bool,
        typer.Option(
            "--live",
            help="Auto-detect active plugins from a running Courier "
                 "instance's Prometheus metrics.",
        ),
    ] = False,
    prom_host: Annotated[
        str,
        typer.Option(
            "--prom-host",
            help="Prometheus metrics host (for --live).",
        ),
    ] = "localhost",
    prom_port: Annotated[
        int,
        typer.Option(
            "--prom-port",
            help="Prometheus metrics port (for --live).",
        ),
    ] = 8000,
    # Panel selection
    only_metrics: Annotated[
        bool,
        typer.Option(
            "--only-metrics",
            help="Only generate Prometheus panels (skip TraceQL).",
        ),
    ] = False,
    only_traces: Annotated[
        bool,
        typer.Option(
            "--only-traces",
            help="Only generate TraceQL panels (skip Prometheus).",
        ),
    ] = False,
    # Dashboard naming
    name: Annotated[
        str | None,
        typer.Option(
            "--name",
            help="Dashboard title override.",
        ),
    ] = None,
    uid: Annotated[
        str | None,
        typer.Option(
            "--uid",
            help="Dashboard UID override.",
        ),
    ] = None,
    datasource: Annotated[
        str,
        typer.Option(
            "--datasource",
            help="Prometheus datasource name/UID.",
        ),
    ] = "Prometheus",
    traces_datasource: Annotated[
        str,
        typer.Option(
            "--traces-datasource",
            help="Tempo datasource name/UID.",
        ),
    ] = "Tempo",
    # Misc
    indent: Annotated[
        int,
        typer.Option(
            "--indent",
            help="JSON indentation level.",
        ),
    ] = 2,
) -> None:
    """Generate Grafana dashboard JSON from a Courier service configuration.

    Reads the service config, analyzes the pipeline structure, and
    generates tailored Grafana dashboard JSON with Prometheus metrics
    panels and TraceQL trace search panels — only for the plugins
    actually configured.

    Examples
    --------
    courier dashboard
    courier dashboard config.yaml --split-by kind
    courier dashboard config.yaml --run-identifiers my-dm --live
    courier dashboard --only-metrics -o dashboard.json
    """
    # Lazy import — grafanalib is only required when the command runs
    try:
        from grafanalib.core import Dashboard  # noqa: PLC0415, F401
    except ImportError:
        typer.echo(
            "The 'dashboard' command requires the grafanalib library.\n"
            "Install it with: pip install courier[grafana]",
        )
        raise typer.Exit(1) from None

    # Resolve config path
    if config is None:
        config = Path("courier.yaml")
        if not config.exists():
            config = Path("courier.yml")
            if not config.exists():
                typer.echo(
                    "No config file specified and no "
                    "courier.yaml/courier.yml found.\n"
                    "Usage: courier dashboard CONFIG [OPTIONS]",
                )
                raise typer.Exit(1)

    if not config.exists():
        typer.echo(f"Config file not found: {config}")
        raise typer.Exit(1)

    # Lazy imports — dashboard modules are only needed when the command runs
    from courier.dashboard import DashboardGenerationMode  # noqa: PLC0415
    from courier.dashboard.config_parser import parse_config  # noqa: PLC0415
    from courier.dashboard.generator import generate_dashboard  # noqa: PLC0415
    from courier.dashboard.live_detector import detect_active_plugins  # noqa: PLC0415
    from courier.dashboard.serializers import serialize_dashboard  # noqa: PLC0415

    # Handle sub-section filtering
    run_id_set: set[str] | None = None
    if run_identifiers:
        run_id_set = {i.strip() for i in run_identifiers.split(",") if i.strip()}

    run_kind_set: set[str] | None = None
    if run_kinds:
        run_kind_set = {k.strip() for k in run_kinds.split(",") if k.strip()}

    model = parse_config(
        config,
        run_identifiers=run_id_set,
        run_kinds=run_kind_set,
    )

    # Live detection — override local_identifiers if --live
    if live:
        typer.echo(
            f"Querying {prom_host}:{prom_port}/metrics for active plugins...",
        )
        live_result = detect_active_plugins(host=prom_host, port=prom_port)

        if not live_result.is_reachable:
            typer.echo(
                f"Warning: Could not reach {prom_host}:{prom_port} — "
                f"{live_result.error_message}",
            )
            typer.echo("Proceeding with config-defined plugins only.")
        elif live_result.identifiers:
            typer.echo(
                f"Detected active plugins: "
                f"{', '.join(sorted(live_result.identifiers))}",
            )
            # Re-parse with live-detected identifiers
            model = parse_config(
                config,
                run_identifiers=live_result.identifiers,
                run_kinds=run_kind_set,
            )
        else:
            typer.echo(
                "No active plugins detected. "
                "Proceeding with config-defined plugins.",
            )

    # Resolve mode
    mode_map = {
        "unified": DashboardGenerationMode.UNIFIED,
        "kind": DashboardGenerationMode.SPLIT_BY_KIND,
        "plugin": DashboardGenerationMode.SPLIT_BY_PLUGIN,
    }
    if split_by not in mode_map:
        typer.echo(
            f"Invalid --split-by value: '{split_by}'. "
            "Choose: unified, kind, plugin.",
        )
        raise typer.Exit(1)

    mode = mode_map[split_by]

    # Generate dashboards
    dashboards = generate_dashboard(
        model,
        mode=mode.name,
        only_metrics=only_metrics,
        only_traces=only_traces,
        datasource=datasource,
        traces_datasource=traces_datasource,
        name=name,
        uid=uid,
    )

    # Serialize and output
    try:
        result = serialize_dashboard(dashboards, output=output, indent=indent)
    except ValueError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(code=1) from None

    if output is None:
        # Print to stdout
        typer.echo(result)
    else:
        typer.echo(result)
