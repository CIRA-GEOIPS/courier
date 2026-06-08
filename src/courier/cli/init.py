"""Interactive service config generator — ``courier init``."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import typer
import yaml
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.table import Table

from courier.cli.init_helpers import (
    find_config_model,
    get_field_metadata,
    get_plugin_description,
)
from courier.interfaces import data_monitors, dispatchers, job_builders
from courier.schema.v1alpha1.service_config import ServiceConfigModel

if TYPE_CHECKING:
    from pluginify.interfaces.base import BaseClassInterface, BaseYamlInterface
    from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Maximum column widths for display tables
_MAX_DESC_LENGTH: int = 80
_MAX_CONFIG_SUMMARY_LENGTH: int = 50
# Trigger for truncation ellipsis
_TRUNCATE_THRESHOLD: int = 3

# ---------------------------------------------------------------------------
# Phase 4.1 — PluginSelection dataclass
# ---------------------------------------------------------------------------


@dataclass
class PluginSelection:
    """A user's selection of a plugin with its configured values."""

    plugin_class: type
    plugin_name: str  # e.g., "rabbit_mq_watcher"
    interface_kind: str  # e.g., "data_monitors"
    yaml_kind: str  # e.g., "data_monitor"
    display_label: str  # e.g., "Data Monitor"
    config_model: type[BaseModel] | None
    config_values: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Kind mapping — interface name (plural) → (display label, singular YAML kind)
# ---------------------------------------------------------------------------

KIND_MAPPING: dict[str, tuple[str, str]] = {
    "data_monitors": ("Data Monitor", "data_monitor"),
    "job_builders": ("Job Builder", "job_builder"),
    "dispatchers": ("Dispatcher", "dispatcher"),
}

PLUGIN_REGISTRIES: dict[str, BaseYamlInterface | BaseClassInterface] = {
    "data_monitors": data_monitors,
    "dispatchers": dispatchers,
    "job_builders": job_builders,
}

# Order matters: Data Monitor → Job Builder → Dispatcher
CATEGORY_ORDER: list[str] = ["data_monitors", "job_builders", "dispatchers"]

# ---------------------------------------------------------------------------
# Phase 5 helpers — pure transformations
# ---------------------------------------------------------------------------


def _make_identifier(yaml_kind: str, plugin_name: str) -> str:
    r"""Create a DNS-safe step identifier from kind + plugin name.

    Replaces underscores with hyphens, lowercases, strips non-DNS chars,
    and truncates to 63 characters following the DNS subdomain regex:
    ``^[a-z0-9]([a-z0-9\-]{0,61}[a-z0-9])?$``.
    """
    raw = f"{yaml_kind}-{plugin_name}"
    sanitized = raw.lower().replace("_", "-")
    sanitized = "".join(c for c in sanitized if c.isalnum() or c == "-")
    sanitized = sanitized.strip("-")
    sanitized = sanitized[:63]
    sanitized = sanitized.rstrip("-")
    return sanitized


def _coerce_value(raw: Any, type_hint: str) -> Any:  # noqa: PLR0911, PLR0912
    """Coerce a raw string input to the target type.

    Returns ``...`` sentinel when parsing fails or input is empty,
    signalling the caller to skip the field.
    """
    if isinstance(raw, str):
        raw = raw.strip()
        if not raw:
            return ...

    # List types — e.g. "list[str]"
    if type_hint.startswith("list["):
        if isinstance(raw, str):
            return [item.strip() for item in raw.split(",") if item.strip()]
        return raw

    # Dict types — e.g. "dict[str, int]"
    if type_hint.startswith("dict["):
        if isinstance(raw, str) and raw.startswith("{"):
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                return ...
        return raw

    # Scalar types
    if type_hint == "int":
        try:
            return int(raw)
        except (ValueError, TypeError):
            return raw

    if type_hint == "float":
        try:
            return float(raw)
        except (ValueError, TypeError):
            return raw

    if type_hint == "bool":
        if isinstance(raw, str):
            return raw.lower() in ("true", "yes", "y", "1")
        return bool(raw)

    return raw


# ---------------------------------------------------------------------------
# Phase 4.2 — Metadata prompt
# ---------------------------------------------------------------------------


def prompt_metadata(console: Console) -> dict[str, str]:
    """Prompt for service metadata.

    Returns a dict with keys ``name``, ``namespace``, and ``description``.
    """
    console.print(
        Panel.fit(
            "[bold]Service Metadata[/bold] — basic information about your service",
            border_style="blue",
        ),
    )

    default_name = Path.cwd().name.lower().replace("_", "-").replace(".", "-")
    if not default_name or default_name == "/":
        default_name = "my-service"

    name = Prompt.ask("Service name", default=default_name)
    name = name.strip().lower().replace("_", "-")
    if not name:
        name = default_name

    namespace = Prompt.ask("Namespace", default=name)
    namespace = namespace.strip().lower().replace("_", "-")
    if not namespace:
        namespace = name

    description = Prompt.ask(
        "Description",
        default=f"A courier service: {name}",
    )
    description = description.strip()
    if not description:
        description = f"A courier service: {name}"

    return {
        "name": name,
        "namespace": namespace,
        "description": description,
    }


# ---------------------------------------------------------------------------
# Phase 4.4 — Plugin config prompt for a single Config model
# ---------------------------------------------------------------------------


def prompt_plugin_config(
    config_model: type[BaseModel],
    console: Console,
) -> dict[str, Any]:
    """Prompt for each field in a plugin's Config model.

    Returns a dict of ``field_name → user-provided value``.
    Only includes fields the user explicitly set (non-empty answers).
    """
    fields = get_field_metadata(config_model)
    if not fields:
        return {}

    console.print(f"  [dim]Configure {config_model.__name__}:[/dim]")
    values: dict[str, Any] = {}

    for field_meta in fields:
        field_name = field_meta["name"]
        type_hint = field_meta["type_hint"]
        required = field_meta["required"]
        default = field_meta["default"]
        description = field_meta.get("description", "")

        # Build the prompt label
        if required:
            prompt_text = f"    {field_name} [bold red]*[/bold red]"
        else:
            prompt_text = f"    {field_name}"

        if description:
            prompt_text += f" [dim]({description})[/dim]"

        # Build default display for optional fields
        default_display: Any = ""
        if not required:
            if default is ... or isinstance(default, (list, dict)) or default is None:
                default_display = ""
            else:
                default_display = str(default)

        answer = Prompt.ask(
            prompt_text,
            default=default_display if default_display != "" else ...,
            show_default=(not required and default_display != ""),
        )

        # Skip empty answers — validation will catch missing required fields
        if not answer or (isinstance(answer, str) and not answer.strip()):
            continue

        parsed = _coerce_value(answer, type_hint)
        if parsed is not ...:
            values[field_name] = parsed

    console.print("  [dim]✓ Configuration complete[/dim]")
    return values


# ---------------------------------------------------------------------------
# Phase 4.3 — Per-category interactive plugin selection
# ---------------------------------------------------------------------------


def prompt_category(
    kind_name: str,
    registry: Any,
    console: Console,
) -> list[PluginSelection]:
    """Interactive plugin selection for one category.

    Shows all available plugins, lets the user pick which ones to add,
    prompts for config on each, and loops until the user is done.

    Parameters
    ----------
    kind_name : str
        Interface name (e.g. ``"data_monitors"``).
    registry : BaseClassInterface
        Plugin registry for this interface.
    console : Console
        Rich console for output.

    Returns
    -------
    list[PluginSelection]
        All plugins the user selected for this category.
    """
    display_label, yaml_kind = KIND_MAPPING[kind_name]

    console.print(
        Panel.fit(
            f"[bold]{display_label}s[/bold]"
            f" — select which {display_label.lower()} plugins to use",
            border_style="magenta",
        ),
    )

    plugins = list(registry.get_plugins())
    if not plugins:
        console.print(f"  [dim]No {display_label.lower()} plugins found.[/dim]")
        return []

    # Render the available-plugin table
    table = Table(title=f"Available {display_label}s", box=box.ROUNDED)
    table.add_column("Name", style="cyan")
    table.add_column("Description", style="dim")
    for plugin in plugins:
        desc = get_plugin_description(type(plugin)) or "(no description)"
        if len(desc) > _MAX_DESC_LENGTH:
            desc = desc[: _MAX_DESC_LENGTH - _TRUNCATE_THRESHOLD] + "..."
        table.add_row(plugin.name, desc)
    console.print(table)

    selections: list[PluginSelection] = []

    while True:
        chosen_name = Prompt.ask(
            f"Add a {display_label.lower()} (name or Enter to skip)",
            default="",
            show_default=False,
        )

        if not chosen_name or not chosen_name.strip():
            if selections:
                break
            if Confirm.ask(
                "No plugins selected. Continue without this category?",
                default=True,
            ):
                break
            continue

        # Resolve the chosen plugin by name (case-insensitive)
        matched = None
        for plugin in plugins:
            if plugin.name.lower() == chosen_name.strip().lower():
                matched = plugin
                break

        if matched is None:
            console.print(f"  [red]Unknown plugin: {chosen_name}[/red]")
            continue

        # Discover the companion Config model
        config_model = find_config_model(type(matched))

        # Prompt for config values
        config_values: dict[str, Any] = {}
        if config_model is not None and Confirm.ask(
            f"  Configure [cyan]{matched.name}[/cyan]?",
            default=True,
        ):
            config_values = prompt_plugin_config(config_model, console)

        selections.append(
            PluginSelection(
                plugin_class=type(matched),
                plugin_name=matched.name,
                interface_kind=kind_name,
                yaml_kind=yaml_kind,
                display_label=display_label,
                config_model=config_model,
                config_values=config_values,
            ),
        )

        if not Confirm.ask(f"  Add another {display_label.lower()}?", default=False):
            break

    return selections


# ---------------------------------------------------------------------------
# Phase 4.5 — Output path prompt
# ---------------------------------------------------------------------------


def prompt_output_path(service_name: str) -> Path:
    """Ask where to write the generated config file."""
    default = Path.cwd() / f"{service_name}-service.yaml"
    path_str = Prompt.ask("Output path", default=str(default))
    return Path(path_str)


# ---------------------------------------------------------------------------
# Phase 4.6 — Configuration preview
# ---------------------------------------------------------------------------


def show_preview(selections: list[PluginSelection], console: Console) -> None:
    """Show a summary table of selected plugins before writing."""
    table = Table(title="Configuration Preview", box=box.ROUNDED)
    table.add_column("Identifier", style="cyan")
    table.add_column("Kind", style="dim")
    table.add_column("Plugin", style="green")
    table.add_column("Config", style="dim")

    seen_ids: Counter[str] = Counter()
    for sel in selections:
        base_id = _make_identifier(sel.yaml_kind, sel.plugin_name)
        seen_ids[base_id] += 1

        if seen_ids[base_id] > 1:
            identifier = f"{base_id}-{seen_ids[base_id]}"
        else:
            identifier = base_id

        if sel.config_values:
            config_summary = ", ".join(sel.config_values.keys())
        else:
            config_summary = "(defaults)"

        if len(config_summary) > _MAX_CONFIG_SUMMARY_LENGTH:
            config_summary = (
                config_summary[: _MAX_CONFIG_SUMMARY_LENGTH - _TRUNCATE_THRESHOLD]
                + "..."
            )

        table.add_row(
            identifier,
            sel.yaml_kind,
            sel.plugin_name,
            config_summary,
        )

    console.print(table)


# ---------------------------------------------------------------------------
# Phase 5.1 — Build a ServiceConfigModel-compatible dict
# ---------------------------------------------------------------------------


def build_service_config(
    metadata: dict[str, str],
    selections: list[PluginSelection],
) -> dict[str, Any]:
    """Build a config dict from user selections.

    Pure function — no side effects.

    Parameters
    ----------
    metadata : dict
        Service metadata with ``name``, ``namespace``, ``description``.
    selections : list[PluginSelection]
        Ordered list of selected plugins (DM → JB → DP).

    Returns
    -------
    dict
        Dict suitable for ``ServiceConfigModel(**config_dict)``.
    """
    run_entries: list[dict[str, Any]] = []
    seen_ids: Counter[str] = Counter()

    for sel in selections:
        base_id = _make_identifier(sel.yaml_kind, sel.plugin_name)
        seen_ids[base_id] += 1

        if seen_ids[base_id] > 1:
            identifier = f"{base_id}-{seen_ids[base_id]}"
        else:
            identifier = base_id

        spec: dict[str, Any] = {
            "kind": sel.yaml_kind,
            "name": sel.plugin_name,
        }
        if sel.config_values:
            spec["config"] = sel.config_values

        run_entries.append(
            {
                "identifier": identifier,
                "spec": spec,
            },
        )

    return {
        "apiVersion": "runcourier.dev/v1alpha1",
        "kind": "Service",
        "metadata": {
            "name": metadata["name"],
            "namespace": metadata.get("namespace", metadata["name"]),
            "description": metadata.get(
                "description",
                f"A courier service: {metadata['name']}",
            ),
        },
        "spec": {
            "run": run_entries,
        },
    }


# ---------------------------------------------------------------------------
# Phase 5.2 — Validate against the schema (PURE)
# ---------------------------------------------------------------------------


def validate_config(config_dict: dict[str, Any]) -> ServiceConfigModel:
    """Validate a config dict against ``ServiceConfigModel``.

    Pure function — no side effects.  Raises ``ValidationError`` on failure.
    """
    return ServiceConfigModel(**config_dict)


# ---------------------------------------------------------------------------
# Phase 5.3 — Serialize and write YAML (IO)
# ---------------------------------------------------------------------------


def write_yaml(
    config: ServiceConfigModel,
    output_path: Path,
    console: Console,
) -> None:
    """Serialize a validated config model to YAML and write to file."""
    config_dict = config.model_dump(exclude_none=False, exclude_unset=False)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w") as f:
        yaml.dump(
            config_dict,
            f,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )

    console.print(f"\n[green]✓[/green] Config written to [bold]{output_path}[/bold]")


# ---------------------------------------------------------------------------
# Phase 5.4 — Post-generation hints
# ---------------------------------------------------------------------------


def print_help_hint(output_path: Path, console: Console) -> None:
    """Print hints for running and validating the generated config."""
    console.print()
    console.print(
        Panel.fit(
            f"[bold]Next steps:[/bold]\n"
            f"  Validate: [cyan]courier validate {output_path}[/cyan]\n"
            f"  Run:      [cyan]courier run {output_path}[/cyan]\n\n"
            f"[dim]The default Memory broker works for local testing.\n"
            f"To configure AMQP for production, add a [bold]broker[/bold]\n"
            f"section to spec in the generated YAML.[/dim]",
            border_style="green",
            title="Ready!",
        ),
    )


# ---------------------------------------------------------------------------
# Phase 6 — Main init command
# ---------------------------------------------------------------------------


def init(
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Preview the generated config without writing to file.",
    ),
) -> None:
    """Interactively create a new courier service configuration file.

    Walks through selecting data monitors, job builders, and dispatchers,
    then generates a validated YAML config file.
    """
    console = Console()

    console.print()
    console.print(
        Panel.fit(
            "[bold]Courier Init[/bold] — interactive service config generator\n"
            "[dim]Follow the prompts to create your service configuration.[/dim]",
            border_style="yellow",
        ),
    )

    # Step 1 — Metadata
    metadata = prompt_metadata(console)

    # Steps 2-4 -- Plugin selection (DM -> JB -> DP order)
    all_selections: list[PluginSelection] = []

    for kind_name in CATEGORY_ORDER:
        registry = PLUGIN_REGISTRIES[kind_name]
        category_selections = prompt_category(kind_name, registry, console)
        all_selections.extend(category_selections)

    if not all_selections:
        console.print(
            "\n[red]Error:[/red] No plugins selected. At least one plugin is required.",
        )
        raise typer.Exit(1)

    # Step 5 — Preview
    console.print()
    show_preview(all_selections, console)

    if not Confirm.ask("\nProceed with this configuration?", default=True):
        console.print("[dim]Cancelled.[/dim]")
        raise typer.Exit(0)

    # Step 6 — Build & validate
    config_dict = build_service_config(metadata, all_selections)

    try:
        config = validate_config(config_dict)
    except Exception as e:
        console.print(f"\n[red]Validation Error:[/red] {e}")
        raise typer.Exit(1) from e

    # Step 7 — Output
    if dry_run:
        console.print("\n[bold]Generated YAML (--dry-run):[/bold]")
        yaml.dump(
            config.model_dump(),
            console.file,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )
    else:
        output_path = prompt_output_path(metadata["name"])
        write_yaml(config, output_path, console)
        print_help_hint(output_path, console)
