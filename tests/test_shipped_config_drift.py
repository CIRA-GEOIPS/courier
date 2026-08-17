"""Drift guards: shipped artefacts must stay consistent with the code.

Courier ships YAML that operators copy — the root ``config.yaml``, the example
configs under ``tests/``, and the satellite metadata configs inside the
package. None of it is exercised by importing the library, so a schema change
or a renamed plugin breaks them silently and the failure only surfaces when
someone runs ``courier run`` in anger.

Each test here pins a contract *between* two artefacts rather than the shape of
either one.
"""

from __future__ import annotations

import importlib
import re
import sys
import tomllib
from importlib.metadata import entry_points
from pathlib import Path
from unittest import mock

import pytest

from courier.cli.config_loader import load_config
from courier.cli.plugins import PLUGIN_REGISTRIES, normalize_kind
from courier.errors import ConfigurationError
from courier.interfaces import data_monitor_configs
from courier.schema import DataMonitorConfig

_REPO_ROOT = Path(__file__).resolve().parent.parent

_SHIPPED_CONFIGS = sorted(
    [_REPO_ROOT / "config.yaml", *(_REPO_ROOT / "tests").glob("*.yaml")],
)

# Not service configs: broker fixtures and compose files live here too.
_NOT_SERVICE_CONFIGS = {"rabbitmq.conf", "docker-compose.rabbitmq-testing.yaml"}

_SERVICE_CONFIGS = [
    path for path in _SHIPPED_CONFIGS if path.name not in _NOT_SERVICE_CONFIGS
]
_IDS = [str(path.relative_to(_REPO_ROOT)) for path in _SERVICE_CONFIGS]


def test_shipped_configs_were_discovered() -> None:
    """Guard the guard: an empty glob would make every test below vacuous."""
    assert len(_SERVICE_CONFIGS) >= 4, f"only found {_IDS}"


@pytest.mark.parametrize("config_path", _SERVICE_CONFIGS, ids=_IDS)
def test_shipped_config_validates(config_path: Path) -> None:
    """Every YAML we ship must pass the same validation ``courier run`` uses."""
    try:
        load_config(config_path)
    except ConfigurationError as exc:
        pytest.fail(f"{config_path.name} no longer validates:\n{exc}")


@pytest.mark.parametrize("config_path", _SERVICE_CONFIGS, ids=_IDS)
def test_shipped_config_references_real_plugins(config_path: Path) -> None:
    """A renamed plugin must not leave a shipped config pointing at nothing.

    ``run_service`` skips unknown kinds silently, so a stale plugin name here
    produces a service that starts up and does nothing at all.
    """
    config = load_config(config_path)
    missing: list[str] = []

    for entry in config.spec.run:
        registry = PLUGIN_REGISTRIES.get(normalize_kind(entry.spec.kind))
        if registry is None:
            missing.append(f"{entry.identifier}: unknown kind {entry.spec.kind!r}")
            continue
        available = {plugin.name for plugin in registry.get_plugins()}
        if entry.spec.name not in available:
            missing.append(
                f"{entry.identifier}: {entry.spec.kind}/{entry.spec.name!r} "
                f"not registered (have {sorted(available)})",
            )

    assert not missing, "\n".join(missing)


@pytest.mark.parametrize("config_path", _SERVICE_CONFIGS, ids=_IDS)
def test_shipped_config_heartbeat_is_plausible_in_seconds(
    config_path: Path,
) -> None:
    """``heartbeat_interval`` is seconds, and was documented as milliseconds.

    Every shipped config once said ``1000 # in milliseconds``, which meant the
    service published health metrics every ~17 minutes. Anything above a few
    minutes is almost certainly the same unit confusion returning.
    """
    interval = load_config(config_path).spec.service_config.heartbeat_interval
    assert 0 < interval <= 300, (
        f"{config_path.name}: heartbeat_interval={interval}s is implausible; "
        f"the field is seconds, not milliseconds"
    )


#: Filter keys removed in 0.2.0, mapped to their canonical replacements.
#: A filter using one of these silently matches nothing -- the lookup falls
#: through both the metadata dict and the File attributes and returns False.
_REMOVED_FILTER_KEYS = {
    "platform": "source",
    "sensor": "instrument",
    "level": "processing_stage",
    "sector": "domain",
}


@pytest.mark.parametrize("config_path", _SERVICE_CONFIGS, ids=_IDS)
def test_shipped_config_avoids_removed_filter_keys(config_path: Path) -> None:
    """Legacy filter keys were removed; a config using one matches nothing.

    Note this checks ``filters`` only. ``field_map`` legitimately uses
    ``platform`` and ``sensor`` as *canonical* names on its left-hand side.
    """
    problems: list[str] = []

    for entry in load_config(config_path).spec.run:
        config = entry.spec.config or {}
        if not isinstance(config, dict):
            continue
        filter_blocks = [config.get("filters") or {}]
        filter_blocks += [
            route.get("filters") or {}
            for route in (config.get("routes") or [])
            if isinstance(route, dict)
        ]
        for block in filter_blocks:
            for key in block:
                if key in _REMOVED_FILTER_KEYS:
                    problems.append(
                        f"{entry.identifier}: filters on removed key {key!r}; "
                        f"use {_REMOVED_FILTER_KEYS[key]!r}",
                    )

    assert not problems, "\n".join(problems)


@pytest.mark.parametrize("config_path", _SERVICE_CONFIGS, ids=_IDS)
def test_shipped_config_filters_reference_real_file_fields(
    config_path: Path,
) -> None:
    """Filter keys must name a ``File`` attribute or be free-form metadata.

    Catches the class of mistake where a config filters on ``instrument:
    goes18`` — a platform value in a sensor field — which only ever matched
    because of a bug in the monitor.
    """
    from courier.types.file import File

    file_attrs = set(File().__dict__)
    platform_like = {"goes16", "goes17", "goes18", "goes19", "himawari9", "gk2a"}
    problems: list[str] = []

    for entry in load_config(config_path).spec.run:
        config = entry.spec.config or {}
        if not isinstance(config, dict):
            continue
        for key, value in (config.get("filters") or {}).items():
            if key not in file_attrs:
                continue  # metadata-dict key, checked at runtime
            if key == "instrument" and value in platform_like:
                problems.append(
                    f"{entry.identifier}: filters on instrument={value!r}, "
                    f"which is a platform — did you mean source?",
                )

    assert not problems, "\n".join(problems)


# ---------------------------------------------------------------------------
# Entry-point declarations
#
# Plugins are declared in pyproject.toml and read from installed distribution
# metadata. Three things can drift apart: the two toml tables, the toml and the
# installed metadata, and the metadata and the plugin classes on disk. Each has
# a silent failure mode, so each gets a guard.
# ---------------------------------------------------------------------------

_PYPROJECT = _REPO_ROOT / "pyproject.toml"

#: Entry-point groups courier reads for plugins, mapped to the interface base
#: class each member must subclass.
_PLUGIN_GROUPS = (
    "courier.data_monitors",
    "courier.job_builders",
    "courier.dispatchers",
)


def _pyproject() -> dict:
    with _PYPROJECT.open("rb") as handle:
        return tomllib.load(handle)


def _declared_entry_points(table: str) -> dict[str, dict[str, str]]:
    """Return ``{group: {name: target}}`` from one pyproject table.

    *table* is ``"poetry"`` for ``[tool.poetry.plugins.*]`` or ``"project"``
    for ``[project.entry-points.*]``.
    """
    data = _pyproject()
    if table == "poetry":
        return data["tool"]["poetry"].get("plugins", {})
    return data["project"].get("entry-points", {})


def test_pyproject_declares_plugin_groups() -> None:
    """Guard the guard: a renamed table would make every check below vacuous."""
    poetry = _declared_entry_points("poetry")
    assert set(_PLUGIN_GROUPS) <= set(poetry), sorted(poetry)


def test_pyproject_tables_agree() -> None:
    """``[tool.poetry.plugins]`` and ``[project.entry-points]`` must match.

    The two tables are alternatives, not additive: poetry-core 1.x ignores
    ``[project]`` and reads the poetry table, while 2.x reads ``[project]`` and
    ignores the poetry table *entirely*. A group declared in only one therefore
    vanishes from wheels built by the other backend — which is exactly what
    happened to ``runcourier.dev.plugin_packages`` when these tables were first
    added, taking plugin discovery down with it.
    """
    poetry = _declared_entry_points("poetry")
    project = _declared_entry_points("project")

    assert set(poetry) == set(project), (
        f"groups only in [tool.poetry.plugins]: {sorted(set(poetry) - set(project))}; "
        f"only in [project.entry-points]: {sorted(set(project) - set(poetry))}"
    )
    for group in sorted(poetry):
        assert poetry[group] == project[group], f"{group} differs between tables"


@pytest.mark.parametrize("group", _PLUGIN_GROUPS)
def test_declared_plugins_are_installed(group: str) -> None:
    """Everything declared in pyproject must be in the installed metadata.

    Entry points are read from the installed distribution, not the source tree,
    so a plugin added to pyproject stays invisible until the package is
    reinstalled. Without this guard the symptom is a config that validates and
    then resolves nothing.
    """
    declared = set(_declared_entry_points("project")[group])
    installed = {ep.name for ep in entry_points(group=group)}

    missing = declared - installed
    assert not missing, (
        f"{group}: {sorted(missing)} declared in pyproject.toml but missing "
        f"from installed metadata.\nRe-run:  pip install -e ."
    )


@pytest.mark.parametrize("group", _PLUGIN_GROUPS)
def test_installed_plugins_are_declared(group: str) -> None:
    """The reverse: nothing may linger in metadata that pyproject dropped."""
    declared = set(_declared_entry_points("project")[group])
    installed = {ep.name for ep in entry_points(group=group)}

    stale = installed - declared
    assert not stale, (
        f"{group}: {sorted(stale)} present in installed metadata but no longer "
        f"declared in pyproject.toml.\nRe-run:  pip install -e ."
    )


@pytest.mark.parametrize("group", _PLUGIN_GROUPS)
def test_every_declared_plugin_loads_to_its_class(group: str) -> None:
    """Each entry point must resolve to a class whose ``name`` matches its key.

    Three separate silent failures live here. A typo'd module path or a renamed
    class makes ``courier run`` resolve nothing for that plugin. A ``name``
    ClassVar disagreeing with the entry-point key is worse: discovery succeeds,
    but ``PluginManager`` registers the plugin under the class's own name, so
    queue names and metrics label it differently from the config that asked for
    it.
    """
    interface = group.removeprefix("courier.")
    problems: list[str] = []

    for entry_point in entry_points(group=group):
        try:
            loaded = entry_point.load()
        except Exception as exc:  # noqa: BLE001 - reported, not handled
            problems.append(f"{entry_point.name}: {entry_point.value} -> {exc!r}")
            continue
        if not isinstance(loaded, type):
            problems.append(f"{entry_point.name}: {loaded!r} is not a class")
            continue
        if loaded.name != entry_point.name:
            problems.append(
                f"{entry_point.name}: class declares name={loaded.name!r}",
            )
        if loaded.interface != interface:
            problems.append(
                f"{entry_point.name}: class declares interface="
                f"{loaded.interface!r}, expected {interface!r}",
            )

    assert not problems, "\n".join(problems)


def test_plugin_classes_on_disk_are_declared() -> None:
    """Every plugin module in the package must be declared as an entry point.

    Discovery used to scan the filesystem, so dropping a file into
    ``plugins/`` was enough. Entry points are explicit, which is the
    point — but it means a new plugin can be written, imported, tested in
    isolation, and still be invisible to ``courier run``. This is the guard that
    turns that into a failed test instead of a silent no-op.
    """
    plugin_root = _REPO_ROOT / "src" / "courier" / "plugins"
    declared_targets = {
        target.split(":")[0]
        for group in _PLUGIN_GROUPS
        for target in _declared_entry_points("project")[group].values()
    }

    undeclared: list[str] = []
    # One directory per interface, named after it. Scoped to the class-plugin
    # interfaces so the sibling data_monitor_configs/ directory -- whose modules
    # hold config instances, not plugin classes -- is not swept up here.
    for group in _PLUGIN_GROUPS:
        interface_dir = plugin_root / group.removeprefix("courier.")
        assert interface_dir.is_dir(), f"missing plugin directory {interface_dir}"
        for path in sorted(interface_dir.glob("*.py")):
            if path.name == "__init__.py":
                continue
            module = ".".join(
                ("courier", "plugins", path.parent.name, path.stem),
            )
            if module not in declared_targets:
                undeclared.append(module)

    assert not undeclared, (
        "plugin modules with no entry point declaration:\n"
        + "\n".join(undeclared)
        + "\nDeclare each in both pyproject.toml tables, then reinstall."
    )


#: The config interface. Separate from :data:`_PLUGIN_GROUPS` because its
#: members are validated model instances rather than plugin classes.
_CONFIG_GROUP = "courier.data_monitor_configs"


def test_every_declared_config_loads_to_a_validated_model() -> None:
    """Config entry points must resolve to models whose name matches the key.

    The name is what a monitor's ``metadata-tools`` list refers to. If the
    declaration and the model disagree, the config is unreachable by the name
    operators write, and a monitor asking for it enriches nothing.
    """
    problems: list[str] = []

    for entry_point in entry_points(group=_CONFIG_GROUP):
        try:
            loaded = entry_point.load()
        except Exception as exc:  # noqa: BLE001 - reported, not handled
            problems.append(f"{entry_point.name}: {entry_point.value} -> {exc!r}")
            continue
        if not isinstance(loaded, DataMonitorConfig):
            problems.append(f"{entry_point.name}: {type(loaded).__name__}, not a config")
            continue
        if loaded.name != entry_point.name:
            problems.append(
                f"{entry_point.name}: config declares name={loaded.name!r}",
            )

    assert not problems, "\n".join(problems)


def test_config_declarations_match_the_modules_on_disk() -> None:
    """Every config module must be declared, and every declaration must exist."""
    config_root = _REPO_ROOT / "src" / "courier" / "plugins" / "data_monitor_configs"
    on_disk = {
        path.stem for path in config_root.glob("*.py") if path.name != "__init__.py"
    }
    declared = set(_declared_entry_points("project")[_CONFIG_GROUP])

    assert on_disk == declared, (
        f"only on disk: {sorted(on_disk - declared)}; "
        f"only declared: {sorted(declared - on_disk)}"
    )


@pytest.mark.parametrize("config_path", _SERVICE_CONFIGS, ids=_IDS)
def test_metadata_tools_reference_real_configs(config_path: Path) -> None:
    """A monitor's ``metadata-tools`` must name configs that exist.

    Previously unguarded, and the failure is silent: an unknown name raises
    only when the monitor is constructed at run time, long after the config
    validated. ``tests/example1.yaml`` names four satellite configs and
    ``tests/geocolor_demo.yaml`` two, none of which any test checked.
    """
    available = set(data_monitor_configs.names())
    missing: list[str] = []

    for entry in load_config(config_path).spec.run:
        config = entry.spec.config or {}
        if not isinstance(config, dict):
            continue
        for tool in config.get("metadata-tools") or []:
            if tool not in available:
                missing.append(f"{entry.identifier}: metadata-tools/{tool!r}")

    assert not missing, (
        "\n".join(missing) + f"\navailable: {sorted(available)}"
    )


def test_shipped_configs_actually_use_metadata_tools() -> None:
    """Guard the guard: the check above is vacuous if nothing declares any."""
    total = 0
    for config_path in _SERVICE_CONFIGS:
        for entry in load_config(config_path).spec.run:
            config = entry.spec.config or {}
            if isinstance(config, dict):
                total += len(config.get("metadata-tools") or [])

    assert total >= 6, f"only {total} metadata-tools references found"


def test_no_yaml_plugins_remain() -> None:
    """Plugins are Python declared via entry points -- there is no YAML loader.

    A ``.yaml`` under the package would look like a plugin to a reader and be
    invisible to courier, which is the ambiguity removing the YAML format was
    meant to end.
    """
    package_root = _REPO_ROOT / "src" / "courier"
    stray = sorted(
        str(path.relative_to(_REPO_ROOT))
        for path in package_root.rglob("*.yaml")
    )

    assert not stray, "YAML files inside the package are never loaded:\n" + "\n".join(
        stray,
    )


# ---------------------------------------------------------------------------
# Optional plugin dependencies
#
# Five plugins need a third-party package courier does not install by default.
# Each imports it lazily so the module stays importable -- which is what lets
# `courier plugins list` enumerate every plugin on a minimal install -- and
# raises InvalidPluginConfigError naming the extra when the dependency is
# actually needed.
# ---------------------------------------------------------------------------

#: ``(plugin name, module, third-party package, extra)``
_OPTIONAL_DEPENDENCY_PLUGINS = [
    ("cron_glob", "courier.plugins.data_monitors.cron_glob", "croniter", "cron"),
    ("s3_poller", "courier.plugins.data_monitors.s3_poller", "boto3", "s3"),
    ("sftp_poller", "courier.plugins.data_monitors.sftp_poller", "paramiko", "sftp"),
    ("kafka_consumer", "courier.plugins.data_monitors.kafka_consumer", "kafka", "kafka"),
    ("http_dispatcher", "courier.plugins.dispatchers.http_dispatcher", "httpx", "http"),
]


@pytest.mark.parametrize(
    ("plugin_name", "module", "package", "extra"),
    _OPTIONAL_DEPENDENCY_PLUGINS,
    ids=[row[0] for row in _OPTIONAL_DEPENDENCY_PLUGINS],
)
def test_optional_dependency_is_declared_as_an_extra(
    plugin_name: str,
    module: str,
    package: str,
    extra: str,
) -> None:
    """Every extra a plugin tells operators to install must actually exist.

    The error messages name ``courier[<extra>]``. If the extra were renamed or
    never declared, that instruction would send the operator to a pip error.
    """
    extras = _pyproject()["tool"]["poetry"]["extras"]

    assert extra in extras, f"{plugin_name} names courier[{extra}], which is not declared"
    normalised = {name.replace("_", "-").lower() for name in extras[extra]}
    assert package.replace("_", "-") in normalised or any(
        package.replace("_", "-") in n for n in normalised
    ), f"courier[{extra}] does not provide {package}: {sorted(extras[extra])}"


@pytest.mark.parametrize(
    ("plugin_name", "module", "package", "extra"),
    _OPTIONAL_DEPENDENCY_PLUGINS,
    ids=[row[0] for row in _OPTIONAL_DEPENDENCY_PLUGINS],
)
def test_optional_dependency_is_imported_lazily(
    plugin_name: str,
    module: str,
    package: str,
    extra: str,
) -> None:
    """The plugin module must import without its third-party dependency.

    ``courier plugins list`` and the entry-point drift guards load every
    declared plugin. A module-scope ``import boto3`` would make a minimal
    install unable to even list its own plugins, and would take the whole
    listing down rather than failing only the plugin that needs it.

    ``sys.modules[name] = None`` makes ``import name`` raise ImportError, which
    is what a missing install looks like from inside the module.
    """
    with mock.patch.dict(sys.modules, {package: None}):
        for loaded in [module, *[m for m in list(sys.modules) if m == module]]:
            sys.modules.pop(loaded, None)
        importlib.import_module(module)  # must not raise


def test_a_missing_optional_dependency_names_its_extra() -> None:
    """The failure an operator actually hits must say how to fix itself.

    Checked on ``cron_glob`` because its dependency is reached from config
    validation, so one construction exercises the guard. A bare ImportError
    here would name ``croniter``, which is not a string the operator ever
    typed and does not tell them which extra supplies it.
    """
    from courier.errors import InvalidPluginConfigError
    from courier.plugins.data_monitors.cron_glob import CronGlobConfig

    with mock.patch.dict(sys.modules, {"croniter": None}):
        with pytest.raises(InvalidPluginConfigError, match=r"courier\[cron\]"):
            CronGlobConfig(path="/tmp", cron_expression="*/5 * * * *")


# ---------------------------------------------------------------------------
# Distribution name and version
#
# The install name (data-courier) differs from the import name (courier)
# because `courier` was taken on PyPI. That split is only safe if every place
# that tells an operator what to install agrees with what is actually
# published -- for most of this project's life it did not, and the docs and six
# plugin error messages pointed at a package it does not own.
# ---------------------------------------------------------------------------

_DISTRIBUTION_NAME = "data-courier"

#: Every extra courier declares, used to spot install instructions in prose.
_KNOWN_EXTRAS = (
    "cron", "s3", "sftp", "kafka", "http", "ha", "grafana", "viz",
    "doc", "lint", "test", "all-monitors", "all-dispatchers",
)


def test_pyproject_name_tables_agree() -> None:
    """Both tables must declare the same distribution name.

    poetry-core 2.x reads ``[project]`` and ignores ``[tool.poetry]``; 1.x does
    the reverse. Disagreeing names mean the wheel is published under a
    different name depending on which backend built it.
    """
    data = _pyproject()
    poetry_name = data["tool"]["poetry"]["name"]
    project_name = data["project"]["name"]

    assert poetry_name == project_name == _DISTRIBUTION_NAME, (
        f"[tool.poetry].name={poetry_name!r}, [project].name={project_name!r}, "
        f"expected {_DISTRIBUTION_NAME!r}"
    )


def _install_instruction_files() -> list[Path]:
    """Files that tell an operator what to install."""
    paths: list[Path] = [_REPO_ROOT / "README.md"]
    for root in ("src", "sphinx", "examples"):
        directory = _REPO_ROOT / root
        if directory.is_dir():
            paths += [
                path for path in directory.rglob("*") if path.suffix in {".py", ".md"}
            ]
    return sorted(paths)


def test_install_instructions_name_the_distribution() -> None:
    """Every ``pip install <name>[<extra>]`` must name the real distribution.

    This is the guard that was missing. The plugin error messages and the docs
    said ``pip install courier[s3]`` while the project published as
    ``runcourier`` -- an instruction that installs somebody else's package, and
    nothing in the suite objected for the life of the repo.

    Scoped to install commands that name one of courier's own extras, so
    ``pip install -e .`` and third-party examples in the plugin-authoring guide
    are left alone.
    """
    extras = "|".join(re.escape(extra) for extra in _KNOWN_EXTRAS)
    # ``pip install NAME[extra]`` (note the whitespace -- omitting it here made
    # this branch match nothing, so only the backticked form was checked) or a
    # backticked ``NAME[extra]`` reference in prose.
    pattern = re.compile(
        rf"(?:pip install\s+|`+)([A-Za-z0-9._-]+)\[({extras})\]",
    )
    wrong: list[str] = []

    for path in _install_instruction_files():
        for line_number, line in enumerate(path.read_text().splitlines(), start=1):
            for name, extra in pattern.findall(line):
                if name != _DISTRIBUTION_NAME:
                    location = path.relative_to(_REPO_ROOT)
                    wrong.append(f"{location}:{line_number}: {name}[{extra}]")

    assert not wrong, (
        f"install instructions naming something other than {_DISTRIBUTION_NAME!r}:\n"
        + "\n".join(wrong)
    )


def test_no_bare_install_of_the_import_name() -> None:
    """``pip install courier`` fetches an unrelated PyPI project."""
    offenders: list[str] = []
    for path in _install_instruction_files():
        for line_number, line in enumerate(path.read_text().splitlines(), start=1):
            if re.search(r"pip install +courier\b", line):
                offenders.append(f"{path.relative_to(_REPO_ROOT)}:{line_number}")

    assert not offenders, (
        "`pip install courier` installs a different project; use "
        f"{_DISTRIBUTION_NAME!r}:\n" + "\n".join(offenders)
    )


def test_version_matches_pyproject() -> None:
    """``courier.__version__`` must be the version the project declares.

    It is derived from installed metadata precisely so this cannot drift, but
    a stale editable install would still report the previous release. Compared
    with ``packaging.version.Version`` so ``1.0.0-alpha.29`` and ``1.0.0a29``
    -- the same version before and after PEP 440 normalisation -- compare equal.
    """
    from packaging.version import Version

    import courier

    declared = _pyproject()["project"]["version"]

    assert Version(courier.__version__) == Version(declared), (
        f"courier.__version__={courier.__version__!r} but pyproject declares "
        f"{declared!r}; re-run: pip install -e ."
    )


def test_version_tuple_matches_version() -> None:
    """The numeric tuple must track the string it is derived from."""
    import courier

    expected = tuple(
        int(part) for part in courier.__version__.split(".")[:3]
        if part.isdigit()
    )

    assert courier.__version_tuple__[: len(expected)] == expected, (
        f"{courier.__version_tuple__} does not match {courier.__version__}"
    )
