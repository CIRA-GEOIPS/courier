"""Unit tests for courier.cli.init — non-interactive functions."""

from __future__ import annotations

import io
import tempfile
from pathlib import Path

import pytest
import yaml
from rich.console import Console

from courier.cli.init import (
    PluginSelection,
    _coerce_value,
    _make_identifier,
    _resolve_plugin_choice,
    build_service_config,
    prompt_category,
    validate_config,
    write_yaml,
)
from courier.plugins.data_monitors.file_system_poller_watchdog import (
    FileSystemPoller,
    FileSystemPollerConfig,
)
from courier.plugins.dispatchers.serial_bash import (
    SerialBashDispatcher,
    SerialBashConfig,
)
from courier.plugins.job_builders.dummy_job_builder import (
    DummyJobBuilder,
    DummyJobBuilderConfig,
)


class TestMakeIdentifier:
    """Tests for _make_identifier()."""

    def test_replaces_underscores(self):
        """Underscores should be converted to hyphens."""
        result = _make_identifier("data_monitor", "rabbit_mq_watcher")
        assert result == "data-monitor-rabbit-mq-watcher"

    def test_lowercases(self):
        """Should output only lowercase."""
        result = _make_identifier("DataMonitor", "S3Poller")
        assert result == "datamonitor-s3poller"

    def test_strips_non_dns_chars(self):
        """Should remove characters that aren't alphanumeric or hyphens."""
        result = _make_identifier("data_monitor", "plugin@test!")
        assert result == "data-monitor-plugintest"

    def test_truncates_long_names(self):
        """Should truncate to 63 chars and not end with hyphen."""
        long_name = "a" * 100
        result = _make_identifier("data_monitor", long_name)
        assert len(result) <= 63
        assert not result.endswith("-")


class _FakePlugin:
    """Stand-in for a registered plugin — the resolver only reads ``.name``."""

    def __init__(self, name: str) -> None:
        self.name = name

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<_FakePlugin {self.name}>"


class _FakeRegistry:
    def __init__(self, *names: str) -> None:
        self._plugins = [_FakePlugin(name) for name in names]

    def get_plugins(self) -> list[_FakePlugin]:
        return list(self._plugins)


def _drive_category(
    monkeypatch: pytest.MonkeyPatch,
    registry: object,
    *answers: str,
    kind_name: str = "data_monitors",
) -> tuple[list[PluginSelection], str]:
    """Run ``prompt_category`` against *registry*, feeding it *answers*.

    Returns the selections and everything the command printed, so a test can
    assert against the table the user was actually looking at.
    """
    from courier.cli import init as init_module

    pending = list(answers)

    def _ask(*_args: object, **_kwargs: object) -> str:
        # Fail loudly rather than feeding "" forever: an answer that stops
        # resolving sends prompt_category round its retry loop, and a helper
        # that never runs out turns that into a hang instead of a failure.
        if not pending:
            msg = f"prompt_category asked for more than {len(answers)} answer(s)"
            raise AssertionError(msg)
        return pending.pop(0)

    monkeypatch.setattr(init_module.Prompt, "ask", staticmethod(_ask))
    # Declines "Configure X?" and "Add another?", so one answer == one pass.
    monkeypatch.setattr(init_module.Confirm, "ask", staticmethod(lambda *a, **k: False))

    console = Console(file=io.StringIO(), width=200, no_color=True)
    selections = prompt_category(kind_name, registry, console)
    return selections, console.file.getvalue()


def _table_rows(output: str) -> dict[str, str]:
    """Parse ``{displayed number: plugin name}`` out of a rendered rich table."""
    rows: dict[str, str] = {}
    for line in output.splitlines():
        if "│" not in line:
            continue
        cells = [cell.strip() for cell in line.split("│")[1:-1]]
        expected_columns = 3
        if len(cells) != expected_columns or not cells[0].isdigit():
            continue
        rows[cells[0]] = cells[1]
    return rows


class TestResolvePluginChoice:
    """Selecting a plugin without typing ``file_system_poller_watchdog``."""

    @pytest.fixture
    def plugins(self) -> list[_FakePlugin]:
        return _FakeRegistry(
            "file_system_poller_watchdog",
            "s3_poller",
            "sftp_poller",
        ).get_plugins()

    @pytest.mark.parametrize(
        ("answer", "expected"),
        [
            ("1", "file_system_poller_watchdog"),
            ("2", "s3_poller"),
            ("3", "sftp_poller"),
            ("  2  ", "s3_poller"),
        ],
    )
    def test_a_number_picks_that_row(
        self,
        plugins: list[_FakePlugin],
        answer: str,
        expected: str,
    ) -> None:
        matched, problem = _resolve_plugin_choice(answer, plugins)
        assert problem is None
        assert matched.name == expected

    @pytest.mark.parametrize(
        "answer",
        ["s3_poller", "S3_POLLER", "  S3_Poller  "],
    )
    def test_a_full_name_still_works(
        self,
        plugins: list[_FakePlugin],
        answer: str,
    ) -> None:
        """Numbers are an addition, not a replacement — scripts and muscle
        memory that spell the name out must keep working."""
        matched, problem = _resolve_plugin_choice(answer, plugins)
        assert problem is None
        assert matched.name == "s3_poller"

    def test_an_unambiguous_prefix_is_enough(
        self,
        plugins: list[_FakePlugin],
    ) -> None:
        matched, problem = _resolve_plugin_choice("s3", plugins)
        assert problem is None
        assert matched.name == "s3_poller"

    def test_an_ambiguous_prefix_is_refused_and_lists_the_candidates(
        self,
        plugins: list[_FakePlugin],
    ) -> None:
        """Guessing on the user's behalf would silently configure the wrong
        plugin; the config only fails much later, at run time."""
        matched, problem = _resolve_plugin_choice("s", plugins)

        assert matched is None
        assert "s3_poller" in problem
        assert "sftp_poller" in problem

    def test_an_exact_name_beats_a_prefix_of_another(self) -> None:
        plugins = _FakeRegistry("poller", "poller_extended").get_plugins()

        matched, problem = _resolve_plugin_choice("poller", plugins)

        assert problem is None
        assert matched.name == "poller"

    @pytest.mark.parametrize("answer", ["0", "4", "99", "-1"])
    def test_a_number_outside_the_table_is_refused(
        self,
        plugins: list[_FakePlugin],
        answer: str,
    ) -> None:
        matched, problem = _resolve_plugin_choice(answer, plugins)

        assert matched is None
        assert "1-3" in problem

    def test_an_unknown_name_is_refused_with_the_valid_range(
        self,
        plugins: list[_FakePlugin],
    ) -> None:
        matched, problem = _resolve_plugin_choice("nope", plugins)

        assert matched is None
        assert "nope" in problem
        assert "1-3" in problem

    def test_a_single_plugin_range_reads_as_one_not_a_span(self) -> None:
        plugins = _FakeRegistry("only_one").get_plugins()

        _, problem = _resolve_plugin_choice("7", plugins)

        assert "1-1" not in problem
        assert "choose 1." in problem


class TestNumberedSelection:
    """The number the user types must mean the row they are looking at."""

    def test_typing_a_number_selects_that_plugin(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        registry = _FakeRegistry("alpha_monitor", "beta_monitor", "gamma_monitor")

        selections, _ = _drive_category(monkeypatch, registry, "2")

        assert [s.plugin_name for s in selections] == ["beta_monitor"]

    def test_every_displayed_number_resolves_to_its_own_row(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The table and the resolver index the same list. If a future change
        sorts one and not the other, ``3`` quietly configures the wrong plugin
        — the config still validates, so nothing else would catch it.
        """
        from courier.interfaces import data_monitors

        plugins = list(data_monitors.get_plugins())
        _, output = _drive_category(monkeypatch, data_monitors, "1")

        rows = _table_rows(output)
        assert rows, f"no numbered rows found in:\n{output}"
        assert len(rows) == len(plugins)

        for number, displayed_name in rows.items():
            matched, problem = _resolve_plugin_choice(number, plugins)
            assert problem is None
            assert matched.name == displayed_name

    def test_the_prompt_advertises_the_number_range(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A numbered table nobody is told they can use is no improvement."""
        from courier.cli import init as init_module

        registry = _FakeRegistry("alpha", "beta", "gamma")
        asked: list[str] = []

        def _ask(prompt: str, *_args: object, **_kwargs: object) -> str:
            asked.append(prompt)
            return "1"

        monkeypatch.setattr(init_module.Prompt, "ask", staticmethod(_ask))
        monkeypatch.setattr(
            init_module.Confirm, "ask", staticmethod(lambda *a, **k: False),
        )

        prompt_category(
            "data_monitors",
            registry,
            Console(file=io.StringIO(), width=200, no_color=True),
        )

        assert "1-3" in asked[0]

    def test_a_rejected_answer_reprompts_instead_of_aborting(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        registry = _FakeRegistry("alpha_monitor", "beta_monitor")

        selections, output = _drive_category(monkeypatch, registry, "9", "2")

        assert [s.plugin_name for s in selections] == ["beta_monitor"]
        assert "out of range" in output

    def test_the_resolved_name_is_echoed_back(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Typing ``2`` gives no feedback about what ``2`` was."""
        registry = _FakeRegistry("alpha_monitor", "beta_monitor")

        _, output = _drive_category(monkeypatch, registry, "2")

        assert "beta_monitor" in output


class TestCoerceValue:
    """Tests for _coerce_value()."""

    def test_int(self):
        assert _coerce_value("42", "int") == 42

    def test_float(self):
        assert _coerce_value("3.14", "float") == 3.14

    def test_bool_true(self):
        assert _coerce_value("true", "bool") is True
        assert _coerce_value("yes", "bool") is True
        assert _coerce_value("1", "bool") is True

    def test_bool_false(self):
        assert _coerce_value("false", "bool") is False
        assert _coerce_value("no", "bool") is False

    def test_list_str(self):
        result = _coerce_value("a, b, c", "list[str]")
        assert result == ["a", "b", "c"]

    def test_empty_string_returns_sentinel(self):
        result = _coerce_value("", "str")
        assert result is ...


class TestBuildServiceConfig:
    """Tests for build_service_config()."""

    @staticmethod
    def _make_selection(plugin_class, plugin_name, yaml_kind, config_values=None):
        return PluginSelection(
            plugin_class=plugin_class,
            plugin_name=plugin_name,
            interface_kind="data_monitors",
            yaml_kind=yaml_kind,
            display_label="Data Monitor",
            config_model=None,
            config_values=config_values or {},
        )

    def test_basic_structure(self):
        sel = self._make_selection(FileSystemPoller, "file_system_poller_watchdog", "data_monitor")
        config = build_service_config(
            metadata={"name": "test-svc", "description": "test"},
            selections=[sel],
        )
        assert config["apiVersion"] == "runcourier.dev/v1alpha1"
        assert config["kind"] == "Service"
        assert config["metadata"]["name"] == "test-svc"
        assert len(config["spec"]["run"]) == 1

    def test_identifier_generation(self):
        sel = self._make_selection(FileSystemPoller, "file_system_poller_watchdog", "data_monitor")
        config = build_service_config(
            metadata={"name": "test", "description": "test"},
            selections=[sel],
        )
        assert config["spec"]["run"][0]["identifier"] == "data-monitor-file-system-poller-watchdog"

    def test_kind_is_singular(self):
        sel = self._make_selection(FileSystemPoller, "file_system_poller_watchdog", "data_monitor")
        config = build_service_config(
            metadata={"name": "test", "description": "test"},
            selections=[sel],
        )
        assert config["spec"]["run"][0]["spec"]["kind"] == "data_monitor"

    def test_config_values_included(self):
        sel = PluginSelection(
            plugin_class=FileSystemPoller,
            plugin_name="file_system_poller_watchdog",
            interface_kind="data_monitors",
            yaml_kind="data_monitor",
            display_label="Data Monitor",
            config_model=FileSystemPollerConfig,
            config_values={"path": "/tmp/watch", "hostname": "myhost"},
        )
        config = build_service_config(
            metadata={"name": "test", "description": "test"},
            selections=[sel],
        )
        assert config["spec"]["run"][0]["spec"]["config"]["path"] == "/tmp/watch"
        assert config["spec"]["run"][0]["spec"]["config"]["hostname"] == "myhost"

    def test_config_omitted_when_empty(self):
        sel = self._make_selection(FileSystemPoller, "file_system_poller_watchdog", "data_monitor")
        config = build_service_config(
            metadata={"name": "test", "description": "test"},
            selections=[sel],
        )
        assert "config" not in config["spec"]["run"][0]["spec"]

    def test_duplicate_names_add_suffix(self):
        sel = self._make_selection(FileSystemPoller, "file_system_poller_watchdog", "data_monitor")
        config = build_service_config(
            metadata={"name": "test", "description": "test"},
            selections=[sel, sel],
        )
        ids = [e["identifier"] for e in config["spec"]["run"]]
        assert ids[0] == "data-monitor-file-system-poller-watchdog"
        assert ids[1] == "data-monitor-file-system-poller-watchdog-2"
        assert len(set(ids)) == 2

    def test_multiple_plugins_different_types(self):
        sel_dm = PluginSelection(
            plugin_class=FileSystemPoller,
            plugin_name="file_system_poller_watchdog",
            interface_kind="data_monitors",
            yaml_kind="data_monitor",
            display_label="Data Monitor",
            config_model=FileSystemPollerConfig,
            config_values={"path": "/tmp"},
        )
        sel_jb = PluginSelection(
            plugin_class=DummyJobBuilder,
            plugin_name="DummyJobBuilder",
            interface_kind="job_builders",
            yaml_kind="job_builder",
            display_label="Job Builder",
            config_model=DummyJobBuilderConfig,
            config_values={},
        )
        sel_dp = PluginSelection(
            plugin_class=SerialBashDispatcher,
            plugin_name="serial_bash",
            interface_kind="dispatchers",
            yaml_kind="dispatcher",
            display_label="Dispatcher",
            config_model=SerialBashConfig,
            config_values={"bash_script": "echo hello"},
        )
        config = build_service_config(
            metadata={"name": "test", "description": "test"},
            selections=[sel_dm, sel_jb, sel_dp],
        )
        assert len(config["spec"]["run"]) == 3
        assert config["spec"]["run"][0]["spec"]["kind"] == "data_monitor"
        assert config["spec"]["run"][1]["spec"]["kind"] == "job_builder"
        assert config["spec"]["run"][2]["spec"]["kind"] == "dispatcher"


class TestValidateConfig:
    """Tests for validate_config()."""

    def test_valid_config_passes(self):
        sel = PluginSelection(
            plugin_class=FileSystemPoller,
            plugin_name="file_system_poller_watchdog",
            interface_kind="data_monitors",
            yaml_kind="data_monitor",
            display_label="Data Monitor",
            config_model=FileSystemPollerConfig,
            config_values={"path": "/tmp"},
        )
        config_dict = build_service_config(
            metadata={"name": "test", "description": "test"},
            selections=[sel],
        )
        validated = validate_config(config_dict)
        assert validated.metadata.name == "test"

    def test_invalid_config_raises(self):
        """Missing required fields should raise."""
        with pytest.raises(Exception):
            validate_config({"apiVersion": "bad", "kind": "Service", "metadata": {}, "spec": {}})

    def test_empty_run_raises(self):
        """Empty run list should be rejected."""
        with pytest.raises(Exception):
            validate_config({
                "apiVersion": "runcourier.dev/v1alpha1",
                "kind": "Service",
                "metadata": {
                    "name": "test",
                    "namespace": "test",
                    "description": "test",
                },
                "spec": {"run": []},
            })


class TestWriteYaml:
    """Tests for write_yaml()."""

    def test_roundtrip(self):
        """Generated YAML should round-trip through validation."""
        from rich.console import Console

        sel = PluginSelection(
            plugin_class=FileSystemPoller,
            plugin_name="file_system_poller_watchdog",
            interface_kind="data_monitors",
            yaml_kind="data_monitor",
            display_label="Data Monitor",
            config_model=FileSystemPollerConfig,
            config_values={"path": "/tmp"},
        )
        config_dict = build_service_config(
            metadata={"name": "test-roundtrip", "description": "roundtrip test"},
            selections=[sel],
        )
        validated = validate_config(config_dict)

        # Target a path that does not exist yet: write_yaml prompts before
        # overwriting, and an unanswered prompt would fail with no stdin.
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir) / "roundtrip.yaml"

            console = Console(file=None, width=80)  # quiet console
            write_yaml(validated, tmp_path, console)

            # Read back and re-validate
            with open(tmp_path) as f:
                written = yaml.safe_load(f)

            revalidated = validate_config(written)
            assert revalidated.metadata.name == "test-roundtrip"

    def test_write_yaml_refuses_to_overwrite_without_confirmation(
        self,
        tmp_path: Path,
        monkeypatch,
    ) -> None:
        """An existing config is left alone unless the operator says otherwise.

        ``courier init`` defaults the output path to ``<name>-service.yaml``,
        so accepting the default twice used to silently clobber hand-edited
        settings.
        """
        from rich.console import Console

        from courier.cli import init as init_module

        target = tmp_path / "existing.yaml"
        target.write_text("apiVersion: keep-me\n")

        config_dict = build_service_config(
            metadata={"name": "test-guard", "description": "guard test"},
            selections=[
                PluginSelection(
                    plugin_class=FileSystemPoller,
                    plugin_name="file_system_poller_watchdog",
                    interface_kind="data_monitors",
                    yaml_kind="data_monitor",
                    display_label="Data Monitor",
                    config_model=FileSystemPollerConfig,
                    config_values={"path": "/tmp"},
                ),
            ],
        )
        validated = validate_config(config_dict)

        monkeypatch.setattr(init_module.Confirm, "ask", lambda *a, **k: False)
        write_yaml(validated, target, Console(file=None, width=80))

        assert target.read_text() == "apiVersion: keep-me\n"

        monkeypatch.setattr(init_module.Confirm, "ask", lambda *a, **k: True)
        write_yaml(validated, target, Console(file=None, width=80))

        assert "test-guard" in target.read_text()
