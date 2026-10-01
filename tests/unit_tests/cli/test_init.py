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
from courier.interfaces.payloads import PayloadConfig
from courier.plugins.data_monitors.file_system_poller_watchdog import (
    FileSystemPoller,
    FileSystemPollerConfig,
)
from courier.plugins.dispatchers.local_dispatcher import (
    LocalDispatcher,
    LocalDispatcherConfig,
)
from courier.plugins.job_builders.dummy_job_builder import (
    DummyJobBuilder,
    DummyJobBuilderConfig,
)
from courier.plugins.payloads.bash_payload import BashPayload


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
        self.nested_values = []

    def get_plugins(self) -> list[_FakePlugin]:
        return list(self._plugins)


def _drive_category(
    monkeypatch: pytest.MonkeyPatch,
    registry: object,
    *answers: str,
    kind_name: str = "data_monitors",
    confirmations: list[str] | None = None,
) -> tuple[list[PluginSelection], str]:
    """Run ``prompt_category`` against *registry*, feeding it *answers*.

    Returns the selections and everything the command printed, so a test can
    assert against the table the user was actually looking at. Every yes/no
    question asked is appended to *confirmations*, when given.
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

    def _confirm(prompt: str, *_args: object, **_kwargs: object) -> bool:
        if confirmations is not None:
            confirmations.append(prompt)
        return False

    monkeypatch.setattr(init_module.Prompt, "ask", staticmethod(_ask))
    # Declines "Configure X?" and "Add another?", so one answer == one pass.
    monkeypatch.setattr(init_module.Confirm, "ask", staticmethod(_confirm))

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
            "slurm_dispatcher",
        ).get_plugins()

    @pytest.mark.parametrize(
        ("answer", "expected"),
        [
            ("1", "file_system_poller_watchdog"),
            ("2", "s3_poller"),
            ("3", "slurm_dispatcher"),
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
        assert "slurm_dispatcher" in problem

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
            init_module.Confirm,
            "ask",
            staticmethod(lambda *a, **k: False),
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
            yaml_kind=yaml_kind,
            display_label="Data Monitor",
            config_model=None,
            config_values=config_values or {},
            nested_values=[],
        )

    def test_basic_structure(self):
        sel = self._make_selection(
            FileSystemPoller, "file_system_poller_watchdog", "data_monitor"
        )
        config = build_service_config(
            metadata={"name": "test-svc", "description": "test"},
            selections=[sel],
        )
        assert config["apiVersion"] == "runcourier.dev/v1alpha1"
        assert config["kind"] == "Service"
        assert config["metadata"]["name"] == "test-svc"
        assert len(config["spec"]["run"]) == 1

    def test_identifier_generation(self):
        sel = self._make_selection(
            FileSystemPoller, "file_system_poller_watchdog", "data_monitor"
        )
        config = build_service_config(
            metadata={"name": "test", "description": "test"},
            selections=[sel],
        )
        assert (
            config["spec"]["run"][0]["identifier"]
            == "data-monitor-file-system-poller-watchdog"
        )

    def test_kind_is_singular(self):
        sel = self._make_selection(
            FileSystemPoller, "file_system_poller_watchdog", "data_monitor"
        )
        config = build_service_config(
            metadata={"name": "test", "description": "test"},
            selections=[sel],
        )
        assert config["spec"]["run"][0]["spec"]["kind"] == "data_monitor"

    def test_config_values_included(self):
        sel = PluginSelection(
            plugin_class=FileSystemPoller,
            plugin_name="file_system_poller_watchdog",
            yaml_kind="data_monitor",
            display_label="Data Monitor",
            config_model=FileSystemPollerConfig,
            config_values={"path": "/tmp/watch", "hostname": "myhost"},
            nested_values=[],
        )
        config = build_service_config(
            metadata={"name": "test", "description": "test"},
            selections=[sel],
        )
        assert config["spec"]["run"][0]["spec"]["config"]["path"] == "/tmp/watch"
        assert config["spec"]["run"][0]["spec"]["config"]["hostname"] == "myhost"

    def test_config_omitted_when_empty(self):
        sel = self._make_selection(
            FileSystemPoller, "file_system_poller_watchdog", "data_monitor"
        )
        config = build_service_config(
            metadata={"name": "test", "description": "test"},
            selections=[sel],
        )
        assert "config" not in config["spec"]["run"][0]["spec"]

    def test_duplicate_names_add_suffix(self):
        sel = self._make_selection(
            FileSystemPoller, "file_system_poller_watchdog", "data_monitor"
        )
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
            yaml_kind="data_monitor",
            display_label="Data Monitor",
            config_model=FileSystemPollerConfig,
            config_values={"path": "/tmp"},
            nested_values=[],
        )
        sel_payload = PluginSelection(
            plugin_class=BashPayload,
            plugin_name="bash_payload",
            yaml_kind="payload",
            display_label="Payload",
            config_model=PayloadConfig,
            config_values={"binary": "echo"},
            nested_values=[],
        )
        sel_jb = PluginSelection(
            plugin_class=DummyJobBuilder,
            plugin_name="DummyJobBuilder",
            yaml_kind="job_builder",
            display_label="Job Builder",
            config_model=DummyJobBuilderConfig,
            config_values={},
            nested_values=[sel_payload],
        )
        sel_dp = PluginSelection(
            plugin_class=LocalDispatcher,
            plugin_name="local_dispatcher",
            yaml_kind="dispatcher",
            display_label="Dispatcher",
            config_model=LocalDispatcherConfig,
            config_values={},
            nested_values=[],
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
            yaml_kind="data_monitor",
            display_label="Data Monitor",
            config_model=FileSystemPollerConfig,
            config_values={"path": "/tmp"},
            nested_values=[],
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
            validate_config(
                {"apiVersion": "bad", "kind": "Service", "metadata": {}, "spec": {}}
            )

    def test_empty_run_raises(self):
        """Empty run list should be rejected."""
        with pytest.raises(Exception):
            validate_config(
                {
                    "apiVersion": "runcourier.dev/v1alpha1",
                    "kind": "Service",
                    "metadata": {
                        "name": "test",
                        "namespace": "test",
                        "description": "test",
                    },
                    "spec": {"run": []},
                }
            )


class TestWriteYaml:
    """Tests for write_yaml()."""

    def test_roundtrip(self):
        """Generated YAML should round-trip through validation."""
        from rich.console import Console

        sel = PluginSelection(
            plugin_class=FileSystemPoller,
            plugin_name="file_system_poller_watchdog",
            yaml_kind="data_monitor",
            display_label="Data Monitor",
            config_model=FileSystemPollerConfig,
            config_values={"path": "/tmp"},
            nested_values=[],
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
                    yaml_kind="data_monitor",
                    display_label="Data Monitor",
                    config_model=FileSystemPollerConfig,
                    config_values={"path": "/tmp"},
                    nested_values=[],
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


# ---------------------------------------------------------------------------
# A job builder nests exactly one payload
# ---------------------------------------------------------------------------


def _nesting_registry(*names: str) -> _FakeRegistry:
    """A job-builder-shaped registry: each plugin nests one ``payload``."""
    registry = _FakeRegistry(*names)
    registry.nested_values = ["payload"]
    return registry


def _payload_selection(config_values: dict | None = None) -> PluginSelection:
    return PluginSelection(
        plugin_class=BashPayload,
        plugin_name="bash_payload",
        yaml_kind="payload",
        display_label="Payload",
        config_model=PayloadConfig,
        config_values=config_values or {},
        nested_values=[],
    )


def _builder_selection(payload: PluginSelection) -> PluginSelection:
    return PluginSelection(
        plugin_class=DummyJobBuilder,
        plugin_name="DummyJobBuilder",
        yaml_kind="job_builder",
        display_label="Job Builder",
        config_model=DummyJobBuilderConfig,
        config_values={},
        nested_values=[payload],
    )


class TestRequiredPayload:
    """``courier run`` rejects a builder without exactly one payload.

    The payload used to be prompted for with the generic category loop, which
    offered "Add another payload?" -- answering yes collected two, and the
    whole session was then thrown away by the exactly-one check. With no
    payload plugins installed the "a builder requires a payload" retry loop
    spun forever without asking anything.
    """

    def test_a_builder_gets_exactly_one_payload(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from courier.cli import init as init_module

        monkeypatch.setitem(
            init_module.PLUGIN_REGISTRIES,
            "payloads",
            _FakeRegistry("bash_payload", "shell_payload"),
        )
        confirmations: list[str] = []

        selections, output = _drive_category(
            monkeypatch,
            _nesting_registry("my_builder"),
            "1",
            "2",
            kind_name="job_builders",
            confirmations=confirmations,
        )

        assert [s.plugin_name for s in selections] == ["my_builder"]
        nested = selections[0].nested_values
        assert [(n.plugin_name, n.yaml_kind) for n in nested] == [
            ("shell_payload", "payload"),
        ]
        assert "shell_payload" in output
        # The trap: a second payload can never be valid, so it is never offered.
        assert not [c for c in confirmations if "another payload" in c.lower()]

    def test_skipping_the_payload_is_refused_and_asks_again(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from courier.cli import init as init_module

        monkeypatch.setitem(
            init_module.PLUGIN_REGISTRIES,
            "payloads",
            _FakeRegistry("bash_payload"),
        )

        selections, output = _drive_category(
            monkeypatch,
            _nesting_registry("my_builder"),
            "1",
            "",
            "nope",
            "1",
            kind_name="job_builders",
        )

        assert [n.plugin_name for n in selections[0].nested_values] == [
            "bash_payload",
        ]
        assert "requires a payload" in output
        assert "No plugin matches 'nope'" in output

    def test_no_installed_payload_plugins_exits_instead_of_looping(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A stale install can register no payloads; that must end init.

        ``_drive_category`` fails the test if it is asked for more answers than
        it was given, so a retry loop shows up as a failure, not a hang.
        """
        import typer

        from courier.cli import init as init_module

        monkeypatch.setitem(init_module.PLUGIN_REGISTRIES, "payloads", _FakeRegistry())
        console = Console(file=io.StringIO(), width=200, no_color=True)
        answers = iter(["1"])

        def _ask(*_args: object, **_kwargs: object) -> str:
            try:
                return next(answers)
            except StopIteration:
                pytest.fail("prompted again after the payload registry was empty")

        monkeypatch.setattr(init_module.Prompt, "ask", staticmethod(_ask))
        monkeypatch.setattr(
            init_module.Confirm,
            "ask",
            staticmethod(lambda *a, **k: False),
        )

        with pytest.raises(typer.Exit) as exc_info:
            prompt_category("job_builders", _nesting_registry("my_builder"), console)

        assert exc_info.value.exit_code == 1
        output = console.file.getvalue()
        assert "no payload plugins are installed" in output
        assert "pip install" in output


class TestPreviewShowsPayloads:
    """The confirmation screen must show what every job will execute."""

    def test_the_payload_is_listed_under_its_builder(self) -> None:
        from courier.cli.init import show_preview

        selections = [
            _builder_selection(_payload_selection({"script": "echo hi"})),
            PluginSelection(
                plugin_class=LocalDispatcher,
                plugin_name="local_dispatcher",
                yaml_kind="dispatcher",
                display_label="Dispatcher",
                config_model=LocalDispatcherConfig,
            ),
        ]
        console = Console(file=io.StringIO(), width=200, no_color=True)

        show_preview(selections, console)

        lines = console.file.getvalue().splitlines()
        builder_row = next(
            i for i, line in enumerate(lines) if "DummyJobBuilder" in line
        )
        payload_row = next(i for i, line in enumerate(lines) if "bash_payload" in line)
        dispatcher_row = next(
            i for i, line in enumerate(lines) if "local_dispatcher" in line
        )
        assert builder_row < payload_row < dispatcher_row
        assert "└─payload-bash-payload" in lines[payload_row]
        assert "script" in lines[payload_row], "the payload's settings are summarised"

    def test_preview_identifiers_match_the_written_config(self) -> None:
        """Two builders' payloads collide on their base identifier; the preview
        must number them exactly as the written YAML does."""
        from courier.cli.init import show_preview

        selections = [
            _builder_selection(_payload_selection()),
            _builder_selection(_payload_selection()),
        ]
        console = Console(file=io.StringIO(), width=200, no_color=True)
        show_preview(selections, console)
        config = build_service_config({"name": "svc"}, selections)

        written = [
            entry["spec"]["config"]["payload"]["identifier"]
            for entry in config["spec"]["run"]
        ]
        assert written == ["payload-bash-payload", "payload-bash-payload-2"]
        output = console.file.getvalue()
        for identifier in written:
            assert f"└─{identifier} " in output


class TestBuildServiceConfigPayloads:
    """The nested payload block is written the way ``courier run`` reads it."""

    def test_the_generated_config_passes_validate_plugin_checks(self) -> None:
        from courier.cli.validate import check_plugins

        config = validate_config(
            build_service_config(
                {"name": "svc"},
                [
                    _builder_selection(_payload_selection({"script": "echo hi"})),
                    PluginSelection(
                        plugin_class=LocalDispatcher,
                        plugin_name="local_dispatcher",
                        yaml_kind="dispatcher",
                        display_label="Dispatcher",
                        config_model=LocalDispatcherConfig,
                    ),
                ],
            ),
        )

        assert check_plugins(config).problems == []


class TestInitCommand:
    """``courier init`` end to end, answering through stdin."""

    _ANSWERS = (
        "svc",  # service name
        "svc",  # namespace
        "a test service",  # description
        "",  # skip data monitors ...
        "y",  # ... and confirm continuing without one
        "DummyJobBuilder",
        "n",  # configure DummyJobBuilder?
        "bash_payload",
        "n",  # configure bash_payload?
        "n",  # add another job builder?
        "local_dispatcher",
        "n",  # configure local_dispatcher?
        "n",  # add another dispatcher?
        "y",  # proceed with this configuration?
    )

    def test_dry_run_writes_the_payload(self) -> None:
        from typer.testing import CliRunner

        from courier.cli.app import app

        result = CliRunner().invoke(
            app,
            ["init", "--dry-run"],
            input="\n".join(self._ANSWERS) + "\n",
        )

        assert result.exit_code == 0, result.output
        generated = result.output.split("Generated YAML (--dry-run):", 1)[1]
        builder = yaml.safe_load(generated)["spec"]["run"][0]
        assert builder["spec"]["config"]["payload"]["spec"] == {
            "kind": "payload",
            "name": "bash_payload",
        }
