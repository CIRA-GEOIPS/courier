"""Unit tests for the Slurm dispatcher: config, parsing, startup, command shape.

Submission itself -- ``sbatch`` and ``sacct`` really run -- is covered with
fake Slurm tools in ``test_slurm_submission.py``.  These tests stop short of
running anything, but still go through the real payload classes and
``initialize_environment``, so the command a job would be submitted with is
the one checked here.
"""

# cspell:ignore partitoin

from __future__ import annotations

import logging
import shlex
import stat
import tempfile
from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError

from courier.errors import InvalidPluginConfigError, PluginStartupError
from courier.plugins.dispatchers.slurm_dispatcher import (
    SlurmDispatcher,
    SlurmDispatcherConfig,
    SlurmSubmission,
    _parse_sacct_output,
    _parse_sbatch_job_id,
    _SacctRecord,
)
from courier.plugins.payloads.bash_payload import BashPayload
from courier.plugins.payloads.python_payload import PythonPayload
from courier.plugins.payloads.shell_payload import ShellPayload
from courier.types.job import Job
from tests._helpers import captured_records, wire_job

if TYPE_CHECKING:
    from pathlib import Path
    from unittest.mock import MagicMock

    from courier.interfaces.payloads import Payload


def _dispatcher(
    service: MagicMock, tmp_path: Path, **config: object
) -> SlurmDispatcher:
    base: dict[str, object] = {"slurm_output_dir": str(tmp_path / "slurm")}
    base.update(config)
    return SlurmDispatcher(service, base, identifier="sd")


def _job(identifier: str = "job-1") -> Job:
    return Job("n", identifier, {})


def _prepare(dispatcher: SlurmDispatcher, job: Job) -> SlurmSubmission:
    env = dispatcher.initialize_environment(job, dispatcher._resolve_job_payload(job))
    assert isinstance(env, SlurmSubmission)
    return env


def _options_end(command: list[str]) -> int:
    """Return the index just past the dispatcher's own sbatch options."""
    index = 1
    while index < len(command) and command[index].startswith("--"):
        if command[index] == "--wrap":
            return index
        index += 1
    return index


class TestConfig:
    def test_slurm_options_are_validated_by_the_slurm_model(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        dispatcher = _dispatcher(service, tmp_path, partition="gpu", ntasks=2)

        assert SlurmDispatcher.config_class is SlurmDispatcherConfig
        assert isinstance(dispatcher.config, SlurmDispatcherConfig)
        assert dispatcher.config.partition == "gpu"
        assert dispatcher.config.ntasks == 2  # noqa: PLR2004

    def test_unknown_key_is_rejected(self, service: MagicMock, tmp_path: Path) -> None:
        with pytest.raises(ValidationError, match="partitoin"):
            _dispatcher(service, tmp_path, partitoin="gpu")

    def test_removed_sbatch_template_explains_the_migration(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        with pytest.raises(
            ValidationError, match="'sbatch_template' is no longer supported"
        ):
            _dispatcher(service, tmp_path, sbatch_template="#!/bin/bash\n")

    def test_output_dir_is_required(self, service: MagicMock) -> None:
        with pytest.raises(ValidationError, match="slurm_output_dir"):
            SlurmDispatcher(service, {}, identifier="sd")

    def test_relative_output_dir_is_made_absolute(
        self,
        service: MagicMock,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        dispatcher = SlurmDispatcher(
            service,
            {"slurm_output_dir": "relative/out"},
            identifier="sd",
        )

        assert dispatcher._output_dir == tmp_path / "relative" / "out"


class TestSbatchArgs:
    def test_minimal_args(self, service: MagicMock, tmp_path: Path) -> None:
        dispatcher = _dispatcher(service, tmp_path)
        pattern = f"{tmp_path / 'slurm'}/job-1-%j"

        assert dispatcher._build_sbatch_args(_job()) == [
            "sbatch",
            "--parsable",
            "--job-name=courier-job-1",
            f"--output={pattern}.out",
            f"--error={pattern}.err",
        ]

    def test_optional_flags_are_included(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        dispatcher = _dispatcher(
            service,
            tmp_path,
            partition="gpu",
            account="acct",
            qos="high",
            time_limit="01:00:00",
            ntasks=4,
            mem_per_node="8G",
            sbatch_extra_args=["--gres=gpu:1"],
        )

        assert dispatcher._build_sbatch_args(_job())[5:] == [
            "--partition=gpu",
            "--account=acct",
            "--qos=high",
            "--time=01:00:00",
            "--ntasks=4",
            "--mem=8G",
            "--gres=gpu:1",
        ]

    def test_empty_options_are_omitted(
        self, service: MagicMock, tmp_path: Path
    ) -> None:
        dispatcher = _dispatcher(service, tmp_path, partition="", account=None)

        assert len(dispatcher._build_sbatch_args(_job())) == 5  # noqa: PLR2004

    def test_percent_in_output_dir_is_not_a_slurm_pattern(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        dispatcher = SlurmDispatcher(
            service,
            {"slurm_output_dir": str(tmp_path / "100%")},
            identifier="sd",
        )

        args = dispatcher._build_sbatch_args(_job())

        assert f"--output={tmp_path}/100%%/job-1-%j.out" in args

    def test_output_paths_resolve_the_job_id_pattern(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        dispatcher = _dispatcher(service, tmp_path)

        out_path, err_path = dispatcher._output_paths(_job("a/b.nc"), "42")

        assert out_path.parent == err_path.parent == tmp_path / "slurm"
        assert out_path.name.endswith("-42.out")
        assert err_path.name.endswith("-42.err")
        assert "/" not in out_path.name


class TestSbatchOutputParsing:
    @pytest.mark.parametrize(
        ("stdout", "expected"),
        [
            ("4321\n", ("4321", None)),
            ("4321;cluster-a\n", ("4321", "cluster-a")),
            ("Submitted batch job 4321\n", ("4321", None)),
            ("Submitted batch job 4321 on cluster b\n", ("4321", "b")),
            ("sbatch: warning: something\n4321\n", ("4321", None)),
            ("", None),
            ("nope", None),
            ("12ab", None),
        ],
    )
    def test_job_id_and_cluster(
        self,
        stdout: str,
        expected: tuple[str, str | None] | None,
    ) -> None:
        assert _parse_sbatch_job_id(stdout) == expected


class TestSacctParsing:
    @pytest.mark.parametrize(
        ("stdout", "expected"),
        [
            ("COMPLETED|0:0|12\n", _SacctRecord("COMPLETED", 0, 12.0)),
            ("FAILED|2:0|3\nFAILED|2:0|3\n", _SacctRecord("FAILED", 2, 3.0)),
            ("CANCELLED by 1234|0:15|0\n", _SacctRecord("CANCELLED", 0, 0.0)),
            ("RUNNING|0:0\n", _SacctRecord("RUNNING", 0, None)),
            ("FAILED|x:0|\n", _SacctRecord("FAILED", -1, None)),
            ("\n|0:0|1\n", _SacctRecord("", 0, None)),
            ("", _SacctRecord("", 0, None)),
        ],
    )
    def test_record(self, stdout: str, expected: _SacctRecord) -> None:
        assert _parse_sacct_output(stdout) == expected


def _tools_dir(tmp_path: Path, *tools: str) -> Path:
    """Return a directory holding do-nothing executables named *tools*."""
    directory = tmp_path / "bin"
    directory.mkdir(exist_ok=True)
    for tool in tools:
        path = directory / tool
        path.write_text("#!/bin/sh\nexit 0\n")
        path.chmod(0o755)
    return directory


class TestStart:
    def test_start_requires_sbatch_on_path(
        self,
        service: MagicMock,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("PATH", str(_tools_dir(tmp_path, "sacct")))
        dispatcher = _dispatcher(service, tmp_path)

        with pytest.raises(PluginStartupError, match="'sbatch'"):
            dispatcher.start()

    def test_waiting_requires_sacct(
        self,
        service: MagicMock,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("PATH", str(_tools_dir(tmp_path, "sbatch")))
        dispatcher = _dispatcher(service, tmp_path)

        with pytest.raises(PluginStartupError, match="'sacct'"):
            dispatcher.start()

    def test_not_waiting_needs_only_sbatch(
        self,
        service: MagicMock,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("PATH", str(_tools_dir(tmp_path, "sbatch")))
        dispatcher = _dispatcher(service, tmp_path, wait_for_completion=False)

        dispatcher.start()
        try:
            assert dispatcher._output_dir.is_dir()
        finally:
            dispatcher.stop()

    def test_uncreatable_output_dir_is_a_config_error(
        self,
        service: MagicMock,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("PATH", str(_tools_dir(tmp_path, "sbatch", "sacct")))
        blocker = tmp_path / "file"
        blocker.write_text("")
        dispatcher = SlurmDispatcher(
            service,
            {"slurm_output_dir": str(blocker / "out")},
            identifier="sd",
        )

        with pytest.raises(InvalidPluginConfigError, match="slurm_output_dir"):
            dispatcher.start()

    def test_local_process_options_are_reported(
        self,
        service: MagicMock,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("PATH", str(_tools_dir(tmp_path, "sbatch", "sacct")))
        dispatcher = _dispatcher(
            service,
            tmp_path,
            log_to_file=True,
            log_dir=str(tmp_path / "logs"),
            log_only_errors=True,
            timeout_seconds=5,
        )

        with captured_records("courier.plugin.slurm_dispatcher") as records:
            dispatcher.start()
        dispatcher.stop()

        warnings = [r.getMessage() for r in records if r.levelno == logging.WARNING]
        assert any(
            all(
                key in message
                for key in ("timeout_seconds", "log_to_file", "log_only_errors")
            )
            for message in warnings
        ), warnings

    def test_output_files_without_waiting_are_reported(
        self,
        service: MagicMock,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("PATH", str(_tools_dir(tmp_path, "sbatch")))
        dispatcher = _dispatcher(
            service,
            tmp_path,
            wait_for_completion=False,
            output_files=[{"pattern": r"(?P<file>/\S+\.nc)"}],
        )

        with captured_records("courier.plugin.slurm_dispatcher") as records:
            dispatcher.start()
        dispatcher.stop()

        assert any(
            "output_files" in r.getMessage()
            for r in records
            if r.levelno == logging.WARNING
        )

    def test_defaults_are_not_reported(
        self,
        service: MagicMock,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("PATH", str(_tools_dir(tmp_path, "sbatch", "sacct")))
        dispatcher = _dispatcher(service, tmp_path, log_to_file=False)

        with captured_records("courier.plugin.slurm_dispatcher") as records:
            dispatcher.start()
        dispatcher.stop()

        assert not [r for r in records if r.levelno >= logging.WARNING]


class TestBatchScriptSubmission:
    """A shell script that runs directly is the batch script itself."""

    @pytest.mark.parametrize(
        ("payload_cls", "shebang"),
        [
            (BashPayload, "#!/usr/bin/env bash"),
            (ShellPayload, "#!/usr/bin/env sh"),
        ],
    )
    def test_inline_script_without_shebang_gets_the_payload_interpreter(
        self,
        service: MagicMock,
        tmp_path: Path,
        payload_cls: type[Payload],
        shebang: str,
    ) -> None:
        dispatcher = _dispatcher(service, tmp_path)
        job = wire_job(service, {"script": "echo hi"}, payload_cls=payload_cls)

        env = _prepare(dispatcher, job)

        assert env.file is not None
        assert env.file.read_text() == f"{shebang}\necho hi"
        assert env.command[_options_end(env.command) :] == [str(env.file)]

    def test_absolute_interpreter_is_used_as_is(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        dispatcher = _dispatcher(service, tmp_path)
        job = wire_job(
            service, {"script": "echo hi", "default_binary": "/opt/bash5/bin/bash"}
        )

        env = _prepare(dispatcher, job)

        assert env.file is not None
        assert env.file.read_text().splitlines()[0] == "#!/opt/bash5/bin/bash"

    @pytest.mark.parametrize("name", ["out{{7*7}}", "out{%x"])
    def test_script_path_is_never_rendered_as_a_template(
        self,
        service: MagicMock,
        tmp_path: Path,
        name: str,
    ) -> None:
        """Only argument templates render; slurm_output_dir stays literal."""
        output_dir = tmp_path / name
        dispatcher = _dispatcher(service, output_dir)
        job = wire_job(service, {"script": "echo hi", "suffix_args": ["{{ 6 * 7 }}"]})

        env = _prepare(dispatcher, job)

        assert env.file is not None
        assert env.file.parent == output_dir / "slurm"
        assert env.command[_options_end(env.command) :] == [str(env.file), "42"]

    def test_existing_shebang_is_kept(self, service: MagicMock, tmp_path: Path) -> None:
        dispatcher = _dispatcher(service, tmp_path)
        job = wire_job(service, {"script": "#!/bin/bash -l\nmodule load x"})

        env = _prepare(dispatcher, job)

        assert env.file is not None
        assert env.file.read_text() == "#!/bin/bash -l\nmodule load x"

    @pytest.mark.parametrize(
        "shebang",
        ["#!/usr/bin/env bash", "#!/usr/bin/env -S bash -l", "#!/usr/local/bin/bash"],
    )
    def test_shebang_naming_the_interpreter_is_kept(
        self,
        service: MagicMock,
        tmp_path: Path,
        shebang: str,
    ) -> None:
        """Any spelling of the payload's interpreter keeps its options."""
        dispatcher = _dispatcher(service, tmp_path)
        job = wire_job(service, {"script": f"{shebang}\necho hi"})

        env = _prepare(dispatcher, job)

        assert env.file is not None
        assert env.file.read_text() == f"{shebang}\necho hi"

    def test_shebang_naming_another_interpreter_is_replaced(
        self,
        service: MagicMock,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Slurm must run the interpreter a local dispatcher would.

        A local dispatcher runs ``bash <script>`` whatever the shebang says;
        keeping ``#!/bin/sh`` would run bash syntax under sh on Slurm only.
        """
        dispatcher = _dispatcher(service, tmp_path)
        script = "#!/bin/sh\n#SBATCH --time=00:05:00\n[[ -n x ]] && echo hi"
        job = wire_job(service, {"script": script})

        with caplog.at_level(logging.WARNING):
            env = _prepare(dispatcher, job)

        assert env.file is not None
        assert env.file.read_text() == (
            "#!/usr/bin/env bash\n#SBATCH --time=00:05:00\n[[ -n x ]] && echo hi"
        )
        assert "replacing shebang '#!/bin/sh'" in caplog.text

    def test_shebang_is_replaced_for_an_absolute_default_binary(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        """A configured binary wins over a shebang naming another bash."""
        dispatcher = _dispatcher(service, tmp_path)
        job = wire_job(
            service,
            {"script": "#!/bin/bash\necho hi", "default_binary": "/opt/bash5/bin/bash"},
        )

        env = _prepare(dispatcher, job)

        assert env.file is not None
        assert env.file.read_text() == "#!/opt/bash5/bin/bash\necho hi"

    def test_shebang_is_left_alone_on_a_wrapped_script(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        """--wrap names the interpreter explicitly; the script is untouched."""
        dispatcher = _dispatcher(service, tmp_path)
        job = wire_job(
            service,
            {"script": "#!/usr/bin/python2\nprint(1)"},
            payload_cls=PythonPayload,
        )

        env = _prepare(dispatcher, job)

        assert env.file is not None
        assert env.file.read_text() == "#!/usr/bin/python2\nprint(1)"


class TestWrappedSubmission:
    """Everything else is ``--wrap``: the local command, each argument quoted."""

    @staticmethod
    def _wrapped(env: SlurmSubmission) -> list[str]:
        assert env.command.count("--wrap") == 1
        index = env.command.index("--wrap")
        assert index == _options_end(env.command)
        assert len(env.command) == index + 2, "nothing may follow --wrap's value"
        return shlex.split(env.command[index + 1])

    def test_python_inline_script_is_wrapped(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        dispatcher = _dispatcher(service, tmp_path)
        job = wire_job(service, {"script": "print(1)"}, payload_cls=PythonPayload)

        env = _prepare(dispatcher, job)

        assert env.file is not None
        assert self._wrapped(env) == ["python", str(env.file)]
        assert env.file.read_text() == "print(1)", "no shebang is added to --wrap"

    def test_prefix_args_are_interpreter_options_not_sbatch_options(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        # Given to sbatch, "-e" would be --error and swallow the script path.
        dispatcher = _dispatcher(service, tmp_path)
        job = wire_job(service, {"script": "echo hi", "prefix_args": ["-e", "-x"]})

        env = _prepare(dispatcher, job)

        assert env.file is not None
        assert "-e" not in env.command
        assert self._wrapped(env) == ["bash", "-e", "-x", str(env.file)]

    def test_toolchain_prepend_does_not_force_wrap(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        """It is never part of a job's command, so #SBATCH lines still apply.

        (A bash payload ignores it; only python_payload uses it, in front of
        its toolchain probes.)
        """
        dispatcher = _dispatcher(service, tmp_path)
        job = wire_job(
            service,
            {"script": "#SBATCH --gres=gpu:1\necho hi", "toolchain_prepend": ["env"]},
        )

        env = _prepare(dispatcher, job)

        assert "--wrap" not in env.command
        assert env.file is not None
        assert env.command[_options_end(env.command) :] == [str(env.file)]

    def test_empty_arguments_survive_the_wrap(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        dispatcher = _dispatcher(service, tmp_path)
        job = wire_job(
            service,
            {"binary": "printf", "prefix_args": ["[%s]", ""], "suffix_args": [""]},
        )

        env = _prepare(dispatcher, job)

        assert self._wrapped(env)[-4:] == ["printf", "[%s]", "", ""]


class TestScriptFiles:
    def test_script_is_created_in_the_output_dir_not_tmpdir(
        self,
        service: MagicMock,
        tmp_path: Path,
        private_tmpdir: Path,
    ) -> None:
        dispatcher = _dispatcher(service, tmp_path)

        env = _prepare(dispatcher, wire_job(service, {"script": "echo hi"}))

        assert env.file is not None
        assert env.file.parent == dispatcher._output_dir
        assert env.file.name.startswith("job-1-")
        assert stat.S_IMODE(env.file.stat().st_mode) == 0o755  # noqa: PLR2004
        assert list(private_tmpdir.iterdir()) == []

    def test_two_submissions_of_one_job_get_distinct_scripts(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        first = _dispatcher(service, tmp_path)
        second = SlurmDispatcher(
            service,
            {"slurm_output_dir": str(tmp_path / "slurm")},
            identifier="sd-2",
        )
        job = wire_job(service, {"script": "echo hi"})

        paths = {_prepare(d, job).file for d in (first, first, second)}

        assert len(paths) == 3  # noqa: PLR2004

    def test_symlink_at_the_old_predictable_path_is_not_followed(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        victim = tmp_path / "victim"
        victim.write_text("precious\n")
        dispatcher = _dispatcher(service, tmp_path)
        dispatcher._output_dir.mkdir(parents=True)
        (dispatcher._output_dir / "job-1.sh").symlink_to(victim)

        env = _prepare(dispatcher, wire_job(service, {"script": "echo hi"}))

        assert victim.read_text() == "precious\n"
        assert env.file is not None
        assert not env.file.is_symlink()
        assert stat.S_IMODE(victim.stat().st_mode) != 0o755  # noqa: PLR2004

    @pytest.mark.skipif(
        not hasattr(tempfile, "_get_candidate_names"),
        reason="relies on CPython's tempfile name source",
    )
    def test_symlink_at_the_chosen_name_is_not_followed(
        self,
        service: MagicMock,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Even a symlink planted at the exact random name is never written through."""
        victim = tmp_path / "victim"
        victim.write_text("precious\n")
        dispatcher = _dispatcher(service, tmp_path)
        dispatcher._output_dir.mkdir(parents=True)
        (dispatcher._output_dir / "job-1-planted.sh").symlink_to(victim)
        monkeypatch.setattr(
            tempfile,
            "_get_candidate_names",
            lambda: iter(["planted", "fresh"]),
        )

        env = _prepare(dispatcher, wire_job(service, {"script": "echo hi"}))

        assert env.file == dispatcher._output_dir / "job-1-fresh.sh"
        assert victim.read_text() == "precious\n"

    def test_command_render_failure_removes_the_script(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        dispatcher = _dispatcher(service, tmp_path)
        job = wire_job(
            service,
            {"script": "echo hi", "suffix_args": ["{{ builder.identifier }}"]},
        )
        payload = dispatcher._resolve_job_payload(job)

        with pytest.raises(Exception, match="builder"):
            dispatcher.initialize_environment(job, payload)

        assert list(dispatcher._output_dir.iterdir()) == []

    def test_output_dir_removed_after_start_is_recreated(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        dispatcher = _dispatcher(service, tmp_path)
        assert not dispatcher._output_dir.exists()

        env = _prepare(dispatcher, wire_job(service, {"script": "echo hi"}))

        assert env.file is not None
        assert env.file.exists()

    def test_output_dir_template_variable(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        dispatcher = _dispatcher(service, tmp_path)
        job = wire_job(
            service,
            {"script": "#!/bin/bash\necho {{ output_dir }} {{ script_path }}"},
        )

        env = _prepare(dispatcher, job)

        assert env.file is not None
        assert env.file.read_text() == (
            f"#!/bin/bash\necho {dispatcher._output_dir} {env.file}"
        )
