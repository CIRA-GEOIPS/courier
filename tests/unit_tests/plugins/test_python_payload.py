import sys
from pathlib import Path
from unittest.mock import MagicMock

from courier.interfaces.payloads import DispatcherGroupConfig
from courier.plugins.payloads.python_payload import (
    SUBPROCESS_WRAPPER,
    PythonPayload,
    PythonPayloadConfig,
)
from courier.types.execution_log import ExecutionLog
from tests._helpers import file_job, run_locally


def _dispatch(service: MagicMock, payload_config: dict, **wire) -> list[ExecutionLog]:
    """Run a python_payload with *payload_config* through a LocalDispatcher."""
    return run_locally(service, payload_config, payload_cls=PythonPayload, **wire)


#: Python source that reports how it was invoked.
_REPORTING_SOURCE = (
    "import sys\n"
    "print('argv', sys.argv[1:])\n"
    "print('debug', __debug__)\n"
    "print('file', '{{ files[0].file }}')\n"
)


class TestPayloadWorkflow:
    def test_python_source_file_runs_without_dash_c(self, service, tmp_path) -> None:
        """A ``.py`` payload runs directly: no ``-c``, no subprocess wrapping."""
        script = tmp_path / "demo.py"
        script.write_text('print("hello from python")')
        payload = PythonPayload(service, {"file": script}, "dummy-payload")

        assert payload.generate_calling_method() == ["python"]
        assert payload.declare_command() == [str(script)]
        command = payload.generate_calling_method() + payload.declare_command()
        result = payload.get_payload_from_job(command, file_job())

        assert result[0].return_code == 0
        assert "hello from python" in result[0].stdout

    def test_non_python_file_is_lowered_to_a_python_subprocess_call(
        self, service, tmp_path
    ) -> None:
        """A non-``.py`` file runs as a Python subprocess (``python -c ...``)."""
        marker = tmp_path / "ran.marker"
        script = tmp_path / "demo.sh"
        script.write_text(f"#!/bin/sh\ntouch {marker}")
        script.chmod(0o755)
        payload = PythonPayload(service, {"file": script}, "dummy-payload")

        assert payload.generate_calling_method() == ["python", "-c"]
        assert payload.declare_command() == [SUBPROCESS_WRAPPER, str(script)]
        command = payload.generate_calling_method() + payload.declare_command()
        result = payload.get_payload_from_job(command, file_job())

        assert result[0].return_code == 0
        assert marker.exists()


class TestConfigClass:
    def test_hydrated_config_uses_the_config_class(self, service: MagicMock) -> None:
        """from_job_spec validates with the plugin's config, not the base one."""
        built = PythonPayload(service, {"script": "print(1)"}, "hydrate-config")
        spec = built.to_job_spec(file_job())

        hydrated = PythonPayload.from_job_spec(spec, service)

        assert isinstance(hydrated.config, PythonPayloadConfig)
        assert hydrated.payload_name == "python_payload"
        assert isinstance(hydrated.base_config, DispatcherGroupConfig)
        assert hydrated.generate_calling_method() == ["python"]


class TestCommandShape:
    def test_inline_script_runs_as_python_source(
        self, service: MagicMock, tmp_path: Path
    ) -> None:
        payload = PythonPayload(
            service,
            {"script": "print(1)", "prefix_args": ["-O"], "suffix_args": ["a"]},
            "dummy-payload",
        )
        script = tmp_path / "courier-x.py"

        assert payload.generate_calling_method() == ["python"]
        assert payload.declare_command(script) == ["-O", str(script), "a"]

    def test_binary_only_passes_no_script_path(self, service: MagicMock) -> None:
        payload = PythonPayload(
            service,
            {"binary": "echo", "suffix_args": ["hi"]},
            "dummy-payload",
        )

        assert payload.generate_calling_method() == ["python", "-c"]
        assert payload.declare_command() == [SUBPROCESS_WRAPPER, "echo", "hi"]


class TestEndToEndThroughLocalDispatcher:
    """Every python_payload mode, builder render -> wire -> LocalDispatcher."""

    def test_py_file_with_interpreter_and_script_args(
        self, service: MagicMock, tmp_path: Path
    ) -> None:
        script = tmp_path / "report.py"
        script.write_text(_REPORTING_SOURCE)

        logs = _dispatch(
            service,
            {
                "file": script,
                "default_binary": sys.executable,
                "prefix_args": ["-O"],
                "suffix_args": ["--in", "{{ files[0].file }}"],
            },
        )

        assert logs[0].return_code == 0, logs[0].stderr
        assert "argv ['--in', '/d/a.nc']" in logs[0].stdout
        assert "debug False" in logs[0].stdout
        assert "file /d/a.nc" in logs[0].stdout

    def test_py_file_exit_code_is_preserved(
        self, service: MagicMock, tmp_path: Path
    ) -> None:
        script = tmp_path / "fail.py"
        script.write_text("import sys\nsys.exit(3)\n")

        logs = _dispatch(service, {"file": script, "default_binary": sys.executable})

        assert logs[0].return_code == 3  # noqa: PLR2004

    def test_inline_script_with_interpreter_and_script_args(
        self, service: MagicMock
    ) -> None:
        """Regression: python was handed the subprocess wrapper as a filename."""
        logs = _dispatch(
            service,
            {
                "script": _REPORTING_SOURCE,
                "default_binary": sys.executable,
                "prefix_args": ["-O"],
                "suffix_args": ["{{ job.identifier }}"],
            },
        )

        assert logs[0].return_code == 0, logs[0].stderr
        assert "argv ['job-1']" in logs[0].stdout
        assert "debug False" in logs[0].stdout
        assert "file /d/a.nc" in logs[0].stdout

    def test_inline_script_with_default_interpreter(self, service: MagicMock) -> None:
        logs = _dispatch(
            service,
            {"script": "print('inline default {{ files[0].file }}')"},
        )

        assert logs[0].return_code == 0, logs[0].stderr
        assert "inline default /d/a.nc" in logs[0].stdout

    def test_non_py_file_is_executed_via_its_shebang(
        self, service: MagicMock, tmp_path: Path
    ) -> None:
        script = tmp_path / "run.sh"
        script.write_text('#!/bin/sh\necho "sh args: $*"\n')

        logs = _dispatch(
            service,
            {
                "file": script,
                "default_binary": sys.executable,
                "suffix_args": ["{{ files[0].file }}", "x"],
            },
        )

        assert logs[0].return_code == 0, logs[0].stderr
        assert "sh args: /d/a.nc x" in logs[0].stdout

    def test_non_py_file_prefix_args_lead_the_subprocess_argv(
        self, service: MagicMock, tmp_path: Path
    ) -> None:
        """Without a binary the first prefix arg is the program run."""
        script = tmp_path / "run.txt"
        script.write_text('echo "no shebang: $1"\n')

        logs = _dispatch(
            service,
            {
                "file": script,
                "default_binary": sys.executable,
                "prefix_args": ["sh"],
                "suffix_args": ["{{ files[0].file }}"],
            },
        )

        assert logs[0].return_code == 0, logs[0].stderr
        assert "no shebang: /d/a.nc" in logs[0].stdout

    def test_failing_subprocess_fails_the_job(
        self, service: MagicMock, tmp_path: Path
    ) -> None:
        script = tmp_path / "fail.sh"
        script.write_text("#!/bin/sh\nexit 4\n")

        logs = _dispatch(service, {"file": script, "default_binary": sys.executable})

        assert logs[0].return_code != 0
        assert "CalledProcessError" in logs[0].stderr

    def test_binary_only_with_prefix_and_suffix_args(self, service: MagicMock) -> None:
        logs = _dispatch(
            service,
            {
                "binary": "echo",
                "default_binary": sys.executable,
                "prefix_args": ["from-prefix"],
                "suffix_args": ["{{ files[0].file }}"],
            },
        )

        assert logs[0].return_code == 0, logs[0].stderr
        assert logs[0].stdout.strip() == "from-prefix /d/a.nc"

    def test_empty_arguments_reach_the_subprocess(self, service: MagicMock) -> None:
        """Subprocess mode keeps an argument that renders to '' as an argument."""
        logs = _dispatch(
            service,
            {
                "binary": sys.executable,
                "default_binary": sys.executable,
                "prefix_args": ["-c", "import sys; print(sys.argv[1:])", ""],
                "suffix_args": ["{{ '' }}", "{{ files[0].file }}", ""],
            },
        )

        assert logs[0].return_code == 0, logs[0].stderr
        assert logs[0].stdout.strip() == "['', '', '/d/a.nc', '']"

    def test_empty_script_arguments_reach_python_source(
        self, service: MagicMock
    ) -> None:
        logs = _dispatch(
            service,
            {
                "script": "import sys; print(sys.argv[1:])",
                "default_binary": sys.executable,
                "suffix_args": ["", "{{ '' }}", "x"],
            },
        )

        assert logs[0].return_code == 0, logs[0].stderr
        assert logs[0].stdout.strip() == "['', '', 'x']"

    def test_file_name_is_never_spliced_into_python_source(
        self, service: MagicMock, tmp_path: Path
    ) -> None:
        """Regression: argv was repr()'d into the -c source, then rendered."""
        marker = tmp_path / "injected"
        name = f"/d/x']);__import__('os').system('touch {marker}');dict(a=['.nc"

        logs = _dispatch(
            service,
            {
                "binary": "echo",
                "default_binary": sys.executable,
                "prefix_args": ["-n"],
                "suffix_args": ["{{ files[0].file }}"],
            },
            job=file_job(name),
        )

        assert logs[0].return_code == 0, logs[0].stderr
        assert logs[0].stdout == name
        assert not marker.exists()

    def test_option_like_arguments_reach_the_program(self, service: MagicMock) -> None:
        """Arguments after the -c program are the program's, not Python's."""
        logs = _dispatch(
            service,
            {
                "binary": "echo",
                "default_binary": sys.executable,
                "prefix_args": ["-c", "-m"],
                "suffix_args": ["{{ job.identifier }}"],
            },
        )

        assert logs[0].return_code == 0, logs[0].stderr
        assert logs[0].stdout.strip() == "-c -m job-1"

    def test_binary_runs_a_py_file_as_a_subprocess(
        self, service: MagicMock, tmp_path: Path
    ) -> None:
        script = tmp_path / "report.py"
        script.write_text(_REPORTING_SOURCE)

        logs = _dispatch(
            service,
            {
                "file": script,
                "binary": sys.executable,
                "default_binary": sys.executable,
                "suffix_args": ["{{ job.identifier }}"],
            },
        )

        assert logs[0].return_code == 0, logs[0].stderr
        assert "argv ['job-1']" in logs[0].stdout
        assert "file /d/a.nc" in logs[0].stdout
