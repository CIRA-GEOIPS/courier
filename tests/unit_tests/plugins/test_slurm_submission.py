"""End-to-end tests for Slurm submission, against fake Slurm client tools.

Fake ``sbatch`` and ``sacct`` executables are put first on ``PATH``, so these
tests drive the dispatcher's real code path: hydrate the payload, write the
script, run ``sbatch``, poll ``sacct``, read the job's output, clean up.  The
fake ``sbatch`` behaves like the real one where it matters here:

* it records its argv, and parses it as ``sbatch`` does: options, then either
  ``--wrap <command>`` or a batch script followed by the script's arguments;
* it rejects a batch script whose first line is not a shebang, and reads the
  script's ``#SBATCH`` directives;
* it keeps its own copy of a batch script, as ``slurmctld`` does, while a
  ``--wrap`` command runs later and reads whatever files it names at that time;
* it expands ``%j`` and ``%%`` in ``--output``/``--error``;
* it prints the job id (``--parsable``) and runs the job in the background --
  when ``FAKE_JOB_HOLD`` names a file, once the test creates that file --
  recording its state for the fake ``sacct``;
* with ``FAKE_SBATCH_CLUSTER`` set it prints ``<id>;fake-cluster``, and the
  fake ``sacct`` then knows the job only when asked with
  ``--clusters=fake-cluster``, like a job on a remote cluster.

Tests that need a job to still be pending while the dispatcher polls hold it
with ``FAKE_JOB_HOLD`` rather than racing a delay against the poll.
"""

# cspell:ignore giveup

from __future__ import annotations

import json
import os
import shlex
import sys
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

import pytest
from prometheus_client import REGISTRY

from courier.constants import FILE_FOUND_EXCHANGE
from courier.plugins.dispatchers.slurm_dispatcher import SlurmDispatcher
from courier.interfaces.payloads import Payload
from courier.plugins.payloads.python_payload import PythonPayload
from courier.plugins.payloads.shell_payload import RUN_ARGV_SCRIPT, ShellPayload
from courier.types.execution_log import ExecutionLog
from courier.types.datum import Datum
from tests._helpers import consume, file_job, wire_job

if TYPE_CHECKING:
    from collections.abc import Callable
    from unittest.mock import MagicMock

#: Upper bound for anything that should finish "promptly".
_PROMPT_SECONDS = 15.0

_FAKE_SLURM = r'''
"""Fake Slurm client tools for courier's tests (sbatch, sacct)."""
import json
import os
import subprocess
import sys
import time
from pathlib import Path

STATE = Path(os.environ["FAKE_SLURM_STATE"])


def _write(path, text):
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text)
    os.replace(tmp, path)


def _allocate_job_id():
    job_id = 1000
    while True:
        try:
            fd = os.open(STATE / f"{job_id}.id", os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            job_id += 1
            continue
        os.close(fd)
        return job_id


def _expand(pattern, job_id):
    return pattern.replace("%%", "\0").replace("%j", str(job_id)).replace("\0", "%")


def _directives(text):
    found = []
    for line in text.splitlines()[1:]:
        stripped = line.strip()
        if stripped.startswith("#SBATCH"):
            found.append(stripped[len("#SBATCH"):].strip())
        elif stripped and not stripped.startswith("#"):
            break
    return found


def _parse(argv):
    options, wrap, script, script_args = [], None, None, []
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg == "--wrap":
            wrap = argv[i + 1]
            i += 2
        elif arg.startswith("--"):
            options.append(arg)
            i += 1
        elif arg.startswith("-"):
            # A short option takes the next argument as its value.
            options.append(arg)
            options.append(argv[i + 1] if i + 1 < len(argv) else "")
            i += 2
        else:
            script, script_args = arg, argv[i + 1:]
            break
    return options, wrap, script, script_args


def _option(options, name):
    for option in options:
        if option.startswith(f"--{name}="):
            return option.split("=", 1)[1]
    return None


def sbatch(argv):
    with open(STATE / "sbatch.log", "a") as log:
        log.write(json.dumps(argv) + "\n")
    if os.environ.get("FAKE_SBATCH_SLEEP"):
        time.sleep(float(os.environ["FAKE_SBATCH_SLEEP"]))
    if os.environ.get("FAKE_SBATCH_FAIL"):
        print(
            "sbatch: error: Batch job submission failed: Invalid account or "
            "account/partition combination specified",
            file=sys.stderr,
        )
        return 1
    options, wrap, script, script_args = _parse(argv)
    if (wrap is None) == (script is None):
        print("sbatch: error: need exactly one of a script or --wrap", file=sys.stderr)
        return 1
    directives = []
    if script is not None:
        text = Path(script).read_text()
        if not text.startswith("#!"):
            print(
                "sbatch: error: This does not look like a batch script.  The first\n"
                "sbatch: error: line must start with #! followed by the path to an "
                "interpreter.",
                file=sys.stderr,
            )
            return 1
        directives = _directives(text)
    job_id = _allocate_job_id()
    if script is not None:
        copy = STATE / f"{job_id}.script"
        copy.write_text(text)
        copy.chmod(0o755)
        runner = [str(copy), *script_args]
    else:
        runner = ["/bin/sh", "-c", wrap]
    record = {
        "argv": argv,
        "options": options,
        "wrap": wrap,
        "script": script,
        "script_args": script_args,
        "directives": directives,
        "runner": runner,
        "output": _expand(_option(options, "output") or "/dev/null", job_id),
        "error": _expand(_option(options, "error") or "/dev/null", job_id),
    }
    _write(STATE / f"{job_id}.json", json.dumps(record))
    _write(STATE / f"{job_id}.state", "PENDING|0:0|0")
    subprocess.Popen(
        [sys.executable, __file__, "__run__", str(job_id)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    print(f"{job_id};fake-cluster" if os.environ.get("FAKE_SBATCH_CLUSTER") else job_id)
    return 0


def run(job_id):
    record = json.loads((STATE / f"{job_id}.json").read_text())
    hold = os.environ.get("FAKE_JOB_HOLD")
    if hold:
        deadline = time.monotonic() + 120
        while not Path(hold).exists() and time.monotonic() < deadline:
            time.sleep(0.02)
    _write(STATE / f"{job_id}.state", "RUNNING|0:0|0")
    started = time.monotonic()
    with open(record["output"], "w") as out, open(record["error"], "w") as err:
        try:
            code = subprocess.call(record["runner"], stdout=out, stderr=err)
        except OSError as exc:
            err.write(f"slurmstepd: error: execve(): {exc}\n")
            code = 1
    elapsed = round(time.monotonic() - started)
    state = os.environ.get("FAKE_JOB_FINAL_STATE") or (
        "COMPLETED" if code == 0 else "FAILED"
    )
    _write(STATE / f"{job_id}.state", f"{state}|{code}:0|{elapsed}")


def sacct(argv):
    with open(STATE / "sacct.log", "a") as log:
        log.write(json.dumps(argv) + "\n")
    job_id = argv[argv.index("-j") + 1]
    if os.environ.get("FAKE_SBATCH_CLUSTER") and "--clusters=fake-cluster" not in argv:
        return 0  # the local cluster does not know a remote cluster's job
    state = STATE / f"{job_id}.state"
    if state.exists():
        line = state.read_text()
        print(line)       # the allocation
        print(line)       # the batch step
    return 0


if __name__ == "__main__":
    tool, args = sys.argv[1], sys.argv[2:]
    if tool == "__run__":
        run(args[0])
        sys.exit(0)
    sys.exit({"sbatch": sbatch, "sacct": sacct}[tool](args))
'''


class FakeSlurm:
    """Handle on the fake Slurm tools' recorded state."""

    def __init__(self, state: Path) -> None:
        self.state = state

    def submissions(self) -> list[list[str]]:
        """Return the argv of every ``sbatch`` call, oldest first."""
        return self._calls("sbatch")

    def polls(self) -> list[list[str]]:
        """Return the argv of every ``sacct`` call, oldest first."""
        return self._calls("sacct")

    def _calls(self, tool: str) -> list[list[str]]:
        log = self.state / f"{tool}.log"
        if not log.exists():
            return []
        return [json.loads(line) for line in log.read_text().splitlines()]

    def hold_jobs(self, monkeypatch: pytest.MonkeyPatch) -> Path:
        """Keep every job PENDING until the returned file is created."""
        release = self.state / "release"
        monkeypatch.setenv("FAKE_JOB_HOLD", str(release))
        return release

    def jobs(self) -> list[dict[str, Any]]:
        """Return the record of every accepted submission, oldest first."""
        return [
            {"id": path.stem, **json.loads(path.read_text())}
            for path in sorted(self.state.glob("*.json"))
        ]

    def only_job(self) -> dict[str, Any]:
        (job,) = self.jobs()
        return job

    def state_of(self, job_id: str) -> str:
        return (self.state / f"{job_id}.state").read_text().split("|")[0]

    def wait_until_done(self, job_id: str) -> str:
        """Wait for the fake job to finish; return its final state."""
        deadline = time.monotonic() + _PROMPT_SECONDS
        while time.monotonic() < deadline:
            state = self.state_of(job_id)
            if state not in {"PENDING", "RUNNING"}:
                return state
            time.sleep(0.05)
        pytest.fail(f"fake Slurm job {job_id} did not finish")


@pytest.fixture
def fake_slurm(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FakeSlurm:
    """Put fake ``sbatch`` and ``sacct`` first on ``PATH``."""
    bin_dir = tmp_path / "fake-bin"
    state = tmp_path / "fake-state"
    bin_dir.mkdir()
    state.mkdir()
    program = bin_dir / "fake_slurm.py"
    program.write_text(_FAKE_SLURM)
    for tool in ("sbatch", "sacct"):
        wrapper = bin_dir / tool
        command = shlex.join([sys.executable, str(program), tool])
        wrapper.write_text(f'#!/bin/sh\nexec {command} "$@"\n')
        wrapper.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_SLURM_STATE", str(state))
    for name in (
        "FAKE_SBATCH_SLEEP",
        "FAKE_SBATCH_FAIL",
        "FAKE_SBATCH_CLUSTER",
        "FAKE_JOB_FINAL_STATE",
        "FAKE_JOB_HOLD",
    ):
        monkeypatch.delenv(name, raising=False)
    return FakeSlurm(state)


@pytest.fixture
def out_dir(tmp_path: Path) -> Path:
    return tmp_path / "slurm out"


def _dispatcher(
    service: MagicMock,
    out_dir: Path,
    identifier: str = "sd",
    **config: object,
) -> SlurmDispatcher:
    base: dict[str, object] = {
        "slurm_output_dir": str(out_dir),
        "poll_interval_seconds": 0.05,
        "submission_timeout_seconds": 10,
    }
    base.update(config)
    return SlurmDispatcher(service, base, identifier=identifier)


def _scripts(out_dir: Path) -> list[Path]:
    """Return the job scripts (not Slurm output files) in *out_dir*."""
    if not out_dir.exists():
        return []
    return sorted(p for p in out_dir.iterdir() if p.suffix not in {".out", ".err"})


def _sample(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


def _submissions(identifier: str, status: str) -> float:
    return _sample(
        "courier_dispatcher_slurm_submissions_total",
        dispatcher_name="slurm_dispatcher",
        dispatcher_identifier=identifier,
        status=status,
    )


def _pending(identifier: str) -> float:
    return _sample(
        "courier_dispatcher_slurm_jobs_pending",
        dispatcher_name="slurm_dispatcher",
        dispatcher_identifier=identifier,
    )


def _payload_jobs(payload_identifier: str, status: str) -> float:
    return _sample(
        "courier_payload_jobs_processed_total",
        payload_name="bash_payload",
        payload_identifier=payload_identifier,
        status=status,
    )


def _payload_duration(payload_identifier: str, suffix: str) -> float:
    return _sample(
        f"courier_payload_job_execution_duration_seconds_{suffix}",
        payload_name="bash_payload",
        payload_identifier=payload_identifier,
    )


def _eventually(predicate: Callable[[], bool], what: str) -> None:
    deadline = time.monotonic() + _PROMPT_SECONDS
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    pytest.fail(f"timed out waiting for {what}")


class TestWaitMode:
    def test_reports_the_jobs_output_and_removes_the_script(
        self,
        service: MagicMock,
        out_dir: Path,
        fake_slurm: FakeSlurm,
    ) -> None:
        dispatcher = _dispatcher(service, out_dir, identifier="sd-wait")
        job = wire_job(
            service,
            {"script": "echo 'out from {{ job.identifier }}'; echo 'err line' >&2"},
        )

        (log,) = dispatcher.get_execution_log(job)

        assert log.return_code == 0
        assert log.stdout == "out from job-1\n"
        assert log.stderr == "err line\n"
        submitted = fake_slurm.only_job()
        assert Path(submitted["output"]) == out_dir / f"job-1-{submitted['id']}.out"
        assert _scripts(out_dir) == []

    def test_failed_job_reports_its_exit_code_and_state(
        self,
        service: MagicMock,
        out_dir: Path,
        fake_slurm: FakeSlurm,  # noqa: ARG002
    ) -> None:
        dispatcher = _dispatcher(service, out_dir)
        job = wire_job(service, {"script": "echo 'going down' >&2; exit 3"})

        (log,) = dispatcher.get_execution_log(job)

        assert log.return_code == 3  # noqa: PLR2004
        assert log.stderr is not None
        assert log.stderr.startswith("going down\n")
        assert log.stderr.rstrip().endswith("ended with state FAILED")

    def test_job_on_a_named_cluster_is_polled_on_that_cluster(
        self,
        service: MagicMock,
        out_dir: Path,
        fake_slurm: FakeSlurm,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``<id>;<cluster>``: the local cluster's sacct does not know the job."""
        monkeypatch.setenv("FAKE_SBATCH_CLUSTER", "1")
        dispatcher = _dispatcher(service, out_dir, polling_timeout_seconds=10)

        (log,) = dispatcher.get_execution_log(wire_job(service, {"script": "echo ok"}))

        assert log.return_code == 0, log.stderr
        assert log.stdout == "ok\n"
        assert all("--clusters=fake-cluster" in argv for argv in fake_slurm.polls())

    def test_output_files_are_found_in_the_jobs_output(
        self,
        service: MagicMock,
        out_dir: Path,
        fake_slurm: FakeSlurm,  # noqa: ARG002
    ) -> None:
        dispatcher = _dispatcher(
            service,
            out_dir,
            output_files=[{"pattern": r"wrote (?P<file>/\S+\.nc)"}],
        )

        consume(
            dispatcher, wire_job(service, {"script": "echo wrote /data/product.nc"})
        )

        emitted = [
            Datum.from_string(call.kwargs["message"]).file
            for call in service.emit.call_args_list
            if call.kwargs.get("queue") == FILE_FOUND_EXCHANGE
        ]
        assert emitted == [Path("/data/product.nc")]


class TestBatchScripts:
    def test_sh_file_is_the_batch_script_with_its_directives(
        self,
        service: MagicMock,
        out_dir: Path,
        tmp_path: Path,
        fake_slurm: FakeSlurm,
    ) -> None:
        template = tmp_path / "job.sh"
        template.write_text(
            "#!/bin/bash\n"
            "#SBATCH --gres=gpu:1\n"
            "#SBATCH --time=00:05:00\n"
            'echo "{{ job.identifier }} args: $1|$2"',
        )
        dispatcher = _dispatcher(service, out_dir)
        job = wire_job(service, {"file": str(template), "suffix_args": ["a", "b c"]})

        (log,) = dispatcher.get_execution_log(job)

        submitted = fake_slurm.only_job()
        assert submitted["wrap"] is None
        assert submitted["script_args"] == ["a", "b c"]
        assert submitted["directives"] == ["--gres=gpu:1", "--time=00:05:00"]
        assert all(option.startswith("--") for option in submitted["options"])
        assert log.stdout == "job-1 args: a|b c\n"

    def test_no_wait_batch_script_is_removed_but_the_job_still_runs(
        self,
        service: MagicMock,
        out_dir: Path,
        fake_slurm: FakeSlurm,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Slurm keeps its own copy of a batch script: ours can go at once."""
        release = fake_slurm.hold_jobs(monkeypatch)
        dispatcher = _dispatcher(service, out_dir, wait_for_completion=False)

        (log,) = dispatcher.get_execution_log(wire_job(service, {"script": "echo ran"}))

        submitted = fake_slurm.only_job()
        assert log.return_code == 0
        assert log.stdout == f"SLURM job {submitted['id']} submitted"
        assert _scripts(out_dir) == []
        release.touch()  # the job starts only after our copy is gone
        assert fake_slurm.wait_until_done(submitted["id"]) == "COMPLETED"
        assert Path(submitted["output"]).read_text() == "ran\n"


class TestWrappedJobs:
    def test_python_file_is_wrapped_and_run_by_its_interpreter(
        self,
        service: MagicMock,
        out_dir: Path,
        tmp_path: Path,
        fake_slurm: FakeSlurm,
    ) -> None:
        template = tmp_path / "job.py"
        template.write_text("import sys; print('{{ job.identifier }}', sys.argv[1:])")
        dispatcher = _dispatcher(service, out_dir)
        job = wire_job(
            service,
            {
                "file": str(template),
                "suffix_args": ["x", "y z"],
                "default_binary": sys.executable,
            },
            payload_cls=PythonPayload,
        )

        (log,) = dispatcher.get_execution_log(job)

        submitted = fake_slurm.only_job()
        assert submitted["script"] is None
        argv = shlex.split(submitted["wrap"])
        assert argv[0] == sys.executable
        assert Path(argv[1]).parent == out_dir
        assert argv[2:] == ["x", "y z"]
        assert log.return_code == 0, log.stderr
        assert log.stdout == "job-1 ['x', 'y z']\n"
        assert _scripts(out_dir) == [], "a finished job's script is removed"

    def test_no_wait_job_finds_its_script_when_it_starts(
        self,
        service: MagicMock,
        out_dir: Path,
        tmp_path: Path,
        fake_slurm: FakeSlurm,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A --wrap job reads the script later: it must outlive the submission."""
        release = fake_slurm.hold_jobs(monkeypatch)
        template = tmp_path / "job.py"
        template.write_text("print('late but fine')")
        dispatcher = _dispatcher(service, out_dir, wait_for_completion=False)
        job = wire_job(
            service,
            {"file": str(template), "default_binary": sys.executable},
            payload_cls=PythonPayload,
        )

        (log,) = dispatcher.get_execution_log(job)

        submitted = fake_slurm.only_job()
        assert log.return_code == 0
        assert fake_slurm.state_of(submitted["id"]) == "PENDING"
        (script,) = _scripts(out_dir)
        assert script.suffix == ".py"
        release.touch()
        assert fake_slurm.wait_until_done(submitted["id"]) == "COMPLETED"
        assert Path(submitted["output"]).read_text() == "late but fine\n"

    def test_lowered_payload_is_wrapped_and_run_by_its_own_interpreter(
        self,
        service: MagicMock,
        out_dir: Path,
        fake_slurm: FakeSlurm,
    ) -> None:
        """A bash script lowered to sh is not the batch script: sh would run it."""

        class ShellOnlySlurm(SlurmDispatcher):
            representations: ClassVar[list[type[Payload]]] = [ShellPayload]

        dispatcher = ShellOnlySlurm(
            service,
            {
                "slurm_output_dir": str(out_dir),
                "poll_interval_seconds": 0.05,
                "submission_timeout_seconds": 10,
            },
            identifier="sd",
        )
        job = wire_job(
            service,
            {"script": 'if [[ -n "$BASH_VERSION" ]]; then echo bash; fi'},
        )

        (log,) = dispatcher.get_execution_log(job)

        submitted = fake_slurm.only_job()
        assert submitted["script"] is None
        argv = shlex.split(submitted["wrap"])
        assert argv[:5] == ["sh", "-c", RUN_ARGV_SCRIPT, "sh", "bash"]
        assert Path(argv[5]).parent == out_dir
        assert log.return_code == 0, log.stderr
        assert log.stdout == "bash\n"

    def test_untrusted_file_names_are_not_run_by_the_wrap_shell(
        self,
        service: MagicMock,
        out_dir: Path,
        tmp_path: Path,
        fake_slurm: FakeSlurm,  # noqa: ARG002
    ) -> None:
        marker = tmp_path / "pwned"
        evil = f"/data/x'; touch {marker}; echo '$(touch {marker}).nc"
        dispatcher = _dispatcher(service, out_dir)
        job = wire_job(
            service,
            {"binary": "echo", "suffix_args": ["{{ files[0].file }}"]},
            job=file_job(evil),
        )

        (log,) = dispatcher.get_execution_log(job)

        assert log.return_code == 0, log.stderr
        assert log.stdout == f"{evil}\n"
        assert not marker.exists()


class TestRejectedSubmissions:
    @pytest.mark.parametrize(
        "payload_config",
        [{"script": "echo hi"}, {"script": "echo hi", "prefix_args": ["-e"]}],
        ids=["batch", "wrap"],
    )
    def test_sbatch_error_is_reported_with_its_stderr(
        self,
        service: MagicMock,
        out_dir: Path,
        fake_slurm: FakeSlurm,  # noqa: ARG002
        monkeypatch: pytest.MonkeyPatch,
        payload_config: dict,
    ) -> None:
        monkeypatch.setenv("FAKE_SBATCH_FAIL", "1")
        dispatcher = _dispatcher(service, out_dir, identifier="sd-reject")
        rejected = _submissions("sd-reject", "rejected")
        submitted = _submissions("sd-reject", "submitted")

        (log,) = dispatcher.get_execution_log(wire_job(service, payload_config))

        assert log.return_code == 1
        assert log.stderr is not None
        assert "return code 1" in log.stderr
        assert "Invalid account or account/partition" in log.stderr
        assert _submissions("sd-reject", "rejected") == rejected + 1
        assert _submissions("sd-reject", "submitted") == submitted
        assert _scripts(out_dir) == [], "nothing was queued, so nothing is kept"

    def test_hung_sbatch_is_bounded_by_the_submission_timeout(
        self,
        service: MagicMock,
        out_dir: Path,
        fake_slurm: FakeSlurm,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("FAKE_SBATCH_SLEEP", "60")
        dispatcher = _dispatcher(
            service,
            out_dir,
            submission_timeout_seconds=2,
            timeout_seconds=3600,
        )
        job = wire_job(service, {"script": "echo hi", "prefix_args": ["-e"]})

        started = time.monotonic()
        (log,) = dispatcher.get_execution_log(job)
        elapsed = time.monotonic() - started

        assert elapsed < _PROMPT_SECONDS
        assert len(fake_slurm.submissions()) <= 1, "sbatch is not retried"
        assert log.return_code == -1
        assert "did not complete" in (log.stderr or "")
        assert "timed out after 2.0s" in (log.stderr or "")
        # sbatch may have queued the job before it was killed; a --wrap job
        # would then read the script, so it is left in place.
        assert len(_scripts(out_dir)) == 1


class TestDispatcherLoggingOptions:
    @pytest.mark.parametrize("wait", [True, False], ids=["wait", "no-wait"])
    def test_do_not_break_submission(
        self,
        service: MagicMock,
        out_dir: Path,
        tmp_path: Path,
        fake_slurm: FakeSlurm,
        wait: bool,  # noqa: FBT001
    ) -> None:
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        dispatcher = _dispatcher(
            service,
            out_dir,
            wait_for_completion=wait,
            log_dir=str(log_dir),
            log_to_file=True,
            log_only_errors=True,
            log_to_logger=True,
        )

        (log,) = dispatcher.get_execution_log(
            wire_job(service, {"script": "echo hi", "toolchain": ["sh"]}),
        )

        assert log.return_code == 0, log.stderr
        assert len(fake_slurm.jobs()) == 1
        if wait:
            assert log.stdout == "hi\n"
        assert list(log_dir.iterdir()) == [], "sbatch is not a logged job run"

    def test_consume_loop_survives_log_to_file(
        self,
        service: MagicMock,
        out_dir: Path,
        tmp_path: Path,
        fake_slurm: FakeSlurm,  # noqa: ARG002
    ) -> None:
        dispatcher = _dispatcher(
            service,
            out_dir,
            log_to_file=True,
            log_dir=str(tmp_path / "logs"),
        )

        consume(dispatcher, wire_job(service, {"script": "echo through the loop"}))

        (message,) = [
            call.kwargs["message"]
            for call in service.emit.call_args_list
            if call.kwargs.get("queue") != FILE_FOUND_EXCHANGE
        ]
        log = ExecutionLog.from_string(message)
        assert log.return_code == 0
        assert log.stdout == "through the loop\n"
        service.park_message.assert_not_called()


class TestPollingGivesUp:
    def test_unfinished_job_keeps_its_script(
        self,
        service: MagicMock,
        out_dir: Path,
        tmp_path: Path,
        fake_slurm: FakeSlurm,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        release = fake_slurm.hold_jobs(monkeypatch)
        template = tmp_path / "job.py"
        template.write_text("print('eventually')")
        dispatcher = _dispatcher(
            service,
            out_dir,
            identifier="sd-giveup",
            polling_timeout_seconds=0.3,
        )
        job = wire_job(
            service,
            {"file": str(template), "default_binary": sys.executable},
            payload_cls=PythonPayload,
            payload_identifier="p-giveup",
        )

        (log,) = dispatcher.get_execution_log(job)

        assert log.return_code == -1
        assert "did not reach a terminal state" in (log.stderr or "")
        assert "PENDING" in (log.stderr or "")
        assert _pending("sd-giveup") == 0
        (script,) = _scripts(out_dir)
        submitted = fake_slurm.only_job()
        release.touch()
        assert fake_slurm.wait_until_done(submitted["id"]) == "COMPLETED"
        assert Path(submitted["output"]).read_text() == "eventually\n"
        assert script.exists()

    def test_requeueable_state_keeps_the_script(
        self,
        service: MagicMock,
        out_dir: Path,
        fake_slurm: FakeSlurm,  # noqa: ARG002
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("FAKE_JOB_FINAL_STATE", "PREEMPTED")
        dispatcher = _dispatcher(service, out_dir)
        job = wire_job(service, {"script": "echo hi", "prefix_args": ["-e"]})

        (log,) = dispatcher.get_execution_log(job)

        assert log.return_code == -1
        assert "PREEMPTED" in (log.stderr or "")
        assert len(_scripts(out_dir)) == 1


class TestMetrics:
    def test_pending_gauge_covers_the_wait_and_submission_is_counted_at_once(
        self,
        service: MagicMock,
        out_dir: Path,
        fake_slurm: FakeSlurm,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        release = fake_slurm.hold_jobs(monkeypatch)
        dispatcher = _dispatcher(service, out_dir, identifier="sd-gauge")
        submitted_before = _submissions("sd-gauge", "submitted")
        job = wire_job(service, {"script": "echo hi"})
        result: list[list[ExecutionLog]] = []

        worker = threading.Thread(
            target=lambda: result.append(dispatcher.get_execution_log(job)),
        )
        worker.start()
        try:
            _eventually(lambda: bool(fake_slurm.jobs()), "the submission")
            _eventually(lambda: _pending("sd-gauge") == 1, "the pending gauge")
            (submitted,) = fake_slurm.jobs()
            assert fake_slurm.state_of(submitted["id"]) == "PENDING"
            assert _submissions("sd-gauge", "submitted") == submitted_before + 1
        finally:
            release.touch()
            worker.join(_PROMPT_SECONDS)

        assert result
        assert result[0][0].return_code == 0
        assert _pending("sd-gauge") == 0
        assert _submissions("sd-gauge", "submitted") == submitted_before + 1

    def test_no_wait_counts_the_submission_without_pending(
        self,
        service: MagicMock,
        out_dir: Path,
        fake_slurm: FakeSlurm,  # noqa: ARG002
    ) -> None:
        dispatcher = _dispatcher(
            service,
            out_dir,
            identifier="sd-nowait-metrics",
            wait_for_completion=False,
        )
        before = _submissions("sd-nowait-metrics", "submitted")
        job = wire_job(service, {"script": "echo hi"}, payload_identifier="p-nowait")

        dispatcher.get_execution_log(job)

        assert _submissions("sd-nowait-metrics", "submitted") == before + 1
        assert _pending("sd-nowait-metrics") == 0
        assert _payload_jobs("p-nowait", "success") == 0
        assert _payload_duration("p-nowait", "count") == 0

    @pytest.mark.parametrize(
        ("script", "status"),
        [("sleep 1.1; echo ok", "success"), ("exit 4", "failure")],
    )
    def test_payload_metrics_record_the_slurm_job_not_sbatch(
        self,
        service: MagicMock,
        out_dir: Path,
        fake_slurm: FakeSlurm,
        script: str,
        status: str,
    ) -> None:
        payload_identifier = f"p-metrics-{status}"
        dispatcher = _dispatcher(service, out_dir)
        job = wire_job(
            service, {"script": script}, payload_identifier=payload_identifier
        )
        other = "failure" if status == "success" else "success"

        dispatcher.get_execution_log(job)

        assert _payload_jobs(payload_identifier, status) == 1
        assert _payload_jobs(payload_identifier, other) == 0
        assert _payload_duration(payload_identifier, "count") == 1
        job_id = fake_slurm.only_job()["id"]
        expected = float(
            (fake_slurm.state / f"{job_id}.state").read_text().split("|")[2],
        )
        assert _payload_duration(payload_identifier, "sum") == expected
