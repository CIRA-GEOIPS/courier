"""Unit tests for the Slurm dispatcher's command and polling helpers."""

from __future__ import annotations

import shlex
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from courier.errors import CourierError
from courier.interfaces.dispatchers import ExecutionPayload
from courier.interfaces.payloads import PayloadSpec
from courier.plugins.dispatchers.slurm_dispatcher import SlurmDispatcher
from courier.types.execution_log import ExecutionLog
from courier.types.job import Job


@pytest.fixture
def service() -> MagicMock:
    svc = MagicMock()
    svc.config = MagicMock(log_level="DEBUG", loki_enabled=False, namespace="ns")
    svc._broker_manager._connection = None
    return svc


def _dispatcher(service: MagicMock, tmp_path: Path, **config: object) -> SlurmDispatcher:
    base = {"slurm_output_dir": str(tmp_path / "slurm")}
    base.update(config)
    return SlurmDispatcher(service, base, identifier="sd")


def _job(identifier: str = "job-1") -> Job:
    return Job("n", identifier, {})


class TestSbatchArgs:
    def test_minimal_args(self, service: MagicMock, tmp_path: Path) -> None:
        dispatcher = _dispatcher(service, tmp_path)
        out_base = Path(dispatcher.config.slurm_output_dir) / "job-1"

        assert dispatcher._build_sbatch_args(_job()) == [
            "sbatch",
            "--parsable",
            "--job-name=courier-job-1",
            f"--output={out_base}.out",
            f"--error={out_base}.err",
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
        out_base = Path(dispatcher.config.slurm_output_dir) / "job-1"

        assert dispatcher._build_sbatch_args(_job()) == [
            "sbatch",
            "--parsable",
            "--job-name=courier-job-1",
            f"--output={out_base}.out",
            f"--error={out_base}.err",
            "--partition=gpu",
            "--account=acct",
            "--qos=high",
            "--time=01:00:00",
            "--ntasks=4",
            "--mem=8G",
            "--gres=gpu:1",
        ]


class TestJobIdParsing:
    def test_parses_submitted_batch_job(self, service: MagicMock, tmp_path: Path) -> None:
        dispatcher = _dispatcher(service, tmp_path)
        result = ExecutionLog(stdout="Submitted batch job 4321")

        assert dispatcher._get_slurm_job_id(result) == "4321"

    def test_parses_bare_digits(self, service: MagicMock, tmp_path: Path) -> None:
        dispatcher = _dispatcher(service, tmp_path)

        assert dispatcher._get_slurm_job_id(ExecutionLog(stdout="99")) == "99"

    def test_returns_none_for_garbage(self, service: MagicMock, tmp_path: Path) -> None:
        dispatcher = _dispatcher(service, tmp_path)

        assert dispatcher._get_slurm_job_id(ExecutionLog(stdout="nope")) is None


class TestSacctParsing:
    def test_parses_state_and_exit_code(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        dispatcher = _dispatcher(service, tmp_path)

        state, exit_code = dispatcher._parse_sacct_output("COMPLETED|0:0\n")

        assert state == "COMPLETED"
        assert exit_code == 0

    def test_empty_output_yields_empty_state(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        dispatcher = _dispatcher(service, tmp_path)

        assert dispatcher._parse_sacct_output("") == ("", 0)


class TestExecuteJob:
    def _payload(self, stdout: str) -> MagicMock:
        payload = MagicMock()
        payload.get_payload_from_job.return_value = [
            ExecutionLog(return_code=0, stdout=stdout, stderr="", hostname="h"),
        ]
        return payload

    def test_submitted_only_when_not_waiting(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        dispatcher = _dispatcher(service, tmp_path, wait_for_completion=False)
        payload = self._payload("Submitted batch job 7")
        env = ExecutionPayload(command=["sbatch"], file=None)

        logs = dispatcher._execute_job(_job(), payload, env)

        assert logs[0].return_code == 0
        assert "7" in (logs[0].stdout or "")

    def test_rejected_when_output_unparseable(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        dispatcher = _dispatcher(service, tmp_path)
        payload = self._payload("totally bogus")
        env = ExecutionPayload(command=["sbatch"], file=None)

        logs = dispatcher._execute_job(_job(), payload, env)

        assert logs[0].return_code == -1

    def test_waiting_reads_the_slurm_output_files(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        dispatcher = _dispatcher(service, tmp_path)
        out_dir = dispatcher._output_dir
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "job-1.out").write_text("done")
        (out_dir / "job-1.err").write_text("")
        payload = self._payload("Submitted batch job 7")
        env = ExecutionPayload(command=["sbatch"], file=None)
        dispatcher._poll_status = MagicMock(return_value=("COMPLETED", 0))  # type: ignore[method-assign]

        logs = dispatcher._execute_job(_job(), payload, env)

        assert logs[0].return_code == 0
        assert logs[0].stdout == "done"


class TestStart:
    def test_start_requires_sbatch_on_path(
        self,
        service: MagicMock,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import courier.plugins.dispatchers.slurm_dispatcher as slurm_module

        dispatcher = _dispatcher(service, tmp_path)
        monkeypatch.setattr(slurm_module.shutil, "which", lambda _name: None)

        with pytest.raises(CourierError, match="sbatch"):
            dispatcher.start()


class TestToolchainValidation:
    def test_missing_toolchain_binary_fails_hydration(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        dispatcher = _dispatcher(service, tmp_path)
        job = Job(
            "n",
            "job-1",
            {},
            payload=PayloadSpec(
                name="bash_payload",
                identifier="p1",
                config={
                    "binary": "echo",
                    "toolchain": ["courier-no-such-binary-xyz"],
                },
            ),
        )

        with pytest.raises(CourierError, match="Toolchain validation failed"):
            dispatcher._resolve_job_payload(job)


class TestInitializeEnvironment:
    def test_wrapped_command_is_quoted_as_a_single_token(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        """``--wrap`` must carry the whole command as one shell-quoted argv token.

        A payload with a binary takes the ``sbatch --wrap`` branch; the rendered
        command is interpolated into a shell string, so it must be ``shlex``
        quoted.  An output path containing a space makes the quoting observable.
        """
        spaced = tmp_path / "slurm output"
        dispatcher = SlurmDispatcher(
            service,
            {"slurm_output_dir": str(spaced)},
            identifier="sd",
        )
        job = Job(
            "n",
            "job-1",
            {},
            payload=PayloadSpec(
                name="bash_payload",
                identifier="p1",
                config={"binary": "echo"},
                script="echo hi",
                defer_nonce="abc",
            ),
        )
        payload = dispatcher._resolve_job_payload(job)

        env = dispatcher.initialize_environment(job, payload)

        assert "--wrap" in env.command
        wrapped = env.command[env.command.index("--wrap") + 1]
        inner = shlex.join(["echo", str(dispatcher._output_dir / "job-1.sh")])
        assert shlex.split(wrapped) == ["bash", "-c", inner]
