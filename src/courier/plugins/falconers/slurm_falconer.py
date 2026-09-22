from pathlib import Path
import shutil
import re
import subprocess
import time
from typing import ClassVar

from courier.constants import PluginRunState
from courier.types.execution_log import ExecutionLog
from pydantic import Field, BaseModel
from courier.interfaces.falconers import Falconer, FalconerPayload
from courier.interfaces.falcons import Falcon
from courier.service import Service
from courier.types.job import Job
from courier.utils.functional import slugify_for_filename

from courier.plugins.falcons.shell_falcon import ShellFalcon
from courier.plugins.falcons.bash_falcon import BashFalcon
from courier.plugins.falcons.python_falcon import PythonFalcon

_SBATCH_JOB_ID_RE = re.compile(r"Submitted batch job (\d+)")
_SACCT_MIN_PARTS = 2

_TERMINAL_STATES: frozenset[str] = frozenset(
    {
        "COMPLETED",
        "FAILED",
        "CANCELLED",
        "TIMEOUT",
        "OUT_OF_MEMORY",
        "NODE_FAIL",
        "PREEMPTED",
        "BOOT_FAIL",
        "DEADLINE",
    },
)

class SlurmFalconerConfig(BaseModel):
    sbatch_template: str
    slurm_output_dir: str
    poll_interval_seconds: float = Field(default=30.0, gt=0)
    max_concurrent_jobs: int = Field(default=10, ge=1)
    partition: str | None = None
    account: str | None = None
    qos: str | None = None
    time_limit: str | None = None
    ntasks: int | None = Field(default=None, ge=1)
    mem_per_node: str | None = None
    wait_for_completion: bool = True
    submission_timeout_seconds: float = Field(default=60.0, gt=0)
    polling_timeout_seconds: float = Field(default=86400.0, gt=0)
    sbatch_extra_args: list[str] = Field(default_factory=list)

class SlurmFalconer(Falconer):
    interface: ClassVar[str] = "falconers"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "slurm_falconer"
    version: ClassVar[str] = "-1"

    representations: list[type[Falcon]] = [ShellFalcon, BashFalcon]

    def __init__(
        self,
        service: Service,
        config: dict | None = None,
        identifier: str | None = None,
    ) -> None:
        super().__init__(service, config, identifier=identifier)
        self.config = SlurmFalconerConfig.model_validate(config or {})
        self._toolchain_reqs = ["sbatch"]
        if self.config.wait_for_completion:
            self._toolchain_reqs.append("sacct")
        self._output_dir = Path(self.config.slurm_output_dir)
        self._last_submit_error: str | None = None
    def start(self) -> None:
        for req in self._toolchain_reqs:
            self.falcon.config.toolchain.append(req)

    def is_healthy(self) -> bool:
        return True
    def _build_sbatch_args(self, job: Job) -> list[str]:
        args: list[str] = ["sbatch", "--parsable"]
        cfg = self.config
        out_base = Path(cfg.slurm_output_dir) / slugify_for_filename(job.identifier)
        args.extend(
        [
            f"--job-name=courier-{slugify_for_filename(job.identifier)}",
            f"--output={out_base}.out",
            f"--error={out_base}.err"
        ],
        )
        if cfg.partition:
            args.append(f"--partition={cfg.partition}")
        if cfg.account:
            args.append(f"--account={cfg.account}")
        if cfg.qos:
            args.append(f"--qos={cfg.qos}")
        if cfg.time_limit:
            args.append(f"--time={cfg.time_limit}")
        if cfg.ntasks is not None:
            args.append(f"--ntasks={cfg.ntasks}")
        if cfg.mem_per_node:
            args.append(f"--mem={cfg.mem_per_node}")
        args.extend(cfg.sbatch_extra_args)
        return args
    def initialize_environment(self, job) -> FalconerPayload:
        clean_path = self._render_script_file(job, self.config.slurm_output_dir)

        command = self._build_sbatch_args(job)
  
        raw_command = self.falcon.declare_command(clean_path)
        if self.falcon.config.binary or self.falcon.config.file.suffix != self.falcon._file_suffix:
            wrap_command = (
                f"{' '.join(self.falcon.generate_calling_method())} "
                f"'{self.render_script(job, raw_command[0])}'"
            )

            command.extend(["--wrap", wrap_command])
        else:
            for c in raw_command:
                command.append(self.render_script(job, c))
        
        self._logger.info(f"COMMAND: {command}")
        return FalconerPayload(
            command=command
        )
    def _get_slurm_job_id(self, result: ExecutionLog) -> str | None:
        slurm_job_id: str | None = None
        stdout: str | None = None
        if result.stdout:
            stdout = result.stdout.strip()
            if stdout.isdigit():
                slurm_job_id = stdout
            else:
                match = _SBATCH_JOB_ID_RE.search(stdout)
                slurm_job_id = match.group(1) if match else None
        if slurm_job_id is None:
            self._logger.error(f"Could not parse sbatch output: {stdout!r}")
            self._last_submit_error = f"unparseable sbatch output: {stdout!r}"
            return None
        return slurm_job_id

    def _parse_sacct_output(self, stdout: str) -> tuple[str, int]:
        """Parse the first data row from ``sacct --parsable2`` output."""
        for line in stdout.splitlines():
            parts = line.strip().split("|")
            if len(parts) < _SACCT_MIN_PARTS or not parts[0]:
                continue
            state = parts[0].split()[0]
            exit_code_raw = parts[1]
            exit_code = 0
            if ":" in exit_code_raw:
                try:
                    exit_code = int(exit_code_raw.split(":")[0])
                except ValueError:
                    exit_code = -1
            return state, exit_code
        return "", 0

    def _poll_status(self, slurm_job_id: str) -> tuple[str, int]:
        deadline = time.time() + self.config.polling_timeout_seconds
        last_state = "PENDING"
        while time.time() < deadline:
            try:
                result = subprocess.run(  # noqa: S603
                    [  # noqa: S607
                        "sacct",
                        "-j",
                        slurm_job_id,
                        "--format=State,ExitCode",
                        "--noheader",
                        "--parsable2",
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=self.config.submission_timeout_seconds,
                )
            except (subprocess.SubprocessError, OSError) as exc:
                self._logger.warning(f"sacct poll failed: {exc}")
                time.sleep(self.config.poll_interval_seconds)
                continue

            state, exit_code = self._parse_sacct_output(result.stdout)
            last_state = state or last_state
            if last_state in _TERMINAL_STATES:
                return last_state, exit_code
            time.sleep(self.config.poll_interval_seconds)

        self._logger.error(
            f"SLURM job {slurm_job_id} did not terminate within "
            f"{self.config.polling_timeout_seconds}s; last state={last_state}",
        )
        return "TIMEOUT", -1

    def _read_output(self, job: Job) -> tuple[str, str]:
        """Read and return the ``.out`` and ``.err`` files for *job*."""
        safe_id = slugify_for_filename(job.identifier)
        out_path = self._output_dir / f"{safe_id}.out"
        err_path = self._output_dir / f"{safe_id}.err"
        stdout = out_path.read_text() if out_path.exists() else ""
        stderr = err_path.read_text() if err_path.exists() else ""
        return stdout, stderr

    def cast_off_falcon(self, job: Job) -> list[ExecutionLog]:
        payload = super().cast_off_falcon(job)

        slurm_job_id = self._get_slurm_job_id(payload[0])
        if slurm_job_id is None:
            return [
                ExecutionLog(
                    return_code=-1,
                    stdout="",
                    stderr=(
                        self._last_submit_error or "sbatch submission failed"
                    ),
                ),
            ]
        
        if not self.config.wait_for_completion:
            return [
                ExecutionLog(
                    return_code=0,
                    stdout=f"SLURM job {slurm_job_id} submitted",
                    stderr=None,
                ),
            ]
        state, exit_code = self._poll_status(slurm_job_id)
        stdout, stderr = self._read_output(job)
        return_code = 0 if state == "COMPLETED" else (exit_code or -1)
        return [
            ExecutionLog(
                return_code=return_code,
                stdout=stdout,
                stderr=stderr
                or f"SLURM job {slurm_job_id} ended with state {state}",
            ),
        ]

