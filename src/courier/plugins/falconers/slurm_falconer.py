from pathlib import Path
import shutil
from typing import ClassVar

from pydantic import Field, BaseModel
from courier.interfaces.falconers import Falconer, FalconerPayload
from courier.service import Service
from courier.types.job import Job
from courier.utils.functional import slugify_for_filename

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
    def start(self) -> None:
        for req in self._toolchain_reqs:
            self.falcon.config.toolchain.append(req)
        super().start()
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
        clean_path = self._render_script_file(job)

        command = self._build_sbatch_args(job)
  
        raw_command = self.falcon.declare_command(clean_path)
        if self.falcon.config.binary:
            command.append(f"--wrap=\"{' '.join(self.falcon.generate_calling_method())} '{self.render_script(job, raw_command[0])}'\"")
        else:
            for c in raw_command:
                command.append(self.render_script(job, c))

        return FalconerPayload(
            command=command
        )
