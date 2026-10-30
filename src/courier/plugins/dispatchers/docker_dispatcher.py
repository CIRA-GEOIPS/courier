"""Deploys docker for a specified payload."""

from pathlib import Path
from typing import ClassVar, cast

import docker
from pydantic import Field, field_validator, model_validator

from courier.interfaces.dispatchers import (
    Dispatcher,
    DispatcherConfig,
    ExecutionPayload,
)
from courier.interfaces.payloads import DispatcherGroupConfig, Payload
from courier.plugins.payloads.bash_payload import BashPayload
from courier.plugins.payloads.python_payload import PythonPayload
from courier.plugins.payloads.shell_payload import ShellPayload
from courier.service import Service
from courier.types import Job


class DockerDispatcherConfig(DispatcherConfig):
    """Extended configuration for the Docker dispatcher."""

    image: str = Field(description="The image path to be dispatched, e.g. alpine")
    tag: str = Field(
        description="The tag to specify which version of the specified image to pull.",
        default="latest",
    )
    volumes: list[str] = Field(
        description="Bind mounts, passed as -v",
        default_factory=list,
    )
    docker_args: list[str] = Field(
        description="Arguments passed to docker, e.g. --rm",
        default_factory=list,
    )
    mount_point: str = Field(
        description="Where courier utilities will be mounted in the docker container.",
        default="/courier",
    )

    @model_validator(mode="after")
    def _validate_image(self) -> "DockerDispatcherConfig":
        client = docker.from_env()
        try:
            image_name = f"{self.image}:{self.tag}"
            filtered_images = client.images.list(filters={"reference": image_name})

            if not filtered_images:
                raise ValueError(
                    f"Image {image_name} not found locally. "
                    "Please validate that it exists and pull it.",
                )
            return self
        finally:
            client.close()

    @field_validator("volumes")
    @classmethod
    def _validate_volume_arg(cls, value: list[str]) -> list[str]:
        for volume_str in value:
            split_sides = volume_str.split(":")
            if len(split_sides) not in (2, 3):
                raise ValueError(
                    f"Invalid volume mount: {value} " "invalid mapping detected.",
                )
            if len(split_sides) == 3 and split_sides[2] not in (  # noqa: PLR2004
                "ro",
                "rw",
            ):
                raise ValueError(
                    f"Invalid volume mode in {volume_str}; expected 'ro' or 'rw'.",
                )
            local = split_sides[0]
            remote = split_sides[1]

            # validation on local path
            if not (Path(local).is_absolute() or local.startswith(("./", "../"))):
                raise ValueError(
                    f"Volume host path {local} must be absolute or start with"
                    " './' or '../'. "
                    "A bare name is parsed by Docker as a named volume.",
                )

            local_path = Path(local).resolve()
            if not local_path.exists():
                raise FileNotFoundError(
                    f"Invalid volume mount" f" Local path {local_path} not found!",
                )

            # validation on remote path
            if remote == "/" or not Path(remote).is_absolute():
                raise ValueError(f"Invalid docker remote mount: {remote}")
        return value


class DockerDispatcher(Dispatcher):
    """Dispatcher that executes payloads in a Docker container."""

    interface: ClassVar[str] = "dispatchers"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "docker_dispatcher"
    version: ClassVar[str] = "-1"

    representations: ClassVar[list[type[Payload]]] = [
        ShellPayload,
        BashPayload,
        PythonPayload,
    ]

    config_class: ClassVar[type[DispatcherGroupConfig]] = DockerDispatcherConfig

    def __init__(
        self,
        service: Service,
        config: dict | None = None,
        identifier: str | None = None,
    ) -> None:
        super().__init__(service, config, identifier)

        docker_config = cast("DockerDispatcherConfig", self.config)
        self._container: str = f"{docker_config.image}:{docker_config.tag}"
        self._docker_args = docker_config.docker_args
        self._volumes = docker_config.volumes
        self._mount_point = docker_config.mount_point

    def _validate_payload_toolchain(self, payload: Payload) -> None:
        if not payload.config.toolchain_prepend:
            payload.config.toolchain_prepend = self._build_docker_args()
        return super()._validate_payload_toolchain(payload)

    def _build_docker_args(self, extra_volumes: list[str] | None = None) -> list[str]:
        """Return the `docker` command, minus the payload command."""
        volumes = [*self._volumes, *(extra_volumes or [])]
        volume_args = [arg for volume in volumes for arg in ("-v", volume)]
        args = ["docker", "run", *self._docker_args, *volume_args, self._container]

        return args

    def initialize_environment(self, job: Job, payload: Payload) -> ExecutionPayload:
        """Initialize the docker command to run and mount the temporary script."""
        # gracefully set the jobs to be valid in the container's environment
        script_path, context = self._materialize_script(job, payload)
        docker_script_path = None
        docker_mount_str = None
        if script_path is not None:

            normalized_script_path = Path(script_path or "").resolve()
            filename = normalized_script_path.name
            docker_script_path = Path(f"{self._mount_point}/script/{filename}")

            docker_mount_str = f"{normalized_script_path}:{docker_script_path}"
        job_volumes = [
            f"{Path(f.file).absolute()}:{Path(f.file).absolute()}:ro"
            for f in job.files
            if f.file
        ]
        try:
            command = [
                *self._build_docker_args(
                    ([docker_mount_str, *job_volumes] if docker_mount_str else None),
                ),
                *self._render_command(job, payload, docker_script_path, context),
            ]
            log_prefix, log_file_path = self._log_destination(job)
        except BaseException:
            if script_path is not None:
                self._discard_script(script_path)
            raise

        self._logger.debug(f"Generated docker command: {command}")
        return ExecutionPayload(
            command=command,
            file=script_path,
            log_prefix=log_prefix,
            log_file_path=log_file_path,
        )
