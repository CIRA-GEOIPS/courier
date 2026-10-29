"""Deploys docker for a specified payload."""

from pathlib import Path
from typing import ClassVar, Any, cast

from pydantic import Field, ValidationError, field_validator, model_validator

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

import docker

from courier.types import Job


class DockerDispatcherConfig(DispatcherConfig):
    """Extended configuration for the Docker dispatcher."""

    image: str = Field(description="The image path to be dispatched, e.g. alpine")
    tag: str = Field(
        description="The tag to speciy which version of the specified image to pull.",
        default="latest",
    )
    volumes: list[str] = Field(
        description="Bind mounts, passed as -v",
        default_factory=list,
    )
    docker_args: list[str] = Field(
        description="Arguments passed to docker, e.g. --rm", default_factory=list
    )
    mount_point: str = Field(
        description="Where courier utilities will be mounted in the docker container.",
        default="/courier",
    )

    @model_validator(mode="after")
    def _validate_image(self):
        client = docker.from_env()
        try:
            image_name = f"{self.image}:{self.tag}"
            filtered_images = client.images.list(filters={"reference": image_name})

            if not filtered_images:
                raise ValueError(
                    f"Image {image_name} not found locally. "
                    "Please validate that it exists and pull it."
                )
            return self
        finally:
            client.close()

    @field_validator("volumes")
    @classmethod
    def _validate_volume_arg(cls, value) -> list[str]:
        for volume_str in value:
            split_sides = volume_str.split(":")
            if len(split_sides) != 2:
                raise ValueError(
                    f"Invalid volume mount: {value} " "invalid mapping detected."
                )
            local = split_sides[0]
            remote = split_sides[1]

            # validation on local path
            if not Path(local).exists():
                raise FileNotFoundError(
                    "Invalid volume mount " f"Local path {local} not found!"
                )

            # validation on remote path
            if remote == "/":
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

        docker_config = cast(DockerDispatcherConfig, self.config)
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
        cfg = self.config

        volumes = [*self._volumes, *(extra_volumes or [])]
        volume_args = [arg for volume in volumes for arg in ("-v", volume)]
        args = ["docker", "run", *self._docker_args, *volume_args, self._container]

        return args

    def initialize_environment(self, job: Job, payload: Payload) -> ExecutionPayload:
        script_path, context = self._materialize_script(job, payload)
        docker_script_path = None
        docker_mount_str = None
        if script_path is not None:

            normalized_script_path = Path(script_path or "").resolve()
            filename = normalized_script_path.name
            docker_script_path = Path(f"{self._mount_point}/script/{filename}")

            docker_mount_str = f"{normalized_script_path}:{docker_script_path}"
        try:
            command = [
                *self._build_docker_args(
                    [docker_mount_str] if docker_mount_str else None
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
