"""Deploys docker for a specified payload."""

from typing import ClassVar, cast

from pydantic import Field, ValidationError, field_validator, model_validator

from courier.interfaces.dispatchers import Dispatcher, DispatcherConfig
from courier.interfaces.payloads import DispatcherGroupConfig, Payload
from courier.plugins.payloads.bash_payload import BashPayload
from courier.plugins.payloads.python_payload import PythonPayload
from courier.plugins.payloads.shell_payload import ShellPayload
from courier.service import Service

import docker


class DockerDispatcherConfig(DispatcherConfig):
    """Extended configuration for the Docker dispatcher."""

    image: str = Field(description="The image path to be dispatched, e.g. alpine")
    tag: str = Field(
        description="The tag to speciy which version of the specified image to pull.",
        default="latest",
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

        self._image: str = docker_config.image
        self._tag: str = docker_config.tag
