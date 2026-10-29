"""All testing for the docker dispatcher plugin."""

from pydantic import ValidationError
import pytest

from courier.cli.plugins import PLUGIN_REGISTRIES
from courier.plugins.dispatchers.docker_dispatcher import (
    DockerDispatcher,
    DockerDispatcherConfig,
)
from courier.service import Service


@pytest.fixture
def docker_client(mocker):
    client = mocker.patch(
        "courier.plugins.dispatchers.docker_dispatcher.docker.from_env"
    ).return_value

    return client


class TestBasicFunctionality:
    # discovery
    def test_plugin_discovery(self) -> None:
        plugin = PLUGIN_REGISTRIES["dispatchers"].get_plugin("docker_dispatcher")

        assert plugin is DockerDispatcher

    def test_plugin_config(self, docker_client) -> None:
        """Test that all extended configuration options are available."""
        docker_client.images.list.return_value = [object()]

        service = Service()
        config = {"image": "alpine", "tag": "3.12"}
        id = "dummy-dispatcher"

        plugin = DockerDispatcher(service, config, id)

        assert isinstance(plugin.config, DockerDispatcherConfig)
        assert plugin.config.image == "alpine"
        assert plugin.config.tag == "3.12"

    def test_plugin_config_requirements(self, docker_client) -> None:
        """Requires an image field, has a default tag field."""
        docker_client.images.list.return_value = [object()]

        service = Service()
        id = "dummy-dispatcher"

        with pytest.raises(ValueError):
            plugin = DockerDispatcher(service, {}, id)

    def test_plugin_config_default(self, docker_client) -> None:
        """Tag field defaults to latest"""
        docker_client.images.list.return_value = [object()]

        service = Service()
        id = "dummy-dispatcher"

        plugin = DockerDispatcher(service, {"image": "alpine"}, id)

        assert isinstance(plugin.config, DockerDispatcherConfig)
        assert plugin.config.tag == "latest"

    def test_image_exists(self, docker_client) -> None:
        docker_client.images.list.return_value = [object()]

        config = DockerDispatcherConfig(image="alpine", tag="3.20")

        assert config.image == "alpine"
        assert config.tag == "3.20"

        docker_client.images.list.assert_called_once_with(
            filters={"reference": "alpine:3.20"}
        )
        docker_client.close.assert_called_once()

    def test_image_not_found(self, docker_client) -> None:
        docker_client.images.list.return_value = []

        with pytest.raises(
            ValidationError,
            match="Image invalid:latest not found locally",
        ):
            config = DockerDispatcherConfig(image="invalid")

        docker_client.close.assert_called_once()

    # TODO test for invalid volume mounts


class TestDispatcherFunctionality:
    """Test things like command building, payload validation, etc"""

    def test_build_command(self, docker_client) -> None:
        docker_client.images.list.return_value = [object()]

        service = Service()
        config = {"image": "alpine"}
        id = "dummy-dispatcher"

        plugin = DockerDispatcher(service, config, id)

        assert plugin._build_docker_args() == ["docker", "run", "alpine:latest"]

    def test_build_command_with_volumes(self, docker_client) -> None:
        docker_client.images.list.return_value = [object()]

        service = Service()
        config = {
            "image": "alpine",
            "volumes": ["./script:/mnt/script", "./other:/tmp/other"],
        }
        id = "dummy-dispatcher"

        plugin = DockerDispatcher(service, config, id)

        assert plugin._build_docker_args() == [
            "docker",
            "run",
            "-v ./script:/mnt/script",
            "-v ./other:/tmp/other",
            "alpine:latest",
        ]

    def test_build_command_with_args(self, docker_client) -> None:
        docker_client.images.list.return_value = [object()]

        service = Service()
        config = {
            "image": "alpine",
            "docker_args": ["--rm", "--it"],
        }
        id = "dummy-dispatcher"

        plugin = DockerDispatcher(service, config, id)

        assert plugin._build_docker_args() == [
            "docker",
            "run",
            "--rm",
            "--it",
            "alpine:latest",
        ]
