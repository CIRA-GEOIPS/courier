"""All testing for the docker dispatcher plugin."""

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, ANY, patch

from _pytest.tmpdir import tmp_path
from pydantic import ValidationError
import pytest

from courier.cli.plugins import PLUGIN_REGISTRIES
from courier.errors import UnexecutableJobError
from courier.interfaces.dispatchers import ExecutionPayload
from courier.plugins.dispatchers.docker_dispatcher import (
    DockerDispatcher,
    DockerDispatcherConfig,
)
from courier.plugins.payloads.shell_payload import ShellPayload, ShellPayloadConfig
from courier.service import Service
from courier.types import Datum, Job
from courier.types.payload import PayloadSpec


@pytest.fixture
def docker_client(mocker):
    client = mocker.patch(
        "courier.plugins.dispatchers.docker_dispatcher.docker.from_env"
    ).return_value

    return client


@pytest.fixture
def _job(
    metadata: dict[str, Any] | None = None,
    config: Any = None,
    files: tuple[str, ...] = ("/data/a.nc",),
) -> Job:
    return Job(
        "n",
        "job-1",
        config if config is not None else {},
        files=[Datum(file=Path(f), metadata=dict(metadata or {})) for f in files],
        targets=("ld",),
    )


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

    def test_build_command_with_volumes(self, docker_client, tmp_path) -> None:
        docker_client.images.list.return_value = [object()]

        a_path = tmp_path / "script.py"
        Path(a_path).write_text("")

        b_path = tmp_path / "other.py"
        Path(b_path).write_text("")

        service = Service()
        config = {
            "image": "alpine",
            "volumes": [f"{a_path}:/mnt/script", f"{b_path}:/tmp/other"],
        }
        id = "dummy-dispatcher"

        plugin = DockerDispatcher(service, config, id)

        assert plugin._build_docker_args() == [
            "docker",
            "run",
            "-v",
            f"{a_path}:/mnt/script",
            "-v",
            f"{b_path}:/tmp/other",
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

    def test_initialize_environment(self, docker_client, _job) -> None:
        docker_client.images.list.return_value = [object()]

        service = Service()
        config = {
            "image": "alpine",
        }
        id = "dummy-dispatcher"

        plugin = DockerDispatcher(service, config, id)
        payload = ShellPayload(
            service=service,
            config={"script": "echo 'hi'"},
            identifier="dummy-payload",
        )

        job = _job
        job.payload = PayloadSpec(
            name=payload.name,
            identifier=id,
            config=payload.config.model_dump(
                mode="json",
                exclude={"script"},
            ),
            script="echo 'hi'",
            suffix=payload.file_suffix,
            defer_nonce="",
        )

        payload = plugin.initialize_environment(job, payload)

        assert payload is not None
        assert isinstance(payload, ExecutionPayload)
        assert payload.command == [
            "docker",
            "run",
            "-v",
            ANY,
            "alpine:latest",
            "sh",
            ANY,
        ]

    def test_validate_payload_toolchain(self, docker_client):
        """The docker dispatcher should prepend `docker run {container} to the toolchain validation.`"""
        docker_client.images.list.return_value = [object()]

        service = Service()
        payload = ShellPayload(
            service=service,
            config={"script": "echo 'hi'", "toolchain": ["echo"]},
            identifier="dummy-payload",
        )

        plugin = DockerDispatcher(service, {"image": "alpine"}, "dummy-dispatcher")

        with patch.object(
            payload, "_probe_toolchain", wraps=payload._probe_toolchain
        ) as mock_probe:
            plugin._validate_payload_toolchain(payload)

            assert len(plugin._validated_toolchains) == 1
            mock_probe.assert_called_once_with(
                ["docker", "run", "alpine:latest", "sh", "-c", "command -v echo"]
            )

    def test_invalid_payload_toolchain(self, docker_client):
        docker_client.images.list.return_value = [object()]

        service = Service()
        payload = ShellPayload(
            service=service,
            config={"script": "echo 'hi'", "toolchain": ["invalid"]},
            identifier="dummy-payload",
        )

        plugin = DockerDispatcher(service, {"image": "alpine"}, "dummy-dispatcher")

        with patch.object(
            payload, "_probe_toolchain", wraps=payload._probe_toolchain
        ) as mock_probe:
            with pytest.raises(
                UnexecutableJobError, match="Toolchain validation failed"
            ):
                plugin._validate_payload_toolchain(payload)

            assert len(plugin._validated_toolchains) == 0
            mock_probe.assert_called_once_with(
                ["docker", "run", "alpine:latest", "sh", "-c", "command -v invalid"]
            )


class TestWorkflow:
    """Test the full docker workflow"""

    def test_get_job_info(self, docker_client, _job):
        docker_client.images.list.return_value = [object()]

        service = Service()
        config = {"image": "alpine"}

        id = "dummy-dispatcher"

        plugin = DockerDispatcher(service, config, id)
        payload = ShellPayload(
            service=service,
            config={"script": "echo {{ files[0].file }}"},
            identifier="dummy-payload",
        )

        dummy_job = _job
        dummy_job.payload = PayloadSpec(
            name=payload.name,
            identifier=id,
            config=payload.config.model_dump(
                mode="json",
                exclude={"script"},
            ),
            script="echo {{ files[0].file }}",
            suffix=payload.file_suffix,
            defer_nonce="",
        )

        plugin.initialize_environment(dummy_job, payload)
