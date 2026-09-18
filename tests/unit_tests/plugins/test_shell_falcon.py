import pytest
from unittest.mock import MagicMock

from courier.plugins.falcons.shell_falcon import ShellFalcon

@pytest.fixture
def service() -> MagicMock:
    svc = MagicMock()
    svc.config = MagicMock(log_level="DEBUG", loki_enabled=False, namespace="ns")
    svc._broker_manager._connection = None
    return svc

@pytest.fixture
def config() -> dict:
    return {
        "file": "./assets/shell_falcon_demo.sh"
    }

class TestConstruction:
    def test_falcon_construction_with_config(self, service: MagicMock, config: dict) -> None:
        res = ShellFalcon(service, config, "dummyfalcon")

        assert res != None
