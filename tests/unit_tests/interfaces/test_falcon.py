import pytest
from unittest.mock import MagicMock

from courier.plugins.falcons.shell_falcon import ShellFalcon
from courier.interfaces.falcons import Falcon
from courier.service import Service

@pytest.fixture
def service() -> MagicMock:
    svc = MagicMock()
    svc.config = MagicMock(log_level="DEBUG", loki_enabled=False, namespace="ns")
    svc._broker_manager._connection = None
    return svc

class TestRepresentationHierarchy:
    def test_child_hierarchy(self) -> None:
        res = ShellFalcon.get_representation_hierarchy()
        
        assert len(res) == 1
        assert res == [ShellFalcon]
    def test_child_of_child_hierarchy(self) -> None:
        class GrandchildFalcon(ShellFalcon):
            def __init__(self, service: Service, config: dict | None = None, identifier: str | None = None) -> None:
                super().__init__(service, config, identifier)
        
        res = GrandchildFalcon.get_representation_hierarchy()
        assert len(res) == 2
        assert res == [ShellFalcon, GrandchildFalcon]
    def test_base_hierarchy(self) -> None:
        res = Falcon.get_representation_hierarchy()

        assert len(res) == 0
        assert res == []
