import pytest
from unittest.mock import MagicMock

@pytest.fixture
def reload_service() -> MagicMock:
    svc = MagicMock()
    svc.config = MagicMock(log_level="DEBUG",
                           loki_enabled=False,
                           namespace="ns",
                           live_reload=True)
    svc._broker_manager._connection = None
    return svc
