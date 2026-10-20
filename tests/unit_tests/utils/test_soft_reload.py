from pathlib import Path

import pytest
from unittest.mock import MagicMock
import tempfile

example_service = """apiVersion: runcourier.dev/v1alpha1
kind: Service
metadata:
  name: service
  namespace: service
  description: 'A courier service: service'
  docstring: null
  labels: {}
  annotations: {}
spec:
  broker:
    transport: memory
    max_retries: 5
  run:
  - identifier: data-monitor-file-system-poller-watchdog
    spec:
      kind: data_monitor
      name: file_system_poller_watchdog
      config:
        path: ./incoming
        hostname: localhost
"""

@pytest.fixture
def reload_service(tmp_path : Path) -> MagicMock:
    dummy_file = tmp_path / "service.yaml"
    dummy_file.write_text(example_service)

    svc = MagicMock()
    svc.config = MagicMock(log_level="DEBUG",
                           loki_enabled=False,
                           namespace="ns",
                           live_reload=True,
                           config_file=dummy_file.name)
    svc._broker_manager._connection = None
    return svc

class TestReloadService:
    def test_
