"""Unit tests for the cron_glob data monitor plugin."""

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from lazylemon.plugins.modules.data_monitors.cron_glob import (
    CronGlob,
    CronGlobConfig,
)
from lazylemon.types.file import File


# ─── Fixtures ───────────────────────────────────────────────────────────────


@pytest.fixture
def mock_service() -> MagicMock:
    """Create a mock Service with the minimum interface needed."""
    service = MagicMock()
    service._config = MagicMock()
    service._config.log_level = "DEBUG"
    service._config.loki_enabled = False
    return service


@pytest.fixture
def tmp_data_dir(tmp_path: Path) -> Path:
    """Create a temporary directory with two .nc files and one .txt file."""
    (tmp_path / "data_a.nc").write_text("a")
    (tmp_path / "data_b.nc").write_text("b")
    (tmp_path / "data_c.txt").write_text("c")
    subdir = tmp_path / "sub"
    subdir.mkdir()
    (subdir / "data_d.nc").write_text("d")
    return tmp_path


def _make_config(tmp_path: Path, **overrides: Any) -> dict[str, Any]:
    """Build a config dict for CronGlob with sensible defaults."""
    defaults: dict[str, Any] = {
        "path": str(tmp_path),
        "glob_pattern": "*.nc",
        "cron_expression": "* * * * *",
    }
    defaults.update(overrides)
    return defaults


# ─── Config Validation ──────────────────────────────────────────────────────


class TestCronGlobConfig:
    """Tests for CronGlobConfig validation."""

    def test_valid_config(self, tmp_path: Path) -> None:
        cfg = CronGlobConfig.model_validate(_make_config(tmp_path))
        assert cfg.path == str(tmp_path)
        assert cfg.glob_pattern == "*.nc"
        assert cfg.cron_expression == "* * * * *"

    def test_defaults(self, tmp_path: Path) -> None:
        cfg = CronGlobConfig.model_validate(_make_config(tmp_path))
        assert cfg.max_seen_files == 100_000
        assert cfg.hostname == "localhost"
        assert cfg.run_on_start is True
        assert cfg.ignore_existing is False

    def test_missing_path_raises(self) -> None:
        with pytest.raises(ValidationError):
            CronGlobConfig.model_validate(
                {"glob_pattern": "*.nc", "cron_expression": "* * * * *"}
            )

    def test_missing_glob_pattern_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ValidationError):
            CronGlobConfig.model_validate(
                {"path": str(tmp_path), "cron_expression": "* * * * *"}
            )

    def test_missing_cron_expression_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ValidationError):
            CronGlobConfig.model_validate(
                {"path": str(tmp_path), "glob_pattern": "*.nc"}
            )

    def test_invalid_cron_expression_raises(self, tmp_path: Path) -> None:
        """A syntactically invalid cron expression must be caught at config time."""
        with pytest.raises(ValidationError, match="Invalid cron expression"):
            CronGlobConfig.model_validate(
                _make_config(tmp_path, cron_expression="not a cron")
            )

    def test_custom_values(self, tmp_path: Path) -> None:
        cfg = CronGlobConfig.model_validate(
            _make_config(
                tmp_path,
                max_seen_files=50,
                hostname="remote-host",
                run_on_start=False,
                ignore_existing=True,
            )
        )
        assert cfg.max_seen_files == 50
        assert cfg.hostname == "remote-host"
        assert cfg.run_on_start is False
        assert cfg.ignore_existing is True


# ─── Scan Directory ─────────────────────────────────────────────────────────


class TestScanDirectory:
    """Tests for the _scan_directory method."""

    def test_yields_matching_files(self, mock_service: MagicMock, tmp_data_dir: Path) -> None:
        plugin = CronGlob(mock_service, _make_config(tmp_data_dir))
        files = list(plugin._scan_directory())
        paths = {f.file for f in files}
        assert (tmp_data_dir / "data_a.nc").resolve() in paths
        assert (tmp_data_dir / "data_b.nc").resolve() in paths
        assert (tmp_data_dir / "data_c.txt").resolve() not in paths

    def test_skips_directories(self, mock_service: MagicMock, tmp_data_dir: Path) -> None:
        plugin = CronGlob(mock_service, _make_config(tmp_data_dir, glob_pattern="*"))
        files = list(plugin._scan_directory())
        assert all(f.file is not None and f.file.is_file() for f in files)

    def test_skips_already_seen(self, mock_service: MagicMock, tmp_data_dir: Path) -> None:
        plugin = CronGlob(mock_service, _make_config(tmp_data_dir))
        first = list(plugin._scan_directory())
        second = list(plugin._scan_directory())
        assert len(first) == 2
        assert len(second) == 0

    def test_recursive_glob(self, mock_service: MagicMock, tmp_data_dir: Path) -> None:
        plugin = CronGlob(
            mock_service, _make_config(tmp_data_dir, glob_pattern="**/*.nc")
        )
        files = list(plugin._scan_directory())
        paths = {f.file for f in files}
        assert (tmp_data_dir / "sub" / "data_d.nc").resolve() in paths
        assert len(paths) == 3

    def test_yields_file_objects_with_correct_hostname(self, mock_service: MagicMock, tmp_data_dir: Path) -> None:
        plugin = CronGlob(mock_service, _make_config(tmp_data_dir, hostname="test-host"))
        files = list(plugin._scan_directory())
        assert all(isinstance(f, File) for f in files)
        assert all(f.hostname == "test-host" for f in files)


# ─── LRU Eviction ───────────────────────────────────────────────────────────


class TestLRUEviction:
    """Tests for the bounded seen-set with inline LRU eviction."""

    def test_seen_set_never_exceeds_cap(self, mock_service: MagicMock, tmp_path: Path) -> None:
        """The seen-set must not exceed max_seen_files at any point during a scan."""
        for i in range(10):
            (tmp_path / f"file_{i}.nc").write_text(str(i))

        plugin = CronGlob(mock_service, _make_config(tmp_path, max_seen_files=3))

        # Consume files one by one and assert the cap holds at every step
        for _ in plugin._scan_directory():
            assert len(plugin._seen) <= 3

    def test_evicts_oldest_entry(self, mock_service: MagicMock, tmp_path: Path) -> None:
        """After a scan, the oldest entries are evicted to satisfy the cap."""
        for i in range(5):
            (tmp_path / f"file_{i}.nc").write_text(str(i))

        plugin = CronGlob(mock_service, _make_config(tmp_path, max_seen_files=3))
        list(plugin._scan_directory())
        assert len(plugin._seen) == 3

    def test_evicted_file_is_re_emitted(self, mock_service: MagicMock, tmp_path: Path) -> None:
        """A file removed from the seen-set must appear again on the next scan."""
        (tmp_path / "file_a.nc").write_text("a")
        (tmp_path / "file_b.nc").write_text("b")

        plugin = CronGlob(mock_service, _make_config(tmp_path, max_seen_files=2))
        list(plugin._scan_directory())  # fills seen-set: {A, B}
        assert len(plugin._seen) == 2

        # Simulate eviction by removing file_a from the seen-set directly
        resolved_a = (tmp_path / "file_a.nc").resolve()
        del plugin._seen[resolved_a]

        # file_a must now be re-emitted
        second_scan = list(plugin._scan_directory())
        emitted_paths = {f.file for f in second_scan}
        assert resolved_a in emitted_paths

    def test_cap_enforced_on_partial_consumption(self, mock_service: MagicMock, tmp_path: Path) -> None:
        """Cap must hold even if the caller stops consuming the generator early."""
        for i in range(10):
            (tmp_path / f"file_{i}.nc").write_text(str(i))

        plugin = CronGlob(mock_service, _make_config(tmp_path, max_seen_files=3))
        gen = plugin._scan_directory()
        next(gen)  # consume just one file, leave the generator suspended
        assert len(plugin._seen) <= 3


# ─── Seed Seen ───────────────────────────────────────────────────────────────


class TestSeedSeen:
    """Tests for the _seed_seen method."""

    def test_populates_seen_set(self, mock_service: MagicMock, tmp_data_dir: Path) -> None:
        plugin = CronGlob(mock_service, _make_config(tmp_data_dir))
        plugin._seed_seen()
        assert len(plugin._seen) == 2

    def test_seeded_files_are_not_emitted(self, mock_service: MagicMock, tmp_data_dir: Path) -> None:
        """Files present at seed time must not appear in the following scan."""
        plugin = CronGlob(mock_service, _make_config(tmp_data_dir))
        plugin._seed_seen()
        assert list(plugin._scan_directory()) == []

    def test_seed_respects_max_seen_files(self, mock_service: MagicMock, tmp_path: Path) -> None:
        """Seeding must not allow the seen-set to exceed max_seen_files."""
        for i in range(10):
            (tmp_path / f"file_{i}.nc").write_text(str(i))

        plugin = CronGlob(mock_service, _make_config(tmp_path, max_seen_files=3))
        plugin._seed_seen()
        assert len(plugin._seen) <= 3


# ─── Wait Until ──────────────────────────────────────────────────────────────


class TestWaitUntil:
    """Tests for _wait_until respecting the stop event."""

    def test_returns_true_when_stop_event_is_set(self, mock_service: MagicMock, tmp_data_dir: Path) -> None:
        plugin = CronGlob(mock_service, _make_config(tmp_data_dir))
        plugin._stop_event.set()
        target = datetime.now() + timedelta(hours=1)
        assert plugin._wait_until(target) is True

    def test_returns_false_when_target_is_in_the_past(self, mock_service: MagicMock, tmp_data_dir: Path) -> None:
        plugin = CronGlob(mock_service, _make_config(tmp_data_dir))
        target = datetime.now() - timedelta(seconds=1)
        assert plugin._wait_until(target) is False


# ─── find_file Integration ──────────────────────────────────────────────────


class TestFindFile:
    """Integration tests for the find_file generator."""

    def test_run_on_start_emits_immediately(self, mock_service: MagicMock, tmp_data_dir: Path) -> None:
        """With run_on_start=True, files are emitted before the first cron tick."""
        plugin = CronGlob(mock_service, _make_config(tmp_data_dir, run_on_start=True))
        plugin._stop_event.set()
        files = list(plugin.find_file())
        assert len(files) == 2

    def test_run_on_start_false_emits_nothing_before_first_tick(
        self, mock_service: MagicMock, tmp_data_dir: Path
    ) -> None:
        """With run_on_start=False and stop set immediately, no files are emitted."""
        plugin = CronGlob(mock_service, _make_config(tmp_data_dir, run_on_start=False))
        plugin._stop_event.set()
        assert list(plugin.find_file()) == []

    def test_ignore_existing_suppresses_initial_files(self, mock_service: MagicMock, tmp_data_dir: Path) -> None:
        """With ignore_existing=True, pre-existing files are not emitted on first scan."""
        plugin = CronGlob(
            mock_service,
            _make_config(tmp_data_dir, run_on_start=True, ignore_existing=True),
        )
        plugin._stop_event.set()
        assert list(plugin.find_file()) == []

    def test_ignore_existing_emits_new_files_after_seeding(
        self, mock_service: MagicMock, tmp_data_dir: Path
    ) -> None:
        """Files added after seeding must be emitted normally."""
        plugin = CronGlob(mock_service, _make_config(tmp_data_dir))
        plugin._seed_seen()  # simulate what ignore_existing does

        (tmp_data_dir / "new_arrival.nc").write_text("new")
        files = list(plugin._scan_directory())
        assert len(files) == 1
        assert files[0].file == (tmp_data_dir / "new_arrival.nc").resolve()

    def test_nonexistent_directory_raises_runtime_error(
        self, mock_service: MagicMock, tmp_path: Path
    ) -> None:
        plugin = CronGlob(mock_service, _make_config(tmp_path / "nonexistent"))
        with pytest.raises(RuntimeError, match="does not exist"):
            list(plugin.find_file())

    def test_health_true_during_operation_false_after(self, mock_service: MagicMock, tmp_data_dir: Path) -> None:
        """Health must be True while the generator is active, False after it exits."""
        plugin = CronGlob(mock_service, _make_config(tmp_data_dir, run_on_start=True))
        assert plugin.health is False

        gen = plugin.find_file()
        plugin._stop_event.set()

        health_during: bool | None = None
        for _ in gen:
            health_during = plugin.health

        assert health_during is True, "health should be True while find_file is running"
        assert plugin.health is False, "health should reset to False after generator exits"
