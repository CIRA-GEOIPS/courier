"""Unit tests for src/geoips_driver/types/file.py"""

import json
import re
from datetime import datetime
from pathlib import Path

import pytest

from geoips_driver.types.file import (
    File,
    FrozenFile,
    build_timestamp_from_components,
    extract_datetime_from_regex,
)


# Fixtures for reusable test data
@pytest.fixture
def sample_path() -> Path:
    """Sample Path for file field."""
    return Path("/tmp/sample_file.nc")


@pytest.fixture
def sample_timestamp() -> datetime:
    """Sample datetime for timestamp field."""
    return datetime(2023, 1, 1, 12, 0, 0)


@pytest.fixture
def minimal_file(sample_path: Path) -> File:
    """Minimal valid File instance."""
    return File(file=sample_path)


@pytest.fixture
def full_file(sample_path: Path, sample_timestamp: datetime) -> File:
    """Fully populated File instance."""
    return File(
        file=sample_path,
        hostname="testhost",
        platform="goes16",
        sensor="abi",
        level="l1b",
        sector="Full-Disk",
        num_expected=16,
        timestamp=sample_timestamp,
    )


@pytest.fixture
def frozen_file(full_file: File) -> FrozenFile:
    """FrozenFile created from a full File."""
    return full_file.freeze()


# Tests for File dataclass
class TestFileCreation:
    """Tests for File creation and field access."""

    def test_creation_with_minimal_fields(self, sample_path: Path) -> None:
        """Test creating File with only required/minimal fields."""
        file_obj = File(file=sample_path)
        assert file_obj.file == sample_path
        assert file_obj.hostname is None
        assert file_obj.num_expected == 1
        assert file_obj.timestamp is None

    def test_creation_with_all_fields(self, full_file: File, sample_path: Path, sample_timestamp: datetime) -> None:
        """Test creating File with all fields populated."""
        assert full_file.file == sample_path
        assert full_file.hostname == "testhost"
        assert full_file.platform == "goes16"
        assert full_file.sensor == "abi"
        assert full_file.level == "l1b"
        assert full_file.sector == "Full-Disk"
        assert full_file.num_expected == 16
        assert full_file.timestamp == sample_timestamp

    def test_immutability_in_practice(self, full_file: File) -> None:
        """Confirm File is mutable (unlike FrozenFile)."""
        original_platform = full_file.platform
        full_file.platform = "modified"
        assert full_file.platform != original_platform


class TestFileSerialization:
    """Tests for File serialization methods."""

    def test_to_dict(self, full_file: File, sample_path: Path, sample_timestamp: datetime) -> None:
        """Test converting File to dict."""
        result = full_file.to_dict()
        expected = {
            "file": str(sample_path),
            "hostname": "testhost",
            "platform": "goes16",
            "sensor": "abi",
            "level": "l1b",
            "sector": "Full-Disk",
            "num_expected": 16,
            "datetime": sample_timestamp.isoformat(),
        }
        assert result == expected

    def test_to_dict_with_none_values(self, minimal_file: File, sample_path: Path) -> None:
        """Test to_dict with None values."""
        result = minimal_file.to_dict()
        assert result["file"] is sample_path.as_posix()
        assert result["hostname"] is None
        assert result["datetime"] is None

    def test_str_method(self, full_file: File) -> None:
        """Test __str__ returns JSON string."""
        result = str(full_file)
        assert json.loads(result) == full_file.to_dict()

    @pytest.mark.parametrize(
        ("file_path", "hostname"),
        [
            ("/tmp/test.nc", "host1"),  # Valid Path string
            (None, None),  # None values
        ],
    )
    def test_from_dict(self, full_file: File, file_path: str | None, hostname: str | None) -> None:
        """Test creating File from dict."""
        data = full_file.to_dict()
        data["file"] = file_path
        data["hostname"] = hostname
        result = File.from_dict(data)
        if file_path is None:
            assert result.file is None
        else:
            assert result.file == Path(file_path)
        assert result.hostname == hostname

    def test_from_dict_invalid_datetime(self) -> None:
        """Test from_dict with invalid datetime string."""
        data = {"file": "/tmp/test.nc", "datetime": "invalid"}
        with pytest.raises(ValueError):  # from fromisoformat
            File.from_dict(data)

    def test_from_string(self, full_file: File) -> None:
        """Test creating File from JSON string."""
        json_str = str(full_file)
        result = File.from_string(json_str)
        assert result == full_file

    def test_from_string_invalid_json(self) -> None:
        """Test from_string with invalid JSON."""
        with pytest.raises(json.JSONDecodeError):
            File.from_string("invalid json")

    def test_from_string_missing_key(self) -> None:
        """Test from_string with missing keys (uses defaults)."""
        data = {"file": "/tmp/test.nc"}
        json_str = json.dumps(data)
        result = File.from_string(json_str)
        assert result.file == Path("/tmp/test.nc")
        assert result.num_expected == 1


class TestFileMethods:
    """Tests for File methods like freeze, with_updates, merge_metadata."""

    def test_freeze(self, full_file: File) -> None:
        """Test freezing a File to FrozenFile."""
        frozen = full_file.freeze()
        assert isinstance(frozen, FrozenFile)
        assert frozen.file == full_file.file
        assert frozen.timestamp == full_file.timestamp

    def test_with_updates(self, minimal_file: File) -> None:
        """Test creating new File with updated fields."""
        updated = minimal_file.with_updates(hostname="newhost", platform="newplatform")
        assert updated.hostname == "newhost"
        assert updated.platform == "newplatform"
        assert updated.file == minimal_file.file  # Original unchanged

    @pytest.mark.parametrize(
        ("initial_num_expected", "merge_num_expected", "expected"),
        [
            (1, 10, 10),  # Update from default
            (5, None, 5),  # Preserve existing
            (1, 1, 1),  # Both default
        ],
    )
    def test_merge_metadata(self, minimal_file: File, sample_timestamp: datetime, initial_num_expected: int, merge_num_expected: int | None, expected: int) -> None:
        """Test merging metadata with selective updates."""
        minimal_file.num_expected = initial_num_expected
        result = minimal_file.merge_metadata(num_expected=merge_num_expected, dt=sample_timestamp, platform="merged")
        assert result.num_expected == expected
        assert result.timestamp == sample_timestamp
        assert result.platform == "merged"
        assert result.hostname is None  # Unmentioned fields None in original, stay None

    def test_merge_metadata_preserve_existing(self, full_file: File) -> None:
        """Test merge_metadata preserves existing non-None values."""
        result = full_file.merge_metadata(platform="overridden", dt=datetime.now())  # Should not override
        assert result.platform == full_file.platform  # Preserved
        assert result.timestamp == full_file.timestamp  # Preserved


class TestFrozenFileCreationAndImmutability:
    """Tests for FrozenFile creation and immutability."""

    def test_creation_from_dict(self, full_file: File) -> None:
        """Test creating FrozenFile from dict (similar to File)."""
        data = full_file.to_dict()
        result = FrozenFile.from_dict(data)
        assert isinstance(result, FrozenFile)
        assert result.file == full_file.file

    def test_immutability(self, frozen_file: FrozenFile) -> None:
        """Test that FrozenFile raises error on mutation."""
        with pytest.raises(AttributeError):  # Dataclasses frozen should raise this
            frozen_file.hostname = "newhost"  # type: ignore[misc]

    def test_thaw(self, frozen_file: FrozenFile) -> None:
        """Test thawing FrozenFile to mutable File."""
        thawed = frozen_file.thaw()
        assert isinstance(thawed, File)
        assert thawed.file == frozen_file.file
        thawed.hostname = "modified"  # Should succeed
        assert thawed.hostname == "modified"


class TestFrozenFileSerialization:
    """Tests for FrozenFile serialization (mirror of File)."""

    def test_to_dict_and_str(self, frozen_file: FrozenFile) -> None:
        """Test FrozenFile to_dict and __str__."""
        result = frozen_file.to_dict()
        assert "file" in result
        assert str(frozen_file) == json.dumps(result)

    def test_from_string(self, frozen_file: FrozenFile) -> None:
        """Test FrozenFile from_string."""
        json_str = str(frozen_file)
        result = FrozenFile.from_string(json_str)
        assert result == frozen_file


class TestFrozenFileMethods:
    """Tests for FrozenFile methods."""

    def test_with_updates(self, frozen_file: FrozenFile) -> None:
        """Test FrozenFile with_updates creates new instance."""
        original_hostname = frozen_file.hostname
        updated = frozen_file.with_updates(hostname="updated")
        assert updated.hostname == "updated"
        assert frozen_file.hostname == original_hostname  # Original unchanged


# Tests for standalone functions
class TestBuildTimestampFromComponents:
    """Tests for build_timestamp_from_components function."""

    @pytest.mark.parametrize(
        ("yyyy", "mm", "dd", "jjj", "hh", "nn", "expected"),
        [
            ("2023", "01", "01", None, "12", "30", datetime(2023, 1, 1, 12, 30)),  # YYYY/MM/DD
            ("2023", None, None, "001", None, None, datetime(2023, 1, 1)),  # YYYY/JJJ
            ("2023", None, None, None, None, None, None),  # Insufficient (no MM/DD or JJJ)
            ("2023", "01", None, None, None, None, None),  # Insufficient (missing DD)
            (None, "01", "01", None, None, None, None),  # Missing YYYY
        ],
    )
    def test_build_timestamp_various_inputs(self, yyyy: str | None, mm: str | None, dd: str | None, jjj: str | None, hh: str | None, nn: str | None, expected: datetime | None) -> None:
        """Test timestamp building with various component combinations."""
        result = build_timestamp_from_components(yyyy=yyyy, mm=mm, dd=dd, jjj=jjj, hh=hh, nn=nn)
        assert result == expected

    @pytest.mark.parametrize(
        ("yyyy", "jjj", "expected_day_of_year"),
        [
            ("2023", "001", 1),  # Day 1
            ("2023", "365", 365),  # Day 365
            ("2024", "366", 366),  # Leap year day 366
        ],
    )
    def test_build_timestamp_julian_day_calculation(self, yyyy: str, jjj: str, expected_day_of_year: int) -> None:
        """Test Julian day calculation in timestamps."""
        result = build_timestamp_from_components(yyyy=yyyy, jjj=jjj)
        assert result is not None
        assert result.timetuple().tm_yday == expected_day_of_year


class TestExtractDatetimeFromRegex:
    """Tests for extract_datetime_from_regex function."""

    @pytest.mark.parametrize(
        ("pattern", "filename", "expected"),
        [
            (r"(\d{4})(\d{2})(\d{2})", "20230101_file.nc", None),  # No named groups, should not extract
            (r"s(?P<YYYY>\d{4})(?P<JJJ>\d{3})", "s2023001_file.nc", datetime(2023, 1, 1)),  # Valid named groups
            (r"(?P<YYYY>\d{4})", "2023_file.nc", None),  # Insufficient components
            (r"no_match", "2020101_file.nc", None),  # Pattern doesn't match
        ],
    )
    def test_extract_datetime_basic(self, pattern: str, filename: str, expected: datetime | None) -> None:
        """Test basic datetime extraction."""
        result = extract_datetime_from_regex(pattern, filename)
        assert result == expected

    def test_extract_datetime_with_manual_components(self, sample_timestamp: datetime) -> None:
        """Test with manual components supplementation."""
        pattern = r"(?P<JJJ>\d{3})"
        filename = "001_day.nc"
        manual = {"YYYY": "2023"}
        result = extract_datetime_from_regex(pattern, filename, manual_components=manual)
        assert result == datetime(2023, 1, 1)  # YYYY from manual, JJJ from regex

    def test_extract_datetime_edge_cases(self) -> None:
        """Test edge cases like invalid pattern or filename."""
        # Invalid regex pattern (should handle gracefully? But re module raises error, function may propagate)
        with pytest.raises(re.error):  # If pattern is invalid
            extract_datetime_from_regex(r"[", "test")  # Invalid regex
        # Function assumes valid pattern, as per docs
