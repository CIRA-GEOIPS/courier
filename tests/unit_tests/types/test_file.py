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


# ─── Fixtures ───────────────────────────────────────────────────────────────


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


# ─── File creation ──────────────────────────────────────────────────────────


class TestFileCreation:
    """Tests for File creation and field access."""

    def test_default_construction(self) -> None:
        """All fields are None/default when no arguments are given."""
        f = File()
        assert f.file is None
        assert f.hostname is None
        assert f.platform is None
        assert f.sensor is None
        assert f.level is None
        assert f.sector is None
        assert f.num_expected == 1
        assert f.timestamp is None

    def test_creation_with_minimal_fields(self, sample_path: Path) -> None:
        """Test creating File with only a file path."""
        file_obj = File(file=sample_path)
        assert file_obj.file == sample_path
        assert file_obj.hostname is None
        assert file_obj.num_expected == 1
        assert file_obj.timestamp is None

    def test_creation_with_all_fields(
        self,
        full_file: File,
        sample_path: Path,
        sample_timestamp: datetime,
    ) -> None:
        """Test creating File with all fields populated."""
        assert full_file.file == sample_path
        assert full_file.hostname == "testhost"
        assert full_file.platform == "goes16"
        assert full_file.sensor == "abi"
        assert full_file.level == "l1b"
        assert full_file.sector == "Full-Disk"
        assert full_file.num_expected == 16
        assert full_file.timestamp == sample_timestamp

    def test_mutability(self, full_file: File) -> None:
        """Confirm File is mutable (unlike FrozenFile)."""
        full_file.platform = "modified"
        assert full_file.platform == "modified"

    def test_equality(self, sample_path: Path, sample_timestamp: datetime) -> None:
        """Two Files with identical fields are equal."""
        f1 = File(file=sample_path, platform="goes16", timestamp=sample_timestamp)
        f2 = File(file=sample_path, platform="goes16", timestamp=sample_timestamp)
        assert f1 == f2

    def test_inequality(self, sample_path: Path) -> None:
        """Two Files with different fields are not equal."""
        f1 = File(file=sample_path, platform="goes16")
        f2 = File(file=sample_path, platform="himawari9")
        assert f1 != f2


# ─── File serialization ─────────────────────────────────────────────────────


class TestFileSerialization:
    """Tests for File serialization methods."""

    def test_to_dict_full(
        self,
        full_file: File,
        sample_path: Path,
        sample_timestamp: datetime,
    ) -> None:
        """Test converting a fully populated File to dict."""
        result = full_file.to_dict()
        assert result == {
            "file": str(sample_path),
            "hostname": "testhost",
            "platform": "goes16",
            "sensor": "abi",
            "level": "l1b",
            "sector": "Full-Disk",
            "num_expected": 16,
            "timestamp": sample_timestamp.isoformat(),
        }

    def test_to_dict_none_file(self) -> None:
        """File field is None when no path is given."""
        result = File().to_dict()
        assert result["file"] is None

    def test_to_dict_none_timestamp(self, minimal_file: File) -> None:
        """Timestamp is None when not set."""
        result = minimal_file.to_dict()
        assert result["timestamp"] is None

    def test_to_dict_none_optional_fields(self, minimal_file: File) -> None:
        """All optional metadata fields are None."""
        result = minimal_file.to_dict()
        assert result["hostname"] is None
        assert result["platform"] is None
        assert result["sensor"] is None
        assert result["level"] is None
        assert result["sector"] is None

    def test_str_returns_valid_json(self, full_file: File) -> None:
        """__str__ returns a JSON string matching to_dict."""
        result = str(full_file)
        assert json.loads(result) == full_file.to_dict()

    def test_str_minimal(self, minimal_file: File) -> None:
        """__str__ works with minimal file and produces parseable JSON."""
        parsed = json.loads(str(minimal_file))
        assert isinstance(parsed, dict)

    def test_from_dict_with_timestamp_string(self) -> None:
        """from_dict parses an ISO timestamp string."""
        data = {
            "file": "/tmp/test.nc",
            "timestamp": "2023-06-15T10:30:00",
        }
        f = File.from_dict(data)
        assert f.file == Path("/tmp/test.nc")
        assert f.timestamp == datetime(2023, 6, 15, 10, 30)

    def test_from_dict_with_timestamp_object(self) -> None:
        """from_dict accepts a datetime object directly."""
        dt = datetime(2023, 6, 15, 10, 30)
        data = {"file": "/tmp/test.nc", "timestamp": dt}
        f = File.from_dict(data)
        assert f.timestamp == dt

    def test_from_dict_no_timestamp(self) -> None:
        """Missing timestamp key results in None."""
        f = File.from_dict({"file": "/tmp/test.nc"})
        assert f.timestamp is None

    def test_from_dict_none_file(self) -> None:
        """Explicit None file stays None."""
        f = File.from_dict({"file": None})
        assert f.file is None

    def test_from_dict_missing_file_key(self) -> None:
        """Missing file key results in None."""
        f = File.from_dict({})
        assert f.file is None

    def test_from_dict_all_fields(self, sample_timestamp: datetime) -> None:
        """All fields are correctly read from a dict."""
        data = {
            "file": "/data/goes16_abi.nc",
            "hostname": "host1",
            "platform": "goes16",
            "sensor": "abi",
            "level": "l1b",
            "sector": "conus",
            "num_expected": 10,
            "timestamp": sample_timestamp.isoformat(),
        }
        f = File.from_dict(data)
        assert f.file == Path("/data/goes16_abi.nc")
        assert f.hostname == "host1"
        assert f.platform == "goes16"
        assert f.sensor == "abi"
        assert f.level == "l1b"
        assert f.sector == "conus"
        assert f.num_expected == 10
        assert f.timestamp == sample_timestamp

    def test_from_dict_defaults_num_expected(self) -> None:
        """num_expected defaults to 1 when missing."""
        f = File.from_dict({"file": "/tmp/test.nc"})
        assert f.num_expected == 1

    def test_from_dict_invalid_timestamp(self) -> None:
        """Invalid timestamp string raises ValueError."""
        with pytest.raises(ValueError):
            File.from_dict({"file": "/tmp/test.nc", "timestamp": "not-a-date"})

    @pytest.mark.parametrize(
        ("file_path", "hostname"),
        [
            ("/tmp/test.nc", "host1"),
            (None, None),
        ],
    )
    def test_from_dict_parametrized(
        self,
        full_file: File,
        file_path: str | None,
        hostname: str | None,
    ) -> None:
        """from_dict correctly handles various file/hostname combinations."""
        data = full_file.to_dict()
        data["file"] = file_path
        data["hostname"] = hostname
        result = File.from_dict(data)
        if file_path is None:
            assert result.file is None
        else:
            assert result.file == Path(file_path)
        assert result.hostname == hostname

    def test_from_string(self) -> None:
        """from_string parses a JSON string into a File."""
        data = {"file": "/tmp/test.nc", "timestamp": "2023-01-01T00:00:00"}
        f = File.from_string(json.dumps(data))
        assert f.file == Path("/tmp/test.nc")
        assert f.timestamp == datetime(2023, 1, 1)

    def test_from_string_invalid_json(self) -> None:
        """Invalid JSON raises JSONDecodeError."""
        with pytest.raises(json.JSONDecodeError):
            File.from_string("{invalid")

    def test_from_string_missing_keys_use_defaults(self) -> None:
        """Missing keys in JSON use default values."""
        f = File.from_string(json.dumps({"file": "/tmp/test.nc"}))
        assert f.file == Path("/tmp/test.nc")
        assert f.num_expected == 1
        assert f.timestamp is None

    def test_round_trip_str_from_string(self, full_file: File) -> None:
        """str() -> from_string() round-trip preserves all fields."""
        reconstructed = File.from_string(str(full_file))
        assert reconstructed == full_file

    def test_round_trip_to_dict_from_dict(self, full_file: File) -> None:
        """to_dict() -> from_dict() round-trip preserves all fields."""
        reconstructed = File.from_dict(full_file.to_dict())
        assert reconstructed == full_file

    def test_round_trip_minimal(self, minimal_file: File) -> None:
        """Round-trip works for a file with only defaults."""
        reconstructed = File.from_string(str(minimal_file))
        assert reconstructed == minimal_file


# ─── File methods ────────────────────────────────────────────────────────────


class TestFileMethods:
    """Tests for File methods: freeze, with_updates, merge_metadata."""

    def test_freeze_returns_frozen_file(self, full_file: File) -> None:
        """freeze() returns a FrozenFile instance."""
        frozen = full_file.freeze()
        assert isinstance(frozen, FrozenFile)

    def test_freeze_preserves_all_fields(self, full_file: File) -> None:
        """freeze() copies every field."""
        frozen = full_file.freeze()
        assert frozen.file == full_file.file
        assert frozen.hostname == full_file.hostname
        assert frozen.platform == full_file.platform
        assert frozen.sensor == full_file.sensor
        assert frozen.level == full_file.level
        assert frozen.sector == full_file.sector
        assert frozen.num_expected == full_file.num_expected
        assert frozen.timestamp == full_file.timestamp

    def test_freeze_default_file(self) -> None:
        """freeze() works on a default-constructed File."""
        frozen = File().freeze()
        assert frozen.file is None
        assert frozen.num_expected == 1

    def test_with_updates_returns_new_instance(self, minimal_file: File) -> None:
        """with_updates returns a different object."""
        updated = minimal_file.with_updates(hostname="newhost")
        assert updated is not minimal_file
        assert updated.hostname == "newhost"
        assert minimal_file.hostname is None

    def test_with_updates_multiple_fields(self, minimal_file: File) -> None:
        """Multiple fields can be updated at once."""
        dt = datetime(2024, 6, 1)
        updated = minimal_file.with_updates(
            hostname="h1",
            platform="goes16",
            timestamp=dt,
        )
        assert updated.hostname == "h1"
        assert updated.platform == "goes16"
        assert updated.timestamp == dt
        assert updated.file == minimal_file.file

    def test_with_updates_no_changes(self, full_file: File) -> None:
        """with_updates() with no args returns equal but distinct object."""
        updated = full_file.with_updates()
        assert updated == full_file
        assert updated is not full_file

    # ── merge_metadata ──

    def test_merge_fills_none_fields(self) -> None:
        """merge_metadata sets fields that are currently None."""
        f = File(file=Path("/tmp/f.nc"))
        result = f.merge_metadata(
            platform="goes16",
            sensor="abi",
            level="l1b",
            sector="conus",
        )
        assert result.platform == "goes16"
        assert result.sensor == "abi"
        assert result.level == "l1b"
        assert result.sector == "conus"

    def test_merge_preserves_existing_fields(self, full_file: File) -> None:
        """merge_metadata does not overwrite existing non-None values."""
        result = full_file.merge_metadata(
            platform="overridden",
            sensor="overridden",
            level="overridden",
            sector="overridden",
            dt=datetime(1999, 1, 1),
        )
        assert result.platform == full_file.platform
        assert result.sensor == full_file.sensor
        assert result.level == full_file.level
        assert result.sector == full_file.sector
        assert result.timestamp == full_file.timestamp

    @pytest.mark.parametrize(
        ("initial_num_expected", "merge_num_expected", "expected"),
        [
            (1, 10, 10),   # Default is overridden by merge
            (5, 10, 5),    # Non-default is preserved
            (5, None, 5),  # Non-default preserved when merge is None
            (1, None, 1),  # Default stays when merge is None
            (1, 1, 1),     # Both default
        ],
    )
    def test_merge_num_expected(
        self,
        initial_num_expected: int,
        merge_num_expected: int | None,
        expected: int,
    ) -> None:
        """num_expected is only updated when current value is the default (1)."""
        f = File(num_expected=initial_num_expected)
        result = f.merge_metadata(num_expected=merge_num_expected)
        assert result.num_expected == expected

    def test_merge_timestamp_fills_none(self) -> None:
        """merge_metadata sets timestamp when it's currently None."""
        dt = datetime(2023, 7, 4, 12, 0)
        result = File().merge_metadata(dt=dt)
        assert result.timestamp == dt

    def test_merge_timestamp_preserves_existing(self, full_file: File) -> None:
        """merge_metadata preserves an existing timestamp."""
        original_ts = full_file.timestamp
        result = full_file.merge_metadata(dt=datetime(1999, 1, 1))
        assert result.timestamp == original_ts

    def test_merge_returns_new_instance(self, minimal_file: File) -> None:
        """merge_metadata returns a new File, not the same one."""
        result = minimal_file.merge_metadata(platform="goes16")
        assert result is not minimal_file

    def test_merge_no_args(self, minimal_file: File) -> None:
        """merge_metadata with no args preserves all fields."""
        result = minimal_file.merge_metadata()
        assert result.platform is None
        assert result.sensor is None
        assert result.level is None
        assert result.sector is None
        assert result.num_expected == 1
        assert result.timestamp is None


# ─── FrozenFile creation & immutability ──────────────────────────────────────


class TestFrozenFileCreation:
    """Tests for FrozenFile creation and immutability."""

    def test_default_construction(self) -> None:
        """Default FrozenFile has all None/default values."""
        ff = FrozenFile()
        assert ff.file is None
        assert ff.hostname is None
        assert ff.platform is None
        assert ff.sensor is None
        assert ff.level is None
        assert ff.sector is None
        assert ff.num_expected == 1
        assert ff.timestamp is None

    def test_full_construction(
        self, sample_path: Path, sample_timestamp: datetime
    ) -> None:
        """FrozenFile can be constructed with all fields."""
        ff = FrozenFile(
            file=sample_path,
            hostname="host",
            platform="goes16",
            sensor="abi",
            level="l1b",
            sector="full-disk",
            num_expected=16,
            timestamp=sample_timestamp,
        )
        assert ff.file == sample_path
        assert ff.hostname == "host"
        assert ff.platform == "goes16"
        assert ff.num_expected == 16
        assert ff.timestamp == sample_timestamp

    def test_immutability(self, frozen_file: FrozenFile) -> None:
        """FrozenFile raises AttributeError on attribute assignment."""
        with pytest.raises(AttributeError):
            frozen_file.hostname = "new"  # type: ignore[misc]

    def test_hashable(self, frozen_file: FrozenFile) -> None:
        """Frozen dataclasses are hashable."""
        assert isinstance(hash(frozen_file), int)

    def test_usable_in_set(self, sample_path: Path) -> None:
        """Equal FrozenFiles deduplicate in a set."""
        ff1 = FrozenFile(file=sample_path, platform="goes16")
        ff2 = FrozenFile(file=sample_path, platform="goes16")
        ff3 = FrozenFile(file=sample_path, platform="himawari9")
        assert len({ff1, ff2, ff3}) == 2

    def test_usable_as_dict_key(self, frozen_file: FrozenFile) -> None:
        """FrozenFile can be used as a dictionary key."""
        d = {frozen_file: "value"}
        assert d[frozen_file] == "value"

    def test_equality(self, sample_path: Path) -> None:
        """Two FrozenFiles with identical fields are equal."""
        ff1 = FrozenFile(file=sample_path, platform="goes16")
        ff2 = FrozenFile(file=sample_path, platform="goes16")
        assert ff1 == ff2

    def test_inequality(self, sample_path: Path) -> None:
        """Two FrozenFiles with different fields are not equal."""
        ff1 = FrozenFile(file=sample_path, platform="goes16")
        ff2 = FrozenFile(file=sample_path, platform="himawari9")
        assert ff1 != ff2

    def test_creation_from_freeze(self, full_file: File) -> None:
        """FrozenFile created via File.freeze() matches field-by-field."""
        frozen = full_file.freeze()
        assert isinstance(frozen, FrozenFile)
        assert frozen.file == full_file.file


# ─── FrozenFile serialization ────────────────────────────────────────────────


class TestFrozenFileSerialization:
    """Tests for FrozenFile serialization methods."""

    def test_to_dict_full(self, frozen_file: FrozenFile) -> None:
        """to_dict returns all fields with correct keys."""
        result = frozen_file.to_dict()
        assert result["file"] == str(frozen_file.file)
        assert result["hostname"] == frozen_file.hostname
        assert result["platform"] == frozen_file.platform
        assert result["sensor"] == frozen_file.sensor
        assert result["level"] == frozen_file.level
        assert result["sector"] == frozen_file.sector
        assert result["num_expected"] == frozen_file.num_expected
        assert result["timestamp"] == frozen_file.timestamp.isoformat()

    def test_to_dict_none_values(self) -> None:
        """to_dict returns None for unset fields."""
        result = FrozenFile().to_dict()
        assert result["file"] is None
        assert result["timestamp"] is None

    def test_str_returns_valid_json(self, frozen_file: FrozenFile) -> None:
        """__str__ returns JSON matching to_dict."""
        assert json.loads(str(frozen_file)) == frozen_file.to_dict()

    def test_from_dict_with_timestamp_string(self) -> None:
        """from_dict parses an ISO timestamp string."""
        data = {"file": "/tmp/test.nc", "timestamp": "2023-06-15T10:30:00"}
        ff = FrozenFile.from_dict(data)
        assert ff.file == Path("/tmp/test.nc")
        assert ff.timestamp == datetime(2023, 6, 15, 10, 30)

    def test_from_dict_with_timestamp_object(self) -> None:
        """from_dict accepts a datetime object directly."""
        dt = datetime(2023, 6, 15, 10, 30)
        ff = FrozenFile.from_dict({"file": "/tmp/test.nc", "timestamp": dt})
        assert ff.timestamp == dt

    def test_from_dict_no_timestamp(self) -> None:
        """Missing timestamp key results in None."""
        ff = FrozenFile.from_dict({"file": "/tmp/test.nc"})
        assert ff.timestamp is None

    def test_from_dict_none_file(self) -> None:
        """Missing file key results in None."""
        ff = FrozenFile.from_dict({})
        assert ff.file is None

    def test_from_dict_defaults_num_expected(self) -> None:
        """num_expected defaults to 1 when missing."""
        ff = FrozenFile.from_dict({"file": "/tmp/test.nc"})
        assert ff.num_expected == 1

    def test_from_dict_all_fields(self, sample_timestamp: datetime) -> None:
        """All fields are correctly read from a dict."""
        data = {
            "file": "/data/test.nc",
            "hostname": "host1",
            "platform": "himawari9",
            "sensor": "ahi",
            "level": "l2",
            "sector": "full-disk",
            "num_expected": 5,
            "timestamp": sample_timestamp.isoformat(),
        }
        ff = FrozenFile.from_dict(data)
        assert ff.file == Path("/data/test.nc")
        assert ff.hostname == "host1"
        assert ff.platform == "himawari9"
        assert ff.sensor == "ahi"
        assert ff.level == "l2"
        assert ff.sector == "full-disk"
        assert ff.num_expected == 5
        assert ff.timestamp == sample_timestamp

    def test_from_dict_invalid_timestamp(self) -> None:
        """Invalid timestamp string raises ValueError."""
        with pytest.raises(ValueError):
            FrozenFile.from_dict({"file": "/tmp/test.nc", "timestamp": "bad"})

    def test_from_string(self) -> None:
        """from_string parses JSON into a FrozenFile."""
        data = {"file": "/tmp/test.nc", "timestamp": "2023-01-01T00:00:00"}
        ff = FrozenFile.from_string(json.dumps(data))
        assert ff.file == Path("/tmp/test.nc")
        assert ff.timestamp == datetime(2023, 1, 1)

    def test_from_string_invalid_json(self) -> None:
        """Invalid JSON raises JSONDecodeError."""
        with pytest.raises(json.JSONDecodeError):
            FrozenFile.from_string("not json")

    def test_round_trip_str_from_string(self, frozen_file: FrozenFile) -> None:
        """str() -> from_string() round-trip preserves all fields."""
        reconstructed = FrozenFile.from_string(str(frozen_file))
        assert reconstructed == frozen_file

    def test_round_trip_to_dict_from_dict(self, frozen_file: FrozenFile) -> None:
        """to_dict() -> from_dict() round-trip preserves all fields."""
        reconstructed = FrozenFile.from_dict(frozen_file.to_dict())
        assert reconstructed == frozen_file


# ─── FrozenFile methods ──────────────────────────────────────────────────────


class TestFrozenFileMethods:
    """Tests for FrozenFile methods: thaw, with_updates."""

    def test_thaw_returns_file(self, frozen_file: FrozenFile) -> None:
        """thaw() returns a mutable File."""
        thawed = frozen_file.thaw()
        assert isinstance(thawed, File)

    def test_thaw_preserves_all_fields(self, frozen_file: FrozenFile) -> None:
        """thaw() copies every field."""
        thawed = frozen_file.thaw()
        assert thawed.file == frozen_file.file
        assert thawed.hostname == frozen_file.hostname
        assert thawed.platform == frozen_file.platform
        assert thawed.sensor == frozen_file.sensor
        assert thawed.level == frozen_file.level
        assert thawed.sector == frozen_file.sector
        assert thawed.num_expected == frozen_file.num_expected
        assert thawed.timestamp == frozen_file.timestamp

    def test_thaw_result_is_mutable(self, frozen_file: FrozenFile) -> None:
        """A thawed file can be mutated."""
        thawed = frozen_file.thaw()
        thawed.hostname = "modified"
        assert thawed.hostname == "modified"

    def test_with_updates_returns_new_instance(
        self, frozen_file: FrozenFile
    ) -> None:
        """with_updates returns a distinct FrozenFile."""
        updated = frozen_file.with_updates(hostname="updated")
        assert updated is not frozen_file
        assert isinstance(updated, FrozenFile)
        assert updated.hostname == "updated"
        assert frozen_file.hostname != "updated"

    def test_with_updates_no_changes(self, frozen_file: FrozenFile) -> None:
        """with_updates() with no args returns equal but distinct instance."""
        updated = frozen_file.with_updates()
        assert updated == frozen_file
        assert updated is not frozen_file

    def test_freeze_thaw_roundtrip(self, full_file: File) -> None:
        """freeze() -> thaw() round-trip preserves all fields."""
        assert full_file.freeze().thaw() == full_file

    def test_thaw_freeze_roundtrip(self, frozen_file: FrozenFile) -> None:
        """thaw() -> freeze() round-trip preserves all fields."""
        assert frozen_file.thaw().freeze() == frozen_file


# ─── build_timestamp_from_components ─────────────────────────────────────────


class TestBuildTimestampFromComponents:
    """Tests for build_timestamp_from_components function."""

    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [
            # YYYY/MM/DD with time
            (
                dict(yyyy="2023", mm="01", dd="01", hh="12", nn="30"),
                datetime(2023, 1, 1, 12, 30),
            ),
            # YYYY/MM/DD without time (defaults to 00:00)
            (
                dict(yyyy="2023", mm="06", dd="15"),
                datetime(2023, 6, 15, 0, 0),
            ),
            # YYYY/JJJ
            (
                dict(yyyy="2023", jjj="001"),
                datetime(2023, 1, 1),
            ),
            # YYYY/JJJ with time
            (
                dict(yyyy="2023", jjj="032", hh="08", nn="45"),
                datetime(2023, 2, 1, 8, 45),
            ),
            # Insufficient: YYYY only
            (dict(yyyy="2023"), None),
            # Insufficient: YYYY + MM, no DD
            (dict(yyyy="2023", mm="06"), None),
            # Insufficient: YYYY + DD, no MM
            (dict(yyyy="2023", dd="15"), None),
            # Missing YYYY
            (dict(mm="01", dd="01"), None),
            # All None
            (dict(), None),
        ],
    )
    def test_various_inputs(
        self, kwargs: dict, expected: datetime | None
    ) -> None:
        """Test timestamp building with various component combinations."""
        assert build_timestamp_from_components(**kwargs) == expected

    def test_jjj_priority_over_mm_dd(self) -> None:
        """When jjj is provided alongside mm/dd, jjj is used."""
        result = build_timestamp_from_components(
            yyyy="2023", mm="06", dd="15", jjj="001"
        )
        assert result == datetime(2023, 1, 1)

    def test_default_hour_minute_are_zero(self) -> None:
        """Hour and minute default to 0 when not provided."""
        result = build_timestamp_from_components(yyyy="2023", mm="01", dd="01")
        assert result is not None
        assert result.hour == 0
        assert result.minute == 0

    @pytest.mark.parametrize(
        ("yyyy", "jjj", "expected_yday"),
        [
            ("2023", "001", 1),
            ("2023", "182", 182),   # July 1
            ("2023", "365", 365),   # Dec 31 non-leap
            ("2024", "366", 366),   # Dec 31 leap year
        ],
    )
    def test_julian_day_calculation(
        self, yyyy: str, jjj: str, expected_yday: int
    ) -> None:
        """Julian day maps to the correct day of year."""
        result = build_timestamp_from_components(yyyy=yyyy, jjj=jjj)
        assert result is not None
        assert result.timetuple().tm_yday == expected_yday

    def test_invalid_month_raises(self) -> None:
        """Invalid month (13) raises ValueError."""
        with pytest.raises(ValueError):
            build_timestamp_from_components(yyyy="2023", mm="13", dd="01")

    def test_invalid_day_raises(self) -> None:
        """Invalid day (Feb 30) raises ValueError."""
        with pytest.raises(ValueError):
            build_timestamp_from_components(yyyy="2023", mm="02", dd="30")


# ─── extract_datetime_from_regex ─────────────────────────────────────────────


class TestExtractDatetimeFromRegex:
    """Tests for extract_datetime_from_regex function."""

    def test_full_named_groups(self) -> None:
        """All date/time components extracted via named groups."""
        pattern = (
            r"(?P<YYYY>\d{4})(?P<MM>\d{2})(?P<DD>\d{2})"
            r"_(?P<HH>\d{2})(?P<NN>\d{2})"
        )
        result = extract_datetime_from_regex(pattern, "data_20230615_1430.nc")
        assert result == datetime(2023, 6, 15, 14, 30)

    def test_yyyy_jjj_named_groups(self) -> None:
        """YYYY + JJJ named groups produce the correct date."""
        pattern = r"s(?P<YYYY>\d{4})(?P<JJJ>\d{3})"
        result = extract_datetime_from_regex(pattern, "s2023001_file.nc")
        assert result == datetime(2023, 1, 1)

    def test_unnamed_groups_no_extraction(self) -> None:
        """Unnamed capture groups are ignored."""
        pattern = r"(\d{4})(\d{2})(\d{2})"
        result = extract_datetime_from_regex(pattern, "20230101_file.nc")
        assert result is None

    def test_no_match_returns_none(self) -> None:
        """Pattern that doesn't match returns None."""
        pattern = r"(?P<YYYY>\d{4})(?P<MM>\d{2})(?P<DD>\d{2})"
        result = extract_datetime_from_regex(pattern, "no_date_here.nc")
        assert result is None

    def test_partial_match_insufficient(self) -> None:
        """YYYY alone is not sufficient to build a datetime."""
        pattern = r"(?P<YYYY>\d{4})"
        result = extract_datetime_from_regex(pattern, "2023_file.nc")
        assert result is None

    def test_manual_components_supplement_regex(self) -> None:
        """Manual components fill in what regex doesn't capture."""
        pattern = r"(?P<JJJ>\d{3})"
        manual = {"YYYY": "2023"}
        result = extract_datetime_from_regex(pattern, "day001.nc", manual)
        assert result == datetime(2023, 1, 1)

    def test_regex_overrides_manual_components(self) -> None:
        """Regex-captured values take precedence over manual components."""
        pattern = r"(?P<YYYY>\d{4})(?P<JJJ>\d{3})"
        manual = {"YYYY": "1999"}
        result = extract_datetime_from_regex(pattern, "2024032_data.nc", manual)
        assert result is not None
        assert result.year == 2024  # regex wins

    def test_manual_components_alone(self) -> None:
        """Manual components alone (no regex match) can build a datetime."""
        manual = {"YYYY": "2023", "MM": "01", "DD": "15", "HH": "10", "NN": "30"}
        result = extract_datetime_from_regex(r"no_match", "file.nc", manual)
        assert result == datetime(2023, 1, 15, 10, 30)

    def test_empty_manual_components(self) -> None:
        """Empty manual dict behaves like no manual components."""
        pattern = r"(?P<YYYY>\d{4})(?P<MM>\d{2})(?P<DD>\d{2})"
        result = extract_datetime_from_regex(pattern, "20230615.nc", {})
        assert result == datetime(2023, 6, 15)

    def test_mixed_regex_and_manual(self) -> None:
        """Time from regex, date from manual components."""
        pattern = r"_(?P<HH>\d{2})(?P<NN>\d{2})"
        manual = {"YYYY": "2023", "MM": "06", "DD": "15"}
        result = extract_datetime_from_regex(pattern, "data_1430.nc", manual)
        assert result == datetime(2023, 6, 15, 14, 30)

    def test_optional_group_not_matched(self) -> None:
        """Named groups present in pattern but not matched are skipped."""
        pattern = r"(?P<YYYY>\d{4})(?:_(?P<HH>\d{2}))?"
        result = extract_datetime_from_regex(pattern, "2023_file.nc")
        assert result is None  # YYYY alone is insufficient

    def test_invalid_regex_raises(self) -> None:
        """Invalid regex pattern raises re.error."""
        with pytest.raises(re.error):
            extract_datetime_from_regex(r"[invalid", "test.nc")

    def test_none_manual_components(self) -> None:
        """Explicit None for manual_components works like omitting it."""
        pattern = r"(?P<YYYY>\d{4})(?P<JJJ>\d{3})"
        result = extract_datetime_from_regex(pattern, "s2023001.nc", None)
        assert result == datetime(2023, 1, 1)
