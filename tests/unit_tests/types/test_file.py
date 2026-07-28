"""Unit tests for src/courier/types/file.py"""

import json
import re
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from courier.types.file import (
    File,
    FrozenFile,
)
from courier.utils.datetime_utils import (
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
    return datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)


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
        source="goes16",
        instrument="abi",
        processing_stage="l1b",
        domain="Full-Disk",
        num_expected=16,
        timestamp=sample_timestamp,
    )


@pytest.fixture
def frozen_file(full_file: File) -> FrozenFile:
    """FrozenFile created from a full File."""
    return full_file.freeze()


# ─── File creation ──────────────────────────────────────────────────────────


# ─── Shared behaviour: File and FrozenFile ──────────────────────────────────
#
# The two types share a serialisation contract, so they are exercised through
# one parametrised suite rather than two hand-maintained copies. The previous
# layout duplicated ~50 tests across TestFile*/TestFrozenFile* classes, which
# meant every behaviour change had to be made twice and it was easy for the
# halves to drift.

_TYPES = [File, FrozenFile]
_TYPE_IDS = ["File", "FrozenFile"]

_ALL_FIELDS = dict(
    file=Path("/tmp/sample_file.nc"),
    hostname="testhost",
    source="goes16",
    instrument="abi",
    processing_stage="l1b",
    domain="Full-Disk",
    num_expected=16,
    timestamp=datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC),
)


@pytest.mark.parametrize("cls", _TYPES, ids=_TYPE_IDS)
class TestConstruction:
    """Both types default the same way and accept the same fields."""

    def test_defaults_are_empty(self, cls: type) -> None:
        obj = cls()
        assert obj.file is None
        assert obj.hostname is None
        assert obj.source is None
        assert obj.instrument is None
        assert obj.processing_stage is None
        assert obj.domain is None
        assert obj.timestamp is None
        assert obj.num_expected == 1
        assert dict(obj.metadata) == {}

    def test_all_fields_are_stored(self, cls: type) -> None:
        obj = cls(**_ALL_FIELDS)
        for name, value in _ALL_FIELDS.items():
            assert getattr(obj, name) == value

    def test_equality_is_by_value(self, cls: type) -> None:
        assert cls(**_ALL_FIELDS) == cls(**_ALL_FIELDS)

    def test_inequality_on_any_field(self, cls: type) -> None:
        other = {**_ALL_FIELDS, "source": "himawari9"}
        assert cls(**_ALL_FIELDS) != cls(**other)


@pytest.mark.parametrize("cls", _TYPES, ids=_TYPE_IDS)
class TestSerialization:
    """``str()``/``from_string()`` is the wire format between plugins."""

    def test_to_dict_exposes_every_field(self, cls: type) -> None:
        result = cls(**_ALL_FIELDS).to_dict()
        assert result["file"] == str(_ALL_FIELDS["file"])
        assert result["hostname"] == "testhost"
        assert result["source"] == "goes16"
        assert result["instrument"] == "abi"
        assert result["processing_stage"] == "l1b"
        assert result["domain"] == "Full-Disk"
        assert result["num_expected"] == 16
        assert result["timestamp"] == _ALL_FIELDS["timestamp"].isoformat()

    def test_to_dict_of_empty_object_is_all_none(self, cls: type) -> None:
        result = cls().to_dict()
        assert result["file"] is None
        assert result["timestamp"] is None

    def test_str_is_json_matching_to_dict(self, cls: type) -> None:
        obj = cls(**_ALL_FIELDS)
        assert json.loads(str(obj)) == obj.to_dict()

    def test_round_trip_preserves_every_field(self, cls: type) -> None:
        obj = cls(**_ALL_FIELDS)
        assert cls.from_string(str(obj)) == obj

    def test_round_trip_of_empty_object(self, cls: type) -> None:
        assert cls.from_string(str(cls())) == cls()

    def test_from_dict_parses_an_iso_timestamp(self, cls: type) -> None:
        obj = cls.from_dict({"file": "/x.nc", "timestamp": "2023-06-15T10:30:00"})
        assert obj.timestamp == datetime(2023, 6, 15, 10, 30, tzinfo=UTC)

    def test_from_dict_accepts_a_datetime_object(self, cls: type) -> None:
        moment = datetime(2023, 6, 15, 10, 30, tzinfo=UTC)
        assert cls.from_dict({"file": "/x.nc", "timestamp": moment}).timestamp == moment

    def test_from_dict_without_timestamp(self, cls: type) -> None:
        assert cls.from_dict({"file": "/x.nc"}).timestamp is None

    def test_from_dict_without_file(self, cls: type) -> None:
        assert cls.from_dict({"hostname": "h"}).file is None

    def test_from_dict_rejects_an_invalid_timestamp(self, cls: type) -> None:
        with pytest.raises(ValueError, match="Invalid isoformat|does not match"):
            cls.from_dict({"file": "/x.nc", "timestamp": "not-a-date"})

    def test_from_string_rejects_invalid_json(self, cls: type) -> None:
        with pytest.raises(json.JSONDecodeError):
            cls.from_string("{not json")

    def test_remote_uris_are_not_mangled(self, cls: type) -> None:
        """``pathlib`` would collapse ``s3://`` to ``s3:/``."""
        obj = cls(file="s3://bucket/key.nc")
        assert obj.to_dict()["file"] == "s3://bucket/key.nc"
        assert str(cls.from_string(str(obj)).file) == "s3://bucket/key.nc"


@pytest.mark.parametrize("cls", _TYPES, ids=_TYPE_IDS)
class TestWithUpdates:
    """``with_updates`` must copy, never mutate in place."""

    def test_returns_a_new_object(self, cls: type) -> None:
        obj = cls(**_ALL_FIELDS)
        updated = obj.with_updates(source="himawari9")
        assert updated is not obj
        assert updated.source == "himawari9"
        assert obj.source == "goes16"

    def test_no_changes_still_returns_an_equal_object(self, cls: type) -> None:
        obj = cls(**_ALL_FIELDS)
        assert obj.with_updates() == obj


class TestFreezeAndThaw:
    """Conversion between the mutable and immutable forms."""

    def test_freeze_preserves_every_field(self) -> None:
        original = File(**_ALL_FIELDS, metadata={"level": "l1b"})
        frozen = original.freeze()
        assert frozen.to_dict() == original.to_dict()

    def test_thaw_preserves_every_field(self) -> None:
        frozen = FrozenFile(**_ALL_FIELDS, metadata={"level": "l1b"})
        assert frozen.thaw().to_dict() == frozen.to_dict()

    def test_freeze_thaw_is_a_round_trip(self) -> None:
        original = File(**_ALL_FIELDS, metadata={"level": "l1b"})
        assert original.freeze().thaw() == original

    def test_frozen_metadata_rejects_writes(self) -> None:
        with pytest.raises(TypeError):
            File(file=Path("/x.nc"), metadata={"a": 1}).freeze().metadata["b"] = 2

    def test_freeze_snapshots_rather_than_aliases(self) -> None:
        """Mutating the origin must not reach through to the frozen copy."""
        original = File(file=Path("/x.nc"), metadata={"level": "l1b"})
        frozen = original.freeze()

        original.metadata["level"] = "TAMPERED"

        assert dict(frozen.metadata) == {"level": "l1b"}

    def test_frozen_files_are_hashable(self) -> None:
        """Jobs hold files in a set, so FrozenFile must hash."""
        a = FrozenFile(file=Path("/x.nc"), metadata={"k": "v"})
        b = FrozenFile(file=Path("/x.nc"), metadata={"k": "v"})
        assert len({a, b}) == 1


class TestMergeMetadata:
    """``merge_metadata`` layers metadata without overwriting what is set.

    ``File`` only; ``FrozenFile`` does not expose it. Data monitors call this
    to enrich a file from several config entries in turn, so "existing values
    win" is the property that keeps the first match authoritative.
    """

    def test_fills_only_unset_fields(self) -> None:
        merged = File(file=Path("/x.nc")).merge_metadata(
            source="goes16", instrument="abi", domain="CONUS",
        )
        assert merged.source == "goes16"
        assert merged.instrument == "abi"
        assert merged.domain == "CONUS"

    def test_existing_values_are_preserved(self) -> None:
        original = File(file=Path("/x.nc"), source="goes16", instrument="abi")
        merged = original.merge_metadata(source="himawari9", instrument="ahi")
        assert merged.source == "goes16"
        assert merged.instrument == "abi"

    def test_num_expected_default_is_replaceable(self) -> None:
        """1 is the unset sentinel for num_expected, so a config may set it."""
        assert File(file=Path("/x.nc")).merge_metadata(num_expected=16).num_expected == 16

    def test_explicit_num_expected_is_preserved(self) -> None:
        original = File(file=Path("/x.nc"), num_expected=4)
        assert original.merge_metadata(num_expected=16).num_expected == 4

    def test_timestamp_fills_when_unset(self) -> None:
        moment = datetime(2023, 6, 15, 10, 30, tzinfo=UTC)
        assert File(file=Path("/x.nc")).merge_metadata(dt=moment).timestamp == moment

    def test_timestamp_is_preserved_when_set(self) -> None:
        kept = datetime(2023, 1, 1, tzinfo=UTC)
        original = File(file=Path("/x.nc"), timestamp=kept)
        other = datetime(2024, 1, 1, tzinfo=UTC)
        assert original.merge_metadata(dt=other).timestamp == kept

    def test_metadata_keys_merge_without_overwriting(self) -> None:
        original = File(file=Path("/x.nc"), metadata={"level": "l1b"})
        merged = original.merge_metadata(metadata={"level": "l2", "band": "13"})
        assert merged.metadata == {"level": "l1b", "band": "13"}

    def test_metadata_merges_into_an_empty_dict(self) -> None:
        merged = File(file=Path("/x.nc")).merge_metadata(metadata={"band": "13"})
        assert merged.metadata == {"band": "13"}

    def test_returns_a_new_object(self) -> None:
        original = File(file=Path("/x.nc"))
        merged = original.merge_metadata(source="goes16")
        assert merged is not original
        assert original.source is None

    def test_no_arguments_is_a_no_op(self) -> None:
        original = File(file=Path("/x.nc"), source="goes16")
        assert original.merge_metadata() == original

    def test_with_updates_preserves_metadata(self) -> None:
        original = File(file=Path("/x.nc"), metadata={"level": "l1b"})
        assert original.with_updates(source="goes16").metadata == {"level": "l1b"}


# ─── Property-based round-trip ──────────────────────────────────────────────
#
# There was no property test over File at all: timestamps were covered by
# fourteen hand-written examples, all naive and all in one timezone, which is
# how a timezone-normalisation bug survived.

_paths = st.one_of(
    st.none(),
    st.builds(Path, st.from_regex(r"/[a-zA-Z0-9/\-_\.]{1,60}", fullmatch=True)),
    st.from_regex(r"s3://[a-z0-9\-]{3,20}/[a-zA-Z0-9/\-_\.]{1,40}", fullmatch=True),
)
_optional_text = st.one_of(st.none(), st.from_regex(r"[a-zA-Z0-9\-]{1,20}", fullmatch=True))
_timestamps = st.one_of(
    st.none(),
    st.datetimes(min_value=datetime(2000, 1, 1), max_value=datetime(2100, 1, 1)),
    st.datetimes(
        min_value=datetime(2000, 1, 1),
        max_value=datetime(2100, 1, 1),
        timezones=st.sampled_from(
            [UTC, timezone(timedelta(hours=-6)), timezone(timedelta(hours=9))],
        ),
    ),
)


@pytest.mark.parametrize("cls", _TYPES, ids=_TYPE_IDS)
@given(
    file=_paths,
    hostname=_optional_text,
    source=_optional_text,
    instrument=_optional_text,
    processing_stage=_optional_text,
    domain=_optional_text,
    num_expected=st.integers(min_value=1, max_value=64),
    timestamp=_timestamps,
)
@settings(max_examples=75)
def test_round_trip_is_lossless(cls: type, **fields: object) -> None:
    """Property: ``cls.from_string(str(obj)) == obj`` for any field values."""
    obj = cls(**fields)
    assert cls.from_string(str(obj)) == obj


@pytest.mark.parametrize("cls", _TYPES, ids=_TYPE_IDS)
@given(timestamp=_timestamps)
@settings(max_examples=50)
def test_timestamps_are_always_utc_aware(cls: type, timestamp: datetime | None) -> None:
    """Property: a stored timestamp is aware UTC whatever form it arrived in."""
    stored = cls(file=Path("/x.nc"), timestamp=timestamp).timestamp
    if timestamp is None:
        assert stored is None
    else:
        assert stored is not None
        assert stored.tzinfo is not None
        assert stored.utcoffset() == timedelta(0)


class TestBuildTimestampFromComponents:
    """Tests for build_timestamp_from_components function."""

    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [
            # YYYY/MM/DD with time
            (
                dict(yyyy="2023", mm="01", dd="01", hh="12", nn="30"),
                datetime(2023, 1, 1, 12, 30, tzinfo=UTC),
            ),
            # YYYY/MM/DD without time (defaults to 00:00)
            (
                dict(yyyy="2023", mm="06", dd="15"),
                datetime(2023, 6, 15, 0, 0, tzinfo=UTC),
            ),
            # YYYY/JJJ
            (
                dict(yyyy="2023", jjj="001"),
                datetime(2023, 1, 1, tzinfo=UTC),
            ),
            # YYYY/JJJ with time
            (
                dict(yyyy="2023", jjj="032", hh="08", nn="45"),
                datetime(2023, 2, 1, 8, 45, tzinfo=UTC),
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
        self, kwargs: dict, expected: datetime | None,
    ) -> None:
        """Test timestamp building with various component combinations."""
        assert build_timestamp_from_components(**kwargs) == expected

    def test_jjj_priority_over_mm_dd(self) -> None:
        """When jjj is provided alongside mm/dd, jjj is used."""
        result = build_timestamp_from_components(
            yyyy="2023", mm="06", dd="15", jjj="001",
        )
        assert result == datetime(2023, 1, 1, tzinfo=UTC)

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
        self, yyyy: str, jjj: str, expected_yday: int,
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
        assert result == datetime(2023, 6, 15, 14, 30, tzinfo=UTC)

    def test_yyyy_jjj_named_groups(self) -> None:
        """YYYY + JJJ named groups produce the correct date."""
        pattern = r"s(?P<YYYY>\d{4})(?P<JJJ>\d{3})"
        result = extract_datetime_from_regex(pattern, "s2023001_file.nc")
        assert result == datetime(2023, 1, 1, tzinfo=UTC)

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
        assert result == datetime(2023, 1, 1, tzinfo=UTC)

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
        assert result == datetime(2023, 1, 15, 10, 30, tzinfo=UTC)

    def test_empty_manual_components(self) -> None:
        """Empty manual dict behaves like no manual components."""
        pattern = r"(?P<YYYY>\d{4})(?P<MM>\d{2})(?P<DD>\d{2})"
        result = extract_datetime_from_regex(pattern, "20230615.nc", {})
        assert result == datetime(2023, 6, 15, tzinfo=UTC)

    def test_mixed_regex_and_manual(self) -> None:
        """Time from regex, date from manual components."""
        pattern = r"_(?P<HH>\d{2})(?P<NN>\d{2})"
        manual = {"YYYY": "2023", "MM": "06", "DD": "15"}
        result = extract_datetime_from_regex(pattern, "data_1430.nc", manual)
        assert result == datetime(2023, 6, 15, 14, 30, tzinfo=UTC)

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
        assert result == datetime(2023, 1, 1, tzinfo=UTC)


# ─── Legacy alias removal ────────────────────────────────────────────────────


class TestLegacyAliasRemoval:
    """Verify that legacy field aliases (platform, sensor, level, sector) are no longer supported."""

    def test_platform_not_fallback_for_source(self) -> None:
        """When 'platform' key is present but 'source' is not, source stays None."""
        f = File.from_dict({"file": "/tmp/test.nc", "platform": "goes-18"})
        assert f.source is None

    def test_sensor_not_fallback_for_instrument(self) -> None:
        """When 'sensor' key is present but 'instrument' is not, instrument stays None."""
        f = File.from_dict({"file": "/tmp/test.nc", "sensor": "ABI"})
        assert f.instrument is None

    def test_level_not_fallback_for_processing_stage(self) -> None:
        """When 'level' key is present but 'processing_stage' is not, processing_stage stays None."""
        f = File.from_dict({"file": "/tmp/test.nc", "level": "l2"})
        assert f.processing_stage is None

    def test_sector_not_fallback_for_domain(self) -> None:
        """When 'sector' key is present but 'domain' is not, domain stays None."""
        f = File.from_dict({"file": "/tmp/test.nc", "sector": "full-disk"})
        assert f.domain is None

    def test_null_source_does_not_fallback(self) -> None:
        """When 'source' is explicit null, it stays None (does not fall back to 'platform')."""
        import json
        data = json.loads('{"file": "/tmp/test.nc", "source": null, "platform": "goes-18"}')
        f = File.from_dict(data)
        assert f.source is None
