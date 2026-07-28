"""Timezone-normalisation guarantees across the pipeline.

Different monitors produce timestamps in different forms for the same instant:
filename regexes give naive values, ``s3_poller`` gives aware UTC from
``LastModified``, and broker payloads give epoch seconds.
``FilterAndGroupJobGroup`` buckets by ``.timestamp()``, which reads a *naive*
datetime as host-local time -- so the same instant used to land in buckets a
whole UTC offset apart and the pairing stages that depend on that bucketing
silently never paired.

These tests run under a deliberately non-UTC ``TZ`` so a regression to
local-time interpretation fails rather than passing by coincidence on a
UTC-configured CI box.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

from courier.plugins.classes.job_builders.filter_and_group import (
    FilterAndGroupConfig,
    FilterAndGroupJobGroup,
)
from courier.types.file import File, FrozenFile
from courier.utils.datetime_utils import (
    build_timestamp_from_components,
    ensure_utc,
    parse_timestamp,
)

_INSTANT = datetime(2026, 7, 27, 18, 0, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _non_utc_timezone(monkeypatch: pytest.MonkeyPatch):
    """Run these tests in a non-UTC zone so local-time bugs cannot hide."""
    monkeypatch.setenv("TZ", "America/Denver")
    time.tzset()
    yield
    monkeypatch.delenv("TZ", raising=False)
    time.tzset()


class TestEnsureUtc:
    """``ensure_utc`` tags naive input rather than shifting it."""

    def test_naive_is_assumed_utc(self) -> None:
        naive = datetime(2026, 7, 27, 18, 0, 0)  # noqa: DTZ001
        assert ensure_utc(naive) == _INSTANT

    def test_aware_is_converted_not_relabelled(self) -> None:
        mountain = _INSTANT.astimezone(timezone(timedelta(hours=-6)))
        assert ensure_utc(mountain) == _INSTANT

    def test_none_passes_through(self) -> None:
        assert ensure_utc(None) is None


class TestParseTimestamp:
    """Every accepted input form lands on the same aware UTC instant."""

    def test_epoch_is_utc_not_local(self) -> None:
        """``fromtimestamp`` without a tz would shift by the host offset."""
        assert parse_timestamp(_INSTANT.timestamp()) == _INSTANT

    def test_iso_without_offset_is_utc(self) -> None:
        assert parse_timestamp("2026-07-27T18:00:00") == _INSTANT

    def test_iso_with_offset_is_converted(self) -> None:
        assert parse_timestamp("2026-07-27T12:00:00-06:00") == _INSTANT

    def test_strptime_result_is_utc(self) -> None:
        assert parse_timestamp("2026-07-27 18:00", "%Y-%m-%d %H:%M") == _INSTANT

    def test_unsupported_type_returns_none(self) -> None:
        assert parse_timestamp(object()) is None


class TestComponentBuiltTimestamps:
    """Filename-derived timestamps are UTC; satellite names express UTC."""

    def test_yyyy_jjj_is_utc(self) -> None:
        built = build_timestamp_from_components(
            yyyy="2026", jjj="208", hh="18", nn="00",
        )
        assert built == _INSTANT

    def test_yyyy_mm_dd_is_utc(self) -> None:
        built = build_timestamp_from_components(
            yyyy="2026", mm="07", dd="27", hh="18", nn="00",
        )
        assert built == _INSTANT


class TestFileNormalisesAtConstruction:
    """The invariant is enforced on the type, not just at parse boundaries."""

    def test_file_tags_naive_timestamp(self) -> None:
        f = File(file=Path("/a.nc"), timestamp=datetime(2026, 7, 27, 18, 0))  # noqa: DTZ001
        assert f.timestamp == _INSTANT
        assert f.timestamp.tzinfo is not None

    def test_frozen_file_tags_naive_timestamp(self) -> None:
        ff = FrozenFile(file=Path("/a.nc"), timestamp=datetime(2026, 7, 27, 18, 0))  # noqa: DTZ001
        assert ff.timestamp == _INSTANT

    def test_round_trip_is_identity(self) -> None:
        """A File that crosses the broker comes back equal to what was sent."""
        f = File(file=Path("/a.nc"), timestamp=datetime(2026, 7, 27, 18, 0))  # noqa: DTZ001
        assert File.from_string(str(f)) == f

    def test_freeze_preserves_normalised_timestamp(self) -> None:
        f = File(file=Path("/a.nc"), timestamp=datetime(2026, 7, 27, 18, 0))  # noqa: DTZ001
        assert f.freeze().timestamp == _INSTANT


class TestTimeGroupingBucketsAgree:
    """The behaviour that actually broke: cross-monitor bucket agreement."""

    @staticmethod
    def _group() -> FilterAndGroupJobGroup:
        return FilterAndGroupJobGroup(
            FilterAndGroupConfig(files_per_job=4, time_grouping={"minutes": 10}),
        )

    def test_all_producer_forms_share_a_bucket(self) -> None:
        """Naive, aware-UTC, epoch and offset input are one instant, one bucket."""
        group = self._group()
        candidates = {
            "naive": datetime(2026, 7, 27, 18, 0, 0),  # noqa: DTZ001
            "aware_utc": _INSTANT,
            "epoch": parse_timestamp(_INSTANT.timestamp()),
            "offset": _INSTANT.astimezone(timezone(timedelta(hours=-6))),
        }
        buckets = {
            label: tuple(
                group.get_job_ids_from_file(File(file=Path("/x.nc"), timestamp=ts)),
            )
            for label, ts in candidates.items()
        }
        assert len(set(buckets.values())) == 1, buckets

    def test_bucket_survives_the_broker_round_trip(self) -> None:
        group = self._group()
        f = File(file=Path("/x.nc"), timestamp=datetime(2026, 7, 27, 18, 0))  # noqa: DTZ001
        assert group.get_job_ids_from_file(f) == group.get_job_ids_from_file(
            File.from_string(str(f)),
        )

    def test_distinct_instants_still_separate(self) -> None:
        """Normalisation must not collapse genuinely different windows."""
        group = self._group()
        later = _INSTANT + timedelta(minutes=30)
        assert group.get_job_ids_from_file(
            File(file=Path("/x.nc"), timestamp=_INSTANT),
        ) != group.get_job_ids_from_file(
            File(file=Path("/y.nc"), timestamp=later),
        )
