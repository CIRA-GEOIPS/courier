"""Datetime parsing utilities for date component extraction from filenames.

Timezone policy
---------------
Every timestamp Courier produces is timezone-aware and in UTC.

Different monitors used to produce different kinds of ``datetime`` for the
same instant: filename regexes yielded naive values, ``s3_poller`` yielded
aware UTC from ``LastModified``, and epoch inputs went through
``datetime.fromtimestamp`` and came out in the *host's local zone*.
``FilterAndGroupJobGroup.get_job_ids_from_file`` then calls ``.timestamp()``,
which interprets a naive value as local time -- so two files representing the
same instant landed in time-grouping buckets an entire UTC offset apart, and
the pairing stages that depend on that bucketing silently never paired.

Naive input is therefore interpreted as UTC (which is what satellite filename
conventions mean) and aware input is converted to UTC.
"""

import re
from datetime import UTC, datetime, timedelta
from typing import Any

# Canonical date component keys used in regex named groups.
_DATE_KEYS = ("YYYY", "MM", "DD", "JJJ", "HH", "NN")


def ensure_utc(value: datetime | None) -> datetime | None:
    """Return *value* as a timezone-aware UTC datetime.

    Naive input is *assumed* to already be UTC and is tagged as such rather
    than shifted -- satellite filenames and the broker payloads Courier reads
    express UTC without saying so. Aware input is converted.

    Parameters
    ----------
    value : datetime | None
        A naive or aware datetime, or ``None``.

    Returns
    -------
    datetime | None
        ``None`` passthrough, otherwise an aware UTC datetime.
    """
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def parse_timestamp(raw: Any, fmt: str | None = None) -> datetime | None:
    """Coerce *raw* into a timezone-aware UTC :class:`datetime`.

    Accepts ISO-8601 strings, Unix epoch numbers (int/float), ``datetime``
    instances, and ``None``. When *fmt* is supplied, string values are parsed
    with ``strptime(fmt)`` instead of ISO-8601.

    Parameters
    ----------
    raw : Any
        The raw value (typically extracted from a broker message body).
    fmt : str | None
        Optional ``strptime`` format string. ``None`` means ISO-8601.

    Returns
    -------
    datetime | None
        Aware UTC datetime, or ``None`` if *raw* is ``None`` or of an
        unsupported type.
    """
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return ensure_utc(raw)
    if isinstance(raw, (int, float)):
        # Epoch seconds are UTC by definition; fromtimestamp() without a tz
        # argument reinterprets them in the host's local zone.
        return datetime.fromtimestamp(float(raw), tz=UTC)
    if not isinstance(raw, str):
        return None
    if fmt:
        return ensure_utc(datetime.strptime(raw, fmt))
    return ensure_utc(datetime.fromisoformat(raw))


# This is a reasonable number of parameters for this function, ignoring ruff here.
def build_timestamp_from_components(  # noqa: PLR0913
    *,
    yyyy: str | None = None,
    mm: str | None = None,
    dd: str | None = None,
    jjj: str | None = None,
    hh: str | None = None,
    nn: str | None = None,
) -> datetime | None:
    """Build a datetime object from date components.

    Supports either (YYYY, MM, DD) or (YYYY, JJJ) formats.
    Hours and minutes default to 0 if not provided.

    Parameters
    ----------
    yyyy : str | None
        Four-digit year.
    mm : str | None
        Two-digit month (01-12).
    dd : str | None
        Two-digit day of month (01-31).
    jjj : str | None
        Three-digit day of year (001-366).
    hh : str | None
        Two-digit hour (00-23).
    nn : str | None
        Two-digit minute (00-59).

    Returns
    -------
    datetime | None
        Constructed datetime, or None if insufficient components.

    Raises
    ------
    ValueError
        If date components are invalid.
    """
    if yyyy is None:
        return None

    year = int(yyyy)
    hour = int(hh) if hh is not None else 0
    minute = int(nn) if nn is not None else 0

    if jjj is not None:
        # Use day of year
        day_of_year = int(jjj)
        base_date = datetime(year, 1, 1, tzinfo=UTC)

        result_date = base_date + timedelta(days=day_of_year - 1)
        return result_date.replace(hour=hour, minute=minute)

    if mm is not None and dd is not None:
        month = int(mm)
        day = int(dd)
        return datetime(year, month, day, hour, minute, tzinfo=UTC)

    return None


def extract_date_components_from_regex(
    pattern: str,
    filename: str,
) -> dict[str, str]:
    """Extract date components from filename using regex named groups.

    Parameters
    ----------
    pattern : str
        Regex pattern with named groups for date components
        (YYYY, MM, DD, JJJ, HH, NN).
    filename : str
        Filename to extract components from.

    Returns
    -------
    dict[str, str]
        Dictionary of extracted date components.
    """
    match = re.search(pattern, filename)
    if not match:
        return {}

    groups = match.groupdict()
    return {
        key: groups[key]
        for key in _DATE_KEYS
        if key in groups and groups[key] is not None
    }


def extract_datetime_from_regex(
    pattern: str,
    filename: str,
    manual_components: dict[str, str] | None = None,
) -> datetime | None:
    """Extract datetime from filename using regex pattern and manual components.

    Parameters
    ----------
    pattern : str
        Regex pattern with named groups for date components.
    filename : str
        Filename to extract datetime from.
    manual_components : dict[str, str] | None
        Manually specified date components to supplement regex extraction.

    Returns
    -------
    datetime | None
        Extracted datetime, or None if extraction failed.
    """
    components: dict[str, str] = dict(manual_components) if manual_components else {}
    components.update(extract_date_components_from_regex(pattern, filename))

    return build_timestamp_from_components(
        yyyy=components.get("YYYY"),
        mm=components.get("MM"),
        dd=components.get("DD"),
        jjj=components.get("JJJ"),
        hh=components.get("HH"),
        nn=components.get("NN"),
    )
