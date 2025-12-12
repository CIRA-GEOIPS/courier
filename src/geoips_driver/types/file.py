"""File dataclass with metadata support for satellite data files.

This module provides a File class that stores file information along with
metadata extracted from satellite data configuration files.
"""

import json
import re
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Self


@dataclass
class File:
    """File dataclass with satellite data metadata.

    Attributes
    ----------
    file : Path | None
        Path to the file.
    hostname : str | None
        Hostname where the file is located.
    platform : str | None
        Platform identifier (e.g., 'goes16', 'himawari9').
    sensor : str | None
        Sensor identifier (e.g., 'abi', 'ahi').
    level : str | None
        Data processing level (e.g., 'l1b').
    sector : str | None
        Geographic sector identifier (e.g., 'full-disk', 'conus').
    num_expected : int
        Expected number of files for this dataset.
    timestamp : datetime | None
        timestamp extracted from filename or manually specified.
    """

    file: Path | None = None
    hostname: str | None = None
    platform: str | None = None
    sensor: str | None = None
    level: str | None = None
    sector: str | None = None
    num_expected: int = 1
    timestamp: datetime | None = None

    def __str__(self) -> str:
        """Convert File to JSON string.

        Returns
        -------
        str
            JSON string representation of the File.
        """
        return json.dumps(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        """Convert File to dictionary.

        Returns
        -------
        dict[str, Any]
            Dictionary representation of the File.
        """
        return {
            "file": str(self.file) if self.file else None,
            "hostname": self.hostname,
            "platform": self.platform,
            "sensor": self.sensor,
            "level": self.level,
            "sector": self.sector,
            "num_expected": self.num_expected,
            "datetime": self.timestamp.isoformat() if self.timestamp else None,
        }

    @classmethod
    def from_string(cls, s: str) -> Self:
        """Initialize File from JSON string.

        Parameters
        ----------
        s : str
            JSON string representation of File.

        Returns
        -------
        Self
            File instance.
        """
        data = json.loads(s)
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """Initialize File from dictionary.

        Parameters
        ----------
        data : dict[str, Any]
            Dictionary representation of File.

        Returns
        -------
        Self
            File instance.
        """
        dt = data.get("datetime")
        parsed_timestamp: datetime | None = None
        if dt is not None:
            if isinstance(dt, datetime):
                parsed_timestamp = dt
            elif isinstance(dt, str):
                parsed_timestamp = datetime.fromisoformat(dt)

        return cls(
            file=Path(data["file"]) if data.get("file") else None,
            hostname=data.get("hostname"),
            platform=data.get("platform"),
            sensor=data.get("sensor"),
            level=data.get("level"),
            sector=data.get("sector"),
            num_expected=data.get("num_expected", 1),
            timestamp=parsed_timestamp,
        )

    def freeze(self) -> "FrozenFile":
        """Create an immutable copy of this File.

        Returns
        -------
        FrozenFile
            Immutable copy of this File.
        """
        return FrozenFile(
            file=self.file,
            hostname=self.hostname,
            platform=self.platform,
            sensor=self.sensor,
            level=self.level,
            sector=self.sector,
            num_expected=self.num_expected,
            timestamp=self.timestamp,
        )

    def with_updates(self, **kwargs: Any) -> Self:
        """Create a new File with updated fields.

        Parameters
        ----------
        **kwargs : Any
            Fields to update.

        Returns
        -------
        Self
            New File instance with updated fields.
        """
        return replace(self, **kwargs)

    # This is a reasonable number of parameters for this function, ignoring ruff here.
    def merge_metadata(  # noqa: PLR0913
        self,
        *,
        platform: str | None = None,
        sensor: str | None = None,
        level: str | None = None,
        sector: str | None = None,
        num_expected: int | None = None,
        dt: datetime | None = None,
    ) -> Self:
        """Merge metadata into this File, only updating None fields.

        Existing non-None values are preserved. This allows layering
        metadata from multiple sources.

        Parameters
        ----------
        platform : str | None
            Platform identifier.
        sensor : str | None
            Sensor identifier.
        level : str | None
            Data processing level.
        sector : str | None
            Geographic sector identifier.
        num_expected : int | None
            Expected number of files.
        dt : datetime | None
            timestamp value.

        Returns
        -------
        Self
            New File instance with merged metadata.
        """
        return replace(
            self,
            platform=self.platform if self.platform is not None else platform,
            sensor=self.sensor if self.sensor is not None else sensor,
            level=self.level if self.level is not None else level,
            sector=self.sector if self.sector is not None else sector,
            num_expected=(
                self.num_expected if self.num_expected != 1 else (num_expected or 1)
            ),
            timestamp=self.timestamp if self.timestamp is not None else dt,
        )


@dataclass(frozen=True)
class FrozenFile:
    """Immutable file dataclass with satellite data metadata.

    Attributes
    ----------
    file : Path | None
        Path to the file.
    hostname : str | None
        Hostname where the file is located.
    platform : str | None
        Platform identifier (e.g., 'goes16', 'himawari9').
    sensor : str | None
        Sensor identifier (e.g., 'abi', 'ahi').
    level : str | None
        Data processing level (e.g., 'l1b').
    sector : str | None
        Geographic sector identifier (e.g., 'full-disk', 'conus').
    num_expected : int
        Expected number of files for this dataset.
    timestamp : datetime | None
        datetime extracted from filename or manually specified.
    """

    file: Path | None = None
    hostname: str | None = None
    platform: str | None = None
    sensor: str | None = None
    level: str | None = None
    sector: str | None = None
    num_expected: int = 1
    timestamp: datetime | None = None

    def __str__(self) -> str:
        """Convert FrozenFile to JSON string.

        Returns
        -------
        str
            JSON string representation of the FrozenFile.
        """
        return json.dumps(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        """Convert FrozenFile to dictionary.

        Returns
        -------
        dict[str, Any]
            Dictionary representation of the FrozenFile.
        """
        return {
            "file": str(self.file) if self.file else None,
            "hostname": self.hostname,
            "platform": self.platform,
            "sensor": self.sensor,
            "level": self.level,
            "sector": self.sector,
            "num_expected": self.num_expected,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
        }

    @classmethod
    def from_string(cls, s: str) -> Self:
        """Initialize FrozenFile from JSON string.

        Parameters
        ----------
        s : str
            JSON string representation of FrozenFile.

        Returns
        -------
        Self
            FrozenFile instance.
        """
        data = json.loads(s)
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """Initialize FrozenFile from dictionary.

        Parameters
        ----------
        data : dict[str, Any]
            Dictionary representation of FrozenFile.

        Returns
        -------
        Self
            FrozenFile instance.
        """
        dt = data.get("timestamp")
        parsed_timestamp: datetime | None = None
        if dt is not None:
            if isinstance(dt, datetime):
                parsed_timestamp = dt
            elif isinstance(dt, str):
                parsed_timestamp = datetime.fromisoformat(dt)

        return cls(
            file=Path(data["file"]) if data.get("file") else None,
            hostname=data.get("hostname"),
            platform=data.get("platform"),
            sensor=data.get("sensor"),
            level=data.get("level"),
            sector=data.get("sector"),
            num_expected=data.get("num_expected", 1),
            timestamp=parsed_timestamp,
        )

    def thaw(self) -> File:
        """Create a mutable copy of this FrozenFile.

        Returns
        -------
        File
            Mutable copy of this FrozenFile.
        """
        return File(
            file=self.file,
            hostname=self.hostname,
            platform=self.platform,
            sensor=self.sensor,
            level=self.level,
            sector=self.sector,
            num_expected=self.num_expected,
            timestamp=self.timestamp,
        )

    def with_updates(self, **kwargs: Any) -> Self:
        """Create a new FrozenFile with updated fields.

        Parameters
        ----------
        **kwargs : Any
            Fields to update.

        Returns
        -------
        Self
            New FrozenFile instance with updated fields.
        """
        return replace(self, **kwargs)


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
        base_date = datetime(year, 1, 1)

        result_date = base_date + timedelta(days=day_of_year - 1)
        return result_date.replace(hour=hour, minute=minute)

    if mm is not None and dd is not None:
        month = int(mm)
        day = int(dd)
        return datetime(year, month, day, hour, minute)

    return None


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

    match = re.search(pattern, filename)
    if match:
        groups = match.groupdict()
        for key in ("YYYY", "MM", "DD", "JJJ", "HH", "NN"):
            if key in groups and groups[key] is not None:
                components[key] = groups[key]

    return build_timestamp_from_components(
        yyyy=components.get("YYYY"),
        mm=components.get("MM"),
        dd=components.get("DD"),
        jjj=components.get("JJJ"),
        hh=components.get("HH"),
        nn=components.get("NN"),
    )
