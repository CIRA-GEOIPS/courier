"""File dataclass with metadata support for data files.

This module provides a File class that stores file information along with metadata.
"""

import json
import types
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import Path
from typing import Any, Self

from courier.utils.datetime_utils import (
    build_timestamp_from_components,
    extract_datetime_from_regex,
)

# Re-export for backward compatibility
__all__ = [
    "File",
    "FrozenFile",
    "build_timestamp_from_components",
    "extract_datetime_from_regex",
]


def _file_to_dict(obj: "File | FrozenFile") -> dict[str, Any]:
    """Convert a File or FrozenFile to a dictionary."""
    return {
        "file": str(obj.file) if obj.file else None,
        "hostname": obj.hostname,
        "source": obj.source,
        "instrument": obj.instrument,
        "processing_stage": obj.processing_stage,
        "domain": obj.domain,
        "metadata": dict(obj.metadata),
        "num_expected": obj.num_expected,
        "timestamp": obj.timestamp.isoformat() if obj.timestamp else None,
    }


def _parse_timestamp_field(dt: Any) -> datetime | None:
    """Parse a timestamp value from a dict into a datetime."""
    if dt is None:
        return None
    if isinstance(dt, datetime):
        return dt
    if isinstance(dt, str):
        return datetime.fromisoformat(dt)
    return None


def _file_fields_from_dict(data: dict[str, Any]) -> dict[str, Any]:
    """Extract File/FrozenFile constructor kwargs from a dictionary."""
    return {
        "file": Path(data["file"]) if data.get("file") else None,
        "hostname": data.get("hostname"),
        "source": data.get("source"),
        "instrument": data.get("instrument"),
        "processing_stage": data.get("processing_stage"),
        "domain": data.get("domain"),
        "metadata": data.get("metadata", {}),
        "num_expected": data.get("num_expected", 1),
        "timestamp": _parse_timestamp_field(data.get("timestamp")),
    }


@dataclass
class File:
    """File dataclass with data metadata.

    Attributes
    ----------
    file : Path | None
        Path to the file.
    hostname : str | None
        Hostname where the file is located.
    source : str | None
        Source identifier (e.g., 'goes16', 'himawari9').
    instrument : str | None
        Instrument identifier (e.g., 'abi', 'ahi').
    processing_stage : str | None
        Data processing stage (e.g., 'l1b').
    domain : str | None
        Domain identifier (e.g., 'full-disk', 'conus').
    metadata : dict[str, Any]
        Arbitrary metadata dictionary.
    num_expected : int
        Expected number of files for this dataset.
    timestamp : datetime | None
        timestamp extracted from filename or manually specified.
    """

    file: Path | None = None
    hostname: str | None = None
    source: str | None = None
    instrument: str | None = None
    processing_stage: str | None = None
    domain: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    num_expected: int = 1
    timestamp: datetime | None = None

    def __str__(self) -> str:
        """Convert File to JSON string."""
        return json.dumps(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        """Convert File to dictionary."""
        return _file_to_dict(self)

    @classmethod
    def from_string(cls, s: str) -> Self:
        """Initialize File from JSON string."""
        return cls.from_dict(json.loads(s))

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """Initialize File from dictionary."""
        return cls(**_file_fields_from_dict(data))

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
            source=self.source,
            instrument=self.instrument,
            processing_stage=self.processing_stage,
            domain=self.domain,
            metadata=types.MappingProxyType(self.metadata),
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
        source: str | None = None,
        instrument: str | None = None,
        processing_stage: str | None = None,
        domain: str | None = None,
        metadata: dict[str, Any] | None = None,
        num_expected: int | None = None,
        dt: datetime | None = None,
    ) -> Self:
        """Merge metadata into this File, only updating None fields.

        Existing non-None values are preserved. This allows layering
        metadata from multiple sources.

        Parameters
        ----------
        source : str | None
            Source identifier.
        instrument : str | None
            Instrument identifier.
        processing_stage : str | None
            Data processing stage.
        domain : str | None
            Domain identifier.
        metadata : dict[str, Any] | None
            Metadata dictionary to shallow-merge. Existing keys are
            preserved; only new keys are added.
        num_expected : int | None
            Expected number of files.
        dt : datetime | None
            timestamp value.

        Returns
        -------
        Self
            New File instance with merged metadata.
        """
        new_metadata = dict(self.metadata)
        if metadata is not None:
            for k, v in metadata.items():
                if k not in new_metadata:
                    new_metadata[k] = v
        return replace(
            self,
            source=self.source if self.source is not None else source,
            instrument=self.instrument if self.instrument is not None else instrument,
            processing_stage=(
                self.processing_stage
                if self.processing_stage is not None
                else processing_stage
            ),
            domain=self.domain if self.domain is not None else domain,
            metadata=new_metadata,
            num_expected=(
                self.num_expected if self.num_expected != 1 else (num_expected or 1)
            ),
            timestamp=self.timestamp if self.timestamp is not None else dt,
        )


@dataclass(frozen=True)
class FrozenFile:
    """Immutable file dataclass with data metadata.

    Attributes
    ----------
    file : Path | None
        Path to the file.
    hostname : str | None
        Hostname where the file is located.
    source : str | None
        Source identifier (e.g., 'goes16', 'himawari9').
    instrument : str | None
        Instrument identifier (e.g., 'abi', 'ahi').
    processing_stage : str | None
        Data processing stage (e.g., 'l1b').
    domain : str | None
        Domain identifier (e.g., 'full-disk', 'conus').
    metadata : Mapping[str, Any]
        Arbitrary metadata dictionary.
    num_expected : int
        Expected number of files for this dataset.
    timestamp : datetime | None
        datetime extracted from filename or manually specified.
    """

    file: Path | None = None
    hostname: str | None = None
    source: str | None = None
    instrument: str | None = None
    processing_stage: str | None = None
    domain: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict, hash=False)
    num_expected: int = 1
    timestamp: datetime | None = None

    def __str__(self) -> str:
        """Convert FrozenFile to JSON string."""
        return json.dumps(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        """Convert FrozenFile to dictionary."""
        return _file_to_dict(self)

    @classmethod
    def from_string(cls, s: str) -> Self:
        """Initialize FrozenFile from JSON string."""
        return cls.from_dict(json.loads(s))

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """Initialize FrozenFile from dictionary."""
        return cls(**_file_fields_from_dict(data))

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
            source=self.source,
            instrument=self.instrument,
            processing_stage=self.processing_stage,
            domain=self.domain,
            metadata=dict(self.metadata),
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
