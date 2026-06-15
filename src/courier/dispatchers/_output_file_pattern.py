"""Output file pattern schema for scanning dispatcher stdout/stderr.

Defines :class:`OutputFilePattern`, a Pydantic model that validates
a regex pattern with a mandatory ``file`` named group and optional
metadata fields — ensuring patterns are valid at config time so the
runtime scanner can trust its inputs.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from courier.schema.v1alpha1.data_monitor_configs import (
    _extract_regex_named_groups,
    _validate_regex_pattern,
)


class OutputFilePattern(BaseModel):
    """A validated regex pattern for extracting output file paths.

    The regex ``pattern`` must contain the named group ``(?P<file>...)``
    to identify file paths in dispatcher text output.  Optional static
    metadata fields mirror the :class:`~courier.types.file.File` schema
    and are applied to every matched file.

    Validation happens at config time (Fail Fast): an invalid regex or
    a pattern missing the ``file`` group raises immediately.
    """

    model_config = ConfigDict(frozen=True, str_strip_whitespace=True)

    pattern: str
    source: str | None = Field(default=None)
    instrument: str | None = Field(default=None)
    processing_stage: str | None = Field(default=None)
    domain: str | None = Field(default=None)
    metadata: dict[str, Any] = Field(default_factory=dict)

    # ── pattern validation ──────────────────────────────────────────────

    @field_validator("pattern", mode="after")
    @classmethod
    def _validate_pattern_has_file_group(cls, v: str) -> str:
        """Validate pattern is valid regex AND contains a ``file`` named group."""
        _validate_regex_pattern(v)
        groups = _extract_regex_named_groups(v)
        if "file" not in groups:
            raise ValueError(
                f"Pattern must contain a 'file' named group: {v!r}",
            )
        return v

    # ── field transform validators ──────────────────────────────────────

    @field_validator("source", mode="after")
    @classmethod
    def _lowercase_source(cls, v: str | None) -> str | None:
        """Auto-lowercase source for FileMetadataEntry convention parity."""
        return v.lower() if v is not None else None

    @field_validator("instrument", mode="after")
    @classmethod
    def _lowercase_instrument(cls, v: str | None) -> str | None:
        """Auto-lowercase instrument for FileMetadataEntry convention parity."""
        return v.lower() if v is not None else None

    @field_validator("processing_stage", mode="after")
    @classmethod
    def _lowercase_processing_stage(cls, v: str | None) -> str | None:
        """Auto-lowercase processing_stage for FileMetadataEntry convention parity."""
        return v.lower() if v is not None else None

    @field_validator("domain", mode="after")
    @classmethod
    def _uppercase_domain(cls, v: str | None) -> str | None:
        """Auto-uppercase domain for FileMetadataEntry convention parity."""
        if v is not None:
            if not v:
                raise ValueError("domain must be a non-empty string")
            return v.upper()
        return None
