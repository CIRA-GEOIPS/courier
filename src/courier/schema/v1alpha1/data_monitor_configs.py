"""Pydantic validators for data monitor configuration files.

This module provides validation for YAML configuration files that define
metadata for Files (and files).
"""

import re
from collections.abc import Mapping
from typing import Annotated, Any, Self

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

# Valid template variables for parent_dir and match patterns
VALID_TEMPLATE_VARS: frozenset[str] = frozenset(
    {"YYYY", "MM", "DD", "JJJ", "HH", "NN"},
)

# Date components that can be manually specified or extracted via regex
DATE_COMPONENTS: frozenset[str] = frozenset({"YYYY", "MM", "DD", "JJJ", "HH", "NN"})

# Template variable pattern for validation
TEMPLATE_VAR_PATTERN: re.Pattern[str] = re.compile(r"\{\{\s*(\w+)\s*\}\}")


def _validate_regex_pattern(pattern: str) -> str:
    """Validate that a string is a valid regex pattern.

    Parameters
    ----------
    pattern : str
        The regex pattern to validate.

    Returns
    -------
    str
        The validated pattern.

    Raises
    ------
    ValueError
        If the pattern is not a valid regex.
    """
    try:
        re.compile(pattern)
    except re.error as e:
        msg = f"Invalid regex pattern: {e}"
        raise ValueError(msg) from e
    return pattern


def _extract_regex_named_groups(pattern: str) -> frozenset[str]:
    """Extract named groups from a regex pattern.

    Parameters
    ----------
    pattern : str
        The regex pattern to analyze.

    Returns
    -------
    frozenset[str]
        Set of named group names found in the pattern.
    """
    try:
        compiled = re.compile(pattern)
        return frozenset(compiled.groupindex.keys())
    except re.error:
        return frozenset()


def _extract_template_variables(text: str) -> frozenset[str]:
    """Extract Jinja-style template variables from a string.

    Parameters
    ----------
    text : str
        The string to analyze for template variables.

    Returns
    -------
    frozenset[str]
        Set of template variable names found.
    """
    return frozenset(TEMPLATE_VAR_PATTERN.findall(text))


def _validate_template_variables(text: str, field_name: str) -> str:
    """Validate that only known template variables are used.

    Parameters
    ----------
    text : str
        The string containing template variables.
    field_name : str
        Name of the field being validated (for error messages).

    Returns
    -------
    str
        The validated string.

    Raises
    ------
    ValueError
        If unknown template variables are found.
    """
    found_vars = _extract_template_variables(text)
    unknown_vars = found_vars - VALID_TEMPLATE_VARS
    if unknown_vars:
        msg = f"Unknown template variables in {field_name}: {sorted(unknown_vars)}"
        raise ValueError(msg)
    return text


class FileMetadataEntry(BaseModel):
    """A single entry in the file-metadata configuration.

    Attributes
    ----------
    source : str | None
        Source identifier (e.g., 'goes16', 'himawari9').
    instrument : str | None
        Instrument identifier (e.g., 'abi', 'ahi').
    processing_stage : str | None
        Data processing stage (e.g., 'L1B').
    domain : str | None
        Domain identifier (e.g., 'Full-Disk', 'CONUS').
    num_expected : int
        Expected number of files, defaults to 1.
    yyyy : str | None
        Manual year specification.
    mm : str | None
        Manual month specification.
    dd : str | None
        Manual day specification.
    jjj : str | None
        Manual day-of-year specification.
    hh : str | None
        Manual hour specification.
    nn : str | None
        Manual minute specification.
    date : str | None
        Regex pattern for extracting date components from filenames.
    match : list[str]
        List of regex patterns for matching files.
    """

    # ``extra="forbid"``: an entry is pure data, so a mistyped key here has no
    # symptom at all -- the field it was meant to set keeps its default and the
    # files it was meant to describe are enriched with nothing.
    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        populate_by_name=True,
    )

    source: Annotated[
        str | None,
        Field(
            default=None,
            validation_alias=AliasChoices("source", "platform"),
        ),
    ] = None
    instrument: Annotated[
        str | None,
        Field(
            default=None,
            validation_alias=AliasChoices("instrument", "sensor"),
        ),
    ] = None
    processing_stage: Annotated[
        str | None,
        Field(
            default=None,
            validation_alias=AliasChoices("processing_stage", "level"),
        ),
    ] = None
    domain: Annotated[
        str | None,
        Field(
            default=None,
            validation_alias=AliasChoices("domain", "sector"),
        ),
    ] = None
    num_expected: Annotated[int, Field(default=1, gt=0)] = 1

    # Manual date components
    yyyy: Annotated[str | None, Field(default=None, alias="YYYY")] = None
    mm: Annotated[str | None, Field(default=None, alias="MM")] = None
    dd: Annotated[str | None, Field(default=None, alias="DD")] = None
    jjj: Annotated[str | None, Field(default=None, alias="JJJ")] = None
    hh: Annotated[str | None, Field(default=None, alias="HH")] = None
    nn: Annotated[str | None, Field(default=None, alias="NN")] = None

    date: Annotated[str | None, Field(default=None)] = None
    match: Annotated[list[str], Field(min_length=1)]

    @field_validator("source", "instrument", "processing_stage", mode="after")
    @classmethod
    def lowercase_string_fields(cls, value: str | None) -> str | None:
        """Convert string fields to lowercase."""
        return value.lower() if value is not None else None

    @field_validator("domain", mode="after")
    @classmethod
    def uppercase_and_validate_domain(cls, value: str | None) -> str | None:
        """Convert domain to uppercase and validate it's non-empty if provided."""
        if value is not None:
            if not value:
                msg = "domain must be a non-empty string"
                raise ValueError(msg)
            return value.upper()
        return None

    @field_validator("date", mode="after")
    @classmethod
    def validate_date_regex(cls, value: str | None) -> str | None:
        """Validate that date is a valid regex pattern."""
        if value is not None:
            _validate_regex_pattern(value)
        return value

    @field_validator("match", mode="after")
    @classmethod
    def validate_match_patterns(cls, value: list[str]) -> list[str]:
        """Validate that all match patterns are valid regex patterns."""
        return [_validate_regex_pattern(pattern) for pattern in value]

    @model_validator(mode="after")
    def validate_has_at_least_one_field(self) -> Self:
        """Validate that entry has at least one non-default field set."""
        non_default_fields = [
            self.source,
            self.instrument,
            self.processing_stage,
            self.domain,
            self.yyyy,
            self.mm,
            self.dd,
            self.jjj,
            self.hh,
            self.nn,
        ]
        has_content = any(f is not None for f in non_default_fields)
        has_non_default_num = self.num_expected != 1

        if not has_content and not has_non_default_num:
            msg = "file-metadata entry must have at least one metadata field set"
            raise ValueError(msg)
        return self

    def get_manual_date_components(self) -> frozenset[str]:
        """Get the set of manually specified date components."""
        components: set[str] = set()
        if self.yyyy is not None:
            components.add("YYYY")
        if self.mm is not None:
            components.add("MM")
        if self.dd is not None:
            components.add("DD")
        if self.jjj is not None:
            components.add("JJJ")
        if self.hh is not None:
            components.add("HH")
        if self.nn is not None:
            components.add("NN")
        return frozenset(components)

    def _get_regex_date_components(self) -> frozenset[str]:
        """Extract date components from the date regex pattern."""
        if self.date is None:
            return frozenset()
        return _extract_regex_named_groups(self.date) & DATE_COMPONENTS

    def _has_sufficient_date_info(self) -> bool:
        """Check if sufficient date information is available."""
        manual_components = self.get_manual_date_components()
        regex_components = self._get_regex_date_components()
        all_components = manual_components | regex_components

        has_yyyy = "YYYY" in all_components
        has_mm_dd = "MM" in all_components and "DD" in all_components
        has_jjj = "JJJ" in all_components

        return has_yyyy and (has_mm_dd or has_jjj)

    @model_validator(mode="after")
    def validate_date_requirements(self) -> Self:
        """Validate date regex and manual components provide sufficient info."""
        if self.source is not None and not self._has_sufficient_date_info():
            manual = self.get_manual_date_components()
            regex = self._get_regex_date_components()
            all_comps = manual | regex
            msg = (
                f"Insufficient date information for entry "
                f"with source '{self.source}'. "
                f"Requires YYYY and (MM + DD or JJJ). "
                f"Found: {sorted(all_comps)}"
            )
            raise ValueError(msg)
        return self


class Spec(BaseModel):
    """Specification section of the data monitor configuration."""

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        populate_by_name=True,
    )

    file_metadata: Annotated[
        dict[str, FileMetadataEntry],
        Field(min_length=1),
    ]

    @field_validator("file_metadata", mode="before")
    @classmethod
    def lowercase_entry_keys(
        cls,
        value: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Convert file-metadata entry keys to lowercase."""
        return {k.lower(): v for k, v in value.items()}

    @model_validator(mode="after")
    def validate_has_source_entry(self) -> Self:
        """Validate that at least one entry has a source specified."""
        has_source = any(
            entry.source is not None for entry in self.file_metadata.values()
        )
        if not has_source:
            msg = "At least one file-metadata entry must have 'source' specified"
            raise ValueError(msg)
        return self


class DataMonitorConfig(BaseModel):
    """A named set of filename-to-metadata rules.

    Declared in Python and registered under the ``courier.data_monitor_configs``
    entry-point group; a data monitor names one in its ``metadata-tools`` list.

    These configs used to be YAML plugin files, which is why this model once
    carried an ``apiVersion`` / ``interface`` / ``family`` / ``kind`` /
    ``description`` / ``docstring`` envelope. Nothing ever read those six
    fields — :func:`courier.utils.metadata.apply_metadata_from_configs` uses
    :attr:`spec` and nothing else — and the entry-point group now supplies the
    interface, so they are gone. Human-readable description belongs in the
    declaring module's docstring.

    ``extra="forbid"``: a config is data, and a mistyped key here fails
    silently at match time rather than loudly at import.
    """

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        populate_by_name=True,
    )

    name: str
    spec: Spec
