"""Pydantic validators for data monitor configuration files.

This module provides validation for YAML configuration files that define
metadata for Files (and files).
"""

import re
from collections.abc import Mapping
from typing import Annotated, Any, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

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


class Metadata(BaseModel):
    """Metadata configuration for file matching entries.

    Attributes
    ----------
    platform : str | None
        Platform identifier (e.g., 'goes16', 'himawari9').
    sensor : str | None
        Sensor identifier (e.g., 'abi', 'ahi').
    level : str | None
        Data processing level (e.g., 'L1B').
    sector : str | None
        Geographic sector identifier.
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
    """

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )

    platform: Annotated[str | None, Field(default=None)] = None
    sensor: Annotated[str | None, Field(default=None)] = None
    level: Annotated[str | None, Field(default=None)] = None
    sector: Annotated[str | None, Field(default=None)] = None
    num_expected: Annotated[int, Field(default=1, gt=0)] = 1

    # Manual date components
    yyyy: Annotated[str | None, Field(default=None, alias="YYYY")] = None
    mm: Annotated[str | None, Field(default=None, alias="MM")] = None
    dd: Annotated[str | None, Field(default=None, alias="DD")] = None
    jjj: Annotated[str | None, Field(default=None, alias="JJJ")] = None
    hh: Annotated[str | None, Field(default=None, alias="HH")] = None
    nn: Annotated[str | None, Field(default=None, alias="NN")] = None

    @field_validator("platform", "sensor", "level", mode="after")
    @classmethod
    def lowercase_string_fields(cls, value: str | None) -> str | None:
        """Convert string fields to lowercase.

        Parameters
        ----------
        value : str | None
            The value to convert.

        Returns
        -------
        str | None
            Lowercased value or None.
        """
        return value.lower() if value is not None else None

    @field_validator("sector", mode="after")
    @classmethod
    def uppercase_and_validate_sector(cls, value: str | None) -> str | None:
        """Convert sector to uppercase and validate it's non-empty if provided.

        Parameters
        ----------
        value : str | None
            The sector value.

        Returns
        -------
        str | None
            The validated, uppercased sector value.

        Raises
        ------
        ValueError
            If sector is empty string.
        """
        if value is not None:
            if not value:
                msg = "sector must be a non-empty string"
                raise ValueError(msg)
            return value.upper()
        return None

    @model_validator(mode="after")
    def validate_has_at_least_one_field(self) -> Self:
        """Validate that metadata has at least one non-default field set.

        Returns
        -------
        Self
            The validated model.

        Raises
        ------
        ValueError
            If no fields are set.
        """
        non_default_fields = [
            self.platform,
            self.sensor,
            self.level,
            self.sector,
            self.yyyy,
            self.mm,
            self.dd,
            self.jjj,
            self.hh,
            self.nn,
        ]
        # num_expected has a default, so we check if any other field is set
        # or if num_expected differs from default
        has_content = any(f is not None for f in non_default_fields)
        has_non_default_num = self.num_expected != 1

        if not has_content and not has_non_default_num:
            msg = "metadata must have at least one field set"
            raise ValueError(msg)
        return self

    def get_manual_date_components(self) -> frozenset[str]:
        """Get the set of manually specified date components.

        Returns
        -------
        frozenset[str]
            Set of date component names that are manually specified.
        """
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


class FileMetadataEntry(BaseModel):
    """A single entry in the file-metadata configuration.

    Attributes
    ----------
    metadata : Metadata
        The metadata configuration for this entry.
    date : str | None
        Regex pattern for extracting date components from filenames.
    match : list[str]
        List of regex patterns for matching files.
    """

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )

    metadata: Metadata
    date: Annotated[str | None, Field(default=None)] = None
    match: Annotated[list[str], Field(min_length=1)]

    @field_validator("date", mode="after")
    @classmethod
    def validate_date_regex(cls, value: str | None) -> str | None:
        """Validate that date is a valid regex pattern.

        Parameters
        ----------
        value : str | None
            The date regex pattern.

        Returns
        -------
        str | None
            The validated pattern.

        Raises
        ------
        ValueError
            If pattern is invalid.
        """
        if value is not None:
            _validate_regex_pattern(value)
        return value

    @field_validator("match", mode="after")
    @classmethod
    def validate_match_patterns(cls, value: list[str]) -> list[str]:
        """Validate that all match patterns are valid regex patterns.

        Parameters
        ----------
        value : list[str]
            List of match patterns.

        Returns
        -------
        list[str]
            The validated patterns.

        Raises
        ------
        ValueError
            If any pattern is invalid.
        """
        return [_validate_regex_pattern(pattern) for pattern in value]

    def _get_regex_date_components(self) -> frozenset[str]:
        """Extract date components from the date regex pattern.

        Returns
        -------
        frozenset[str]
            Set of date component names found in the regex.
        """
        if self.date is None:
            return frozenset()
        return _extract_regex_named_groups(self.date) & DATE_COMPONENTS

    def _has_sufficient_date_info(self) -> bool:
        """Check if sufficient date information is available.

        Requires YYYY and either (MM + DD) or JJJ.

        Returns
        -------
        bool
            True if sufficient date information is available.
        """
        manual_components = self.metadata.get_manual_date_components()
        regex_components = self._get_regex_date_components()
        all_components = manual_components | regex_components

        has_yyyy = "YYYY" in all_components
        has_mm_dd = "MM" in all_components and "DD" in all_components
        has_jjj = "JJJ" in all_components

        return has_yyyy and (has_mm_dd or has_jjj)

    @model_validator(mode="after")
    def validate_date_requirements(self) -> Self:
        """Validate date regex and manual components provide sufficient info.

        Returns
        -------
        Self
            The validated model.

        Raises
        ------
        ValueError
            If insufficient date information is provided.
        """
        if not self._has_sufficient_date_info():
            manual = self.metadata.get_manual_date_components()
            regex = self._get_regex_date_components()
            all_comps = manual | regex
            msg = (
                f"Insufficient date information. "
                f"Requires YYYY and (MM + DD or JJJ). "
                f"Found: {sorted(all_comps)}"
            )
            raise ValueError(msg)
        return self


class Spec(BaseModel):
    """Specification section of the data monitor configuration.

    Attributes
    ----------
    file_metadata : dict[str, FileMetadataEntry]
        Mapping of entry names to their configurations.
    """

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        populate_by_name=True,
    )

    file_metadata: Annotated[
        dict[str, FileMetadataEntry],
        Field(alias="file-metadata", min_length=1),
    ]

    @field_validator("file_metadata", mode="before")
    @classmethod
    def lowercase_entry_keys(
        cls,
        value: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Convert file-metadata entry keys to lowercase.

        Parameters
        ----------
        value : Mapping[str, Any]
            The file-metadata mapping.

        Returns
        -------
        dict[str, Any]
            Mapping with lowercased keys.
        """
        return {k.lower(): v for k, v in value.items()}

    @model_validator(mode="after")
    def validate_has_platform_entry(self) -> Self:
        """Validate that at least one entry has a platform specified.

        Returns
        -------
        Self
            The validated model.

        Raises
        ------
        ValueError
            If no entry has platform specified.
        """
        has_platform = any(
            entry.metadata.platform is not None for entry in self.file_metadata.values()
        )
        if not has_platform:
            msg = "At least one file-metadata entry must have 'platform' specified"
            raise ValueError(msg)
        return self


class DataMonitorConfig(BaseModel):
    """Root configuration model for data monitor YAML files.

    Attributes
    ----------
    api_version : str
        API version string, must be 'geoips_driver/v1'.
    interface : str
        Interface identifier, must be 'data_monitor_configs'.
    family : str
        Configuration family, must be 'standard'.
    kind : str
        Configuration kind, must be 'data_monitor_config'.
    name : str
        Configuration name identifier.
    description : str
        Human-readable description.
    docstring : str
        Optional documentation string, can be empty.
    spec : Spec
        The specification section containing file metadata.
    """

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        populate_by_name=True,
    )

    api_version: Annotated[
        str,
        Field(alias="apiVersion", pattern=r"^geoips_driver/v1$"),
    ]
    interface: Annotated[str, Field(pattern=r"^data_monitor_configs$")]
    family: Annotated[str, Field(pattern=r"^standard$")]
    kind: Annotated[str, Field(pattern=r"^data_monitor_config$")] = (
        "data_monitor_config"
    )
    name: str
    description: str
    docstring: str
    spec: Spec

    @field_validator("name", mode="after")
    @classmethod
    def lowercase_name(cls, value: str) -> str:
        """Convert name to lowercase.

        Parameters
        ----------
        value : str
            The name value.

        Returns
        -------
        str
            Lowercased name.
        """
        return value.lower()

    @field_validator("description", mode="after")
    @classmethod
    def validate_description_format(cls, value: str) -> str:
        """Validate description starts with capital and ends with period.

        Parameters
        ----------
        value : str
            The description value.

        Returns
        -------
        str
            The validated description.

        Raises
        ------
        ValueError
            If description format is invalid.
        """
        if not value:
            msg = "description cannot be empty"
            raise ValueError(msg)
        if not value[0].isupper():
            msg = "description must start with a capital letter"
            raise ValueError(msg)
        if not value.endswith("."):
            msg = "description must end with a period"
            raise ValueError(msg)
        return value
