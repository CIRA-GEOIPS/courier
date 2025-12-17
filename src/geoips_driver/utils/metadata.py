"""Metadata matching and application for satellite data files.

This module provides functions to match filenames against configuration
patterns and apply metadata to File objects.
"""

import re
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from geoips_driver.pydantic.data_monitor_configs import (
    DataMonitorConfig,
    FileMetadataEntry,
)
from geoips_driver.types.file import File, build_timestamp_from_components


class MetadataConflictError(Exception):
    """Raised when metadata values conflict during application.

    Attributes
    ----------
    field_name : str
        Name of the conflicting field.
    existing_value : Any
        The existing value in the File.
    new_value : Any
        The new value that conflicts.
    entry_name : str
        Name of the config entry that caused the conflict.
    """

    def __init__(
        self,
        field_name: str,
        existing_value: Any,
        new_value: Any,
        entry_name: str,
    ) -> None:
        """Initialize MetadataConflictError.

        Parameters
        ----------
        field_name : str
            Name of the conflicting field.
        existing_value : Any
            The existing value in the File.
        new_value : Any
            The new value that conflicts.
        entry_name : str
            Name of the config entry that caused the conflict.
        """
        self.field_name = field_name
        self.existing_value = existing_value
        self.new_value = new_value
        self.entry_name = entry_name
        super().__init__(
            f"Metadata conflict for field '{field_name}': "
            f"existing value '{existing_value}' conflicts with "
            f"new value '{new_value}' from entry '{entry_name}'",
        )


class NoMatchError(Exception):
    """Raised when no config entries match the filename.

    Attributes
    ----------
    filename : str
        The filename that had no matches.
    configs_checked : list[str]
        Names of configs that were checked.
    """

    def __init__(self, filename: str, configs_checked: list[str]) -> None:
        """Initialize NoMatchError.

        Parameters
        ----------
        filename : str
            The filename that had no matches.
        configs_checked : list[str]
            Names of configs that were checked.
        """
        self.filename = filename
        self.configs_checked = configs_checked
        super().__init__(
            f"No matching config entries found for filename '{filename}'. "
            f"Checked configs: {configs_checked}",
        )


def _matches_any_pattern(filename: str, patterns: list[str]) -> bool:
    """Check if filename matches any of the regex patterns.

    Parameters
    ----------
    filename : str
        The filename to check.
    patterns : list[str]
        List of regex patterns to match against.

    Returns
    -------
    bool
        True if filename matches at least one pattern.
    """
    return any(re.search(pattern, filename) for pattern in patterns)


def _extract_date_components_from_regex(
    pattern: str,
    filename: str,
) -> dict[str, str]:
    """Extract date components from filename using regex pattern.

    Parameters
    ----------
    pattern : str
        Regex pattern with named groups for date components.
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

    components: dict[str, str] = {}
    groups = match.groupdict()

    for key in ("YYYY", "MM", "DD", "JJJ", "HH", "NN"):
        if key in groups and groups[key] is not None:
            components[key] = groups[key]

    return components


def _get_manual_date_components(entry: FileMetadataEntry) -> dict[str, str]:
    """Extract manually specified date components from entry.

    Parameters
    ----------
    entry : FileMetadataEntry
        The entry to extract from.

    Returns
    -------
    dict[str, str]
        Dictionary of manual date components.
    """
    components: dict[str, str] = {}

    if entry.yyyy is not None:
        components["YYYY"] = entry.yyyy
    if entry.mm is not None:
        components["MM"] = entry.mm
    if entry.dd is not None:
        components["DD"] = entry.dd
    if entry.jjj is not None:
        components["JJJ"] = entry.jjj
    if entry.hh is not None:
        components["HH"] = entry.hh
    if entry.nn is not None:
        components["NN"] = entry.nn

    return components


def _build_timestamp_from_entry(
    entry: FileMetadataEntry,
    filename: str,
    accumulated_components: dict[str, str],
) -> tuple[datetime | None, dict[str, str]]:
    """Build datetime from entry's date regex and accumulated components.

    Parameters
    ----------
    entry : FileMetadataEntry
        The config entry with date pattern.
    filename : str
        The filename to extract date from.
    accumulated_components : dict[str, str]
        Previously accumulated date components.

    Returns
    -------
    tuple[datetime | None, dict[str, str]]
        Tuple of (built datetime, updated accumulated components).
    """
    components = dict(accumulated_components)

    # Add manual components from entry
    manual = _get_manual_date_components(entry)
    components.update(manual)

    # Extract components from regex if date pattern exists
    if entry.date is not None:
        extracted = _extract_date_components_from_regex(entry.date, filename)
        components.update(extracted)

    dt = build_timestamp_from_components(
        yyyy=components.get("YYYY"),
        mm=components.get("MM"),
        dd=components.get("DD"),
        jjj=components.get("JJJ"),
        hh=components.get("HH"),
        nn=components.get("NN"),
    )

    return dt, components


def _check_and_set_field(
    file_obj: File,
    field_name: str,
    new_value: Any,
    entry_name: str,
) -> None:
    """Check for conflicts and set field value on File object.

    Parameters
    ----------
    file_obj : File
        The File object to update.
    field_name : str
        Name of the field to set.
    new_value : Any
        The new value to set.
    entry_name : str
        Name of the config entry (for error messages).

    Raises
    ------
    MetadataConflictError
        If existing value conflicts with new value.
    """
    if new_value is None:
        return

    existing_value = getattr(file_obj, field_name)

    # Handle default values
    if field_name == "num_expected" and existing_value == 1 and new_value != 1:
        setattr(file_obj, field_name, new_value)
        return

    if existing_value is None:
        setattr(file_obj, field_name, new_value)
        return

    # Check for conflict
    if existing_value != new_value:
        raise MetadataConflictError(
            field_name=field_name,
            existing_value=existing_value,
            new_value=new_value,
            entry_name=entry_name,
        )


def _apply_metadata_from_entry(
    file_obj: File,
    entry: FileMetadataEntry,
    entry_name: str,
    filename: str,
    date_components: dict[str, str],
) -> dict[str, str]:
    """Apply metadata from a single config entry to File object.

    Parameters
    ----------
    file_obj : File
        The File object to update.
    entry : FileMetadataEntry
        The config entry with metadata.
    entry_name : str
        Name of the config entry.
    filename : str
        The filename being processed.
    date_components : dict[str, str]
        Accumulated date components from previous entries.

    Returns
    -------
    dict[str, str]
        Updated accumulated date components.

    Raises
    ------
    MetadataConflictError
        If metadata values conflict.
    """
    # Apply simple metadata fields directly from entry
    _check_and_set_field(file_obj, "platform", entry.platform, entry_name)
    _check_and_set_field(file_obj, "sensor", entry.sensor, entry_name)
    _check_and_set_field(file_obj, "level", entry.level, entry_name)
    _check_and_set_field(file_obj, "sector", entry.sector, entry_name)
    _check_and_set_field(file_obj, "num_expected", entry.num_expected, entry_name)

    # Build and apply timestamp
    dt, updated_components = _build_timestamp_from_entry(
        entry,
        filename,
        date_components,
    )

    if dt is not None:
        _check_and_set_field(file_obj, "timestamp", dt, entry_name)

    return updated_components


def _find_matching_entries(
    config: DataMonitorConfig,
    filename: str,
) -> list[tuple[str, FileMetadataEntry]]:
    """Find all entries in a config that match the filename.

    Parameters
    ----------
    config : DataMonitorConfig
        The config to search.
    filename : str
        The filename to match.

    Returns
    -------
    list[tuple[str, FileMetadataEntry]]
        List of (entry_name, entry) tuples for matching entries.
    """
    matches: list[tuple[str, FileMetadataEntry]] = []

    for entry_name, entry in config.spec.file_metadata.items():
        if _matches_any_pattern(filename, entry.match):
            matches.append((entry_name, entry))

    return matches


def apply_metadata_from_configs(
    configs: Sequence[DataMonitorConfig],
    file_obj: File,
    require_match: bool = True,
) -> File:
    """Apply metadata from matching config entries to a File object.

    Searches through all provided configs and their file-metadata entries,
    applying metadata from any entry whose match patterns match the filename.
    If a field is already set to a non-default value and a new entry would
    set it to a different value, raises MetadataConflictError.

    Parameters
    ----------
    configs : Sequence[DataMonitorConfig]
        Sequence of validated config models to search.
    file_obj : File
        The File object to update with metadata.
    require_match : bool
        If True, raise NoMatchError if no entries match. Default True.

    Returns
    -------
    File
        The updated File object (same object, mutated in place).

    Raises
    ------
    MetadataConflictError
        If existing metadata conflicts with new values.
    NoMatchError
        If require_match is True and no config entries match.

    Examples
    --------
    >>> from validator import load_config_from_file
    >>> config = load_config_from_file("goes16_abi.yaml")
    >>> f = File(file=Path("/data/goes16/file.nc"))
    >>> apply_metadata_from_configs([config], f, "somefile.nc")
    >>> print(f.platform)
    goes16
    """
    filename = file_obj.file.resolve() if file_obj.file is not None else None
    if not filename:
        raise ValueError("File object must have a valid 'file' path set.")
    date_components: dict[str, str] = {}
    matched_entries: list[str] = []
    configs_checked: list[str] = [c.name for c in configs]

    for config in configs:
        matching = _find_matching_entries(config, filename)

        for entry_name, entry in matching:
            full_entry_name = f"{config.name}/{entry_name}"
            matched_entries.append(full_entry_name)

            date_components = _apply_metadata_from_entry(
                file_obj=file_obj,
                entry=entry,
                entry_name=full_entry_name,
                filename=filename,
                date_components=date_components,
            )

    if require_match and not matched_entries:
        raise NoMatchError(filename, configs_checked)

    return file_obj


def create_file_with_metadata(
    configs: Sequence[DataMonitorConfig],
    filename: str,
    *,
    hostname: str | None = None,
    require_match: bool = True,
) -> File:
    """Create a new File object with metadata from matching configs.

    Convenience function that creates a new File and applies metadata
    from configs in one step.

    Parameters
    ----------
    configs : Sequence[DataMonitorConfig]
        Sequence of validated config models to search.
    filename : str
        The filename (and path) to match against config patterns.
    hostname : str | None
        Optional hostname where file is located.
    require_match : bool
        If True, raise NoMatchError if no entries match. Default True.

    Returns
    -------
    File
        New File object with applied metadata.

    Raises
    ------
    MetadataConflictError
        If metadata values conflict.
    NoMatchError
        If require_match is True and no config entries match.
    """
    file_obj = File(
        file=Path(filename),
        hostname=hostname,
    )

    return apply_metadata_from_configs(
        configs=configs,
        file_obj=file_obj,
        require_match=require_match,
    )
