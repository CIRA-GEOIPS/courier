"""Metadata matching and application for data files.

This module provides functions to match filenames against configuration
patterns and apply metadata to File objects.
"""

import re
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from courier.errors import MetadataConflictError, NoMatchError
from courier.schema import DataMonitorConfig, FileMetadataEntry
from courier.types.file import File
from courier.utils.datetime_utils import (
    build_timestamp_from_components,
    extract_date_components_from_regex,
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
        extracted = extract_date_components_from_regex(entry.date, filename)
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


def _check_field_conflict(
    existing_value: Any,
    new_value: Any,
    field_name: str,
    entry_name: str,
) -> Any:
    """Return the resolved value for a field, raising on conflict.

    Parameters
    ----------
    existing_value : Any
        The current value of the field.
    new_value : Any
        The new value to apply.
    field_name : str
        Name of the field (for error messages).
    entry_name : str
        Name of the config entry (for error messages).

    Returns
    -------
    Any
        The resolved value (new_value if applicable, else existing_value).

    Raises
    ------
    MetadataConflictError
        If existing value conflicts with new value.
    """
    if new_value is None:
        return existing_value

    # Handle default values
    if field_name == "num_expected" and existing_value == 1 and new_value != 1:
        return new_value

    if existing_value is None:
        return new_value

    # Check for conflict
    if existing_value != new_value:
        raise MetadataConflictError(
            field_name=field_name,
            existing_value=existing_value,
            new_value=new_value,
            entry_name=entry_name,
        )

    return existing_value


def _collect_metadata_from_entry(  # noqa: PLR0913
    file_obj: File,
    updates: dict[str, Any],
    entry: FileMetadataEntry,
    entry_name: str,
    filename: str,
    date_components: dict[str, str],
) -> dict[str, str]:
    """Collect metadata updates from a single config entry.

    Parameters
    ----------
    file_obj : File
        The original File object (read-only).
    updates : dict[str, Any]
        Accumulated field updates to mutate in place.
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
    for field_name, new_value in [
        ("source", entry.source),
        ("instrument", entry.instrument),
        ("processing_stage", entry.processing_stage),
        ("domain", entry.domain),
        ("num_expected", entry.num_expected),
    ]:
        current = updates.get(field_name, getattr(file_obj, field_name))
        updates[field_name] = _check_field_conflict(
            current,
            new_value,
            field_name,
            entry_name,
        )

    # Build and resolve timestamp
    dt, updated_components = _build_timestamp_from_entry(
        entry,
        filename,
        date_components,
    )

    if dt is not None:
        current_ts = updates.get("timestamp", file_obj.timestamp)
        updates["timestamp"] = _check_field_conflict(
            current_ts,
            dt,
            "timestamp",
            entry_name,
        )

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
        The File object to enrich with metadata. Not mutated.
    require_match : bool
        If True, raise NoMatchError if no entries match. Default True.

    Returns
    -------
    File
        A new File object with applied metadata.

    Raises
    ------
    MetadataConflictError
        If existing metadata conflicts with new values.
    NoMatchError
        If require_match is True and no config entries match.
    """
    filename = str(file_obj.file.resolve()) if file_obj.file is not None else None
    if not filename:
        raise ValueError
    date_components: dict[str, str] = {}
    matched_entries: list[str] = []
    configs_checked: list[str] = [c.name for c in configs]
    updates: dict[str, Any] = {}

    for config in configs:
        matching = _find_matching_entries(config, filename)

        for entry_name, entry in matching:
            full_entry_name = f"{config.name}/{entry_name}"
            matched_entries.append(full_entry_name)

            date_components = _collect_metadata_from_entry(
                file_obj=file_obj,
                updates=updates,
                entry=entry,
                entry_name=full_entry_name,
                filename=filename,
                date_components=date_components,
            )

    if require_match and not matched_entries:
        raise NoMatchError(filename, configs_checked)

    # Filter out updates that match existing values (no-ops)
    effective_updates = {k: v for k, v in updates.items() if v != getattr(file_obj, k)}
    return file_obj.with_updates(**effective_updates) if effective_updates else file_obj


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
