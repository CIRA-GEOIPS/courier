"""Output file scanner for bash dispatcher plugin output.

Scans stdout (and optionally stderr) text with regex patterns to discover
output file paths emitted by a bash script, constructs :class:`File` objects
with metadata, and forwards them via an ``emit_file`` callback.
"""

from __future__ import annotations

import re
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING

from courier.types.file import File

if TYPE_CHECKING:
    from collections.abc import Callable

    from courier.plugins.classes.dispatchers._output_file_pattern import (
        OutputFilePattern,
    )

# ── Recognised File field names from OutputFilePattern ──────────────────
# Named regex groups matching these keys are applied as field overrides
# rather than being placed in metadata.
#
# ``timestamp`` was added to support multi-stage pipelines where a downstream
# ``filter_and_group`` builder uses ``time_grouping`` to pair files by
# observation time.  Without a timestamp on the re-emitted File object,
# ``time_grouping`` silently drops the file (``get_job_ids_from_file`` returns
# ``[]`` when ``timestamp is None``), breaking pairing stages such as the
# GOES-18 3D cloud pipeline where a CLAVR-x file and its CWC counterpart
# must arrive in the same time bucket.  The regex pattern captures a compact
# ``YYYYmmddTHHMM`` string via a ``(?P<timestamp>...)`` named group and the
# transform below parses it into a :class:`datetime`.
_FILE_FIELD_KEYS: frozenset[str] = frozenset(
    {"source", "instrument", "processing_stage", "domain", "timestamp"},
)


# ── Case transforms for regex-extracted File field values ──────────────
# Regex-extracted values receive the same case normalisation that
# OutputFilePattern's static field validators apply.
def _parse_compact_timestamp(raw: str) -> datetime | None:
    """Parse a ``YYYYmmddTHHMM`` string into a timezone-naive :class:`datetime`.

    Output files produced by cwc_prof, unetcomp, and the L1b preprocessor
    follow the naming convention ``{SENSOR}_{PRODUCT}_{YYYYmmdd}T{HHMM}Z[_v{V}].h5``.
    The ``(?P<timestamp>...)`` named group in an output_files pattern captures
    the compact ``YYYYmmddTHHMM`` token; this transform converts it so the
    re-emitted :class:`File` carries a real timestamp for ``time_grouping``.
    """
    try:
        return datetime.strptime(raw, "%Y%m%dT%H%M")
    except (ValueError, TypeError):
        return None


_FIELD_TRANSFORMS: dict[str, Callable[[str], str | datetime | None]] = {
    "source": str.lower,
    "instrument": str.lower,
    "processing_stage": str.lower,
    "domain": str.upper,
    "timestamp": _parse_compact_timestamp,
}


def _collect_updates_from_match(
    entry: OutputFilePattern,
    match_obj: re.Match[str],
) -> dict:
    """Build a dict of :class:`File` field updates from one regex match.

    Static metadata fields from *entry* are included first; any named
    regex groups matching File field keys override them.  All other named
    groups are placed into ``metadata``, overriding static entry metadata
    on key collision.
    """
    updates: dict = {}
    extracted_metadata: dict = {}

    # ── static fields from the pattern entry ────────────────────────────
    if entry.source is not None:
        updates["source"] = entry.source
    if entry.instrument is not None:
        updates["instrument"] = entry.instrument
    if entry.processing_stage is not None:
        updates["processing_stage"] = entry.processing_stage
    if entry.domain is not None:
        updates["domain"] = entry.domain

    # ── regex-extracted groups ──────────────────────────────────────────
    for group_name, group_value in match_obj.groupdict().items():
        if not group_value:
            continue
        if group_name == "file":
            continue
        if group_name in _FILE_FIELD_KEYS:
            transform = _FIELD_TRANSFORMS.get(group_name)
            updates[group_name] = transform(group_value) if transform else group_value
        else:
            extracted_metadata[group_name] = group_value

    # Merge: extracted overrides static on key collision
    merged_metadata = dict(entry.metadata)
    merged_metadata.update(extracted_metadata)
    updates["metadata"] = merged_metadata

    return updates


@lru_cache(maxsize=128)
def _compile_pattern(pattern: str) -> re.Pattern[str]:
    """Return a compiled regex pattern, cached to avoid repeated compilation."""
    return re.compile(pattern, flags=re.MULTILINE)


def _scan_and_emit_output_files(  # noqa: PLR0913
    *,
    stdout: str,
    stderr: str,
    patterns: list[OutputFilePattern],
    scan_stderr: bool = False,
    hostname: str = "",
    emit_file: Callable[[File], None],
) -> None:
    r"""Scan dispatcher output text for file paths and emit :class:`File`\s.

    Parameters
    ----------
    stdout : str
        Standard output text from the bash script execution.
    stderr : str
        Standard error text from the bash script execution.
    patterns : list[OutputFilePattern]
        Validated patterns to search with.
    scan_stderr : bool
        When ``True``, also scan stderr (appended to stdout).
    hostname : str
        Hostname assigned to every discovered :class:`File`.
    emit_file : Callable[[File], None]
        Side-effect callback invoked once per unique discovered file.
    """
    # ── guard: nothing to scan ──────────────────────────────────────────
    if not patterns:
        return

    text = stdout
    if scan_stderr and stderr:
        text = f"{text}\n{stderr}"

    if not text:
        return

    # ── deduplicate per scan-call ───────────────────────────────────────
    seen: set[str] = set()

    for entry in patterns:
        compiled = _compile_pattern(entry.pattern)

        for match_obj in compiled.finditer(text):
            file_path = match_obj.group("file")
            if not file_path:
                continue
            if file_path in seen:
                continue
            seen.add(file_path)

            # Build base File, collect updates, apply atomically
            file_obj = File(file=Path(file_path), hostname=hostname)
            updates = _collect_updates_from_match(entry, match_obj)
            file_obj = file_obj.with_updates(**updates)
            emit_file(file_obj)
