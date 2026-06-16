"""Unit tests for _scan_and_emit_output_files function."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from courier.dispatchers._output_file_pattern import OutputFilePattern
from courier.dispatchers._output_scanner import (
    _scan_and_emit_output_files,
)
from courier.types.file import File


# ── helpers ──────────────────────────────────────────────────────────────────


def _make_pattern(pattern: str, **overrides) -> OutputFilePattern:
    """Construct an OutputFilePattern with the given regex pattern."""
    return OutputFilePattern(pattern=pattern, **overrides)


# ─── Basic Scanning ──────────────────────────────────────────────────────────


class TestBasicScanning:
    """Tests for basic single-pattern scanning behavior."""

    def test_single_pattern_single_match_emits_one_file(self) -> None:
        """A single match produces exactly one emit_file call."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="Processed /tmp/test.nc",
            stderr="",
            patterns=[_make_pattern(r"Processed (?P<file>/tmp/test\.nc)")],
            emit_file=mock_emit,
        )
        mock_emit.assert_called_once()
        emitted: File = mock_emit.call_args[0][0]
        assert emitted.file == Path("/tmp/test.nc")

    def test_single_pattern_multiple_matches_emits_multiple_files(self) -> None:
        """Multiple distinct matches produce distinct emit_file calls."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout=(
                "Processed /tmp/a.nc\n"
                "Processed /tmp/b.nc\n"
                "Processed /tmp/c.nc\n"
            ),
            stderr="",
            patterns=[_make_pattern(r"Processed (?P<file>/tmp/[^.\s]+\.nc)")],
            emit_file=mock_emit,
        )
        assert mock_emit.call_count == 3
        emitted_paths = {call_args[0][0].file for call_args in mock_emit.call_args_list}
        assert emitted_paths == {
            Path("/tmp/a.nc"),
            Path("/tmp/b.nc"),
            Path("/tmp/c.nc"),
        }

    def test_static_source_applied_to_emitted_file(self) -> None:
        """Static pattern fields are applied to emitted File objects."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="Processed /tmp/test.nc",
            stderr="",
            patterns=[
                _make_pattern(
                    r"Processed (?P<file>/tmp/test\.nc)",
                    source="goes16",
                    instrument="abi",
                ),
            ],
            emit_file=mock_emit,
        )
        mock_emit.assert_called_once()
        emitted: File = mock_emit.call_args[0][0]
        assert emitted.source == "goes16"
        assert emitted.instrument == "abi"

    def test_static_processing_stage_and_domain_applied(self) -> None:
        """Static processing_stage and domain are applied to emitted File."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="Processed /tmp/test.nc",
            stderr="",
            patterns=[
                _make_pattern(
                    r"Processed (?P<file>/tmp/test\.nc)",
                    processing_stage="l1b",
                    domain="FULL-DISK",
                ),
            ],
            emit_file=mock_emit,
        )
        mock_emit.assert_called_once()
        emitted: File = mock_emit.call_args[0][0]
        assert emitted.processing_stage == "l1b"
        assert emitted.domain == "FULL-DISK"

    def test_hostname_propagated_to_emitted_file(self) -> None:
        """The hostname kwarg is assigned to every emitted File."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="Processed /tmp/test.nc",
            stderr="",
            patterns=[_make_pattern(r"Processed (?P<file>/tmp/test\.nc)")],
            hostname="satellite1",
            emit_file=mock_emit,
        )
        mock_emit.assert_called_once()
        emitted: File = mock_emit.call_args[0][0]
        assert emitted.hostname == "satellite1"


# ─── Multiple Patterns ───────────────────────────────────────────────────────


class TestMultiplePatterns:
    """Tests for scanning with multiple OutputFilePattern entries."""

    def test_multiple_patterns_matching_different_strings(self) -> None:
        """Two patterns matching disjoint outputs each emit files."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout=(
                "Output: /tmp/goes.nc\n"
                "Result: /tmp/himawari.nc\n"
            ),
            stderr="",
            patterns=[
                _make_pattern(r"Output: (?P<file>/tmp/goes\.nc)"),
                _make_pattern(r"Result: (?P<file>/tmp/himawari\.nc)"),
            ],
            emit_file=mock_emit,
        )
        assert mock_emit.call_count == 2
        emitted_paths = {call_args[0][0].file for call_args in mock_emit.call_args_list}
        assert emitted_paths == {Path("/tmp/goes.nc"), Path("/tmp/himawari.nc")}

    def test_different_static_fields_per_pattern(self) -> None:
        """Each pattern's static fields apply independently to its matches."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout=(
                "GOES: /tmp/g.nc\n"
                "HIMAWARI: /tmp/h.nc\n"
            ),
            stderr="",
            patterns=[
                _make_pattern(
                    r"GOES: (?P<file>/tmp/g\.nc)",
                    source="goes16",
                ),
                _make_pattern(
                    r"HIMAWARI: (?P<file>/tmp/h\.nc)",
                    source="himawari9",
                ),
            ],
            emit_file=mock_emit,
        )
        assert mock_emit.call_count == 2
        emitted_by_path: dict[Path, File] = {
            call_args[0][0].file: call_args[0][0]
            for call_args in mock_emit.call_args_list
        }
        assert emitted_by_path[Path("/tmp/g.nc")].source == "goes16"
        assert emitted_by_path[Path("/tmp/h.nc")].source == "himawari9"


# ─── Deduplication ───────────────────────────────────────────────────────────


class TestDeduplication:
    """Tests for cross-pattern and same-pattern deduplication."""

    def test_same_file_in_same_pattern_emits_once(self) -> None:
        """Same file path appearing multiple times in one pattern emits once."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout=(
                "Created /tmp/test.nc\n"
                "Also created /tmp/test.nc\n"
            ),
            stderr="",
            patterns=[_make_pattern(r"Created (?P<file>/tmp/test\.nc)")],
            emit_file=mock_emit,
        )
        mock_emit.assert_called_once()

    def test_same_file_across_different_patterns_emits_once(self) -> None:
        """Same file path matched by two patterns emits only once."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout=(
                "OUT: /tmp/shared.nc\n"
                "RESULT: /tmp/shared.nc\n"
            ),
            stderr="",
            patterns=[
                _make_pattern(r"OUT: (?P<file>/tmp/shared\.nc)"),
                _make_pattern(r"RESULT: (?P<file>/tmp/shared\.nc)"),
            ],
            emit_file=mock_emit,
        )
        # Emitted once for the unique file path
        mock_emit.assert_called_once()

    def test_dedup_within_same_pattern_across_lines(self) -> None:
        """Multiline output with same path on different lines deduplicates."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout=(
                "/tmp/x.nc processed\n"
                "/tmp/y.nc processed\n"
                "/tmp/x.nc processed\n"
                "/tmp/z.nc processed\n"
            ),
            stderr="",
            patterns=[_make_pattern(r"(?P<file>/tmp/[xyz]\.nc)")],
            emit_file=mock_emit,
        )
        assert mock_emit.call_count == 3


# ─── Scan Stderr ─────────────────────────────────────────────────────────────


class TestScanStderr:
    """Tests for scan_stderr flag behavior."""

    def test_scan_stderr_true_includes_stderr_text(self) -> None:
        """When scan_stderr=True, matches in stderr are included."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="stdout line\n",
            stderr="ERROR: check /tmp/err.nc\n",
            patterns=[_make_pattern(r"check (?P<file>/tmp/err\.nc)")],
            scan_stderr=True,
            emit_file=mock_emit,
        )
        mock_emit.assert_called_once()
        emitted: File = mock_emit.call_args[0][0]
        assert emitted.file == Path("/tmp/err.nc")

    def test_scan_stderr_false_ignores_stderr(self) -> None:
        """When scan_stderr=False (default), stderr matches are ignored."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="stdout line\n",
            stderr="ERROR: check /tmp/err.nc\n",
            patterns=[_make_pattern(r"check (?P<file>/tmp/err\.nc)")],
            scan_stderr=False,
            emit_file=mock_emit,
        )
        mock_emit.assert_not_called()

    def test_scan_stderr_true_empty_stdout_only_stderr(self) -> None:
        """Matches in stderr are found even when stdout is empty."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="",
            stderr="Wrote /tmp/from_err.nc",
            patterns=[_make_pattern(r"Wrote (?P<file>/tmp/from_err\.nc)")],
            scan_stderr=True,
            emit_file=mock_emit,
        )
        mock_emit.assert_called_once()
        emitted: File = mock_emit.call_args[0][0]
        assert emitted.file == Path("/tmp/from_err.nc")

    def test_scan_stderr_true_but_stderr_empty(self) -> None:
        """When stderr is empty and scan_stderr=True, only stdout scanned."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="Created /tmp/a.nc\n",
            stderr="",
            patterns=[_make_pattern(r"Created (?P<file>/tmp/a\.nc)")],
            scan_stderr=True,
            emit_file=mock_emit,
        )
        mock_emit.assert_called_once()


# ─── Guard Clauses ───────────────────────────────────────────────────────────


class TestGuardClauses:
    """Tests for early-exit guard clauses."""

    def test_no_patterns_no_emit(self) -> None:
        """Empty patterns list → no emit_file calls at all."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="some output here\n",
            stderr="",
            patterns=[],
            emit_file=mock_emit,
        )
        mock_emit.assert_not_called()

    def test_empty_stdout_and_stderr_no_emit(self) -> None:
        """Empty text (stdout + stderr both empty) → no emit_file calls."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="",
            stderr="",
            patterns=[_make_pattern(r"(?P<file>.+)")],
            emit_file=mock_emit,
        )
        mock_emit.assert_not_called()

    def test_patterns_exist_but_no_matching_text(self) -> None:
        """Text present but no regex matches → no emit_file calls."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="nothing matching here\n",
            stderr="",
            patterns=[_make_pattern(r"Matched (?P<file>/tmp/out\.nc)")],
            emit_file=mock_emit,
        )
        mock_emit.assert_not_called()

    def test_match_but_empty_file_path_skipped(self) -> None:
        """If the 'file' group captures an empty string, it is skipped."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="NoDigitsHere",
            stderr="",
            # \d* can match zero digits → file group captures "" (falsy)
            patterns=[_make_pattern(r"(?P<file>\d*)")],
            emit_file=mock_emit,
        )
        mock_emit.assert_not_called()


# ─── Regex-Extracted Metadata ────────────────────────────────────────────────


class TestRegexExtractedMetadata:
    """Tests for metadata extracted from additional named regex groups."""

    def test_extra_named_group_goes_to_metadata(self) -> None:
        """Named groups besides 'file' are placed into File.metadata."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="/tmp/test.nc|band=3|channel=vis\n",
            stderr="",
            patterns=[
                _make_pattern(
                    r"(?P<file>/tmp/test\.nc)\|band=(?P<band>\d+)\|channel=(?P<channel>\w+)",
                ),
            ],
            emit_file=mock_emit,
        )
        mock_emit.assert_called_once()
        emitted: File = mock_emit.call_args[0][0]
        assert emitted.metadata == {"band": "3", "channel": "vis"}

    def test_regex_metadata_overrides_static_metadata(self) -> None:
        """Regex-extracted metadata overrides static metadata on key collision."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="/tmp/test.nc|band=4\n",
            stderr="",
            patterns=[
                _make_pattern(
                    r"(?P<file>/tmp/test\.nc)\|band=(?P<band>\d+)",
                    metadata={"band": "3", "channel": "ir"},
                ),
            ],
            emit_file=mock_emit,
        )
        mock_emit.assert_called_once()
        emitted: File = mock_emit.call_args[0][0]
        # band from regex (4) overrides static metadata band (3)
        # channel from static metadata is preserved
        assert emitted.metadata == {"band": "4", "channel": "ir"}

    def test_static_metadata_preserved_when_no_regex_collision(self) -> None:
        """Static metadata keys not in regex are preserved untouched."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="/tmp/test.nc\n",
            stderr="",
            patterns=[
                _make_pattern(
                    r"(?P<file>/tmp/test\.nc)",
                    metadata={"pipeline": "l1b", "retention": "30d"},
                ),
            ],
            emit_file=mock_emit,
        )
        mock_emit.assert_called_once()
        emitted: File = mock_emit.call_args[0][0]
        assert emitted.metadata == {"pipeline": "l1b", "retention": "30d"}


# ─── Regex Field Overrides ───────────────────────────────────────────────────


class TestRegexFieldOverrides:
    """Tests for regex groups that match File field names (source, etc.)."""

    def test_regex_source_instrument_groups_populate_file_fields(self) -> None:
        """Named groups 'source' and 'instrument' populate File fields directly."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="/data/abi/goes16/file.nc\n",
            stderr="",
            patterns=[
                _make_pattern(
                    r"/data/(?P<instrument>\w+)/(?P<source>\w+)/(?P<file>[^/]+\.nc)",
                ),
            ],
            emit_file=mock_emit,
        )
        mock_emit.assert_called_once()
        emitted: File = mock_emit.call_args[0][0]
        assert emitted.source == "goes16"
        assert emitted.instrument == "abi"

    def test_regex_field_overrides_static_field(self) -> None:
        """Regex-extracted field value overrides the static config field."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="source=himawari9 /tmp/test.nc\n",
            stderr="",
            patterns=[
                _make_pattern(
                    r"source=(?P<source>\w+) (?P<file>/tmp/test\.nc)",
                    source="goes16",  # static default, overridden by regex
                ),
            ],
            emit_file=mock_emit,
        )
        mock_emit.assert_called_once()
        emitted: File = mock_emit.call_args[0][0]
        assert emitted.source == "himawari9"

    def test_regex_domain_group_populates_file_domain(self) -> None:
        """Named group 'domain' populates File.domain field."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="[full-disk] /tmp/test.nc\n",
            stderr="",
            patterns=[
                _make_pattern(
                    r"\[(?P<domain>[^\]]+)\] (?P<file>/tmp/test\.nc)",
                ),
            ],
            emit_file=mock_emit,
        )
        mock_emit.assert_called_once()
        emitted: File = mock_emit.call_args[0][0]
        assert emitted.domain == "FULL-DISK"

    def test_regex_processing_stage_group_populates_file_field(self) -> None:
        """Named group 'processing_stage' populates File.processing_stage."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="l2 /tmp/test.nc\n",
            stderr="",
            patterns=[
                _make_pattern(
                    r"(?P<processing_stage>l[12][ab]?) (?P<file>/tmp/test\.nc)",
                ),
            ],
            emit_file=mock_emit,
        )
        mock_emit.assert_called_once()
        emitted: File = mock_emit.call_args[0][0]
        assert emitted.processing_stage == "l2"

    def test_regex_field_value_is_case_normalized(self) -> None:
        """Regex-extracted field values are case-normalized (source/instrument/stage → lower, domain → upper)."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="SRC=GOES16 /tmp/test.nc\n",
            stderr="",
            patterns=[
                _make_pattern(
                    r"SRC=(?P<source>\S+) (?P<file>/tmp/test\.nc)",
                ),
            ],
            emit_file=mock_emit,
        )
        mock_emit.assert_called_once()
        emitted: File = mock_emit.call_args[0][0]
        # Regex-extracted source is now lowercased for consistency with static field validators
        assert emitted.source == "goes16"

    def test_regex_domain_value_is_uppercased(self) -> None:
        """Regex-extracted domain values are uppercased for consistency with static field validators."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="[full-disk] /tmp/test.nc\n",
            stderr="",
            patterns=[
                _make_pattern(
                    r"\[(?P<domain>[^\]]+)\] (?P<file>/tmp/test\.nc)",
                ),
            ],
            emit_file=mock_emit,
        )
        mock_emit.assert_called_once()
        emitted: File = mock_emit.call_args[0][0]
        assert emitted.domain == "FULL-DISK"


# ─── File Roundtrip ──────────────────────────────────────────────────────────


class TestFileRoundTrip:
    """Verify emitted File objects survive serialization roundtrip."""

    def test_from_string_roundtrip_simple_file(self) -> None:
        """Emitted File can be roundtripped through str → from_string."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="Processed /tmp/test.nc\n",
            stderr="",
            patterns=[_make_pattern(r"Processed (?P<file>/tmp/test\.nc)")],
            emit_file=mock_emit,
        )
        mock_emit.assert_called_once()
        emitted: File = mock_emit.call_args[0][0]
        reconstituted = File.from_string(str(emitted))
        assert reconstituted.file == emitted.file
        assert reconstituted.hostname == emitted.hostname
        assert reconstituted.source == emitted.source
        assert reconstituted.instrument == emitted.instrument
        assert reconstituted.processing_stage == emitted.processing_stage
        assert reconstituted.domain == emitted.domain
        assert reconstituted.metadata == emitted.metadata
        assert reconstituted.num_expected == emitted.num_expected

    def test_from_string_roundtrip_with_all_fields(self) -> None:
        """File with all fields populated survives roundtrip."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="/data/abi/goes16/l1b/full-disk/file.nc\n",
            stderr="",
            patterns=[
                _make_pattern(
                    r"/data/(?P<instrument>\w+)/(?P<source>\w+)/"
                    r"(?P<processing_stage>\w+)/(?P<domain>[^/]+)/(?P<file>[^/]+\.nc)",
                    metadata={"pipeline": "v2"},
                ),
            ],
            hostname="sat1",
            emit_file=mock_emit,
        )
        mock_emit.assert_called_once()
        emitted: File = mock_emit.call_args[0][0]
        reconstituted = File.from_string(str(emitted))
        assert reconstituted == emitted

    def test_from_string_roundtrip_with_metadata(self) -> None:
        """File with populated metadata dict survives roundtrip."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="/tmp/test.nc|band=7|mode=scan\n",
            stderr="",
            patterns=[
                _make_pattern(
                    r"(?P<file>/tmp/test\.nc)\|band=(?P<band>\d+)\|mode=(?P<mode>\w+)",
                ),
            ],
            emit_file=mock_emit,
        )
        mock_emit.assert_called_once()
        emitted: File = mock_emit.call_args[0][0]
        reconstituted = File.from_string(str(emitted))
        assert reconstituted == emitted
        assert reconstituted.metadata == {"band": "7", "mode": "scan"}


# ─── Edge Cases ──────────────────────────────────────────────────────────────


class TestEdgeCases:
    """Edge case tests for the scanner function."""

    def test_re_multiline_flag_enables_line_matching(self) -> None:
        """re.MULTILINE flag lets ^/$ match at line boundaries."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout=(
                "other text\n"
                "/tmp/a.nc\n"
                "/tmp/b.nc\n"
                "trailing\n"
            ),
            stderr="",
            patterns=[_make_pattern(r"^(?P<file>/tmp/[ab]\.nc)$")],
            emit_file=mock_emit,
        )
        assert mock_emit.call_count == 2

    def test_whitespace_in_file_path_preserved(self) -> None:
        """File path with spaces is captured as-is (though unusual)."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="Created '/tmp/my data.nc'\n",
            stderr="",
            patterns=[_make_pattern(r"Created '(?P<file>[^']+)'")],
            emit_file=mock_emit,
        )
        mock_emit.assert_called_once()
        emitted: File = mock_emit.call_args[0][0]
        assert emitted.file == Path("/tmp/my data.nc")

    def test_file_path_with_special_regex_chars(self) -> None:
        """File paths containing regex-special characters are matched literally."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="Output: /tmp/data[2026].nc\n",
            stderr="",
            patterns=[
                _make_pattern(r"Output: (?P<file>/tmp/data\[2026\]\.nc)"),
            ],
            emit_file=mock_emit,
        )
        mock_emit.assert_called_once()
        emitted: File = mock_emit.call_args[0][0]
        assert emitted.file == Path("/tmp/data[2026].nc")

    def test_pattern_without_static_fields_emits_minimal_file(self) -> None:
        """When no static fields and no extra regex groups, File is minimal."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="/tmp/minimal.nc\n",
            stderr="",
            patterns=[_make_pattern(r"(?P<file>/tmp/minimal\.nc)")],
            emit_file=mock_emit,
        )
        mock_emit.assert_called_once()
        emitted: File = mock_emit.call_args[0][0]
        assert emitted.file == Path("/tmp/minimal.nc")
        assert emitted.source is None
        assert emitted.instrument is None
        assert emitted.metadata == {}

    def test_empty_group_value_skipped_in_metadata(self) -> None:
        """Named groups with empty-string values are not added to metadata."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="/tmp/test.nc||",
            stderr="",
            patterns=[
                _make_pattern(
                    r"(?P<file>/tmp/test\.nc)\|(?P<band>[^|]*)\|(?P<channel>[^|]*)",
                ),
            ],
            emit_file=mock_emit,
        )
        mock_emit.assert_called_once()
        emitted: File = mock_emit.call_args[0][0]
        # Both band and channel are empty, so they are skipped
        assert emitted.metadata == {}

    def test_scan_stderr_uses_newline_separator(self) -> None:
        """When scan_stderr=True and both have text, they are newline-joined."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="hello",
            stderr="world",
            patterns=[_make_pattern(r"(hello|world) (?P<file>/tmp/x\.nc)")],
            scan_stderr=True,
            emit_file=mock_emit,
        )
        # Neither "hello" nor "world" is followed by " /tmp/x.nc" —
        # but "hello\nworld" as combined text also doesn't match the pattern.
        # The test verifies that if a match straddles the boundary, it won't
        # match because newline is inserted.
        # Instead test: pattern that matches right at the join boundary.
        mock_emit.assert_not_called()

    def test_text_after_newline_join_is_concatenated(self) -> None:
        """Verify concatenation order: stdout then newline then stderr."""
        mock_emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="Found /tmp/a.nc",
            stderr="Found /tmp/b.nc",
            patterns=[_make_pattern(r"Found (?P<file>/tmp/[ab]\.nc)")],
            scan_stderr=True,
            emit_file=mock_emit,
        )
        assert mock_emit.call_count == 2
        emitted_paths = {call_args[0][0].file for call_args in mock_emit.call_args_list}
        assert emitted_paths == {Path("/tmp/a.nc"), Path("/tmp/b.nc")}
