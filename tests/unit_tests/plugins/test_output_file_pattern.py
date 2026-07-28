"""Unit tests for OutputFilePattern schema validation."""

from __future__ import annotations

import pytest
import pydantic

from courier.dispatchers._output_file_pattern import OutputFilePattern


# ─── Valid Construction ──────────────────────────────────────────────────────


class TestValidPattern:
    """Tests for valid OutputFilePattern construction."""

    def test_valid_pattern_with_file_group(self) -> None:
        """Pattern with 'file' named group constructs successfully."""
        entry = OutputFilePattern(pattern=r"(?P<file>/tmp/test\.nc)")
        assert entry.pattern == r"(?P<file>/tmp/test\.nc)"
        assert entry.source is None
        assert entry.metadata == {}

    def test_all_fields_optional_except_pattern(self) -> None:
        """Only pattern is required; all optional fields default to None."""
        entry = OutputFilePattern(pattern=r"(?P<file>.+)")
        assert entry.source is None
        assert entry.instrument is None
        assert entry.processing_stage is None
        assert entry.domain is None

    def test_default_metadata_empty_dict(self) -> None:
        """metadata defaults to an empty dict, not None."""
        entry = OutputFilePattern(pattern=r"(?P<file>.+)")
        assert entry.metadata == {}
        assert isinstance(entry.metadata, dict)

    def test_extra_named_groups_are_captured_as_metadata(self) -> None:
        """A pattern's non-``file`` groups must reach the emitted File.

        Asserting ``entry.pattern is not None`` only proved construction did
        not crash — it said nothing about whether the extra group is ever
        extracted, which is the entire reason for declaring one.
        """
        from unittest.mock import MagicMock

        from courier.dispatchers._output_scanner import _scan_and_emit_output_files

        entry = OutputFilePattern(
            pattern=r"(?P<file>/tmp/test\.nc)\|band=(?P<band>\d+)",
        )
        emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="/tmp/test.nc|band=13",
            stderr="",
            patterns=[entry],
            emit_file=emit,
        )

        emitted = emit.call_args[0][0]
        assert str(emitted.file) == "/tmp/test.nc"
        assert emitted.metadata["band"] == "13"

    def test_explicit_none_fields_remain_none(self) -> None:
        """Fields explicitly set to None should remain None (not coerced)."""
        entry = OutputFilePattern(pattern=r"(?P<file>.+)", source=None, domain=None)
        assert entry.source is None
        assert entry.domain is None

    def test_named_groups_matching_file_fields_populate_those_fields(
        self,
    ) -> None:
        """``source``/``instrument`` groups set File attributes, not metadata.

        This routing decision is what downstream filters depend on; a test
        that only checks the pattern constructs cannot see it go wrong.
        """
        from unittest.mock import MagicMock

        from courier.dispatchers._output_scanner import _scan_and_emit_output_files

        entry = OutputFilePattern(
            pattern=(
                r"/data/(?P<instrument>\w+)/(?P<source>\w+)/"
                r"(?P<file>[^/]+\.nc)"
            ),
        )
        emit = MagicMock()
        _scan_and_emit_output_files(
            stdout="/data/abi/goes18/product.nc",
            stderr="",
            patterns=[entry],
            emit_file=emit,
        )

        emitted = emit.call_args[0][0]
        assert emitted.instrument == "abi"
        assert emitted.source == "goes18"
        assert "instrument" not in emitted.metadata


# ─── Auto-Lowercase Validators ───────────────────────────────────────────────


class TestAutoLowercase:
    """Auto-lowercase validators for source, instrument, processing_stage."""

    def test_source_lowercased(self) -> None:
        """source is automatically lowercased."""
        entry = OutputFilePattern(pattern=r"(?P<file>.+)", source="GOES16")
        assert entry.source == "goes16"

    def test_instrument_lowercased(self) -> None:
        """instrument is automatically lowercased."""
        entry = OutputFilePattern(pattern=r"(?P<file>.+)", instrument="ABI")
        assert entry.instrument == "abi"

    def test_processing_stage_lowercased(self) -> None:
        """processing_stage is automatically lowercased."""
        entry = OutputFilePattern(pattern=r"(?P<file>.+)", processing_stage="L1B")
        assert entry.processing_stage == "l1b"

    def test_mixed_case_source_lowercased(self) -> None:
        """Mixed-case source is lowercased entirely."""
        entry = OutputFilePattern(pattern=r"(?P<file>.+)", source="Goes16")
        assert entry.source == "goes16"

    def test_already_lowercase_unchanged(self) -> None:
        """Already-lowercase values pass through unchanged."""
        entry = OutputFilePattern(pattern=r"(?P<file>.+)", source="goes16")
        assert entry.source == "goes16"


# ─── Auto-Uppercase Validator ────────────────────────────────────────────────


class TestAutoUppercase:
    """Auto-uppercase validator for domain."""

    def test_domain_uppercased(self) -> None:
        """domain is automatically uppercased."""
        entry = OutputFilePattern(pattern=r"(?P<file>.+)", domain="conus")
        assert entry.domain == "CONUS"

    def test_mixed_case_domain_uppercased(self) -> None:
        """Mixed-case domain is uppercased entirely."""
        entry = OutputFilePattern(pattern=r"(?P<file>.+)", domain="Full-Disk")
        assert entry.domain == "FULL-DISK"

    def test_already_uppercase_unchanged(self) -> None:
        """Already-uppercase domain passes through unchanged."""
        entry = OutputFilePattern(pattern=r"(?P<file>.+)", domain="MESOSCALE-1")
        assert entry.domain == "MESOSCALE-1"


# ─── Validation Errors ───────────────────────────────────────────────────────


class TestValidationErrors:
    """Validation error cases for OutputFilePattern."""

    def test_pattern_without_file_group_raises(self) -> None:
        """Pattern missing 'file' named group raises ValidationError."""
        with pytest.raises(pydantic.ValidationError) as exc_info:
            OutputFilePattern(pattern=r"(?P<notfile>.*)")
        assert "file" in str(exc_info.value).lower()

    def test_invalid_regex_syntax_raises(self) -> None:
        """Uncompilable regex pattern raises ValidationError."""
        with pytest.raises(pydantic.ValidationError):
            OutputFilePattern(pattern="[")

    def test_unclosed_parenthesis_raises(self) -> None:
        """Regex with unclosed group raises ValidationError."""
        with pytest.raises(pydantic.ValidationError):
            OutputFilePattern(pattern=r"(?P<file>")

    def test_empty_domain_string_raises(self) -> None:
        """Empty domain string raises ValidationError."""
        with pytest.raises(pydantic.ValidationError):
            OutputFilePattern(pattern=r"(?P<file>.+)", domain="")

    def test_whitespace_only_domain_raises(self) -> None:
        """Domain that strips to empty string raises (str_strip_whitespace=True)."""
        with pytest.raises(pydantic.ValidationError):
            OutputFilePattern(pattern=r"(?P<file>.+)", domain="   ")

    def test_pattern_without_named_groups_raises(self) -> None:
        """Pattern with no named groups at all raises because 'file' is missing."""
        with pytest.raises(pydantic.ValidationError) as exc_info:
            OutputFilePattern(pattern=r"/tmp/test\.nc")
        assert "file" in str(exc_info.value).lower()


# ─── Static Metadata ─────────────────────────────────────────────────────────


class TestStaticMetadata:
    """Tests for metadata static field handling."""

    def test_custom_metadata_preserved(self) -> None:
        """Explicitly provided metadata is stored as-is."""
        entry = OutputFilePattern(
            pattern=r"(?P<file>.+)",
            metadata={"band": "3", "channel": "visible"},
        )
        assert entry.metadata == {"band": "3", "channel": "visible"}

    def test_metadata_default_is_mutable_dict(self) -> None:
        """Default metadata is a plain dict (mutable)."""
        entry = OutputFilePattern(pattern=r"(?P<file>.+)")
        # The default factory creates a new dict each time
        entry.metadata["key"] = "value"
        assert entry.metadata["key"] == "value"
        # A second instance gets its own empty dict
        entry2 = OutputFilePattern(pattern=r"(?P<file>.+)")
        assert entry2.metadata == {}


# ─── Frozen Model ────────────────────────────────────────────────────────────


class TestFrozenModel:
    """Tests for frozen model behavior."""

    def test_model_is_frozen(self) -> None:
        """OutputFilePattern instances are immutable (frozen=True)."""
        entry = OutputFilePattern(pattern=r"(?P<file>.+)", source="goes16")
        with pytest.raises(pydantic.ValidationError):
            entry.source = "himawari9"

    def test_new_instance_from_existing(self) -> None:
        """model_copy can derive a modified instance."""
        entry = OutputFilePattern(pattern=r"(?P<file>.+)", source="goes16")
        updated = entry.model_copy(update={"source": "himawari9"})
        assert updated.source == "himawari9"
        assert entry.source == "goes16"
