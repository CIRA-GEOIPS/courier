"""Regression tests for the shipped satellite metadata configs.

These configs are pure data, so a typo in a regex fails silently: files stop
matching, ``num_expected`` is never reached, and jobs simply never emit. The
GOES-16/18/19 configs previously carried ``M6C[01][1-6]``, which matches
C01-C06 and C11-C16 but silently drops C07-C10 -- and the fix was applied to
goes18 only, leaving goes16 and goes19 broken. These tests pin the behaviour
that actually matters: every declared channel matches, and ``num_expected``
agrees with the number of channels the pattern accepts.
"""

from __future__ import annotations

import re

import pytest

from courier.interfaces import data_monitor_configs
from courier.schema import DataMonitorConfig

# ABI flies 16 channels; each config declares num_expected: 16.
_ABI_CHANNELS = range(1, 17)

_ABI_CONFIGS = ["goes16_abi", "goes18_abi", "goes19_abi"]

# Real L1b product names, one per domain entry in the configs.
_ABI_DOMAIN_PRODUCTS = {
    "full-disk": "RadF",
    "conus": "RadC",
    "meso1": "RadM1",
    "meso2": "RadM2",
}


def _abi_filename(product: str, channel: int, satellite: str) -> str:
    """Build a realistic GOES ABI L1b filename for *channel*."""
    return (
        f"OR_ABI-L1b-{product}-M6C{channel:02d}_{satellite}"
        f"_s20241631200205_e20241631209513_c20241631209578.nc"
    )


def _load(name: str) -> DataMonitorConfig:
    return DataMonitorConfig(**data_monitor_configs.get_plugin(name))


def _satellite_code(config_name: str) -> str:
    """``goes18_abi`` -> ``G18`` (the token used in real filenames)."""
    return "G" + config_name.removeprefix("goes").split("_")[0]


@pytest.mark.parametrize("config_name", _ABI_CONFIGS)
@pytest.mark.parametrize("channel", _ABI_CHANNELS)
def test_abi_base_entry_matches_every_channel(
    config_name: str,
    channel: int,
) -> None:
    """The instrument-level entry must match all 16 ABI channels."""
    config = _load(config_name)
    entry = config.spec.file_metadata[config_name.replace("_abi", "_abi_l1b")]
    filename = _abi_filename("RadF", channel, _satellite_code(config_name))

    assert any(re.search(p, filename) for p in entry.match), (
        f"{config_name}: channel C{channel:02d} matches none of {entry.match}"
    )


@pytest.mark.parametrize("config_name", _ABI_CONFIGS)
@pytest.mark.parametrize(("entry_name", "product"), _ABI_DOMAIN_PRODUCTS.items())
@pytest.mark.parametrize("channel", _ABI_CHANNELS)
def test_abi_domain_entry_matches_every_channel(
    config_name: str,
    entry_name: str,
    product: str,
    channel: int,
) -> None:
    """Each domain entry must match all 16 channels for its own product."""
    config = _load(config_name)
    entry = config.spec.file_metadata[entry_name]
    filename = _abi_filename(product, channel, _satellite_code(config_name))

    assert any(re.search(p, filename) for p in entry.match), (
        f"{config_name}/{entry_name}: channel C{channel:02d} "
        f"matches none of {entry.match}"
    )


@pytest.mark.parametrize("config_name", _ABI_CONFIGS)
@pytest.mark.parametrize(("entry_name", "product"), _ABI_DOMAIN_PRODUCTS.items())
def test_abi_num_expected_matches_matchable_channel_count(
    config_name: str,
    entry_name: str,
    product: str,
) -> None:
    """``num_expected`` must equal the channels the pattern actually accepts.

    A mismatch means a job keyed on the count can never complete: the builder
    waits for files the pattern will never let through.
    """
    config = _load(config_name)
    entry = config.spec.file_metadata[entry_name]
    satellite = _satellite_code(config_name)

    matched = sum(
        any(
            re.search(p, _abi_filename(product, channel, satellite))
            for p in entry.match
        )
        for channel in _ABI_CHANNELS
    )

    assert matched == entry.num_expected, (
        f"{config_name}/{entry_name}: num_expected={entry.num_expected} but "
        f"only {matched} of {len(_ABI_CHANNELS)} channels match"
    )


@pytest.mark.parametrize("config_name", _ABI_CONFIGS)
@pytest.mark.parametrize("channel", _ABI_CHANNELS)
def test_abi_domain_entries_are_mutually_exclusive(
    config_name: str,
    channel: int,
) -> None:
    """A file for one domain must not match another domain's patterns.

    Overlapping domain patterns would tag one file with two domains and trip
    :class:`~courier.errors.MetadataConflictError` during enrichment.
    """
    config = _load(config_name)
    satellite = _satellite_code(config_name)

    for entry_name, product in _ABI_DOMAIN_PRODUCTS.items():
        filename = _abi_filename(product, channel, satellite)
        matching = {
            other
            for other in _ABI_DOMAIN_PRODUCTS
            if any(
                re.search(p, filename)
                for p in config.spec.file_metadata[other].match
            )
        }
        assert matching == {entry_name}, (
            f"{config_name}: {filename} matched domains {sorted(matching)}, "
            f"expected only {entry_name!r}"
        )
