"""Pydantic models for the monitor_configs interface.

Used for validation of monitor_config plugins.
"""

from pathlib import Path
from typing import ClassVar

from geoips.pydantic.bases import (
    FrozenModel,
    PermissiveFrozenModel,
    PluginModel,
)
from pydantic import Field, model_validator


class ObservationArea(FrozenModel):
    """Configuration for an ObservationArea model."""

    parent_dir: Path = Field(..., description="Path template for data storage.")
    patterns: list[str] = Field(..., description="List of filename patterns to match.")
    num_expected: int = Field(
        ...,
        description="Expected number of files per time step.",
    )


class MonitorConfigSpec(PermissiveFrozenModel):
    """Defines the specification for the monitor_config plugin."""

    obs_areas: dict[str, ObservationArea] = Field(
        ...,
        description=(
            "A dictionary of observation areas containing information for data "
            "monitoring."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _cast_input_to_obs_areas(cls, values):
        """Dynamically cast input values to an obs_areas model.

        Parameters
        ----------
        values: dict[Any]
            - The values provided to this model.

        Raises
        ------
        TypeError:
            - Raised if values is not an instance of a dictionary
        """
        if not isinstance(values, dict):  # Ensure it's a dict
            raise TypeError("Input must be a dictionary")
        if "obs_areas" not in values:
            return {"obs_areas": values}
        return values


class MonitorConfigPlugin(PluginModel):
    """Represents the monitor_config plugin configuration."""

    apiVersion: ClassVar[str] = "geoips_driver/v1"

    spec: MonitorConfigSpec = Field(
        ...,
        description="Specification for the monitor_config plugin.",
    )
