"""Pydantic models for the monitor_configs interface.

Used for validation of monitor_config plugins.
"""

from pathlib import Path
from typing import Any, ClassVar

from geoips.pydantic_models.v1.bases import (  # type: ignore
    FrozenModel,
    PermissiveFrozenModel,
    PluginModel,
)
from pydantic import Field, model_validator


class NotADictionaryError(TypeError):
    """Raised when an object is expected to be a dictionary but is not."""

    def __init__(self, obj: str | None = None, message: str | None = None) -> None:
        """Initialize NotADictionaryError.

        Parameters
        ----------
        obj : str or None, optional
            Object that was expected to be a dictionary.
        message : str or None, optional
            Custom error message. If None, generates default message.
        """
        if message is None:
            message = f"Expected a dictionary, but got {type(obj).__name__}: {obj}"
        super().__init__(message)


class ObservationArea(FrozenModel):
    """Configuration for an ObservationArea model.

    Attributes
    ----------
    parent_dir : Path
        Path template for data storage.
    patterns : list of str
        List of filename patterns to match.
    num_expected : int
        Expected number of files per time step.
    """

    parent_dir: Path = Field(..., description="Path template for data storage.")
    patterns: list[str] = Field(..., description="List of filename patterns to match.")
    num_expected: int = Field(
        ...,
        description="Expected number of files per time step.",
    )


class MonitorConfigSpec(PermissiveFrozenModel):
    """Specification for the monitor_config plugin.

    Attributes
    ----------
    obs_areas : dict of str to ObservationArea
        Dictionary of observation areas containing information for data monitoring.
    """

    obs_areas: dict[str, ObservationArea] = Field(
        ...,
        description=(
            "A dictionary of observation areas containing information for data "
            "monitoring."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _cast_input_to_obs_areas(
        cls,
        values: dict[Any, Any],
    ) -> dict[str, Any]:
        """Cast input values to obs_areas model format.

        Parameters
        ----------
        values : dict
            Values provided to this model.

        Returns
        -------
        dict
            Dictionary with 'obs_areas' key containing validated values.

        Raises
        ------
        NotADictionaryError
            If values is not a dictionary instance.
        """
        if not isinstance(values, dict):  # Ensure it's a dict
            raise NotADictionaryError(values)
        if "obs_areas" not in values:
            return {"obs_areas": values}
        return values


class MonitorConfigPlugin(PluginModel):
    """Monitor_config plugin configuration.

    Attributes
    ----------
    apiVersion : str
        API version string (class variable).
    spec : MonitorConfigSpec
        Specification for the monitor_config plugin.
    """

    apiVersion: ClassVar[str] = "geoips_driver/v1"  # noqa: N815

    spec: MonitorConfigSpec = Field(
        ...,
        description="Specification for the monitor_config plugin.",
    )
