"""Pydantic models for the controller_configs interface.

Used for validation of controller_config plugins.
"""

from datetime import datetime
from typing import ClassVar, Dict, List, Optional, Union

from pydantic import Field, model_validator

from geoips.pydantic.bases import FrozenModel, PluginModel, PythonIdentifier
from geoips.pydantic.workflows import WorkflowStepDefinitionModel


class MonitorConfig(FrozenModel):
    """Configuration for a specific satellite monitor."""

    name: PythonIdentifier = Field(
        ...,
        description=(
            "Name of the monitor_config plugin, must be a valid Python identifier."
        ),
    )
    arguments: Dict[str, List[str]] = Field(
        ...,
        description=(
            "Dictionary of monitor-specific arguments, currently expects 'obs_area' as "
            "a list of strings."
        ),
    )


class DispatcherArgs(FrozenModel):
    """Required and optional arguments for a dispatcher plugin."""

    display_name: Optional[str] = Field(
        None, description="Optional name which can be used to name the process running."
    )
    template: str = Field(
        ..., description="The name of the template spawn your process or job."
    )
    steps: List[WorkflowStepDefinitionModel] = Field(
        ...,
        description=("A list of steps that are needed to create your product(s)."),
    )


class PoolDispatcherArgs(DispatcherArgs):
    """Required and optional arguments for a pool dispatcher plugin."""

    core_count: int = Field(
        ..., description="The number of cores used for your processing."
    )


class SerialDispatcherArgs(DispatcherArgs):
    """Required and optional arguments for a serial dispatcher plugin."""

    pass


class SlurmDispatcherArgs(DispatcherArgs):
    """Required and optional arguments for a slurm dispatcher plugin."""

    pass


class DataMonitorArgs(FrozenModel):
    """Required and optional arguments for the data_monitor plugin."""

    start_time: Optional[datetime] = Field(
        None,
        description=(
            "The start datetime to begin monitoring at. Optional. If provided, it must "
            "be a valid isoformat utc datetime string, which will be converted into a "
            "datetime object. If no end datetime is provided, then the data monitor "
            "will search continuously from this time on."
        ),
    )
    end_time: Optional[datetime] = Field(
        None,
        description=(
            "The end datetime to stop monitoring at. Optional. If provided, it must "
            "be a valid isoformat utc datetime string, which will be converted into a "
            "datetime object."
        ),
    )
    monitor_configs: List[MonitorConfig] = Field(
        ...,
        description=(
            "A list of monitor_config plugins used to direct your data_monitor. "
        ),
    )


class QuerierArgs(FrozenModel):
    """Required and optional arguments for a querier plugin."""

    source_names: List[str] = Field(
        ...,
        description=(
            "List of source names needed for this dispatcher. Corresponds to the source"
            " in which the data comes from. Essentially, this acts as a filter to a "
            "larger set of data files for the dispatcher. For example, if you had files"
            " sourced from 'abi', 'seviri', and 'ahi', you could filter what files are "
            "needed by specifying one or more of those source names."
        ),
    )


class Querier(FrozenModel):
    """Configuration for a querier plugin."""

    name: PythonIdentifier = Field(
        ..., description="The name of the queier plugin to use."
    )
    arguments: QuerierArgs = (
        Field(
            ...,
            description=(
                "Arguments used to inform your querier how to search for information."
            ),
        ),
    )


class Dispatcher(FrozenModel):
    """Configuration for a dispatcher plugin."""

    name: PythonIdentifier = Field(
        ..., description="The name of the dispatcher plugin to use."
    )
    arguments: Union[PoolDispatcherArgs, SerialDispatcherArgs, SlurmDispatcherArgs] = (
        Field(
            ...,
            description=(
                "Arguments used to inform your dispatcher how to spawn processes."
            ),
        ),
    )

    # @model_validator(mode="before")
    # def _validate_arguments(cls, values):
    #     """Validate that the set of args matches the dispatcher's arg format.

    #     Raises
    #     ------
    #     pydantic.ValidationError:
    #         - Raised if the set of arguments provided does not match the argument set
    #           specified by the dispatcher chosen.
    #     KeyError:
    #         - Raised if either 'name' or 'arguments' aren't provided for a dispatcher
    #     """
    #     arg_map = {
    #         "pool": PoolDispatcherArgs,
    #         "serial": SerialDispatcherArgs,
    #         "slurm": SlurmDispatcherArgs,
    #     }
    #     try:
    #         arg_map[values["name"]](**values["arguments"])
    #     except KeyError:
    #         raise KeyError(
    #             "Error: dispatcher object was missing one or more of the following keys"
    #             ": ['name', 'arguments']. Please add these key value pairs before "
    #             "continuing."
    #         )
    #     return values


class DriverArgs(FrozenModel):
    """Required and optional arguments for the driver plugin."""

    cadence: str = Field(
        ...,
        description=(
            "How often a job should be dispatched. Formatted using dateparser's "
            "natural language format. See "
            "https://dateparser.readthedocs.io/en/latest/index.html for more info."
        ),
    )
    offset: Optional[str] = Field(
        "0 min",
        description=(
            "An optional time offset from the top of the hour to dispatch a process at."
            " Formatted using dateparser's natural language format. See "
            "https://dateparser.readthedocs.io/en/latest/index.html for more info."
        ),
    )
    dispatcher: Dispatcher = Field(
        ...,
        description=(
            "The dispatcher plugin used to spawn processes/jobs via this driver."
        ),
    )
    querier: Querier = Field(
        ...,
        description=(
            "The querier plugin used to query an information storage system via this "
            "driver."
        ),
    )


class Driver(FrozenModel):
    """Represents the configuration for a driver plugin in YAML."""

    name: PythonIdentifier = Field(
        ...,
        description=("Name of the driver plugin, must be a valid Python identifier."),
    )
    arguments: DriverArgs = Field(
        ...,
        description=(
            "Dictionary of plugin arguments, must include 'monitor_configs' and "
            "optionally can include 'start_time', 'end_time'."
        ),
    )


class DataMonitor(FrozenModel):
    """Represents the data monitoring configuration."""

    name: PythonIdentifier = Field(
        ...,
        description=(
            "Name of the data_monitor plugin, must be a valid Python identifier."
        ),
    )
    arguments: DataMonitorArgs = Field(
        ...,
        description=(
            "Dictionary of plugin arguments, must include 'monitor_configs' and "
            "optionally can include 'start_time', 'end_time'."
        ),
    )


class ControllerConfigSpec(FrozenModel):
    """Defines the specification for the controller_config plugin."""

    data_monitors: List[DataMonitor] = Field(
        ..., description="List of data_monitor plugins to use with your controller."
    )
    drivers: List[Driver] = Field(
        ..., description="List of driver plugins to use with your controller."
    )


class ControllerConfigPlugin(PluginModel):
    """Represents the controller_config plugin configuration."""

    apiVersion: ClassVar[str] = "geoips_driver/v1"

    spec: ControllerConfigSpec = Field(
        ..., description="Specification for the controller_config plugin."
    )
