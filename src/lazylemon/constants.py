"""Typed constants and enums for lazylemon."""

from enum import Enum, auto

# Queue names
FILE_FOUND_QUEUE = "FilesFoundQueue"
JOB_READY_QUEUE = "JobReadyQueue"


class PluginRunState(Enum):
    """Enumeration of possible plugin states.

    Attributes
    ----------
    STOPPED : int
        Plugin is not running.
    STARTING : int
        Plugin is in the process of starting.
    RUNNING : int
        Plugin is running normally.
    STOPPING : int
        Plugin is in the process of stopping.
    FAILED : int
        Plugin has failed.
    RESTARTING : int
        Plugin is being restarted after failure.
    """

    STOPPED = auto()
    STARTING = auto()
    RUNNING = auto()
    STOPPING = auto()
    FAILED = auto()
    RESTARTING = auto()
