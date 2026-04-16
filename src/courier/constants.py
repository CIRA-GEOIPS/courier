"""Typed constants and enums for courier."""

from enum import Enum, StrEnum, auto


class QueueName(StrEnum):
    """Queue names used for inter-plugin messaging."""

    FILE_FOUND = "FilesFoundQueue"
    JOB_READY = "JobReadyQueue"
    DISPATCHER = "DispatcherQueue"


# Module-level aliases for backward compatibility
FILE_FOUND_QUEUE = QueueName.FILE_FOUND
JOB_READY_QUEUE = QueueName.JOB_READY
DISPATCHER_QUEUE = QueueName.DISPATCHER


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
