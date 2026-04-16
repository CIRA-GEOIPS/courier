"""ExecutionLog domain type for recording job dispatch results."""

import json
from dataclasses import dataclass


@dataclass(frozen=True)
class ExecutionLog:
    """Execution log DataClass.

    Attributes
    ----------
    return_code : int or None
        Process return code.
    stdout : str or None
        Standard output from the process.
    stderr : str or None
        Standard error from the process.
    hostname : str or None
        Hostname where execution occurred.
    """

    return_code: int | None = None
    stdout: str | None = None
    stderr: str | None = None
    hostname: str | None = None

    def __str__(self) -> str:
        """Convert ExecutionLog to JSON string."""
        return json.dumps(
            {
                "return_code": self.return_code,
                "stdout": self.stdout,
                "stderr": self.stderr,
                "hostname": self.hostname,
            },
        )

    @classmethod
    def from_string(cls, s: str) -> "ExecutionLog":
        """Initialize ExecutionLog from JSON string."""
        return cls(**json.loads(s))
