"""Implementation for the bash_payload payload class."""

from typing import ClassVar

from courier.interfaces.payloads import PayloadConfig
from courier.plugins.payloads.shell_payload import ShellPayload


# classes for courier init discovery
class BashPayloadConfig(PayloadConfig):  # noqa: D101
    pass


class BashPayload(ShellPayload):
    """Payload class for Bash script execution."""

    interface: ClassVar[str] = "payloads"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "bash_payload"
    default_binary: ClassVar[str] = "bash"
    config_class: ClassVar[type[PayloadConfig]] = BashPayloadConfig
