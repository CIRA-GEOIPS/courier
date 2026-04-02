"""Config loading utilities for the CLI."""

import json
from pathlib import Path

import yaml

from lazylemon.schema.service_config import ServiceConfigModel


class UnsupportedFileTypeError(ValueError):
    """Error for unsupported config file types."""

    def __init__(self, file_path: Path) -> None:
        self.file_path = file_path
        super().__init__(f"Unsupported file type: {file_path.suffix}")


def load_config(file_path: Path) -> ServiceConfigModel:
    """Load a service config file (.json or .yml/.yaml).

    Parameters
    ----------
    file_path : Path
        Path to the config file.

    Returns
    -------
    ServiceConfigModel
        Validated service configuration model.

    Raises
    ------
    UnsupportedFileTypeError
        If the file extension is not .json, .yml, or .yaml.
    """
    if file_path.suffix == ".json":
        with Path.open(file_path) as f:
            return ServiceConfigModel(**json.load(f))
    elif file_path.suffix in [".yml", ".yaml"]:
        with Path.open(file_path) as f:
            return ServiceConfigModel(**yaml.safe_load(f))
    else:
        raise UnsupportedFileTypeError(file_path)
