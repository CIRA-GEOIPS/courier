"""Config loading utilities for the CLI."""

import json
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from courier.errors import ConfigurationError
from courier.schema import ServiceConfigModel, get_model_for_version


class UnsupportedFileTypeError(ValueError):
    """Error for unsupported config file types."""

    def __init__(self, file_path: Path) -> None:
        self.file_path = file_path
        super().__init__(f"Unsupported file type: {file_path.suffix}")


def _load_raw(file_path: Path) -> dict[str, Any]:
    """Read a JSON or YAML file into a raw dict.

    Parameters
    ----------
    file_path : Path
        Path to the config file.

    Returns
    -------
    dict[str, Any]
        Raw parsed data.

    Raises
    ------
    UnsupportedFileTypeError
        If the file extension is not .json, .yml, or .yaml.
    """
    if file_path.suffix == ".json":
        with Path.open(file_path) as f:
            return json.load(f)  # type: ignore[no-any-return]
    elif file_path.suffix in [".yml", ".yaml"]:
        try:
            with Path.open(file_path) as f:
                return yaml.safe_load(f)  # type: ignore[no-any-return]
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in {file_path}: {e}") from e
    else:
        raise UnsupportedFileTypeError(file_path)


def load_config(file_path: Path) -> ServiceConfigModel:
    """Load a service config file (.json or .yml/.yaml).

    The ``apiVersion`` field is read first to select the correct schema
    version.  Currently only ``courier.dev/v1alpha1`` is supported.

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
    ValueError
        If the ``apiVersion`` is missing or unsupported.
    """
    raw = _load_raw(file_path)
    api_version = raw.get("apiVersion", "")
    model_cls = get_model_for_version(api_version)
    try:
        return model_cls(**raw)
    except ValidationError as e:
        raise ConfigurationError(str(e)) from e
