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


_MAX_CONFIG_SIZE = 10 * 1024 * 1024  # 10 MB


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
    ConfigurationError
        If the file is too large or the parsed YAML is not a dict.
    """
    file_size = file_path.stat().st_size
    if file_size > _MAX_CONFIG_SIZE:
        raise ConfigurationError(
            f"Config file {file_path} is {file_size} bytes; "
            f"exceeds maximum of {_MAX_CONFIG_SIZE} bytes",
        )
    if file_path.suffix == ".json":
        with Path.open(file_path) as f:
            return json.load(f)  # type: ignore[no-any-return]
    elif file_path.suffix in [".yml", ".yaml"]:
        try:
            with Path.open(file_path) as f:
                loaded = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in {file_path}: {e}") from e
        if not isinstance(loaded, dict):
            raise ConfigurationError(
                f"Config file {file_path} must be a YAML mapping, "
                f"got {type(loaded).__name__}",
            )
        return loaded
    else:
        raise UnsupportedFileTypeError(file_path)


def load_config(file_path: Path) -> ServiceConfigModel:
    """Load a service config file (.json or .yml/.yaml).

    The ``apiVersion`` field is read first to select the correct schema
    version.  Currently only ``runcourier.dev/v1alpha1`` (canonical) and
    ``courier.dev/v1alpha1`` are supported.

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
