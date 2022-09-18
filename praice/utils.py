import datetime
import os
from pathlib import PosixPath
from typing import Union

import yaml


def load_yaml(fp: Union[str, PosixPath]):
    """Load a yaml file from disk.

    Args:
        fp (Union[str, PosixPath]): File path to yaml file.

    Raises:
        FileNotFoundError: If file path is incorrect.

    Returns:
        dict: Loaded yaml.
    """
    if not os.path.exists(fp):
        raise FileNotFoundError(f"There is no such a file named {fp}")
    with open(str(fp), "r") as f:
        config = yaml.safe_load(f)
    return config


def format_time(seconds: Union[int, float]):
    """Convert seconds to hh:mm:ss representation.

    Args:
        seconds (Union[int, float]): Time in seconds.

    Returns:
        str: Time in hh:mm:ss format.
    """
    return str(datetime.timedelta(seconds=int(seconds)))
