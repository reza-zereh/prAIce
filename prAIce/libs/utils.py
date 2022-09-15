import datetime
import os
from pathlib import PosixPath
from typing import Union

import yaml


def load_yaml(fp: Union[str, PosixPath]):
    if not os.path.exists(fp):
        raise FileNotFoundError(f"There is no such a file named {fp}")
    with open(str(fp), "r") as f:
        config = yaml.safe_load(f)
    return config


def format_time(seconds: Union[int, float]):
    return str(datetime.timedelta(seconds=int(seconds)))
