import datetime
import os
from pathlib import PosixPath
from typing import List, Union

import yaml

from . import ml


def validate_instruments_config(config: dict):
    pass


def validate_learners_config(config: dict):
    assert (
        "learners" in config.keys()
    ), "learners config should contain 'learners' key."
    learners: List[dict] = config["learners"]
    for model in learners:
        assert (
            "model" in model.keys()
        ), "Each item below 'learners' should contain a 'model' key."
        estimator_ = ml.learner(model["model"])
        assert (
            "settings" in model.keys()
        ), "Each item below 'learners' should contain a 'settings' key."
        for setting in model["settings"]:
            assert set(setting.keys()).issubset(estimator_.valid_params), (
                f"Provided settings ({list(setting.keys())}) is not a subset of {model['model']}"
                f"valid parameters ({estimator_.valid_params})"
            )


def validate_ta_indicators_config(config: dict):
    pass


CONFIG_VALIDATORS = {
    "instruments": validate_instruments_config,
    "learners": validate_learners_config,
    "ta_indicators": validate_ta_indicators_config,
}


def load_yaml(
    fp: Union[str, PosixPath], validate: bool = False, config_type: str = None
):
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

    if validate:
        assert (
            config_type is not None
            or config_type not in CONFIG_VALIDATORS.keys()
        ), f"'config_type' should be one of {list(CONFIG_VALIDATORS.keys())} when 'validate' is True"
        CONFIG_VALIDATORS[config_type]()

    return config


def format_time(seconds: Union[int, float]):
    """Convert seconds to hh:mm:ss representation.

    Args:
        seconds (Union[int, float]): Time in seconds.

    Returns:
        str: Time in hh:mm:ss format.
    """
    return str(datetime.timedelta(seconds=int(seconds)))
