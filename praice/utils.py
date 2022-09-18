import datetime
import os
from pathlib import PosixPath
from typing import List, Union

import yaml

from . import ml


def validate_instruments_config(config: dict):
    pass


def validate_learners_config(config: dict):
    """Validate learners yaml config file.

    Args:
        config (dict): Loaded yaml.

    Raises:
        AssertionError: If something is wrong in config file.

    Returns:
        bool: Returns True if the config file is in proper format.
    """
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
                f"Provided settings ({list(setting.keys())}) is not a subset of {model['model']} "
                f"valid parameters ({estimator_.valid_params})"
            )
    return True


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
        validate (bool, optional): If True, validate config file to make sure it
            has the correct format. Defaults to False.
        config_type (str, optional): Type of yaml config file. Valid config types:
            instruments, learners, ta_indicators. Defaults to None.

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
            config_type is not None and config_type in CONFIG_VALIDATORS.keys()
        ), f"'config_type' should be one of {list(CONFIG_VALIDATORS.keys())} when 'validate' is True"
        CONFIG_VALIDATORS[config_type](config=config)

    return config


def format_time(seconds: Union[int, float]):
    """Convert seconds to hh:mm:ss representation.

    Args:
        seconds (Union[int, float]): Time in seconds.

    Returns:
        str: Time in hh:mm:ss format.
    """
    return str(datetime.timedelta(seconds=int(seconds)))
