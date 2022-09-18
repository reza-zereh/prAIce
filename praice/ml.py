from abc import ABC, abstractmethod
from typing import Union

import flaml
import numpy as np
import pandas as pd
import supervised.automl as mljar
import tpot


class IEstimator(ABC):
    """Base Interface for AutoML classes."""

    __valid_params = []

    @abstractmethod
    def fit(
        self,
        X_train: Union[np.array, pd.DataFrame],
        y_train: Union[np.array, pd.DataFrame],
        settings: Union[dict, None] = None,
    ):
        """Train the model.

        Args:
            X_train (Union[np.array, pd.DataFrame]): Training data in shape (n, m).
            y_train (Union[np.array, pd.DataFrame]): Labels in shape (n, ).
            settings (Union[dict, None], optional): AutoML model params. Defaults to None.
        """
        pass

    @abstractmethod
    def predict(self, X_test: Union[np.array, pd.DataFrame]):
        """Predict label from features.

        Args:
            X_test (Union[np.array, pd.DataFrame]): Test data in shape (n, m).
        """
        pass

    @property
    @abstractmethod
    def valid_params(self):
        """Valid IEstimator parameters."""
        pass


class FlamlEstimator(IEstimator):
    """FLAML AutoML Library.
        https://microsoft.github.io/FLAML/docs/Getting-Started

    Args:
        task (str, optional): ML task. Valid tasks: regression, classification. Defaults to "regression".
    """

    __model__ = "flaml"

    def __init__(self, task: str = "regression"):
        estimator_list = [
            "lgbm",
            "rf",
            "xgboost",
            "extra_tree",
            "xgb_limitdepth",
            "catboost",
        ]
        if task == "regression":
            self.estimator = flaml.AutoML(
                task="regression",
                metric="rmse",
                estimator_list=estimator_list,
                verbose=2,
            )
        elif task == "classification":
            self.estimator = flaml.AutoML(
                task="classification",
                metric="accuracy",
                estimator_list=estimator_list,
                verbose=2,
            )
        else:
            raise ValueError(
                f"Expected 'task' to be 'regression' or 'classification', but got '{task}'."
            )
        self.__task__ = task
        self.__valid_params = list(self.estimator.get_params().keys())

    @property
    def valid_params(self):
        """Valid parameters for a FlamlEstimator.

        Returns:
            list: parameters names.
        """
        return self.__valid_params

    def fit(
        self,
        X_train: Union[np.array, pd.DataFrame],
        y_train: Union[np.array, pd.DataFrame],
        settings: Union[dict, None] = None,
    ):
        """Find and tune a model for a given task.

        Args:
            X_train (Union[np.array, pd.DataFrame]): Training data in shape (n, m).
            y_train (Union[np.array, pd.DataFrame]): Labels in shape (n, ).
            settings (Union[dict, None], optional): Flaml AutoML model params. Defaults to None.
                Valid params:
                - task
                - metric
                - estimator_list
                - time_budget
                - n_jobs
                - eval_method
                - split_ratio
                - n_splits
                - auto_augment
                - log_file_name
                - max_iter
                - sample
                - ensemble
                - log_type
                - model_history
                - log_training_metric
                - mem_thres
                - pred_time_limit
                - train_time_limit
                - verbose
                - retrain_full
                - split_type
                - hpo_method
                - learner_selector
                - starting_points
                - n_concurrent_trials
                - keep_search_state
                - early_stop
                - append_log
                - min_sample_size
                - use_ray
                - metric_constraints
                - fit_kwargs_by_estimator
                - custom_hp
                - skip_transform
        """
        settings = {} if settings is None else settings
        self.estimator.fit(X_train=X_train, y_train=y_train, **settings)

    def predict(self, X_test: Union[np.array, pd.DataFrame]):
        """Predict label from features.

        Args:
            X_test (Union[np.array, pd.DataFrame]): Test data in shape (n, m).

        Returns:
            An array-like of shape n * 1: each element is a predicted
                label for an instance.
        """
        assert (
            self.estimator.model is not None
        ), "No estimator is trained. Please run fit with enough budget."
        return self.estimator.predict(X_test)


class TpotEstimator(IEstimator):
    """TPOT AutoML Library.
        http://epistasislab.github.io/tpot/

    Args:
        task (str, optional): ML task. Valid tasks: regression, classification. Defaults to "regression".
    """

    __model__ = "tpot"

    def __init__(self, task: str = "regression"):
        if task == "regression":
            self.estimator = tpot.TPOTRegressor(
                generations=10,
                population_size=50,
                scoring="neg_root_mean_squared_error",
                verbosity=2,
            )
        elif task == "classification":
            self.estimator = tpot.TPOTClassifier(
                generations=10,
                population_size=50,
                scoring="accuracy",
                verbosity=2,
            )
        else:
            raise ValueError(
                f"Expected 'task' to be 'regression' or 'classification', but got '{task}'."
            )
        self.__task__ = task
        self.__valid_params = list(self.estimator.get_params().keys())

    @property
    def valid_params(self):
        """Valid parameters for a TpotEstimator.

        Returns:
            list: parameters names.
        """
        return self.__valid_params

    def fit(
        self,
        X_train: Union[np.array, pd.DataFrame],
        y_train: Union[np.array, pd.DataFrame],
        settings: Union[dict, None] = None,
    ):
        """Find and tune a model for a given task.

        Args:
            X_train (Union[np.array, pd.DataFrame]): Training data in shape (n, m).
            y_train (Union[np.array, pd.DataFrame]): Labels in shape (n, ).
            settings (Union[dict, None], optional): TPOT AutoML model params. Defaults to None.
                Valid params:
                - config_dict
                - crossover_rate
                - cv
                - disable_update_check
                - early_stop
                - generations
                - log_file
                - max_eval_time_mins
                - max_time_mins
                - memory
                - mutation_rate
                - n_jobs
                - offspring_size
                - periodic_checkpoint_folder
                - population_size
                - random_state
                - scoring
                - subsample
                - template
                - use_dask
                - verbosity
                - warm_start
        """
        if settings is not None and type(settings) == dict:
            self.estimator.set_params(**settings)
        self.estimator.fit(X_train, y_train)

    def predict(self, X_test: Union[np.array, pd.DataFrame]):
        """Predict label from features.

        Args:
            X_test (Union[np.array, pd.DataFrame]): Test data in shape (n, m).

        Returns:
            An array-like of shape n * 1: each element is a predicted
                label for an instance.
        """
        return self.estimator.predict(X_test)


class MljarEstimator(IEstimator):
    """mljar-supervised AutoML Library.
        https://supervised.mljar.com/

    Args:
        task (str, optional): ML task. Valid tasks: regression, classification. Defaults to "regression".
    """

    __model__ = "mljar"

    def __init__(self, task: str = "regression"):
        if task == "regression":
            self.estimator = mljar.AutoML(
                mode="Perform",
                ml_task="regression",
                eval_metric="rmse",
                total_time_limit=400,
            )
        elif task == "classification":
            self.estimator = mljar.AutoML(
                mode="Perform",
                ml_task="auto",
                eval_metric="accuracy",
                total_time_limit=400,
            )
        else:
            raise ValueError(
                f"Expected 'task' to be 'regression' or 'classification', but got '{task}'."
            )
        self.__task__ = task
        self.__valid_params = list(self.estimator.get_params().keys())

    @property
    def valid_params(self):
        """Valid parameters for a MljarEstimator.

        Returns:
            list: parameters names.
        """
        return self.__valid_params

    def fit(
        self,
        X_train: Union[np.array, pd.DataFrame],
        y_train: Union[np.array, pd.DataFrame],
        settings: Union[dict, None] = None,
    ):
        """Find and tune a model for a given task.

        Args:
            X_train (Union[np.array, pd.DataFrame]): Training data in shape (n, m).
            y_train (Union[np.array, pd.DataFrame]): Labels in shape (n, ).
            settings (Union[dict, None], optional): MLJar AutoML model params. Defaults to None.
                Valid params:
                - algorithms
                - boost_on_errors
                - eval_metric
                - explain_level
                - features_selection
                - golden_features
                - hill_climbing_steps
                - kmeans_features
                - max_single_prediction_time
                - mix_encoding
                - ml_task
                - mode
                - model_time_limit
                - n_jobs
                - optuna_init_params
                - optuna_time_budget
                - optuna_verbose
                - random_state
                - results_path
                - stack_models
                - start_random_models
                - top_models_to_improve
                - total_time_limit
                - train_ensemble
                - validation_strategy
                - verbose
        """
        if settings is not None and type(settings) == dict:
            self.estimator.set_params(**settings)
        self.estimator.fit(X_train, y_train)

    def predict(self, X_test: Union[np.array, pd.DataFrame]):
        """Predict label from features.

        Args:
            X_test (Union[np.array, pd.DataFrame]): Test data in shape (n, m).

        Returns:
            An array-like of shape n * 1: each element is a predicted
                label for an instance.
        """
        return self.estimator.predict(X_test)


ESTIMATORS = {
    "flaml": FlamlEstimator,
    "tpot": TpotEstimator,
    "mljar": MljarEstimator,
}


def learner(model: str, task: str = "regression") -> IEstimator:
    """Create a new learner.

    Args:
        model (str): Name of the estimator. Valid models: flaml, tpot, mljar.
        task (str, optional): ML task. Valid tasks: regression, classification. Defaults to "regression".

    Returns:
        IEstimator: A concrete instance of one of valid IEstimator classes.
    """
    assert (
        model in ESTIMATORS
    ), f"Expected 'model' to be one of {list(ESTIMATORS.keys())}, but got '{model}'."

    return ESTIMATORS[model](task=task)
