from abc import ABC, abstractmethod
from typing import Union

import flaml
import numpy as np
import pandas as pd
import supervised.automl as mljar
import tpot


class IEstimator(ABC):
    @abstractmethod
    def fit(
        self,
        X_train: Union[np.array, pd.DataFrame],
        y_train: Union[np.array, pd.DataFrame],
        settings: Union[dict, None] = None,
    ):
        pass

    @abstractmethod
    def predict(self, X_test: Union[np.array, pd.DataFrame]):
        pass


class FlamlEstimator(IEstimator):
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
                task="regression", metric="rmse", estimator_list=estimator_list
            )
        elif task == "classification":
            self.estimator = flaml.AutoML(
                task="classification",
                metric="accuracy",
                estimator_list=estimator_list,
            )
        else:
            raise ValueError(
                f"Expected 'task' to be 'regression' or 'classification', but got '{task}'."
            )

    def fit(
        self,
        X_train: Union[np.array, pd.DataFrame],
        y_train: Union[np.array, pd.DataFrame],
        settings: Union[dict, None] = None,
    ):
        settings = {} if settings is None else settings
        self.estimator.fit(X_train=X_train, y_train=y_train, **settings)

    def predict(self, X_test: Union[np.array, pd.DataFrame]):
        assert (
            self.estimator.model is not None
        ), "No estimator is trained. Please run fit with enough budget."
        return self.estimator.predict(X_test)


class TpotEstimator(IEstimator):
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

    def fit(
        self,
        X_train: Union[np.array, pd.DataFrame],
        y_train: Union[np.array, pd.DataFrame],
        settings: Union[dict, None] = None,
    ):
        if settings is not None and type(settings) == dict:
            self.estimator.set_params(**settings)
        self.estimator.fit(X_train, y_train)

    def predict(self, X_test: Union[np.array, pd.DataFrame]):
        return self.estimator.predict(X_test)


class MljarEstimator(IEstimator):
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

    def fit(
        self,
        X_train: Union[np.array, pd.DataFrame],
        y_train: Union[np.array, pd.DataFrame],
        settings: Union[dict, None] = None,
    ):
        if settings is not None and type(settings) == dict:
            self.estimator.set_params(**settings)
        self.estimator.fit(X_train, y_train)

    def predict(self, X_test: Union[np.array, pd.DataFrame]):
        return self.estimator.predict(X_test)
