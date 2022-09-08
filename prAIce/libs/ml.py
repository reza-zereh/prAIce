from abc import ABC, abstractmethod
from typing import Union

import flaml
import tpot


class IEstimator(ABC):
    @abstractmethod
    def fit(self, X_train, y_train, settings: Union[dict, None] = None):
        pass

    @abstractmethod
    def predict(self, X_test):
        pass


class FlamlEstimator(IEstimator):
    __model__ = "flaml"

    def __init__(self):
        self.estimator = flaml.AutoML()

    def fit(self, X_train, y_train, settings: Union[dict, None] = None):
        settings = {} if settings is None else settings
        self.estimator.fit(X_train=X_train, y_train=y_train, **settings)

    def predict(self, X_test):
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
                scoring="neg_mean_squared_error",
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

    def fit(self, X_train, y_train, settings: Union[dict, None] = None):
        if settings is not None and type(settings) == dict:
            self.estimator.set_params(**settings)

        self.estimator.fit(X_train, y_train)

    def predict(self, X_test):
        return self.estimator.predict(X_test)
