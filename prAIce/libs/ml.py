from abc import ABC, abstractmethod

import flaml
import tpot


class IEstimator(ABC):
    @abstractmethod
    def fit(self, X_train, y_train):
        pass

    @abstractmethod
    def predict(self, X_test):
        pass


class FlamlEstimator(IEstimator):
    __model__ = "flaml"

    def __init__(self):
        self.estimator = flaml.AutoML()

    def fit(self, X_train, y_train, settings):
        self.estimator.fit(X_train=X_train, y_train=y_train, **settings)

    def predict(self, X_test):
        assert (
            self.estimator.model is not None
        ), "No estimator is trained. Please run fit with enough budget."
        return self.estimator.predict(X_test)
