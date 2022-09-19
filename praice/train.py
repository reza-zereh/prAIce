import datetime
import time
from pathlib import PosixPath
from typing import Union

import mlflow
from sklearn import metrics

from . import ml, paths, utils
from .tickers import Instrument


class Trainer:
    def __init__(
        self,
        ticker: str,
        learners_cnf: str = "default_ml_config",
        datasets_cnf: str = "default_instrument_config",
        custom_learners_cnf_fp: Union[str, PosixPath] = None,
        custom_datasets_cnf_fp: Union[str, PosixPath] = None,
        experiment_name: str = None,
    ):
        if learners_cnf is None and custom_learners_cnf_fp is None:
            raise AssertionError(
                "One of the 'learners_cnf' or 'custom_learners_cnf_fp' should be provided."
            )

        if datasets_cnf is None and custom_datasets_cnf_fp is None:
            raise AssertionError(
                "One of the 'datasets_cnf' or 'custom_datasets_cnf_fp' should be provided."
            )

        self.ticker = ticker
        self.experiment_name = (
            experiment_name
            if experiment_name is not None
            else f"{datetime.datetime.utcnow().date()}__{ticker}"
        )
        self.learners_cnf_fp = (
            str(paths.ML_CONFIGS_DIR / f"{learners_cnf}.yaml")
            if custom_learners_cnf_fp is None
            else str(custom_learners_cnf_fp)
        )
        self.datasets_cnf_fp = (
            str(paths.INSTRUMENT_CONFIGS_DIR / f"{datasets_cnf}.yaml")
            if custom_datasets_cnf_fp is None
            else str(custom_datasets_cnf_fp)
        )

    @staticmethod
    def __get_instrument_datasets(ticker, data_params):
        X_train, X_val, X_test, y_train, y_val, y_test = Instrument(
            ticker=ticker,
            period=data_params["period"],
            add_ta_indicators=data_params["add_ta_indicators"],
            ta_indicators_config_fn=data_params["ta_indicators_config_fn"],
            lookback_period=data_params["lookback_period"],
            forecast_period=data_params["forecast_period"],
        ).get_data(
            train_size=data_params["train_size"],
            val_size=data_params["val_size"],
            test_size=data_params["test_size"],
            separate_y=data_params["separate_y"],
            dropna=data_params["dropna"],
            environment=data_params["environment"],
        )
        return (X_train, X_val, X_test, y_train, y_val, y_test)

    def run(self):
        ml_config = utils.load_yaml(
            fp=self.learners_cnf_fp, validate=True, config_type="learners"
        )
        instrument_config = utils.load_yaml(
            fp=self.datasets_cnf_fp, validate=True, config_type="instruments"
        )

        mlflow.set_experiment(self.experiment_name)

        for data_params in instrument_config["data"]:
            (
                X_train,
                X_val,
                X_test,
                y_train,
                y_val,
                y_test,
            ) = self.__get_instrument_datasets(
                ticker=self.ticker, data_params=data_params
            )

            if len(X_val) == 0 and len(y_val) == 0:
                X_val = X_train
                y_val = y_train

            for learner in ml_config["learners"]:
                estimator = ml.learner(learner["model"])
                print(f"Training {estimator.__model__} estimator ...\n")

                for ml_params in learner["settings"]:
                    t1 = time.perf_counter()
                    estimator.fit(
                        X_train=X_train, y_train=y_train, settings=ml_params
                    )
                    t2 = time.perf_counter()
                    fit_time = utils.format_time(seconds=t2 - t1)
                    with mlflow.start_run(nested=True):
                        mlflow.set_tag("ticker", self.ticker)
                        mlflow.set_tag("estimator", estimator.__model__)
                        mlflow.set_tag("fit time", fit_time)
                        mlflow.set_tag("train set count", len(y_train))
                        mlflow.log_params(dict(**data_params, **ml_params))
                        y_pred = estimator.predict(X_val)

                        if estimator.__task__ == "regression":
                            mlflow.log_metric(
                                "rmse",
                                metrics.mean_squared_error(
                                    y_val, y_pred, squared=False
                                ),
                            )
                            mlflow.log_metric(
                                "mae",
                                metrics.mean_absolute_error(y_val, y_pred),
                            )
                            mlflow.log_metric(
                                "r2",
                                metrics.r2_score(y_val, y_pred),
                            )
                        elif estimator.__task__ == "classification":
                            mlflow.log_metric(
                                "accuracy",
                                metrics.accuracy_score(y_val, y_pred),
                            )
                            mlflow.log_metric(
                                "f1_micro",
                                metrics.f1_score(
                                    y_val, y_pred, average="micro"
                                ),
                            )
                            mlflow.log_metric(
                                "avg_precision_macro",
                                metrics.average_precision_score(
                                    y_val, y_pred, average="macro"
                                ),
                            )
