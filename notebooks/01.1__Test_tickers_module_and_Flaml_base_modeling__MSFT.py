# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.14.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# +
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from flaml import AutoML
from sklearn import metrics
from sklearn.model_selection import train_test_split

plt.style.use("ggplot")
pd.set_option("display.max_columns", None)

# +
ROOT_DIR = Path(".").resolve().parent
LIBS_DIR = ROOT_DIR / "prAIce/libs"

sys.path.append(str(LIBS_DIR))
# -

from tickers import Instrument

# +
# %%time

ticker = "MSFT"
period = "3y"
forecast_period = 1
lookback_period = 0

data = Instrument(
    ticker=ticker,
    period=period,
    lookback_period=lookback_period,
    forecast_period=forecast_period,
).get_data()
y = f"target_{forecast_period}_period"
# -

print(f"Original shape: {data.shape}")
data = data.dropna()
print(f"Shape after dropping null values: {data.shape}")

data

# +
split_date = "2022-05-01"
X_tmp = data[data.index < split_date].copy()
y_tmp = X_tmp.pop(y)
X_train, X_val, y_train, y_val = train_test_split(
    X_tmp, y_tmp, test_size=0.17, shuffle=False
)

X_test = data[data.index >= split_date].copy()
y_test = X_test.pop(y)

X_train.shape, y_train.shape, X_val.shape, y_val.shape, X_test.shape, y_test.shape
# -

automl = AutoML()
automl_settings = {
    "time_budget": 400,
    "metric": "rmse",
    "task": "regression",
    "ensemble": False,
    "estimator_list": [
        "lgbm",
        "rf",
        "xgboost",
        "extra_tree",
        "xgb_limitdepth",
        "catboost",
    ],
    "early_stop": True,
}
automl.fit(
    X_train=X_train,
    y_train=y_train,
    X_val=X_val,
    y_val=y_val,
    **automl_settings,
)

print(automl.model)

y_pred_val = automl.predict(X_val)
y_pred = automl.predict(X_test)

# +
y_df = pd.DataFrame(index=y_test.index)
y_df["actual"] = y_test.values
y_df["preds"] = y_pred
y_df["error"] = y_df["actual"] - y_df["preds"]

test_df = pd.concat([X_test, y_df], axis=1)

rmse_test = metrics.mean_squared_error(
    y_df["actual"], y_df["preds"], squared=False
)
rmse_val = metrics.mean_squared_error(y_val, y_pred_val, squared=False)
print(f"RMSE test: {rmse_test:.4f}")
print(f"RMSE val: {rmse_val:.4f}")
# -

plt.plot(y_df["actual"], label="Actual")
plt.plot(y_df["preds"], label="Predicted")
plt.title(f"{ticker} - RMSE: {rmse_test:.4f}")
plt.xticks(rotation=45)
plt.legend()
plt.show()

plt.scatter(y_df.index, y_df["error"], label="Errors")
plt.xticks(rotation=45)
plt.legend()
plt.show()

test_df[(test_df["error"] > -2) & (test_df["error"] < 2)]
