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
from sklearn import metrics
from sklearn.model_selection import train_test_split
from supervised.automl import AutoML

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

ticker = "AAPL"
period = "3y"
add_ta_indicators = True
ta_indicators_config_fn = "all_default"
add_date_features = True
lookback_period = 0
add_past_close_prices = False
add_past_pct_changes = False
forecast_period = 1


X_train, X_val, X_test, y_train, y_val, y_test = Instrument(
    ticker=ticker,
    period=period,
    add_ta_indicators=add_ta_indicators,
    ta_indicators_config_fn=ta_indicators_config_fn,
    lookback_period=lookback_period,
    forecast_period=forecast_period,
).get_data(
    train_size=0.75,
    val_size=0.20,
    test_size=0.05,
    separate_y=True,
    dropna=True,
)

print(
    X_train.shape,
    X_val.shape,
    X_test.shape,
    y_train.shape,
    y_val.shape,
    y_test.shape,
)

# +
# %%time

mljar = AutoML(
    mode="Perform",
    ml_task="regression",
    eval_metric="rmse",
    model_time_limit=10,
)
mljar.fit(X_train, y_train)
# -

y_pred_val = mljar.predict(X_val.copy())
y_pred = mljar.predict(X_test.copy())

# +
# RMSE val: 3.4721
# MAE val: 2.8122

# RMSE test: 2.7319
# MAE test: 2.1133

y_df = pd.DataFrame(index=y_test.index)
y_df["actual"] = y_test.values
y_df["preds"] = y_pred
y_df["error"] = y_df["actual"] - y_df["preds"]

test_df = pd.concat([X_test, y_df], axis=1)

rmse_val = metrics.mean_squared_error(y_val, y_pred_val, squared=False)
mae_val = metrics.mean_absolute_error(y_val, y_pred_val)
print(f"RMSE val: {rmse_val:.4f}")
print(f"MAE val: {mae_val:.4f}\n")

rmse_test = metrics.mean_squared_error(
    y_df["actual"], y_df["preds"], squared=False
)
mae_test = metrics.mean_absolute_error(y_df["actual"], y_df["preds"])
print(f"RMSE test: {rmse_test:.4f}")
print(f"MAE test: {mae_test:.4f}")
# -

plt.plot(y_df["actual"], label="Actual")
plt.plot(y_df["preds"], label="Predicted")
plt.title(f"{ticker} - RMSE: {rmse_test:.4f}")
plt.xticks(rotation=45)
plt.legend()
plt.show()

plt.scatter(y_df.index, y_df["error"], label="Errors")
plt.title(f"{ticker} - MAE: {mae_test:.4f}")
plt.xticks(rotation=45)
plt.legend()
plt.show()

test_df[(test_df["error"] > -1) & (test_df["error"] < 1)]

test_df[["open", "close", "preds", "actual", "error"]]
