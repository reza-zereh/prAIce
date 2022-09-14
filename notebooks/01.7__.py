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
from tpot import TPOTRegressor

plt.style.use("ggplot")
pd.set_option("display.max_columns", None)

# +
ROOT_DIR = Path(".").resolve().parent
LIBS_DIR = ROOT_DIR / "prAIce/libs"

sys.path.append(str(LIBS_DIR))
# -

from tickers import Instrument
from ml import Trainer

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
    train_size=0.95,
    val_size=0.0,
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
# -

trainer = Trainer(experiment_name="exp1", ml_models_config_fn="default")

trainer.run(X_train=X_train, y_train=y_train)


