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

from train import Trainer

# +
# %%time

trainer = Trainer(
    ticker="PYPL",
    ml_models_config_fn="default_ml_config",
    instrument_config_fn="default_instrument_config",
)
trainer.run()
