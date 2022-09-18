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
# import sys
# from pathlib import Path

# ROOT_DIR = Path(".").resolve().parent
# sys.path.append(str(ROOT_DIR))

# +
import praice
from praice import Trainer

print(praice.__version__)
# -

trainer = Trainer(
    ticker="AMZN",
    learners_cnf="default_ml_config",
    datasets_cnf="default_instrument_config",
)

# +
# %%time

trainer.run()
# -
