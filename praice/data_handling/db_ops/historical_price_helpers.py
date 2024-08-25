from datetime import date
from typing import Union

import pandas as pd

from praice.data_handling.db_ops import crud
from praice.data_handling.models import Symbol


def get_historical_prices_df(
    symbol: Union[Symbol, str], start_date: date = None, end_date: date = None
) -> pd.DataFrame:
    """
    Retrieve historical price records for a symbol within a date range and return as a pandas DataFrame.

    Args:
        symbol (Union[Symbol, str]): The Symbol object or symbol string.
        start_date (date, optional): The start date of the range (inclusive).
        end_date (date, optional): The end date of the range (inclusive).

    Returns:
        pd.DataFrame: A DataFrame with columns 'date', 'open', 'high', 'low', 'close', 'volume'.
    """

    historical_prices = crud.get_historical_prices(symbol, start_date, end_date)

    df = pd.DataFrame(
        [
            {
                "date": price.date,
                "open": float(price.open),
                "high": float(price.high),
                "low": float(price.low),
                "close": float(price.close),
                "volume": int(price.volume),
            }
            for price in historical_prices
        ]
    )

    if not df.empty:
        df.set_index("date", inplace=True)
        df.sort_index(inplace=True)

    return df
