import pathlib

import pandas as pd
import paths
import talib as ta
import yfinance as yf
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.utils.validation import check_is_fitted


class Ticker:
    """This class is an interface for 'yfinance' library.

    Args:
        ticker (str): Ticker name e.g MSFT, APPL, GLD
    """

    def __init__(self, ticker: str, save: bool = True) -> None:
        # TODO: Check whether ticker is valid yfinance ticker
        self.ticker = ticker
        self.yfticker = yf.Ticker(ticker)

    def __store(self) -> pathlib.PosixPath:
        """Store historical prices as a parquet file.

        Returns:
            pathlib.PosixPath: Absolute file path to stored parquet file.
        """
        if hasattr(self, "history"):
            self.fp_ = paths.DATA_TICKERS_DIR / f"{self.ticker}.parquet"
            self.history.to_parquet(self.fp_)
            return self.fp_

    def get_history(
        self,
        period: str = "max",
        interval: str = "1d",
        start_date: str = None,
        end_date: str = None,
        save: bool = True,
    ) -> pd.DataFrame:
        """Returns historical prices from Yahoo Finance source.

        Args:
            period (str, optional): Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max.
                Either Use period parameter or use start_date and end_date. Defaults to "max".
            interval (str, optional): Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo.
                Defaults to "1d".
            start_date (str, optional): Download start date string (YYYY-MM-DD). Defaults to None.
            end_date (str, optional): Download end date string (YYYY-MM-DD). Defaults to None.
            save (bool, optional): Whether to store historical data into disk. Defaults to True.

        Returns:
            pd.DataFrame: Historical prices.
        """
        self.history: pd.DataFrame = self.yfticker.history(
            period=period, interval=interval, start=start_date, end=end_date
        )
        self.history = self.history.rename(
            columns={
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
                "Dividends": "dividends",
                "Stock Splits": "stock_splits",
            }
        )
        if save:
            self.__store()
        return self.history


class TechnicalAnalysis:
    def __init__(self, data: pd.DataFrame):
        self.data = data.copy()
        self.has_open = "open" in data.columns
        self.has_high = "high" in data.columns
        self.has_low = "low" in data.columns
        self.has_close = "close" in data.columns
        self.has_volume = "volume" in data.columns

        if not all(
            [self.has_open, self.has_high, self.has_low, self.has_close]
        ):
            raise KeyError(
                "All of 'open', 'high', 'low', and 'close' columns should be in data."
            )

    def sma(self, period=30):
        self.data[f"sma_{period}"] = ta.SMA(self.data["close"], period)
        return self

    def ema(self, period=30):
        self.data[f"ema_{period}"] = ta.EMA(self.data["close"], period)
        return self

    def wma(self, period=30):
        self.data[f"wma_{period}"] = ta.WMA(self.data["close"], period)
        return self

    # def bbands(self, period=20, )
