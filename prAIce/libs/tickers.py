import pathlib

import pandas as pd
import paths
import yfinance as yf


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
