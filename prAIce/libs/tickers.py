import pathlib

import pandas as pd
import paths
import talib as ta
import yfinance as yf
from talib import abstract


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
    """This class is an interface for 'TA-LIB' library.

    Args:
        data (pd.DataFrame): DataFrame that you want to add technical analysis stuff to it.

    Raises:
        KeyError: Throws error if given DataFrame doesn't include any of 'open',
            'high', 'low', 'close' columns.
    """

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

    def moving_average(
        self, ma_type: str = "SMA", period: int = 30, source: str = "close"
    ):
        """Add moving average to data.

        Args:
            ma_type (str, optional): Moving average kind. Valid types: SMA, EMA, WMA, DEMA, KAMA,
                TRIMA, MAMA. Defaults to "SMA".
            period (int, optional): Moving average length. Defaults to 30.
            source (str, optional): Column to use as source for calculating moving average.

        Returns:
            self: Instantiated class object. Use self.data property to get transformed data.
        """
        assert (
            ma_type == "SMA"
            or ma_type == "EMA"
            or ma_type == "WMA"
            or ma_type == "DEMA"
            or ma_type == "KAMA"
            or ma_type == "TRIMA"
            or ma_type == "MAMA"
        ), (
            "Expected ma_type to be one of 'SMA', 'EMA', "
            f"'WMA', 'DEMA', 'KAMA', 'TRIMA', 'MAMA' but got '{ma_type}'"
        )
        assert (
            source in self.data.columns
        ), f"'{source}' not found in the axis."

        func = eval(f"ta.{ma_type}")
        period = int(period)
        col = f"{ma_type}_{period}"
        self.data[col] = func(self.data[source], period)
        return self

    def bollinger_bands(
        self, period: int = 20, std_dev: int = 2, source: str = "close"
    ):
        """Add bollinger bands to data.

        Args:
            period (int, optional): Moving average length. Defaults to 20.
            std_dev (int, optional): Standard deviation for calculating upper and lower bands. Defaults to 2.
            source (str, optional): Column to use as source for calculating BBands.

        Returns:
            self: Instantiated class object. Use self.data property to get transformed data.
        """
        assert (
            source in self.data.columns
        ), f"'{source}' not found in the axis."

        bb = ta.BBANDS(self.data[source], period, std_dev, std_dev)
        self.data[f"BB_upper_{period}"] = bb[0]
        self.data[f"BB_middle_{period}"] = bb[1]
        self.data[f"BB_lower_{period}"] = bb[2]
        return self

    def momentum(self, indicator, period: int = 14):
        """Add momentum indicators to data. Indicators in this group only return one output value.

        Args:
            indicator (str): Momentum indicator to be added to data. Valid indicators: ADX,
                ADXR, CCI, DX, MFI, MOM, ROC, ROCP, ROCR, RSI, TRIX, WILLR.
            period (int, optional): Length for calculating the indicator. Defaults to 14.

        Returns:
            self: Instantiated class object. Use self.data property to get transformed data.
        """
        assert (
            indicator == "ADX"
            or indicator == "ADXR"
            or indicator == "CCI"
            or indicator == "DX"
            or indicator == "MFI"
            or indicator == "MOM"
            or indicator == "ROC"
            or indicator == "ROCP"
            or indicator == "ROCR"
            or indicator == "RSI"
            or indicator == "TRIX"
            or indicator == "WILLR"
        ), (
            "Expected indicator to be one of 'ADX', 'ADXR', 'CCI', 'DX', 'MFI', 'MOM', "
            f"'ROC', 'ROCP', 'ROCR', 'RSI', 'TRIX', 'WILLR' but got '{indicator}'"
        )
        func = eval(f"abstract.{indicator}")
        period = int(period)
        col = f"{indicator}_{period}"
        self.data[col] = func(self.data, period)
        return self

    def macd(
        self,
        fast_period: int = 12,
        slow_period: int = 26,
        signal_period: int = 9,
        source: str = "close",
    ):
        """Add MACD indicator to data.

        Args:
            fast_period (int, optional): Fast period length. Defaults to 12.
            slow_period (int, optional): Slow period length. Defaults to 26.
            signal_period (int, optional): Signal period length. Defaults to 9.
            source (str, optional): Column to use as source for calculating MACD. Defaults to "close".

        Returns:
            self: Instantiated class object. Use self.data property to get transformed data.

        """
        assert (
            source in self.data.columns
        ), f"'{source}' not found in the axis."
        outputs = abstract.MACD(
            self.data[source], fast_period, slow_period, signal_period
        )
        self.data[
            f"MACD_{fast_period}_{slow_period}_{signal_period}"
        ] = outputs[0]
        self.data[
            f"MACD_SIGNAL_{fast_period}_{slow_period}_{signal_period}"
        ] = outputs[1]
        self.data[
            f"MACD_HIST_{fast_period}_{slow_period}_{signal_period}"
        ] = outputs[2]
        return self

    def stochastic(
        self,
        fastk_period: int = 5,
        slowk_period: int = 3,
        slowk_matype: int = 0,
        slowd_period: int = 3,
        slowd_matype: int = 0,
    ):
        outputs = abstract.STOCH(
            self.data,
            fastk_period,
            slowk_period,
            slowk_matype,
            slowd_period,
            slowd_matype,
        )
        self.data[
            f"STOCH_SLOWK_{fastk_period}_{slowk_period}_{slowk_matype}_{slowd_period}_{slowd_matype}"
        ] = outputs[0]
        self.data[
            f"STOCH_SLOWD_{fastk_period}_{slowk_period}_{slowk_matype}_{slowd_period}_{slowd_matype}"
        ] = outputs[1]
        return self
