import os
import pathlib

import pandas as pd
import paths
import talib as ta
import yaml
import yfinance as yf
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.utils.validation import check_is_fitted
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
        ), (
            "Expected ma_type to be one of 'SMA', 'EMA', "
            f"'WMA', 'DEMA', 'KAMA', 'TRIMA' but got '{ma_type}'"
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
        """Add Stochastic indicator to data.

        Args:
            fastk_period (int, optional): Fast K period. Defaults to 5.
            slowk_period (int, optional): Slow K Period. Defaults to 3.
            slowk_matype (int, optional): Slow K MA type. Defaults to 0.
            slowd_period (int, optional): Slow D period. Defaults to 3.
            slowd_matype (int, optional): Slow D MA type. Defaults to 0.

        Returns:
            self: Instantiated class object. Use self.data property to get transformed data.
        """
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
        ] = outputs["slowk"]
        self.data[
            f"STOCH_SLOWD_{fastk_period}_{slowk_period}_{slowk_matype}_{slowd_period}_{slowd_matype}"
        ] = outputs["slowd"]
        return self

    def cycle(self, indicator: str, source: str = "close"):
        """Add Cyclical indicators to data.

        Args:
            indicator (str): Name of the cyclical indicator. Valid indicators: HT_DCPERIOD,
                HT_DCPHASE, HT_TRENDMODE.
            source (str, optional): Column to use as source for calculating cyclical indicator. Defaults to 'close'.

        Returns:
            self: Instantiated class object. Use self.data property to get transformed data.
        """
        assert (
            source in self.data.columns
        ), f"'{source}' not found in the axis."
        assert (
            indicator == "HT_DCPERIOD"
            or indicator == "HT_DCPHASE"
            or indicator == "HT_TRENDMODE"
        ), (
            "Expected indicator to be one of 'HT_DCPERIOD', 'HT_DCPHASE', 'HT_TRENDMODE' "
            f"but got '{indicator}'"
        )
        func = eval(f"abstract.{indicator}")
        self.data[indicator] = func(self.data[source])
        return self

    def cdl_pattern(self, pattern: str):
        """Candlestick pattern recognition.

        Args:
            pattern (str): Candlestick pattern to be recognized. Valid patterns:
                "CDL3STARSINSOUTH", "CDLABANDONEDBABY", "CDLCLOSINGMARUBOZU",
                "CDLCOUNTERATTACK", "CDLDARKCLOUDCOVER", "CDLDOJI",
                "CDLDOJISTAR", "CDLDRAGONFLYDOJI", "CDLENGULFING",
                "CDLEVENINGDOJISTAR", "CDLEVENINGSTAR", "CDLGRAVESTONEDOJI",
                "CDLHAMMER", "CDLHANGINGMAN", "CDLHARAMI", "CDLINVERTEDHAMMER",
                "CDLMARUBOZU", "CDLMORNINGDOJISTAR", "CDLMORNINGSTAR", "CDLPIERCING",
                "CDLSHOOTINGSTAR", "CDLTAKURI", "CDLTRISTAR"

        Raises:
            ValueError: If given pattern is not in list of valid patterns.

        Returns:
            self: Instantiated class object. Use self.data property to get transformed data.
        """
        patterns = [
            "CDL3STARSINSOUTH",
            "CDLABANDONEDBABY",
            "CDLCLOSINGMARUBOZU",
            "CDLCOUNTERATTACK",
            "CDLDARKCLOUDCOVER",
            "CDLDOJI",
            "CDLDOJISTAR",
            "CDLDRAGONFLYDOJI",
            "CDLENGULFING",
            "CDLEVENINGDOJISTAR",
            "CDLEVENINGSTAR",
            "CDLGRAVESTONEDOJI",
            "CDLHAMMER",
            "CDLHANGINGMAN",
            "CDLHARAMI",
            "CDLINVERTEDHAMMER",
            "CDLMARUBOZU",
            "CDLMORNINGDOJISTAR",
            "CDLMORNINGSTAR",
            "CDLPIERCING",
            "CDLSHOOTINGSTAR",
            "CDLTAKURI",
            "CDLTRISTAR",
        ]
        if pattern not in patterns:
            raise ValueError(
                f"Expected pattern to be one of {patterns} but got '{pattern}'."
            )
        func = eval(f"abstract.{pattern}")
        self.data[pattern] = func(self.data)
        return self


class VariablesBuilder(BaseEstimator, TransformerMixin):
    def __init__(
        self,
        forecast_period: int = 1,
        lookback_period: int = 0,
        target_col_prefix: str = "target_",
        add_past_close_prices: bool = True,
        add_past_pct_changes: bool = True,
    ):
        """Create forecast and lookback columns for a given DataFrame.

        Args:
            forecast_period (int, optional): Forecast period; 1 means next period, 7 means next 7 period, etc.
                Defaults to 1.
            lookback_period (int, optional): Number of periods in past to build
                features from. Defaults to 0.
            target_col_prefix (str, optional): Target colum prefix. Defaults to "target_".
            add_past_close_prices (bool, optional): Whether to add N previous
                close prices. Defaults to True.
            add_past_pct_changes (bool, optional): Whether to add N previous
                close price percentage changes. Defaults to True.
        """
        self.forecast_period = forecast_period
        self.lookback_period = lookback_period
        self.target_col_prefix = target_col_prefix
        self.add_past_close_prices = add_past_close_prices
        self.add_past_pct_changes = add_past_pct_changes

    def fit(self, X: pd.DataFrame = None, source: str = "close"):
        """Fit the transformer.

        Args:
            X (pd.DataFrame, optional): DataFrame to be transformed. Defaults to None.
            source (str, optional): Source colum in X. Defaults to "close".

        Returns:
            VariablesBuilder: Fitted object.
        """
        self.source_col_ = source
        return self

    def transform(self, X: pd.DataFrame):
        """Transform the DataFrame.

        Args:
            X (pd.DataFrame): DataFrame to be transformed.

        Raises:
            KeyError: Throws error if source column is not found in index.

        Returns:
            pd.DataFrame: Transformed DataFrame.
        """
        check_is_fitted(self, "source_col_")
        if self.source_col_ not in X.columns:
            raise KeyError(f"{self.source_col_} not found in index.")

        X = X.copy()
        if self.lookback_period > 0:
            for i in range(1, self.lookback_period + 1):
                if self.add_past_close_prices:
                    X[f"{self.source_col_}_minus_{i}_period"] = X[
                        self.source_col_
                    ].shift(i)

                if self.add_past_pct_changes:
                    X[f"{self.source_col_}_minus_{i}_pct_change"] = X[
                        self.source_col_
                    ].pct_change(i)
        X[f"{self.target_col_prefix}{self.forecast_period}_period"] = X[
            self.source_col_
        ].shift(-self.forecast_period)
        return X


class AddDateParts:
    """Add date features to a DataFrame.

    Args:
        has_date_index (bool, optional): Whether the DataFrame has DateTimeIndex.
            Defaults to True.
        date_col (str, optional): Name of the column that stores dates if
            'has_date_index = False'. Defaults to "date".
    """

    def __init__(self, has_date_index: bool = True, date_col: str = "date"):
        self.has_date_index = has_date_index
        self.date_col = date_col

    def transform(self, X: pd.DataFrame, drop: bool = True) -> pd.DataFrame:
        """Transform DataFrame by adding date features.

        Args:
            X (pd.DataFrame): DataFrame to be transformed.
            drop (bool, optional): If dates are stored in a column other than index,
                drops that column. Defaults to True.

        Returns:
            pd.DataFrame: Transformed DataFrame with these new columns:
                'day_of_wee', 'day_of_month', 'day_of_year', 'week', 'month', 'quarter'.
        """
        if self.has_date_index:
            self.date_ = pd.to_datetime(X.index)
        else:
            self.date_ = pd.to_datetime(X[self.date_col])
        X = X.copy()
        X["day_of_week"] = self.date_.day_of_week
        X["day_of_month"] = self.date_.day
        X["day_of_year"] = self.date_.day_of_year
        X["week"] = self.date_.isocalendar().week
        X["month"] = self.date_.month
        X["quarter"] = self.date_.quarter
        if drop and not self.has_date_index:
            X = X.drop(columns=[self.date_col])
        return X


class Instrument:
    # TODO: Write docstring for Instrument class
    def __init__(
        self,
        ticker: str,
        period: str = "max",
        interval: str = "1d",
        start_date: str = None,
        end_date: str = None,
        add_ta_indicators: bool = True,
        ta_indicators_config_fn="all_default",
        add_date_features=True,
        lookback_period: int = 0,
        forecast_period: int = 1,
    ):
        self.ticker = ticker
        self.period = period
        self.interval = interval
        self.start_date = start_date
        self.end_date = end_date
        self.add_ta_indicators = add_ta_indicators
        self.ta_indicators_config_fn = ta_indicators_config_fn
        self.add_date_features = add_date_features
        self.lookback_period = lookback_period
        self.forecast_period = forecast_period

    @staticmethod
    def add_indicators_from_config(
        data: pd.DataFrame, config_fn: str
    ) -> pd.DataFrame:
        """Add technical analysis indicators specified in YAML config file to given DataFrame.

        Args:
            data (pd.DataFrame): DataFrame to be transformed.
            config_fn (str, optional): Name of the YAML config file with '.yaml' suffix.

        Raises:
            FileNotFoundError: Throws error if YAML file doesn't exist.

        Returns:
            pd.DataFrame: DataFrame with technical indicators added to it.
        """
        fp = str(paths.TA_CONFIGS_DIR / f"{config_fn}.yaml")
        if not os.path.exists(fp):
            raise FileNotFoundError(f"There is no such a file named {fp}")

        with open(fp, "r") as f:
            config = yaml.safe_load(f)
        indicators = config["indicators"]
        ta_obj = TechnicalAnalysis(data)
        for method, settings in indicators.items():
            for kwargs in settings:
                func = getattr(TechnicalAnalysis, method)
                func(ta_obj, **kwargs)
        return ta_obj.data

    def get_data(self):
        """Prepare the historical data with technical analysis features,
            and also lookback and forecast variables.

        Returns:
            pd.DataFrame: Processed DataFrame.
        """
        self.data_ = Ticker(ticker=self.ticker).get_history(
            period=self.period,
            interval=self.interval,
            start_date=self.start_date,
            end_date=self.end_date,
        )
        if self.add_ta_indicators:
            self.data_ = self.add_indicators_from_config(
                self.data_, self.ta_indicators_config_fn
            )

        if self.add_date_features:
            self.data_ = AddDateParts(has_date_index=True).transform(
                X=self.data_
            )

        self.data_ = VariablesBuilder(
            forecast_period=self.forecast_period,
            lookback_period=self.lookback_period,
        ).fit_transform(self.data_)
        return self.data_
