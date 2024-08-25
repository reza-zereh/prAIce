from typing import Any, Dict

import pandas as pd
import talib


def calculate_technical_indicators(data: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate all available technical indicators from TA-Lib.

    Args:
        data (pd.DataFrame): DataFrame with 'open', 'high', 'low', 'close', 'volume' columns and 'date' index.

    Returns:
        pd.DataFrame: DataFrame with added technical indicator columns.
    """
    # Ensure data is sorted by date
    data = data.sort_index()

    open_data, high_data, low_data, close_data, volume_data = (
        data["open"],
        data["high"],
        data["low"],
        data["close"],
        data["volume"],
    )

    # Overlap Studies
    for i in [5, 10, 20, 50, 100, 200]:
        data[f"EMA_{i}"] = talib.EMA(close_data, timeperiod=i)
    data["BBANDS_upper"], data["BBANDS_middle"], data["BBANDS_lower"] = talib.BBANDS(
        close_data
    )
    data["SAR"] = talib.SAR(high_data, low_data)
    data["T3"] = talib.T3(close_data)

    # Momentum Indicators
    data["ADX"] = talib.ADX(high_data, low_data, close_data)
    data["ADXR"] = talib.ADXR(high_data, low_data, close_data)
    data["APO"] = talib.APO(close_data)
    data["AROON_down"], data["AROON_up"] = talib.AROON(high_data, low_data)
    data["AROONOSC"] = talib.AROONOSC(high_data, low_data)
    data["BOP"] = talib.BOP(open_data, high_data, low_data, close_data)
    data["CCI"] = talib.CCI(high_data, low_data, close_data)
    data["CMO"] = talib.CMO(close_data)
    data["DX"] = talib.DX(high_data, low_data, close_data)
    data["MACD"], data["MACD_signal"], data["MACD_hist"] = talib.MACD(close_data)
    data["MFI"] = talib.MFI(high_data, low_data, close_data, volume_data)
    data["MINUS_DI"] = talib.MINUS_DI(high_data, low_data, close_data)
    data["MINUS_DM"] = talib.MINUS_DM(high_data, low_data)
    data["MOM"] = talib.MOM(close_data)
    data["PLUS_DI"] = talib.PLUS_DI(high_data, low_data, close_data)
    data["PLUS_DM"] = talib.PLUS_DM(high_data, low_data)
    data["PPO"] = talib.PPO(close_data)
    data["ROC"] = talib.ROC(close_data)
    data["ROCP"] = talib.ROCP(close_data)
    data["ROCR"] = talib.ROCR(close_data)
    data["ROCR100"] = talib.ROCR100(close_data)
    data["RSI_9"] = talib.RSI(close_data, timeperiod=9)
    data["RSI_14"] = talib.RSI(close_data, timeperiod=14)
    data["STOCH_k"], data["STOCH_d"] = talib.STOCH(high_data, low_data, close_data)
    data["STOCHF_k"], data["STOCHF_d"] = talib.STOCHF(high_data, low_data, close_data)
    data["STOCHRSI_k"], data["STOCHRSI_d"] = talib.STOCHRSI(close_data)
    data["ULTOSC"] = talib.ULTOSC(high_data, low_data, close_data)
    data["WILLR"] = talib.WILLR(high_data, low_data, close_data)

    # Volume Indicators
    data["AD"] = talib.AD(high_data, low_data, close_data, volume_data)
    data["ADOSC"] = talib.ADOSC(high_data, low_data, close_data, volume_data)
    data["OBV"] = talib.OBV(close_data, volume_data)

    # Price Transform
    data["AVGPRICE"] = talib.AVGPRICE(open_data, high_data, low_data, close_data)
    data["MEDPRICE"] = talib.MEDPRICE(high_data, low_data)

    # Volatility Indicators
    data["ATR"] = talib.ATR(high_data, low_data, close_data)
    data["NATR"] = talib.NATR(high_data, low_data, close_data)
    data["TRANGE"] = talib.TRANGE(high_data, low_data, close_data)

    # Statistic Functions
    data["BETA"] = talib.BETA(high_data, low_data)
    data["CORREL"] = talib.CORREL(high_data, low_data)
    data["LINEARREG"] = talib.LINEARREG(close_data)
    data["LINEARREG_ANGLE"] = talib.LINEARREG_ANGLE(close_data)
    data["LINEARREG_INTERCEPT"] = talib.LINEARREG_INTERCEPT(close_data)
    data["LINEARREG_SLOPE"] = talib.LINEARREG_SLOPE(close_data)
    data["STDDEV"] = talib.STDDEV(close_data)
    data["TSF"] = talib.TSF(close_data)
    data["VAR"] = talib.VAR(close_data)

    return data


def identify_candlestick_patterns(data: pd.DataFrame) -> pd.DataFrame:
    """
    Identify all available candlestick patterns from TA-Lib.

    Args:
        data (pd.DataFrame): DataFrame with 'open', 'high', 'low', 'close' columns and 'date' index.

    Returns:
        pd.DataFrame: DataFrame with added candlestick pattern columns.
    """
    # Ensure data is sorted by date
    data = data.sort_index()

    open_data, high_data, low_data, close_data = (
        data["open"],
        data["high"],
        data["low"],
        data["close"],
    )

    # Two Crows
    data["CDL2CROWS"] = talib.CDL2CROWS(open_data, high_data, low_data, close_data)
    # Three Black Crows
    data["CDL3BLACKCROWS"] = talib.CDL3BLACKCROWS(
        open_data, high_data, low_data, close_data
    )
    # Three Inside Up/Down
    data["CDL3INSIDE"] = talib.CDL3INSIDE(open_data, high_data, low_data, close_data)
    # Three-Line Strike
    data["CDL3LINESTRIKE"] = talib.CDL3LINESTRIKE(
        open_data, high_data, low_data, close_data
    )
    # Three Outside Up/Down
    data["CDL3OUTSIDE"] = talib.CDL3OUTSIDE(open_data, high_data, low_data, close_data)
    # Three Stars In The South
    data["CDL3STARSINSOUTH"] = talib.CDL3STARSINSOUTH(
        open_data, high_data, low_data, close_data
    )
    # Three Advancing White Soldiers
    data["CDL3WHITESOLDIERS"] = talib.CDL3WHITESOLDIERS(
        open_data, high_data, low_data, close_data
    )
    # Abandoned Baby
    data["CDLABANDONEDBABY"] = talib.CDLABANDONEDBABY(
        open_data, high_data, low_data, close_data
    )
    # Advance Block
    data["CDLADVANCEBLOCK"] = talib.CDLADVANCEBLOCK(
        open_data, high_data, low_data, close_data
    )
    # Belt-hold
    data["CDLBELTHOLD"] = talib.CDLBELTHOLD(open_data, high_data, low_data, close_data)
    # Breakaway
    data["CDLBREAKAWAY"] = talib.CDLBREAKAWAY(
        open_data, high_data, low_data, close_data
    )
    # Closing Marubozu
    data["CDLCLOSINGMARUBOZU"] = talib.CDLCLOSINGMARUBOZU(
        open_data, high_data, low_data, close_data
    )
    # Concealing Baby Swallow
    data["CDLCONCEALBABYSWALL"] = talib.CDLCONCEALBABYSWALL(
        open_data, high_data, low_data, close_data
    )
    # Counterattack
    data["CDLCOUNTERATTACK"] = talib.CDLCOUNTERATTACK(
        open_data, high_data, low_data, close_data
    )
    # Dark Cloud Cover
    data["CDLDARKCLOUDCOVER"] = talib.CDLDARKCLOUDCOVER(
        open_data, high_data, low_data, close_data
    )
    # Doji
    data["CDLDOJI"] = talib.CDLDOJI(open_data, high_data, low_data, close_data)
    # Doji Star
    data["CDLDOJISTAR"] = talib.CDLDOJISTAR(open_data, high_data, low_data, close_data)
    # Dragonfly Doji
    data["CDLDRAGONFLYDOJI"] = talib.CDLDRAGONFLYDOJI(
        open_data, high_data, low_data, close_data
    )
    # Engulfing Pattern
    data["CDLENGULFING"] = talib.CDLENGULFING(
        open_data, high_data, low_data, close_data
    )
    # Evening Doji Star
    data["CDLEVENINGDOJISTAR"] = talib.CDLEVENINGDOJISTAR(
        open_data, high_data, low_data, close_data
    )
    # Evening Star
    data["CDLEVENINGSTAR"] = talib.CDLEVENINGSTAR(
        open_data, high_data, low_data, close_data
    )
    # Up/Down-gap side-by-side white lines
    data["CDLGAPSIDESIDEWHITE"] = talib.CDLGAPSIDESIDEWHITE(
        open_data, high_data, low_data, close_data
    )
    # Gravestone Doji
    data["CDLGRAVESTONEDOJI"] = talib.CDLGRAVESTONEDOJI(
        open_data, high_data, low_data, close_data
    )
    # Hammer
    data["CDLHAMMER"] = talib.CDLHAMMER(open_data, high_data, low_data, close_data)
    # Hanging Man
    data["CDLHANGINGMAN"] = talib.CDLHANGINGMAN(
        open_data, high_data, low_data, close_data
    )
    # Harami Pattern
    data["CDLHARAMI"] = talib.CDLHARAMI(open_data, high_data, low_data, close_data)
    # Harami Cross Pattern
    data["CDLHARAMICROSS"] = talib.CDLHARAMICROSS(
        open_data, high_data, low_data, close_data
    )
    # High-Wave Candle
    data["CDLHIGHWAVE"] = talib.CDLHIGHWAVE(open_data, high_data, low_data, close_data)
    # Hikkake Pattern
    data["CDLHIKKAKE"] = talib.CDLHIKKAKE(open_data, high_data, low_data, close_data)
    # Modified Hikkake Pattern
    data["CDLHIKKAKEMOD"] = talib.CDLHIKKAKEMOD(
        open_data, high_data, low_data, close_data
    )
    # Homing Pigeon
    data["CDLHOMINGPIGEON"] = talib.CDLHOMINGPIGEON(
        open_data, high_data, low_data, close_data
    )
    # Identical Three Crows
    data["CDLIDENTICAL3CROWS"] = talib.CDLIDENTICAL3CROWS(
        open_data, high_data, low_data, close_data
    )
    # In-Neck Pattern
    data["CDLINNECK"] = talib.CDLINNECK(open_data, high_data, low_data, close_data)
    # Inverted Hammer
    data["CDLINVERTEDHAMMER"] = talib.CDLINVERTEDHAMMER(
        open_data, high_data, low_data, close_data
    )
    # Kicking
    data["CDLKICKING"] = talib.CDLKICKING(open_data, high_data, low_data, close_data)
    # Kicking - bull/bear determined by the longer marubozu
    data["CDLKICKINGBYLENGTH"] = talib.CDLKICKINGBYLENGTH(
        open_data, high_data, low_data, close_data
    )

    return data


def process_technical_analysis(data: pd.DataFrame) -> pd.DataFrame:
    """
    Process technical analysis for the given data.

    Args:
        data (pd.DataFrame): DataFrame with 'open', 'high', 'low', 'close', 'volume' columns and 'date' index.

    Returns:
        pd.DataFrame: DataFrame with added technical analysis columns.
    """
    data = calculate_technical_indicators(data)
    data = identify_candlestick_patterns(data)
    return data


def technical_analysis_to_dict(
    data: pd.DataFrame,
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Convert a DataFrame with technical analysis data to a dictionary format.

    Args:
        data (pd.DataFrame): DataFrame with technical indicators and candlestick patterns.

    Returns:
        Dict[str, Dict[str, Dict[str, Any]]]: A dictionary with the format:
        {
            'YYYY-MM-DD': {
                'technical_indicators': {...},
                'candlestick_patterns': {...}
            },
            ...
        }
    """
    result = {}

    # Identify technical indicator and candlestick pattern columns
    tech_indicator_cols = [
        col
        for col in data.columns
        if col not in ["open", "high", "low", "close", "volume"]
    ]
    candlestick_cols = [col for col in tech_indicator_cols if col.startswith("CDL")]
    indicator_cols = [col for col in tech_indicator_cols if col not in candlestick_cols]

    for date, row in data.iterrows():
        date_str = date.strftime("%Y-%m-%d")
        result[date_str] = {
            "technical_indicators": {
                col: float(row[col]) if not pd.isna(row[col]) else None
                for col in indicator_cols
            },
            "candlestick_patterns": {
                col: int(row[col]) if not pd.isna(row[col]) else None
                for col in candlestick_cols
            },
        }

    return result


def process_and_format_technical_analysis(
    data: pd.DataFrame,
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Process technical analysis for the given data and format the result as a dictionary.

    Args:
        data (pd.DataFrame): DataFrame with 'open', 'high', 'low', 'close', 'volume' columns.
        timeframe (Timeframe): The timeframe of the data.

    Returns:
        Dict[str, Dict[str, Dict[str, Any]]]: A dictionary with processed technical analysis data.
    """
    processed_data = process_technical_analysis(data)
    return technical_analysis_to_dict(processed_data)
