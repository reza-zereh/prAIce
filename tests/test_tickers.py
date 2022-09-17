import pandas as pd
import pytest

from praice.tickers import TechnicalAnalysis, Ticker

symbol = "MSFT"
ticker_obj = Ticker(symbol)
data = ticker_obj.get_history(period="1mo")


def test_Ticker():
    assert ticker_obj.ticker == symbol
    assert type(data) == pd.DataFrame
    assert len(data) > 0
    assert "open" in data.columns
    assert "high" in data.columns
    assert "low" in data.columns
    assert "close" in data.columns
    assert "volume" in data.columns
    assert "Dividends" not in data.columns
    assert data.shape[1] == 5


def test_TechnicalAnalysis():
    ta = TechnicalAnalysis(data=data)
    assert data.equals(ta.data)


def test_TechnicalAnalysis_bad_data():
    x = data.drop(columns=["close"])
    with pytest.raises(KeyError):
        TechnicalAnalysis(data=x)


def test_TechnicalAnalysis_moving_average():
    ta = TechnicalAnalysis(data=data)
    ta.moving_average()
    assert "SMA_30" in ta.data.columns
    ta.moving_average(ma_type="EMA", period=10)
    assert "EMA_10" in ta.data.columns
    ta.moving_average(ma_type="WMA", period=20)
    assert "WMA_20" in ta.data.columns
    ta.moving_average(ma_type="DEMA", period=40)
    assert "DEMA_40" in ta.data.columns
    ta.moving_average(ma_type="KAMA", period=15)
    assert "KAMA_15" in ta.data.columns
    ta.moving_average(ma_type="TRIMA", period=35)
    assert "TRIMA_35" in ta.data.columns

    with pytest.raises(AssertionError):
        ta.moving_average(source="not_valid_column")


def test_TechnicalAnalysis_bollinger_bands():
    ta = TechnicalAnalysis(data=data)
    ta.bollinger_bands()
    assert "BB_upper_20" in ta.data.columns
    assert "BB_middle_20" in ta.data.columns
    assert "BB_lower_20" in ta.data.columns

    ta.bollinger_bands(period=30)
    assert "BB_upper_30" in ta.data.columns
    assert "BB_middle_30" in ta.data.columns
    assert "BB_lower_30" in ta.data.columns

    with pytest.raises(AssertionError):
        ta.bollinger_bands(source="not_valid_column")


def test_TechnicalAnalysis_momentum():
    ta = TechnicalAnalysis(data=data)
    with pytest.raises(AssertionError):
        ta.momentum(indicator="not_valid_indicator")

    ta.momentum(indicator="ADX")
    assert "ADX_14" in ta.data.columns
    ta.momentum(indicator="ADXR")
    assert "ADXR_14" in ta.data.columns
    ta.momentum(indicator="CCI", period=15)
    assert "CCI_15" in ta.data.columns
    ta.momentum(indicator="DX", period=30)
    assert "DX_30" in ta.data.columns
    ta.momentum(indicator="MFI")
    assert "MFI_14" in ta.data.columns
    ta.momentum(indicator="MOM")
    assert "MOM_14" in ta.data.columns
    ta.momentum(indicator="ROC")
    assert "ROC_14" in ta.data.columns
    ta.momentum(indicator="ROCP")
    assert "ROCP_14" in ta.data.columns
    ta.momentum(indicator="ROCR")
    assert "ROCR_14" in ta.data.columns
    ta.momentum(indicator="RSI", period=45)
    assert "RSI_45" in ta.data.columns
    ta.momentum(indicator="TRIX")
    assert "TRIX_14" in ta.data.columns
    ta.momentum(indicator="WILLR")
    assert "WILLR_14" in ta.data.columns


def test_TechnicalAnalysis_macd():
    ta = TechnicalAnalysis(data=data)
    ta.macd()
    assert "MACD_12_26_9" in ta.data.columns
    assert "MACD_SIGNAL_12_26_9" in ta.data.columns
    assert "MACD_HIST_12_26_9" in ta.data.columns
    ta.macd(fast_period=15, slow_period=30, signal_period=12)
    assert "MACD_15_30_12" in ta.data.columns
    assert "MACD_SIGNAL_15_30_12" in ta.data.columns
    assert "MACD_HIST_15_30_12" in ta.data.columns
    with pytest.raises(AssertionError):
        ta.macd(source="not_valid_column")


def test_TechnicalAnalysis_stochastic():
    ta = TechnicalAnalysis(data=data)
    ta.stochastic()
    assert "STOCH_SLOWK_5_3_0_3_0" in ta.data.columns
    ta.stochastic(
        fastk_period=7,
        slowk_period=3,
        slowk_matype=1,
        slowd_period=5,
        slowd_matype=1,
    )
    assert "STOCH_SLOWK_7_3_1_5_1" in ta.data.columns


def test_TechnicalAnalysis_cycle():
    ta = TechnicalAnalysis(data=data)
    ta.cycle(indicator="HT_DCPERIOD")
    assert "HT_DCPERIOD" in ta.data.columns
    ta.cycle(indicator="HT_DCPHASE")
    assert "HT_DCPHASE" in ta.data.columns
    ta.cycle(indicator="HT_TRENDMODE")
    assert "HT_TRENDMODE" in ta.data.columns

    with pytest.raises(AssertionError):
        ta.cycle(indicator="invalid_indicator")
    with pytest.raises(AssertionError):
        ta.cycle(indicator="HT_DCPERIOD", source="not_valid_column")


def test_TechnicalAnalysis_cdl_pattern():
    ta = TechnicalAnalysis(data=data)
    with pytest.raises(ValueError):
        ta.cdl_pattern(pattern="invalid_pattern")
    ta.cdl_pattern("CDL3STARSINSOUTH")
    assert "CDL3STARSINSOUTH" in ta.data.columns
    ta.cdl_pattern("CDLABANDONEDBABY")
    assert "CDLABANDONEDBABY" in ta.data.columns
    ta.cdl_pattern("CDLCLOSINGMARUBOZU")
    assert "CDLCLOSINGMARUBOZU" in ta.data.columns
    ta.cdl_pattern("CDLCOUNTERATTACK")
    assert "CDLCOUNTERATTACK" in ta.data.columns
    ta.cdl_pattern("CDLDARKCLOUDCOVER")
    assert "CDLDARKCLOUDCOVER" in ta.data.columns
    ta.cdl_pattern("CDLDOJI")
    assert "CDLDOJI" in ta.data.columns
    # TODO: write test for other patterns
