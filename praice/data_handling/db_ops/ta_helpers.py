import datetime
from typing import Union

from praice.data_handling.db_ops import crud, historical_price_helpers
from praice.data_handling.db_ops.crud import Timeframe
from praice.data_handling.models import Symbol, TechnicalAnalysis
from praice.data_handling.processors import ta_processor


def calculate_and_store_technical_analysis(
    symbol: Union[Symbol, str],
    start_date: Union[datetime.date, str] = None,
    end_date: Union[datetime.date, str] = None,
    timeframe: Timeframe = Timeframe.DAYS_1.value,
) -> int:
    """
    Calculate and store technical analysis data for a symbol within a date range.

    Args:
        symbol (Union[Symbol, str]): The Symbol object or symbol string.
        start_date (Union[datetime.date, str], optional): The start date of the range (inclusive).
        end_date (Union[datetime.date, str], optional): The end date of the range (inclusive).
        timeframe (Timeframe, optional): The timeframe for the technical analysis data.

    Returns:
        int: The number of records upserted.
    """
    # get all historical prices
    df = historical_price_helpers.get_historical_prices_df(symbol)
    # calculate TA for the whole range but only get the processed data within the date range
    processed_data = ta_processor.process_and_format_technical_analysis(
        data=df, start_date=start_date, end_date=end_date
    )
    upsert_count = crud.bulk_upsert_technical_analysis(
        symbol=symbol, data=processed_data, timeframe=timeframe
    )
    return upsert_count


def delete_technical_analysis_by_symbol(
    symbol: Union[Symbol, str],
    timeframe: Union[Timeframe, str] = Timeframe.DAYS_1,
) -> int:
    """
    Delete all TechnicalAnalysis records for a specific symbol.

    Args:
        symbol (Union[Symbol, str]): Symbol object or symbol string.

    Returns:
        int: The number of records deleted.

    Raises:
        ValueError: If the symbol doesn't exist or if the timeframe is invalid.
    """
    try:
        symbol_obj = crud._ensure_symbol(symbol)
    except Symbol.DoesNotExist:
        raise ValueError(f"Symbol '{symbol}' does not exist.")

    if isinstance(timeframe, str):
        try:
            timeframe = Timeframe(timeframe)
        except ValueError:
            raise ValueError(f"Invalid timeframe: {timeframe}")

    query = TechnicalAnalysis.delete().where(
        (TechnicalAnalysis.symbol == symbol_obj)
        & (TechnicalAnalysis.timeframe == timeframe.value)
    )
    deleted_count = query.execute()
    return deleted_count
