from datetime import UTC, datetime, timedelta
from typing import Dict, List, Optional

import yfinance as yf
from loguru import logger

from praice.data_handling.db_ops.crud import bulk_upsert_historical_prices
from praice.data_handling.db_ops.symbol_helpers import get_active_symbols


def collect_historical_prices(
    symbol: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    period: str = "max",
) -> List[Dict]:
    """
    Collect historical price data for a given symbol using yfinance.

    Args:
        symbol (str): The stock symbol to collect data for.
        start_date (Optional[datetime]): The start date for historical data.
        end_date (Optional[datetime]): The end date for historical data.
        period (str): The period to fetch data for, used if start_date and end_date are None.
                      Options: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max

    Returns:
        List[Dict]: A list of dictionaries containing historical price data.
    """
    symbol = symbol.upper()
    logger.info(f"Collecting historical prices for {symbol}")

    ticker = yf.Ticker(symbol)

    try:
        if start_date and end_date:
            hist = ticker.history(start=start_date, end=end_date)
        else:
            hist = ticker.history(period=period)

        hist = hist.reset_index()
        hist = hist.rename(columns=str.lower)
        hist = hist.rename(columns={"stock splits": "stock_splits"})
        price_data = hist.to_dict(orient="records")
        bulk_upsert_historical_prices(symbol, price_data)
        return price_data

    except Exception as e:
        logger.error(f"Error collecting historical prices for {symbol}: {str(e)}")
        return []


def update_historical_prices(symbol: str, lookback_days: int = 30) -> int:
    """
    Update historical prices for a given symbol.

    Args:
        symbol (str): The stock symbol to update data for.
        lookback_days (int): The number of days to look back for updating prices.

    Returns:
        int: The number of price records updated or inserted.
    """
    logger.info(f"Updating historical prices for {symbol}")

    end_date = datetime.now(UTC)
    start_date = end_date - timedelta(days=lookback_days)

    price_data = collect_historical_prices(symbol, start_date, end_date)

    if not price_data:
        logger.warning(f"No price data collected for {symbol}")
        return 0

    try:
        updated_count = bulk_upsert_historical_prices(symbol, price_data)
        logger.info(f"Updated {updated_count} historical price records for {symbol}")
        return updated_count

    except Exception as e:
        logger.error(f"Error updating historical prices for {symbol}: {str(e)}")
        return 0


def update_all_symbols_prices(lookback_days: int = 30) -> Dict[str, int]:
    """
    Update historical prices for all active symbols in the database.

    Args:
        lookback_days (int): The number of days to look back for updating prices.

    Returns:
        Dict[str, int]: A dictionary with symbols as keys and the number of updated records as values.
    """
    logger.info("Updating historical prices for all active symbols")

    results = {}
    active_symbols = get_active_symbols()

    for symbol in active_symbols:
        updated_count = update_historical_prices(symbol.symbol, lookback_days)
        results[symbol.symbol] = updated_count

    logger.info(f"Completed updating historical prices for {len(results)} symbols")
    return results
