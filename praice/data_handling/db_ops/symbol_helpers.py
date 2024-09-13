from typing import Dict

import yfinance as yf
from loguru import logger

from praice.data_handling.db_ops.crud import add_symbol
from praice.data_handling.models import Symbol


def get_or_create_symbol(symbol: str) -> Symbol:
    """
    Get an existing symbol from the database or create a new one with info from Yahoo Finance.

    Args:
        symbol (str): The stock symbol to get or create.

    Returns:
        Symbol: The Symbol object from the database.
    """
    symbol = symbol.upper()

    try:
        return Symbol.get(Symbol.symbol == symbol)
    except Symbol.DoesNotExist:
        logger.info(
            f"Symbol {symbol} not found in database. Fetching info from Yahoo Finance."
        )
        return create_symbol_from_yahoo(symbol)


def create_symbol_from_yahoo(symbol: str) -> Symbol:
    """
    Create a new Symbol object with information fetched from Yahoo Finance.

    Args:
        symbol (str): The stock symbol to create.

    Returns:
        Symbol: The newly created Symbol object.

    Raises:
        ValueError: If unable to fetch the required information from Yahoo Finance.
    """
    ticker = yf.Ticker(symbol)
    info = ticker.info

    if not info:
        raise ValueError(
            f"Unable to fetch information for symbol {symbol} from Yahoo Finance."
        )

    new_symbol = add_symbol(
        symbol=symbol,
        name=info.get("longName", info.get("shortName", "Unknown")),
        asset_class=get_asset_class(info),
        sector=info.get("sector"),
        industry=info.get("industry"),
        exchange=info.get("exchange"),
        description=info.get("longBusinessSummary"),
    )

    logger.info(f"Created new symbol: {new_symbol.symbol}")
    return new_symbol


def get_asset_class(info: Dict[str, any]) -> str:
    """
    Determine the asset class based on the information from Yahoo Finance.

    Args:
        info (Dict[str, any]): The info dictionary from yfinance.

    Returns:
        str: The determined asset class.
    """
    if "quoteType" in info:
        quote_type = info["quoteType"].lower()
        if quote_type == "equity":
            return "stock"
        elif quote_type == "future":
            return "futures"
        elif quote_type in ["etf", "mutualfund", "currency", "commodity"]:
            return quote_type

    # Default to 'stock' if we can't determine the asset class
    return "stock"


def get_active_symbols() -> list:
    """
    Get all active symbols from the database.

    Returns:
        list: A list of active Symbol objects.
    """
    return list(Symbol.select().where(Symbol.is_active == True))  # noqa: E712
