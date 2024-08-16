from typing import Dict, List, Optional

import yfinance as yf
from loguru import logger

from praice.data_handling.models import ScrapingUrl, Symbol, db


def add_symbol(
    symbol: str,
    name: str,
    asset_class: str,
    sector: Optional[str] = None,
    industry: Optional[str] = None,
    exchange: Optional[str] = None,
) -> Symbol:
    """Add a new symbol to the database."""
    with db.atomic():
        return Symbol.create(
            symbol=symbol,
            name=name,
            asset_class=asset_class,
            sector=sector,
            industry=industry,
            exchange=exchange,
        )


def list_symbols() -> List[Symbol]:
    """List all symbols in the database."""
    return list(Symbol.select())


def update_symbol(
    symbol: str,
    name: Optional[str] = None,
    asset_class: Optional[str] = None,
    sector: Optional[str] = None,
    industry: Optional[str] = None,
    exchange: Optional[str] = None,
) -> Symbol:
    """Update an existing symbol in the database."""
    sym = Symbol.get(Symbol.symbol == symbol.upper())
    if name:
        sym.name = name
    if asset_class:
        sym.asset_class = asset_class
    if sector is not None:
        sym.sector = sector
    if industry is not None:
        sym.industry = industry
    if exchange is not None:
        sym.exchange = exchange
    sym.save()
    return sym


def delete_symbol(symbol: str) -> bool:
    """Delete a symbol from the database."""
    sym = Symbol.get(Symbol.symbol == symbol.upper())
    return bool(sym.delete_instance())


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


def add_scraping_url(symbol: str, url: str, source: str) -> ScrapingUrl:
    """Add a new scraping URL for a symbol."""
    with db.atomic():
        symbol_obj = Symbol.get(Symbol.symbol == symbol.upper())
        return ScrapingUrl.create(symbol=symbol_obj, url=url, source=source)


def list_scraping_urls(symbol: Optional[str] = None) -> List[ScrapingUrl]:
    """List scraping URLs, optionally filtered by symbol."""
    query = ScrapingUrl.select(ScrapingUrl, Symbol).join(Symbol)
    if symbol:
        query = query.where(Symbol.symbol == symbol.upper())
    return list(query)


def update_scraping_url(
    id: int,
    url: Optional[str] = None,
    source: Optional[str] = None,
    is_active: Optional[bool] = None,
) -> ScrapingUrl:
    """Update an existing scraping URL."""
    scraping_url = ScrapingUrl.get_by_id(id)
    if url:
        scraping_url.url = url
    if source:
        scraping_url.source = source
    if is_active is not None:
        scraping_url.is_active = is_active
    scraping_url.save()
    return scraping_url


def delete_scraping_url(id: int) -> bool:
    """Delete a scraping URL."""
    scraping_url = ScrapingUrl.get_by_id(id)
    return bool(scraping_url.delete_instance())
