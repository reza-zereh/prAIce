from datetime import UTC, date, datetime
from typing import Dict, List, Optional, Tuple, Union

from peewee import IntegrityError

from praice.data_handling.models import (
    HistoricalPrice1D,
    News,
    NewsSymbol,
    ScrapingUrl,
    Symbol,
    SymbolConfig,
    db,
)
from praice.utils import helpers

# ############################
# Symbol CRUD operations
# ############################


def get_symbol(symbol: str) -> Symbol:
    """
    Retrieves a Symbol object based on the given symbol.

    Parameters:
        symbol (str): The symbol to search for.

    Returns:
        Symbol: The Symbol object matching the given symbol.

    Raises:
        DoesNotExist: If no Symbol object is found for the given
    """
    return Symbol.get(Symbol.symbol == symbol.upper())


def _ensure_symbol(symbol: Union[Symbol, str]) -> Symbol:
    """
    Ensure that we have a Symbol object.
    If a string is provided, fetch the corresponding Symbol object.
    """
    if isinstance(symbol, str):
        return get_symbol(symbol)
    return symbol


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


# ############################
# SymbolConfig CRUD operations
# ############################


def create_symbol_config(
    symbol: Union[Symbol, str],
    collect_price_data: bool = True,
    collect_yfinance_news: bool = True,
    collect_technical_indicators: bool = True,
    collect_fundamental_data: bool = True,
) -> SymbolConfig:
    """
    Create a new symbol configuration.
    """
    symbol_obj = _ensure_symbol(symbol)
    return SymbolConfig.create(
        symbol=symbol_obj,
        collect_price_data=collect_price_data,
        collect_yfinance_news=collect_yfinance_news,
        collect_technical_indicators=collect_technical_indicators,
        collect_fundamental_data=collect_fundamental_data,
    )


def get_symbol_config(symbol: Union[Symbol, str]) -> SymbolConfig:
    """
    Retrieve the configuration for a specific symbol.
    """
    symbol_obj = _ensure_symbol(symbol)
    return SymbolConfig.get(SymbolConfig.symbol == symbol_obj)


def update_symbol_config(symbol: Union[Symbol, str], **kwargs) -> bool:
    """
    Update the configuration for a specific symbol.
    """
    symbol_obj = _ensure_symbol(symbol)
    query = SymbolConfig.update(**kwargs).where(SymbolConfig.symbol == symbol_obj)
    return query.execute() > 0


def delete_symbol_config(symbol: Union[Symbol, str]) -> bool:
    """
    Delete the configuration for a specific symbol.
    """
    symbol_obj = _ensure_symbol(symbol)
    query = SymbolConfig.delete().where(SymbolConfig.symbol == symbol_obj)
    return query.execute() > 0


def get_or_create_symbol_config(
    symbol: Union[Symbol, str],
) -> Tuple[SymbolConfig, bool]:
    """
    Get the existing symbol configuration or create a new one with default values.
    """
    symbol_obj = _ensure_symbol(symbol)
    return SymbolConfig.get_or_create(symbol=symbol_obj)


# ############################
# ScrapingUrl CRUD operations
# ############################


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


# ############################
# News CRUD operations
# ############################


def create_news(
    title: str,
    url: str,
    source: str,
    content: Optional[str] = None,
    published_at: Optional[datetime] = None,
) -> News:
    """
    Create a news object with the given parameters.

    Args:
        title (str): The title of the news.
        url (str): The URL of the news.
        source (str): The source of the news.
        content (Optional[str], optional): The content of the news.
            Defaults to None.
        published_at (Optional[datetime], optional): The published date and
            time of the news. Defaults to None.

    Returns:
        News: The created news object.
    """
    with db.atomic():
        return News.create(
            title=title,
            url=url,
            source=source,
            content=content,
            published_at=published_at,
            scraped_at=datetime.now(UTC),
        )


def get_or_create_news(
    title: str,
    url: str,
    source: str,
    content: Optional[str] = None,
    published_at: Optional[datetime] = None,
    scraped_at: Optional[datetime] = None,
) -> Tuple[News, bool]:
    """
    Get or create a news object.

    Args:
        title (str): The title of the news.
        url (str): The URL of the news.
        source (str): The source of the news.
        content (Optional[str], optional): The content of the news.
            Defaults to None.
        published_at (Optional[datetime], optional): The published date and
            time of the news. Defaults to None.
        scraped_at (Optional[datetime], optional): The date and time the news was scraped.

    Returns:
        Tuple[News, bool]: A tuple containing the news object and a boolean
            indicating if the news was created.
    """
    return News.get_or_create(
        url=url,
        defaults={
            "title": title,
            "source": source,
            "content": content,
            "published_at": published_at,
            "scraped_at": scraped_at,
        },
    )


def get_news(news_id: int) -> News:
    """
    Retrieve a news item by its ID.

    Parameters:
        news_id (int): The ID of the news item to retrieve.

    Returns:
        News: The news item with the specified ID.

    Raises:
        DoesNotExist: If no news item with the specified ID is found.
    """
    return News.get_by_id(news_id)


def update_news(news_id: int, **kwargs) -> bool:
    """
    Update a news entry in the database.

    Args:
        news_id (int): The ID of the news entry to update.
        **kwargs: Keyword arguments representing the fields to update.

    Returns:
        bool: True if the news entry was successfully updated, False otherwise.
    """
    query = News.update(**kwargs).where(News.id == news_id)
    return query.execute() > 0


def delete_news(news_id: int) -> bool:
    """
    Deletes a news item from the database.

    Args:
        news_id (int): The ID of the news item to be deleted.

    Returns:
        bool: True if the news item was successfully deleted, False otherwise.
    """
    query = News.delete().where(News.id == news_id)
    return query.execute() > 0


# ############################
# NewsSymbol CRUD operations
# ############################


def create_news_symbol(news: News, symbol: Symbol) -> NewsSymbol:
    """
    Create a NewsSymbol object.

    Args:
        news (News): The news object.
        symbol (Symbol): The symbol object.

    Returns:
        NewsSymbol: The created NewsSymbol object.
    """
    return NewsSymbol.create(news=news, symbol=symbol)


def delete_news_symbol(news_symbol_id: int) -> bool:
    """
    Deletes a news symbol from the database.

    Args:
        news_symbol_id (int): The ID of the news symbol to be deleted.

    Returns:
        bool: True if the news symbol was deleted successfully, False otherwise.
    """
    query = NewsSymbol.delete().where(NewsSymbol.id == news_symbol_id)
    return query.execute() > 0


# ############################
# HistoricalPrice CRUD operations
# ############################


def create_historical_price(
    symbol: Union[Symbol, str],
    date: date,
    open_price: float,
    high: float,
    low: float,
    close: float,
    volume: int,
    dividends: float = 0.0,
    stock_splits: float = 0.0,
) -> HistoricalPrice1D:
    """
    Create a new historical price record.

    Args:
        symbol (Union[Symbol, str]): The Symbol object or symbol string.
        date (date): The date of this price record.
        open_price (float): The opening price.
        high (float): The highest price of the day.
        low (float): The lowest price of the day.
        close (float): The closing price.
        volume (int): The trading volume.
        dividends (float, optional): The dividend amount. Defaults to 0.0.
        stock_splits (float, optional): The stock split ratio. Defaults to 0.0.

    Returns:
        HistoricalPrice: The created HistoricalPrice object.
    """
    symbol_obj = _ensure_symbol(symbol)
    return HistoricalPrice1D.create(
        symbol=symbol_obj,
        date=date,
        open=open_price,
        high=high,
        low=low,
        close=close,
        volume=volume,
        dividends=dividends,
        stock_splits=stock_splits,
    )


def get_historical_price(symbol: Union[Symbol, str], date: date) -> HistoricalPrice1D:
    """
    Retrieve a specific historical price record.

    Args:
        symbol (Union[Symbol, str]): The Symbol object or symbol string.
        date (date): The date of the price record.

    Returns:
        HistoricalPrice: The retrieved HistoricalPrice object.

    Raises:
        DoesNotExist: If no matching record is found.
    """
    symbol_obj = _ensure_symbol(symbol)
    return HistoricalPrice1D.get(
        (HistoricalPrice1D.symbol == symbol_obj) & (HistoricalPrice1D.date == date)
    )


def get_historical_prices(
    symbol: Union[Symbol, str], start_date: date = None, end_date: date = None
) -> List[HistoricalPrice1D]:
    """
    Retrieve historical price records for a symbol within a date range.

    Args:
        symbol (Union[Symbol, str]): The Symbol object or symbol string.
        start_date (date, optional): The start date of the range (inclusive).
        end_date (date, optional): The end date of the range (inclusive).

    Returns:
        List[HistoricalPrice]: A list of HistoricalPrice objects.
    """
    symbol_obj = _ensure_symbol(symbol)
    query = HistoricalPrice1D.select().where(HistoricalPrice1D.symbol == symbol_obj)

    if start_date:
        query = query.where(HistoricalPrice1D.date >= start_date)
    if end_date:
        query = query.where(HistoricalPrice1D.date <= end_date)

    return list(query.order_by(HistoricalPrice1D.date))


def update_historical_price(symbol: Union[Symbol, str], date: date, **kwargs) -> bool:
    """
    Update a specific historical price record.

    Args:
        symbol (Union[Symbol, str]): The Symbol object or symbol string.
        date (date): The date of the record to update.
        **kwargs: The fields to update and their new values.

    Returns:
        bool: True if the update was successful, False otherwise.
    """
    symbol_obj = _ensure_symbol(symbol)
    query = HistoricalPrice1D.update(**kwargs).where(
        (HistoricalPrice1D.symbol == symbol_obj) & (HistoricalPrice1D.date == date)
    )
    return query.execute() > 0


def delete_historical_price(symbol: Union[Symbol, str], date: date) -> bool:
    """
    Delete a specific historical price record.

    Args:
        symbol (Union[Symbol, str]): The Symbol object or symbol string.
        date (date): The date of the record to delete.

    Returns:
        bool: True if the deletion was successful, False otherwise.
    """
    symbol_obj = _ensure_symbol(symbol)
    query = HistoricalPrice1D.delete().where(
        (HistoricalPrice1D.symbol == symbol_obj) & (HistoricalPrice1D.date == date)
    )
    return query.execute() > 0


def bulk_upsert_historical_prices(
    symbol: Union[Symbol, str], price_data: List[Dict]
) -> int:
    """
    Bulk upsert (insert or update) historical price records for a symbol.

    Args:
        symbol (Union[Symbol, str]): The Symbol object or symbol string.
        price_data (List[Dict]): A list of dictionaries containing price data.
            Each dict should have keys: date, open, high, low, close, volume, dividends, stock_splits

    Returns:
        int: The number of records inserted or updated.
    """
    symbol_obj = _ensure_symbol(symbol)
    upserted_count = 0

    with db.atomic():
        for batch in helpers.chunked(price_data, 100):  # Process in batches of 100
            data_to_upsert = [{**data, "symbol": symbol_obj} for data in batch]

            for data in data_to_upsert:
                try:
                    HistoricalPrice1D.insert(data).on_conflict(
                        conflict_target=[
                            HistoricalPrice1D.symbol,
                            HistoricalPrice1D.date,
                        ],
                        update={
                            HistoricalPrice1D.open: data["open"],
                            HistoricalPrice1D.high: data["high"],
                            HistoricalPrice1D.low: data["low"],
                            HistoricalPrice1D.close: data["close"],
                            HistoricalPrice1D.volume: data["volume"],
                            HistoricalPrice1D.dividends: data["dividends"],
                            HistoricalPrice1D.stock_splits: data["stock_splits"],
                        },
                    ).execute()
                    upserted_count += 1
                except IntegrityError:
                    # Handle any integrity errors if necessary
                    pass

    return upserted_count
