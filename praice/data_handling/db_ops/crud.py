from datetime import UTC, datetime
from typing import Dict, List, Optional, Tuple

from praice.data_handling.models import News, NewsSymbol, ScrapingUrl, Symbol, db


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
