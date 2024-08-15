from typing import List, Optional

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
