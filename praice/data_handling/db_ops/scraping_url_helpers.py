from praice.data_handling.db_ops.crud import get_symbol
from praice.data_handling.models import ScrapingUrl


def get_scraping_url_by_symbol_and_source(symbol: str, source: str) -> ScrapingUrl:
    """
    Retrieves a ScrapingUrl object based on the given symbol and source.

    Args:
        symbol (str): The symbol to search for.
        source (str): The source to search for.

    Returns:
        ScrapingUrl: The ScrapingUrl object matching the symbol and source.

    Raises:
        DoesNotExist: If no ScrapingUrl object is found for the given symbol and source.
    """
    symbol_obj = get_symbol(symbol)
    return ScrapingUrl.get(
        (ScrapingUrl.symbol == symbol_obj) & (ScrapingUrl.source == source)
    )
