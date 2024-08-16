from datetime import UTC, datetime
from typing import Dict, List, Optional

from loguru import logger

from praice.data_handling.crud import get_or_create_symbol
from praice.data_handling.models import News, NewsSymbol, ScrapingUrl, Symbol, db
from praice.data_handling.scrapers.scraper_factory import ScraperFactory


def collect_news_headlines(
    symbol: str, source: str, proxy: Optional[Dict[str, str]] = None
) -> List[Dict[str, str]]:
    """
    Collects news headlines for a given symbol and source.

    Args:
        symbol (str): The symbol for which to collect news headlines.
        source (str): The source from which to collect news headlines.
        proxy (Optional[Dict[str, str]], optional): Proxy configuration. Defaults to None.

    Returns:
        List[Dict[str, str]]: A list of dictionaries representing the collected news headlines.
    """

    logger.info(f"Starting news collection for symbol: {symbol}, source: {source}")

    with db.atomic():
        try:
            symbol_obj = Symbol.get(Symbol.symbol == symbol.upper())
            scraping_url = ScrapingUrl.get(
                (ScrapingUrl.symbol == symbol_obj) & (ScrapingUrl.source == source)
            )
        except Symbol.DoesNotExist:
            logger.error(f"Symbol {symbol} not found in the database.")
            return []
        except ScrapingUrl.DoesNotExist:
            logger.error(
                f"ScrapingUrl not found for symbol {symbol} and source {source}."
            )
            return []

        scraper = ScraperFactory.get_scraper(source=source, proxy=proxy)
        news_items = scraper.scrape_headlines(scraping_url.url)

        for item in news_items:
            News.get_or_create(
                url=item["link"],
                defaults={
                    "title": item["headline"],
                    "source": source,
                    "scraped_at": datetime.now(UTC),
                },
            )

    logger.info(
        f"Completed news collection for symbol: {symbol}, source: {source}. Total items: {len(news_items)}"
    )
    return news_items


def collect_news_articles(proxy: Optional[Dict[str, str]] = None) -> None:
    """
    Collects news articles with null content and scrapes their full content.

    Args:
        proxy (Optional[Dict[str, str]]): A dictionary containing proxy information. Defaults to None.

    Returns:
        None
    """
    logger.info("Starting full article scraping for items with null content")

    with db.atomic():
        news_to_scrape = News.select().where(News.content.is_null(True))

        for news in news_to_scrape:
            try:
                scraper = ScraperFactory.get_scraper(source=news.source, proxy=proxy)
                article_data = scraper.scrape_article(news.url)

                news.content = article_data["content"]
                news.published_at = article_data["published_at"]
                news.scraped_at = datetime.now(UTC)
                news.save()

                # Create NewsSymbol entries for each symbol mentioned in the article
                for symbol in article_data["symbols"]:
                    try:
                        symbol_obj = get_or_create_symbol(symbol)
                        NewsSymbol.get_or_create(news=news, symbol=symbol_obj)
                    except ValueError as e:
                        logger.error(f"Error creating symbol {symbol}: {str(e)}")

            except Exception as e:
                logger.error(f"Error scraping article {news.url}: {str(e)}")

    logger.info("Completed full article scraping")
