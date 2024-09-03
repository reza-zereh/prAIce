from typing import Any, Dict, List, Union

from peewee import ModelSelect, fn

from praice.data_handling.models import News, NewsSymbol, Symbol


def find_news_by_symbol(
    symbol: str, limit: int = 50, offset: int = 0, lazy: bool = False
) -> Union[ModelSelect, List[News]]:
    """
    Find news articles associated with a given symbol.

    Args:
        symbol (str): The symbol to search for news articles.
        limit (int, optional): The maximum number of news articles to retrieve.
            Defaults to 50.
        offset (int, optional): The number of news articles to skip.
            Defaults to 0.
        lazy (bool, optional): If True, returns a query object.
            If False, returns a list of news articles. Defaults to False.

    Returns:
        Union[ModelSelect, List[News]]: If lazy is True, returns a query object.
            If lazy is False, returns a list of news articles.
    """
    query = (
        News.select()
        .join(NewsSymbol)
        .join(Symbol)
        .where(Symbol.symbol == symbol.upper())
        .order_by(News.published_at.desc())
        .limit(limit)
        .offset(offset)
    )

    return query if lazy else list(query)


def get_news_count_by_symbol(symbol: str) -> int:
    """
    Returns the count of news articles associated with a given symbol.

    Parameters:
        symbol (str): The symbol for which to retrieve the news count.

    Returns:
        int: The count of news articles associated with the symbol.
    """
    return (
        News.select()
        .join(NewsSymbol)
        .join(Symbol)
        .where(Symbol.symbol == symbol.upper())
        .count()
    )


def search_news(
    query: str, limit: int = 50, offset: int = 0, lazy: bool = False
) -> Union[ModelSelect, List[News]]:
    """
    Search for news articles based on a query.

    Args:
        query (str): The search query.
        limit (int, optional): The maximum number of results to return. Defaults to 50.
        offset (int, optional): The number of results to skip. Defaults to 0.
        lazy (bool, optional): If True, returns a lazy query object. If False, returns a list of News objects. Defaults to False.

    Returns:
        Union[ModelSelect, List[News]]: The search results. If lazy is True, returns a ModelSelect object. If lazy is False, returns a list of News objects.
    """
    search_query = (
        News.select()
        .where((News.title.contains(query)) | (News.content.contains(query)))
        .order_by(News.published_at.desc())
        .limit(limit)
        .offset(offset)
    )

    return search_query if lazy else list(search_query)


def get_news_stats() -> Dict[str, Any]:
    """
    Retrieves statistics about news.

    Returns:
        A dictionary containing the following information:
        - total_news (int): The total number of news.
        - news_by_source (List[Dict[str, Union[str, int]]]): A list of dictionaries representing the count of news by source.
          Each dictionary contains the following information:
          - source (str): The source of the news.
          - count (int): The number of news from the source.
    """
    total_news = News.select().count()
    news_by_source = list(
        News.select(News.source, fn.COUNT(News.id).alias("count"))
        .group_by(News.source)
        .dicts()
    )

    return {"total_news": total_news, "news_by_source": news_by_source}


def get_news_with_null_content(limit: int = 50) -> List[News]:
    """
    Retrieve a list of news articles with null content.

    Args:
        limit (int, optional): The maximum number of news articles to retrieve. Defaults to 50.

    Returns:
        List[News]: A list of news articles with null content.
    """
    return list(News.select().where(News.content.is_null()).limit(limit))


def count_news_with_null_content() -> int:
    """
    Count the number of news articles with null content.

    Returns:
        int: The number of news articles with null content.
    """
    return News.select().where(News.content.is_null()).count()


def count_news_by_symbol(n: int = -1) -> Dict[str, Dict[str, int]]:
    """
    Count the number of news articles for each symbol.

    Args:
        n (int, optional): The number of symbols to return.
            Defaults to -1, which returns all symbols.

    Returns:
        A dictionary containing the count of news articles for each symbol.
        The dictionary has the following structure:
        {
            "news_count_by_symbol": {
                <symbol>: <count>,
                ...
    """
    news_by_symbol = list(
        News.select(Symbol.symbol, fn.COUNT(News.id).alias("count"))
        .join(NewsSymbol)
        .join(Symbol)
        .group_by(Symbol.symbol)
        .order_by(fn.COUNT(News.id).desc())
        .dicts()
    )
    return {
        "news_count_by_symbol": {o["symbol"]: o["count"] for o in news_by_symbol[:n]}
    }


def get_words_count_stats():
    """
    Retrieves statistics about the words_count field in the News table.

    Returns:
        dict: A dictionary containing the following statistics:
            - total_news: Total number of news entries
            - news_with_content: Number of news entries with non-null content
            - news_with_word_count: Number of news entries with non-null words_count
            - avg_word_count: Average word count (for entries with non-null words_count)
            - min_word_count: Minimum word count
            - max_word_count: Maximum word count
    """
    total_news = News.select().count()
    news_with_content = News.select().where(News.content.is_null(False)).count()
    news_with_word_count = News.select().where(News.words_count.is_null(False)).count()

    avg_word_count = News.select(fn.AVG(News.words_count)).scalar()
    min_word_count = News.select(fn.MIN(News.words_count)).scalar()
    max_word_count = News.select(fn.MAX(News.words_count)).scalar()

    return {
        "total_news": total_news,
        "news_with_content": news_with_content,
        "news_with_word_count": news_with_word_count,
        "avg_word_count": int(avg_word_count) if avg_word_count else None,
        "min_word_count": min_word_count,
        "max_word_count": max_word_count,
    }
