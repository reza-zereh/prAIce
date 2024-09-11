from typing import List, Tuple

from loguru import logger

from praice.data_handling.models import News, db
from praice.libs.summarizers import SummarizerFactory
from praice.utils import helpers


def populate_words_count(batch_size: int = 200) -> int:
    """
    Populates the words_count field for News entries with non-null content.

    This function efficiently processes News entries in batches, updating
    the words_count field for entries with non-null content. It uses a
    regular expression to split the content into words and count them.

    Args:
        batch_size (int): The number of entries to process in each batch.
            Defaults to 200.

    Returns:
        int: The total number of entries updated.
    """
    total_updated = 0

    # Query to get all News entries with non-null content and null words_count
    query = News.select().where(
        (News.content.is_null(False)) & (News.words_count.is_null(True))
    )

    # Process in batches
    for batch in helpers.chunked(iterable=query, chunk_size=batch_size):
        with db.atomic():
            for news in batch:
                word_count = helpers.count_words(text=news.content)
                news.words_count = word_count
                news.save()
                total_updated += 1

    return total_updated


def populate_content_summary(
    news_min_words_count=300, summary_max_tokens=200, limit=5, model="bart"
) -> Tuple[int, List[int]]:
    """
    Populates the content summary of news entries.

    Args:
        news_min_words_count (int): The minimum number of words required for a news entry to be considered.
        summary_max_tokens (int): The maximum number of tokens to include in the summary.
        limit (int): The maximum number of news entries to process.
        model (str): The summarization model to use. Defaults to "bart".

    Returns:
        Tuple[int, List[int]]: A tuple containing the total number of entries updated and a list of news IDs.
    """
    summarizer = SummarizerFactory.get_summarizer(model)
    total_updated = 0
    news_ids = []

    # Query to get News entries with words_count greater than or equal to
    # news_min_words_count and null content_summary
    query = (
        News.select()
        .where(
            (News.words_count >= news_min_words_count)
            & (News.content_summary.is_null(True))
        )
        .limit(limit)
    )

    for news in query:
        logger.info(f"Generating summary for News entry with ID: {news.id}")
        summary = summarizer.summarize(text=news.content, max_tokens=summary_max_tokens)
        news.content_summary = summary
        news.save()
        news_ids.append(news.id)
        total_updated += 1

    return (total_updated, news_ids)


def populate_sentiment_score(limit=5) -> Tuple[int, List[int]]:
    """
    Populates the sentiment score for news articles that have a content summary but do not have a sentiment score.

    Args:
        limit (int): The maximum number of news articles to process. Defaults to 5.

    Returns:
        Tuple[int, List[int]]: A tuple containing the total number of news articles updated and a list of news IDs that were updated.
    """
    total_updated = 0
    news_ids = []
    query = (
        News.select()
        .where(
            (News.content_summary.is_null(False)) & (News.sentiment_score.is_null(True))
        )
        .limit(limit)
    )

    for news in query:
        sentiment_score = helpers.calculate_sentiment_score(text=news.content_summary)
        news.sentiment_score = sentiment_score
        news.save()
        news_ids.append(news.id)
        total_updated += 1

    return (total_updated, news_ids)
