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
    news_min_words_count=300, summary_max_tokens=200, limit=5
) -> int:
    """
    Populates the content summary of news entries.

    Args:
        news_min_words_count (int): The minimum number of words required for a news entry to be considered.
        summary_max_tokens (int): The maximum number of tokens to include in the summary.
        limit (int): The maximum number of news entries to process.

    Returns:
        int: The total number of news entries whose content summary was updated.
    """
    summarizer = SummarizerFactory.get_summarizer("bart")
    total_updated = 0

    # Query to get N news entries with words_count greater than or equal to news_min_words_count
    query = News.select().where(News.words_count >= news_min_words_count).limit(limit)

    for news in query:
        summary = summarizer.summarize(text=news.content, max_tokens=summary_max_tokens)
        news.content_summary = summary
        news.save()
        total_updated += 1

    return total_updated
