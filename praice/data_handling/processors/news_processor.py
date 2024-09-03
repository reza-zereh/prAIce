import re

from praice.data_handling.models import News, db
from praice.utils.helpers import chunked


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

    Note:
        This function uses a database transaction for efficiency and
        to ensure data consistency.
    """
    total_updated = 0

    # Query to get all News entries with non-null content and null words_count
    query = News.select().where(
        (News.content.is_null(False)) & (News.words_count.is_null(True))
    )

    # Process in batches
    for batch in chunked(iterable=query, chunk_size=batch_size):
        with db.atomic():
            for news in batch:
                # Count words using regex (splits on whitespace)
                word_count = len(re.findall(r"\w+", news.content))
                news.words_count = word_count
                news.save()
                total_updated += 1

    return total_updated
