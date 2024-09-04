import re


def chunked(iterable, chunk_size):
    """Yield successive chunk_size-sized chunks from iterable."""
    for i in range(0, len(iterable), chunk_size):
        yield iterable[i : i + chunk_size]


def count_words(text: str) -> int:
    """Count the number of words in a text string."""
    return len(re.findall(r"\w+", text))
