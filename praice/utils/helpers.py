import re
import time
from functools import wraps

from loguru import logger


def chunked(iterable, chunk_size):
    """Yield successive chunk_size-sized chunks from iterable."""
    for i in range(0, len(iterable), chunk_size):
        yield iterable[i : i + chunk_size]


def count_words(text: str) -> int:
    """Count the number of words in a text string."""
    return len(re.findall(r"\w+", text))


def log_execution_time(func):
    """
    Decorator that logs the execution time of a function.

    Args:
        func (function): The function to be decorated.

    Returns:
        function: The decorated function.

    Example:
        @log_execution_time
        def my_function():
            # Function code here
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        """
        Wrapper function that logs the execution time of the decorated function.
        """
        func_name = func.__name__
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Function {func_name} took {duration:.2f} seconds to execute")
        return result

    return wrapper
