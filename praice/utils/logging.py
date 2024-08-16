import sys

from loguru import logger

from praice.config import settings
from praice.constants import PATHS


def setup_logging():
    """
    Set up logging configuration using loguru.
    """
    # Remove the default handler
    logger.remove()

    # Add a new handler with the configured log level
    logger.add(
        sys.stderr,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | <cyan>{name}</cyan>:"
            "<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            "<level>{message}</level>"
        ),
        level=settings.LOG_LEVEL,
        colorize=True,
    )

    # Add a file handler for persistent logging
    logger.add(
        PATHS["logs"] / "praice.log",
        rotation="10 MB",
        retention="1 week",
        compression="zip",
        format=(
            "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | "
            "{name}:{function}:{line} - {message}"
        ),
        level=settings.LOG_LEVEL,
    )

    logger.info(f"Logging initialized with level: {settings.LOG_LEVEL}")
