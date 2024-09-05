from datetime import datetime, timedelta

import pytz
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from praice.data_handling.collectors import (
    fundamental_collector,
    news_collector,
    price_collector,
)
from praice.data_handling.db_ops import ta_helpers
from praice.data_handling.processors import news_processor
from praice.utils.logging import get_scheduler_logger

# Get the scheduler-specific logger
logger = get_scheduler_logger()

# Set up the job stores
jobstores = {"default": MemoryJobStore()}

# Set up the executor
executors = {"default": ThreadPoolExecutor(20)}

# Create the scheduler
scheduler = BackgroundScheduler(jobstores=jobstores, executors=executors)


def collect_headlines_by_source_job(source: str):
    """
    Collects headlines from a specific source.

    Args:
        source (str): The source from which to collect headlines.
    """
    logger.info(f"Starting headline collection job from source {source}")
    try:
        # Collects news headlines from the specified source
        news_collector.collect_news_headlines_by_source(source)
        logger.info("Headline collection job completed successfully")
    except Exception as e:
        logger.error(f"Error in headline collection job: {str(e)}")


def collect_articles_job():
    """
    Executes the article collection job.
    """
    logger.info("Starting article collection job")
    try:
        # Collects news articles with null content and scrapes their full content
        news_collector.collect_news_articles(limit=100)
        logger.info("Article collection job completed successfully")
    except Exception as e:
        logger.error(f"Error in article collection job: {str(e)}")


def collect_price_data_job():
    """
    Executes the price data collection job.
    """
    logger.info("Starting price data collection job")
    try:
        # Collect historical price data for all symbols that have collect_price_data
        # set to True in the SymbolConfig table for the last 5 days
        price_collector.collect_historical_prices_all(period="5d")
        logger.info("Price data collection job completed successfully")
    except Exception as e:
        logger.error(f"Error in price data collection job: {str(e)}")


def calculate_and_store_technical_analysis_job():
    """
    Executes the technical analysis calculation and storage job.
    """
    logger.info("Starting technical analysis calculation and storage job")
    try:
        end_date = datetime.now(
            tz=pytz.timezone("US/Eastern")
        ).date()  # get today's date
        start_date = end_date - timedelta(days=2)  # get the date 2 days ago
        # Calculate and store technical analysis for all symbols
        # that have collect_technical_indicators set to True in the SymbolConfig table
        ta_helpers.calculate_and_store_technical_analysis_for_all_symbols(
            start_date=start_date, end_date=end_date
        )
        logger.info(
            "Technical analysis calculation and storage job completed successfully"
        )
    except Exception as e:
        logger.error(
            f"Error in technical analysis calculation and storage job: {str(e)}"
        )


def collect_and_store_fundamental_data_job():
    """
    Executes the fundamental data collection and storage job.
    """
    logger.info("Starting fundamental data collection and storage job")
    try:
        # Collect and store fundamental data for all symbols
        # that have collect_fundamental_data set to True in the SymbolConfig table
        fundamental_collector.collect_and_store_fundamental_data_for_all_symbols()
        logger.info(
            "Fundamental data collection and storage job completed successfully"
        )
    except Exception as e:
        logger.error(f"Error in fundamental data collection and storage job: {str(e)}")


def populate_news_words_count_job():
    """
    Executes the news words count population job.
    """
    logger.info("Starting news words count population job")
    try:
        # Populate the words_count field for News entries with non-null content
        news_processor.populate_words_count()
        logger.info("News words count population job completed successfully")
    except Exception as e:
        logger.error(f"Error in news words count population job: {str(e)}")


def generate_news_summaries_job(limit: int = 5, model: str = "bart"):
    """
    Executes the news summaries generation job.
    """
    logger.info("Starting news summaries generation job")
    try:
        # Generate content summaries for News entries with words_count greater than or equal to 300
        news_processor.populate_content_summary(limit=limit, model=model)
        logger.info("News summaries generation job completed successfully")
    except Exception as e:
        logger.error(f"Error in news summaries generation job: {str(e)}")


def init_scheduler():
    """
    Initializes and starts the scheduler.

    Jobs:
        - collect_headlines_by_source_job: Collects headlines from the 'yfinance' source.
        - collect_articles_job: Collects news articles.
        - collect_price_data_job: Collects price data.
        - calculate_and_store_technical_analysis_job: Calculates and stores technical analysis.

    Returns:
        None
    """

    # Add jobs to the scheduler

    # Collect headlines from the 'yfinance' source every 80 minutes
    scheduler.add_job(
        collect_headlines_by_source_job,
        trigger="interval",
        minutes=80,
        id="collect_yfinance_headlines",
        kwargs={"source": "yfinance"},
    )
    logger.info("Added job: collect_yfinance_headlines")

    # Collect news articles every 170 minutes
    scheduler.add_job(
        collect_articles_job,
        trigger="interval",
        minutes=170,
        id="collect_articles",
    )
    logger.info("Added job: collect_articles")

    # Collect price data daily at 6:00 PM ET
    scheduler.add_job(
        collect_price_data_job,
        trigger=CronTrigger(hour=18, minute=0, timezone=pytz.timezone("US/Eastern")),
        id="collect_price_data",
    )
    logger.info("Added job: collect_price_data (runs daily at 6:00 PM ET)")

    # Calculate and store technical analysis daily at 6:30 PM ET
    scheduler.add_job(
        calculate_and_store_technical_analysis_job,
        trigger=CronTrigger(hour=18, minute=30, timezone=pytz.timezone("US/Eastern")),
        id="calculate_and_store_technical_analysis",
    )
    logger.info(
        "Added job: calculate_and_store_technical_analysis (runs daily at 6:30 PM ET)"
    )

    # Collect and store fundamental data monthly on the 1st day of the month at 7:00 PM ET
    scheduler.add_job(
        collect_and_store_fundamental_data_job,
        trigger=CronTrigger(
            day=1, hour=19, minute=0, timezone=pytz.timezone("US/Eastern")
        ),
        id="collect_and_store_fundamental_data",
    )
    logger.info(
        "Added job: collect_and_store_fundamental_data "
        "(runs monthly on the 1st day of the month at 7:00 PM ET)"
    )

    # Generate news summaries every 15 minutes for the 5 news entries
    scheduler.add_job(
        generate_news_summaries_job,
        trigger="interval",
        minutes=15,
        kwargs={"limit": 5, "model": "bart"},
        id="generate_news_summaries",
    )
    logger.info("Added job: generate_news_summaries")

    # Populate news words count once every 24 hours
    scheduler.add_job(
        populate_news_words_count_job,
        trigger=CronTrigger(hour=0, minute=0),
        id="populate_news_words_count",
    )
    logger.info("Added job: populate_news_words_count (runs daily at 12:00 AM)")

    # Start the scheduler
    scheduler.start()
    logger.info("Scheduler started")


if __name__ == "__main__":
    init_scheduler()

    try:
        # Keep the script running
        while True:
            pass
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Scheduler shut down")
