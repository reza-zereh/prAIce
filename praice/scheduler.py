import pytz
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from praice.data_handling.collectors import news_collector, price_collector
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
        price_collector.collect_historical_prices_all(period="5d")
        logger.info("Price data collection job completed successfully")
    except Exception as e:
        logger.error(f"Error in price data collection job: {str(e)}")


def init_scheduler():
    """
    Initializes and starts the scheduler.

    Jobs added:
    - collect_headlines_by_source_job: Collects headlines from the 'yfinance' source every hour.
    - collect_articles_job: Collects articles every 2 hours.

    Returns:
        None
    """

    # Add jobs to the scheduler
    scheduler.add_job(
        collect_headlines_by_source_job,
        trigger="interval",
        minutes=80,
        id="collect_yfinance_headlines",
        kwargs={"source": "yfinance"},
    )
    logger.info("Added job: collect_yfinance_headlines")

    scheduler.add_job(
        collect_articles_job,
        trigger="interval",
        minutes=170,
        id="collect_articles",
    )
    logger.info("Added job: collect_articles")

    scheduler.add_job(
        collect_price_data_job,
        trigger=CronTrigger(hour=18, minute=0, timezone=pytz.timezone("US/Eastern")),
        id="collect_price_data",
    )
    logger.info("Added job: collect_price_data (runs daily at 6:00 PM ET)")

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
