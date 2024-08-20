from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.background import BackgroundScheduler

from praice.data_handling.collectors.news_collector import (
    collect_news_articles,
    collect_news_headlines_by_source,
)
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

    Returns:
        None

    Raises:
        Exception: If there is an error in the headline collection job.
    """
    logger.info(f"Starting headline collection job from source {source}")
    try:
        collect_news_headlines_by_source(source)
        logger.info("Headline collection job completed successfully")
    except Exception as e:
        logger.error(f"Error in headline collection job: {str(e)}")


def collect_articles_job():
    """
    Executes the article collection job.

    Returns:
        None

    Raises:
        Exception: If an error occurs during the article collection job.

    """
    logger.info("Starting article collection job")
    try:
        collect_news_articles(limit=100)
        logger.info("Article collection job completed successfully")
    except Exception as e:
        logger.error(f"Error in article collection job: {str(e)}")


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
        "interval",
        minutes=80,
        id="collect_yfinance_headlines",
        kwargs={"source": "yfinance"},
    )
    logger.info("Added job: collect_yfinance_headlines")

    scheduler.add_job(
        collect_articles_job, "interval", minutes=170, id="collect_articles"
    )
    logger.info("Added job: collect_articles")

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
