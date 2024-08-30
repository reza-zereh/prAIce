from datetime import UTC, datetime, timedelta
from typing import Optional

import typer
from peewee import DoesNotExist, IntegrityError
from rich import print as rprint
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from praice.data_handling.collectors import fundamental_collector as fdc
from praice.data_handling.collectors import price_collector
from praice.data_handling.collectors.news_collector import (
    collect_news_articles,
    collect_news_headlines,
)
from praice.data_handling.db_ops import crud, news_helpers, ta_helpers
from praice.data_handling.models import db
from praice.utils import logging

# Set up logging
logging.setup_logging()

app = typer.Typer()
symbol_app = typer.Typer()
symbol_config_app = typer.Typer()
scraping_url_app = typer.Typer()
news_app = typer.Typer()
price_app = typer.Typer()
ta_app = typer.Typer()
fd_app = typer.Typer()

app.add_typer(symbol_app, name="symbol", help="Symbol commands")
app.add_typer(symbol_config_app, name="symbol-config", help="Symbol Config commands")
app.add_typer(scraping_url_app, name="scraping-url", help="Scraping URL commands")
app.add_typer(news_app, name="news", help="News commands")
app.add_typer(price_app, name="price", help="Price commands")
app.add_typer(ta_app, name="ta", help="Technical Analysis commands")
app.add_typer(fd_app, name="fd", help="Fundamental Data commands")


# #################
# Symbol commands
# #################


@symbol_app.command("add")
def cli_add_symbol(
    symbol: str = typer.Option(..., prompt=True),
    name: str = typer.Option(..., prompt=True),
    asset_class: str = typer.Option(..., prompt=True),
    sector: Optional[str] = typer.Option(None),
    industry: Optional[str] = typer.Option(None),
    exchange: Optional[str] = typer.Option(None),
):
    """Add a new symbol to the database."""

    # Prompt for optional fields only if not provided
    if sector is None:
        sector = typer.prompt("Sector", default="")
    if industry is None:
        industry = typer.prompt("Industry", default="")
    if exchange is None:
        exchange = typer.prompt("Exchange", default="")

    # Convert empty strings to None
    sector = sector or None
    industry = industry or None
    exchange = exchange or None

    try:
        new_symbol = crud.add_symbol(
            symbol, name, asset_class, sector, industry, exchange
        )
        rprint(f"[green]Symbol {new_symbol.symbol} added successfully.[/green]")
    except Exception as e:
        rprint(f"[red]Error adding symbol: {str(e)}[/red]")


@symbol_app.command("list")
def cli_list_symbols():
    """List all symbols in the database."""
    symbols = crud.list_symbols()
    table = Table(title="Symbols")
    table.add_column("Symbol", style="cyan")
    table.add_column("Name", style="magenta")
    table.add_column("Asset Class", style="green")
    table.add_column("Sector", style="yellow")
    table.add_column("Industry", style="blue")
    table.add_column("Exchange", style="red")

    for symbol in symbols:
        table.add_row(
            symbol.symbol,
            symbol.name,
            symbol.asset_class,
            symbol.sector or "",
            symbol.industry or "",
            symbol.exchange or "",
        )

    rprint(table)


@symbol_app.command("update")
def cli_update_symbol(
    symbol: str = typer.Argument(..., help="Symbol to update"),
    name: Optional[str] = typer.Option(None),
    asset_class: Optional[str] = typer.Option(None),
    sector: Optional[str] = typer.Option(None),
    industry: Optional[str] = typer.Option(None),
    exchange: Optional[str] = typer.Option(None),
):
    """Update an existing symbol in the database."""
    try:
        crud.update_symbol(symbol, name, asset_class, sector, industry, exchange)
        rprint(f"[green]Symbol {symbol} updated successfully.[/green]")
    except Exception as e:
        rprint(f"[red]Error updating symbol: {str(e)}[/red]")


@symbol_app.command("delete")
def cli_delete_symbol(symbol: str = typer.Argument(..., help="Symbol to delete")):
    """Delete a symbol from the database."""
    try:
        if crud.delete_symbol(symbol):
            rprint(f"[green]Symbol {symbol} deleted successfully.[/green]")
        else:
            rprint(f"[red]Symbol {symbol} not found.[/red]")
    except Exception as e:
        rprint(f"[red]Error deleting symbol: {str(e)}[/red]")


# #################
# Symbol Config commands
# #################


@symbol_config_app.command("show")
def show_symbol_config(symbol: str):
    """
    Show the configuration for a specific symbol.
    """
    try:
        config = crud.get_symbol_config(symbol)
        table = Table(title=f"Configuration for {symbol}")
        table.add_column("Setting", style="cyan")
        table.add_column("Value", style="magenta")

        table.add_row("Collect Price Data", str(config.collect_price_data))
        table.add_row("Collect YFinance News", str(config.collect_yfinance_news))
        table.add_row(
            "Collect Technical Indicators", str(config.collect_technical_indicators)
        )
        table.add_row("Collect Fundamental Data", str(config.collect_fundamental_data))
        table.add_row("Custom Settings", config.custom_settings or "None")

        rprint(table)
    except DoesNotExist:
        rprint(f"[red]No configuration found for symbol {symbol}[/red]")


@symbol_config_app.command("create")
def create_symbol_config_cli(
    symbol: str,
    collect_price_data: bool = typer.Option(
        True, "--price/--no-price", help="Collect price data"
    ),
    collect_yfinance_news: bool = typer.Option(
        True, "--news/--no-news", help="Collect YFinance news"
    ),
    collect_technical_indicators: bool = typer.Option(
        True, "--tech/--no-tech", help="Collect technical indicators"
    ),
    collect_fundamental_data: bool = typer.Option(
        True, "--fund/--no-fund", help="Collect fundamental data"
    ),
):
    """
    Create a new configuration for a symbol.
    """
    try:
        crud.create_symbol_config(
            symbol,
            collect_price_data=collect_price_data,
            collect_yfinance_news=collect_yfinance_news,
            collect_technical_indicators=collect_technical_indicators,
            collect_fundamental_data=collect_fundamental_data,
        )
        rprint(f"[green]Configuration created successfully for {symbol}[/green]")
    except IntegrityError:
        rprint(f"[red]Configuration already exists for symbol {symbol}[/red]")
    except DoesNotExist:
        rprint(f"[red]Symbol {symbol} not found in the database[/red]")


@symbol_config_app.command("update")
def update_symbol_config_cli(
    symbol: str,
    collect_price_data: Optional[bool] = typer.Option(
        None, "--price/--no-price", help="Collect price data"
    ),
    collect_yfinance_news: Optional[bool] = typer.Option(
        None, "--news/--no-news", help="Collect YFinance news"
    ),
    collect_technical_indicators: Optional[bool] = typer.Option(
        None, "--tech/--no-tech", help="Collect technical indicators"
    ),
    collect_fundamental_data: Optional[bool] = typer.Option(
        None, "--fund/--no-fund", help="Collect fundamental data"
    ),
):
    """
    Update the configuration for a symbol.
    """
    update_data = {}
    if collect_price_data is not None:
        update_data["collect_price_data"] = collect_price_data
    if collect_yfinance_news is not None:
        update_data["collect_yfinance_news"] = collect_yfinance_news
    if collect_technical_indicators is not None:
        update_data["collect_technical_indicators"] = collect_technical_indicators
    if collect_fundamental_data is not None:
        update_data["collect_fundamental_data"] = collect_fundamental_data

    if update_data:
        if crud.update_symbol_config(symbol, **update_data):
            rprint(f"[green]Configuration updated successfully for {symbol}[/green]")
        else:
            rprint(f"[red]Failed to update configuration for {symbol}[/red]")
    else:
        rprint("[yellow]No updates specified[/yellow]")


@symbol_config_app.command("delete")
def delete_symbol_config_cli(symbol: str):
    """
    Delete the configuration for a symbol.
    """
    if crud.delete_symbol_config(symbol):
        rprint(f"[green]Configuration deleted successfully for {symbol}[/green]")
    else:
        rprint(f"[red]Failed to delete configuration for {symbol}[/red]")


@symbol_config_app.command("list")
def list_symbol_configs():
    """
    List all symbol configurations.
    """
    configs = crud.list_symbol_configs()
    table = Table(title="Symbol Configurations")
    table.add_column("Symbol", style="cyan")
    table.add_column("Collect Price Data", style="magenta")
    table.add_column("Collect YFinance News", style="green")
    table.add_column("Collect Technical Indicators", style="yellow")
    table.add_column("Collect Fundamental Data", style="blue")

    for config in configs:
        table.add_row(
            config.symbol.symbol,
            str(config.collect_price_data),
            str(config.collect_yfinance_news),
            str(config.collect_technical_indicators),
            str(config.collect_fundamental_data),
        )

    rprint(table)


# #################
# Scraping URL commands
# #################


@scraping_url_app.command("add")
def cli_add_scraping_url(
    symbol: str = typer.Option(..., prompt=True),
    url: str = typer.Option(..., prompt=True),
    source: str = typer.Option(..., prompt=True),
):
    """Add a new scraping URL for a symbol."""
    try:
        new_url = crud.add_scraping_url(symbol, url, source)
        rprint(
            f"[green]Scraping URL for {new_url.symbol.symbol} added successfully.[/green]"
        )
    except Exception as e:
        rprint(f"[red]Error adding scraping URL: {str(e)}[/red]")


@scraping_url_app.command("add-yfinance")
def cli_add_yfinance_scraping_url(
    symbol: str = typer.Option(..., prompt=True),
):
    """Add a new scraping URL for a symbol using Yahoo Finance."""
    try:
        symbol_obj = crud.get_symbol(symbol)
        url = f"https://finance.yahoo.com/quote/{symbol_obj.symbol}/news/"
        new_url = crud.add_scraping_url(symbol, url, "yfinance")
        rprint(
            f"[green]Scraping URL for {new_url.symbol.symbol} added successfully.[/green]"
        )
    except DoesNotExist:
        rprint(f"[red]Symbol {symbol} not found in the database[/red]")
    except IntegrityError:
        rprint(f"[red]Scraping URL already exists for symbol {symbol}[/red]")
    except Exception as e:
        rprint(f"[red]Error adding scraping URL: {str(e)}[/red]")


@scraping_url_app.command("add-yfinance-all")
def cli_add_yfinance_scraping_url_all_symbol_configs():
    """
    Add scraping URLs for all symbols that have yfinance news collection enabled in `symbol-config` table.
    """
    configs = crud.list_symbol_configs()
    for config in configs:
        if config.collect_yfinance_news:
            try:
                url = f"https://finance.yahoo.com/quote/{config.symbol.symbol}/news/"
                new_url = crud.add_scraping_url(config.symbol.symbol, url, "yfinance")
                rprint(
                    f"[green]Scraping URL for {new_url.symbol.symbol} added successfully.[/green]"
                )
            except IntegrityError:
                rprint(
                    f"[red]Scraping URL already exists for symbol {config.symbol.symbol}[/red]"
                )
            except Exception as e:
                rprint(f"[red]Error adding scraping URL: {str(e)}[/red]")


@scraping_url_app.command("list")
def cli_list_scraping_urls(
    symbol: Optional[str] = typer.Option(None, help="Filter by symbol"),
):
    """List scraping URLs, optionally filtered by symbol."""
    urls = crud.list_scraping_urls(symbol)
    table = Table(title="Scraping URLs")
    table.add_column("ID", style="cyan")
    table.add_column("Symbol", style="magenta")
    table.add_column("URL", style="green")
    table.add_column("Source", style="yellow")
    table.add_column("Is Active", style="blue")

    for url in urls:
        table.add_row(
            str(url.id),
            url.symbol.symbol,
            url.url,
            url.source,
            str(url.is_active),
        )

    rprint(table)


@scraping_url_app.command("update")
def cli_update_scraping_url(
    id: int = typer.Argument(..., help="ID of the scraping URL to update"),
    url: Optional[str] = typer.Option(None),
    source: Optional[str] = typer.Option(None),
    is_active: Optional[bool] = typer.Option(None),
):
    """Update an existing scraping URL."""
    try:
        crud.update_scraping_url(id, url, source, is_active)
        rprint(f"[green]Scraping URL (ID: {id}) updated successfully.[/green]")
    except Exception as e:
        rprint(f"[red]Error updating scraping URL: {str(e)}[/red]")


@scraping_url_app.command("delete")
def cli_delete_scraping_url(
    id: int = typer.Argument(..., help="ID of the scraping URL to delete"),
):
    """Delete a scraping URL."""
    try:
        if crud.delete_scraping_url(id):
            rprint(f"[green]Scraping URL (ID: {id}) deleted successfully.[/green]")
        else:
            rprint(f"[red]Scraping URL with ID {id} not found.[/red]")
    except Exception as e:
        rprint(f"[red]Error deleting scraping URL: {str(e)}[/red]")


# #################
# News commands
# #################


@news_app.command("collect-headlines")
def cli_collect_news_headlines(
    symbol: str = typer.Argument(..., help="Symbol to collect news for"),
    source: str = typer.Argument(..., help="Source to collect news from"),
    use_proxy: bool = typer.Option(False, "--proxy", help="Use a proxy server"),
):
    """Collect news headlines for a given symbol and source."""
    proxy = None
    if use_proxy:
        # TODO: You might want to load these from environment variables or a config file
        proxy = {
            "http": "http://your-proxy-server:port",
            "https": "https://your-proxy-server:port",
        }

    try:
        collect_news_headlines(symbol=symbol, source=source, proxy=proxy)
    except Exception as e:
        rprint(f"[red]Error collecting news headlines: {str(e)}[/red]")


@news_app.command("collect-articles")
def cli_collect_news_articles(
    use_proxy: bool = typer.Option(False, "--proxy", help="Use a proxy server"),
    limit: int = typer.Option(
        50, "--limit", help="Limit the number of articles to scrape"
    ),
):
    """Collect full content for news articles with null content."""
    proxy = None
    if use_proxy:
        # TODO: You might want to load these from environment variables or a config file
        proxy = {
            "http": "http://your-proxy-server:port",
            "https": "https://your-proxy-server:port",
        }

    try:
        collect_news_articles(proxy=proxy, limit=limit)
    except Exception as e:
        # logger.error(f"Error during full article collection: {str(e)}")
        rprint(f"[red]Error during full article collection: {str(e)}[/red]")


@news_app.command("count-null-content")
def cli_count_news_with_null_content():
    """Count the number of news articles with null content."""
    count = news_helpers.count_news_with_null_content()
    rprint(f"[cyan]Number of news articles with null content: {count}[/cyan]")


@news_app.command("count-by-symbol")
def cli_count_news_by_symbol(
    n: int = typer.Option(-1, "-n", help="Number of symbols to return"),
):
    """Count the number of news articles for each symbol."""
    counts = news_helpers.count_news_by_symbol(n)["news_count_by_symbol"]

    table = Table(title="News Counts by Symbol")
    table.add_column("Symbol", style="cyan")
    table.add_column("Count", style="magenta")

    for symbol, count in counts.items():
        table.add_row(symbol, str(count))

    rprint(table)


# #################
# Price commands
# #################


@price_app.command("collect")
def cli_collect_prices(
    symbol: str = typer.Argument(..., help="The stock symbol to collect data for"),
    days: Optional[int] = typer.Option(None, help="Number of days to collect data for"),
    period: Optional[str] = typer.Option(
        "max",
        help=(
            "Period to collect data for. "
            "Choices: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max"
        ),
    ),
):
    """
    Collect historical price data for a given symbol.
    """

    if days:
        end_date = datetime.now(UTC)
        start_date = end_date - timedelta(days=days)
        price_data = price_collector.collect_historical_prices(
            symbol=symbol, start_date=start_date, end_date=end_date
        )
    else:
        price_data = price_collector.collect_historical_prices(
            symbol=symbol, period=period
        )

    if not price_data:
        rprint(f"[red]No price data collected for {symbol}[/red]")
        return

    rprint(f"[green]Collected {len(price_data)} price records for {symbol}[/green]")


@price_app.command("collect-all")
def cli_collect_all_prices(
    days: Optional[int] = typer.Option(None, help="Number of days to collect data for"),
    period: Optional[str] = typer.Option(
        "max",
        help=(
            "Period to collect data for. "
            "Choices: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max"
        ),
    ),
):
    """
    Collect historical price data for all symbols
    that have price data collection enabled in `symbol_configs`.
    """
    try:
        if days:
            end_date = datetime.now(UTC)
            start_date = end_date - timedelta(days=days)
            results = price_collector.collect_historical_prices_all(
                start_date=start_date, end_date=end_date
            )
        else:
            results = price_collector.collect_historical_prices_all(period=period)

        rprint(f"[green]Collected prices for {len(results)} symbols[/green]")
    except Exception as e:
        rprint(f"[red]Error collecting prices: {str(e)}[/red]")


@price_app.command("update")
def cli_update_prices(
    symbol: str = typer.Argument(..., help="The stock symbol to update data for"),
    days: int = typer.Option(
        30, help="Number of days to look back for updating prices"
    ),
):
    """
    Update historical prices for a given symbol in the database.
    """
    updated_count = price_collector.update_historical_prices(symbol, days)
    rprint(f"[green]Updated {updated_count} price records for {symbol}[/green]")


@price_app.command("update-all")
def cli_update_all_prices(
    days: int = typer.Option(
        30, help="Number of days to look back for updating prices"
    ),
):
    """
    Update historical prices for all active symbols in the database.
    """
    results = price_collector.update_all_symbols_prices(days)
    total_updated = sum(results.values())
    rprint(
        f"[green]Updated prices for {len(results)} symbols. Total records updated: {total_updated}[/green]"
    )


@price_app.command("show")
def cli_show_prices(
    symbol: str = typer.Argument(..., help="The stock symbol to show prices for"),
    days: int = typer.Option(30, help="Number of days of price history to show"),
):
    """
    Show historical prices for a given symbol.
    """
    end_date = datetime.now(UTC).date()
    start_date = end_date - timedelta(days=days)

    prices = crud.get_historical_prices(symbol, start_date, end_date)

    if not prices:
        rprint(f"[red]No price data found for {symbol} in the last {days} days[/red]")
        return

    table = Table(title=f"Historical Prices for {symbol}")
    table.add_column("Date", style="cyan")
    table.add_column("Open", style="magenta")
    table.add_column("High", style="green")
    table.add_column("Low", style="red")
    table.add_column("Close", style="blue")
    table.add_column("Volume", style="yellow")

    for price in prices:
        table.add_row(
            str(price.date),
            f"{price.open:.2f}",
            f"{price.high:.2f}",
            f"{price.low:.2f}",
            f"{price.close:.2f}",
            f"{price.volume:,}",
        )

    rprint(table)


# #################
# TechnicalAnalysis commands
# #################


@ta_app.command("calculate")
def cli_calculate_ta(
    symbol: str = typer.Argument(..., help="The stock symbol to calculate TA for"),
    days: int = typer.Option(None, help="Number of days to calculate TA for"),
):
    """
    Calculate and store technical analysis data for a given symbol.
    """

    if days:
        end_date = datetime.now(UTC).date()
        start_date = end_date - timedelta(days=days)
    else:
        start_date = None
        end_date = None

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
            progress.add_task(description="Processing...", total=None)
            upsert_count = ta_helpers.calculate_and_store_technical_analysis(
                symbol, start_date, end_date
            )

        rprint(
            f"[green]Calculated and stored {upsert_count} records for {symbol}[/green]"
        )
    except Exception as e:
        rprint(f"[red]Error calculating technical analysis: {str(e)}[/red]")


@ta_app.command("calculate-all")
def cli_calculate_all_ta(
    days: int = typer.Option(None, help="Number of days to calculate TA for"),
):
    """
    Calculate and store technical analysis data for all symbols.
    """
    if days:
        end_date = datetime.now(UTC).date()
        start_date = end_date - timedelta(days=days)
    else:
        start_date = None
        end_date = None

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
            progress.add_task(description="Processing...", total=None)
            upsert_count = (
                ta_helpers.calculate_and_store_technical_analysis_for_all_symbols(
                    start_date=start_date, end_date=end_date
                )
            )

        rprint(
            f"[green]Calculated and stored {upsert_count} records for all symbols[/green]"
        )
    except Exception as e:
        rprint(f"[red]Error calculating technical analysis: {str(e)}[/red]")


@ta_app.command("delete")
def cli_delete_ta(
    symbol: str = typer.Argument(..., help="The stock symbol to delete TA for"),
    timeframe: str = typer.Option(
        "1D", "--timeframe", help="Timeframe to delete TA for"
    ),
):
    """
    Delete technical analysis data for a given symbol and timeframe.
    """
    try:
        deleted_count = ta_helpers.delete_technical_analysis_by_symbol(
            symbol, timeframe=timeframe
        )
        rprint(
            f"[green]Deleted {deleted_count} records for {symbol} timeframe {timeframe}[/green]"
        )
    except Exception as e:
        rprint(f"[red]Error deleting technical analysis: {str(e)}[/red]")


# #################
# FundamentalData commands
# #################


@fd_app.command("collect")
def cli_collect_fundamental_data(
    symbol: str = typer.Argument(
        ..., help="The stock symbol to collect fundamental data for"
    ),
):
    """
    Collect and store fundamental data for a given symbol.
    """
    try:
        fdc.collect_and_store_fundamental_data(symbol)
        rprint(
            f"[green]Successfully collected and stored fundamental data for {symbol}[/green]"
        )
    except Exception as e:
        rprint(f"[red]Error collecting fundamental data for {symbol}: {str(e)}[/red]")


@fd_app.command("collect-all")
def cli_collect_all_fundamental_data():
    """
    Collect and store fundamental data for all symbols with fundamental data collection enabled.
    """
    try:
        fdc.collect_and_store_fundamental_data_for_all_symbols()
        rprint(
            "[green]Successfully collected and stored fundamental data for all eligible symbols[/green]"
        )
    except Exception as e:
        rprint(
            f"[red]Error collecting fundamental data for all symbols: {str(e)}[/red]"
        )


@fd_app.command("delete")
def cli_delete_fundamental_data(
    symbol: str = typer.Argument(
        ..., help="The stock symbol to delete fundamental data for"
    ),
    date: str = typer.Option(
        None, help="The date of the fundamental data to delete (YYYY-MM-DD)"
    ),
    period: str = typer.Option(
        None, help="The period of the fundamental data to delete (annual or quarterly)"
    ),
):
    """
    Delete fundamental data for a given symbol, optionally filtered by date and period.
    """
    try:
        if date:
            date = datetime.strptime(date, "%Y-%m-%d").date()

        if period and period not in ["annual", "quarterly"]:
            raise ValueError("Period must be either 'annual' or 'quarterly'")

        if date and period:
            deleted = crud.delete_fundamental_data(symbol, date, period)
        elif date:
            deleted = crud.delete_fundamental_data(
                symbol, date, "annual"
            ) + crud.delete_fundamental_data(symbol, date, "quarterly")
        elif period:
            all_data = crud.get_fundamental_data(symbol, period=period)
            deleted = sum(
                crud.delete_fundamental_data(symbol, item.date, period)
                for item in all_data
            )
        else:
            all_data = crud.get_fundamental_data(symbol)
            deleted = sum(
                crud.delete_fundamental_data(symbol, item.date, item.period)
                for item in all_data
            )

        if deleted:
            rprint(
                f"[green]Successfully deleted {deleted} fundamental data record(s) for {symbol}[/green]"
            )
        else:
            rprint(f"[yellow]No fundamental data found to delete for {symbol}[/yellow]")
    except ValueError as ve:
        rprint(f"[red]Error: {str(ve)}[/red]")
    except Exception as e:
        rprint(f"[red]Error deleting fundamental data for {symbol}: {str(e)}[/red]")


@fd_app.command("show")
def cli_show_fundamental_data(
    symbol: str = typer.Argument(
        ..., help="The stock symbol to show fundamental data for"
    ),
    date: str = typer.Option(
        None, help="The date of the fundamental data to show (YYYY-MM-DD)"
    ),
    period: str = typer.Option(
        None, help="The period of the fundamental data to show (annual or quarterly)"
    ),
    days: int = typer.Option(
        365, help="Number of days of fundamental data history to show"
    ),
):
    """
    Show fundamental data for a given symbol, optionally filtered by date and period.
    """
    try:
        if date:
            date = datetime.strptime(date, "%Y-%m-%d").date()
            start_date = date
            end_date = date
        else:
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days)

        if period and period not in ["annual", "quarterly"]:
            raise ValueError("Period must be either 'annual' or 'quarterly'")

        fundamental_data = crud.get_fundamental_data(
            symbol, start_date, end_date, period
        )

        if not fundamental_data:
            rprint(
                f"[yellow]No fundamental data found for {symbol} with the given criteria[/yellow]"
            )
            return

        table = Table(title=f"Fundamental Data for {symbol}")
        table.add_column("Date", style="cyan")
        table.add_column("Period", style="magenta")
        table.add_column("Data", style="green")

        for item in fundamental_data:
            table.add_row(
                str(item.date),
                item.period,
                str(item.data)[:100] + "..."
                if len(str(item.data)) > 100
                else str(item.data),
            )

        rprint(table)

    except ValueError as ve:
        rprint(f"[red]Error: {str(ve)}[/red]")
    except Exception as e:
        rprint(f"[red]Error showing fundamental data for {symbol}: {str(e)}[/red]")


if __name__ == "__main__":
    db.connect()
    app()
    db.close()
