from typing import Optional

import typer
from rich import print as rprint
from rich.table import Table

from praice.data_handling.crud import (
    add_scraping_url,
    add_symbol,
    delete_scraping_url,
    delete_symbol,
    list_scraping_urls,
    list_symbols,
    update_scraping_url,
    update_symbol,
)
from praice.data_handling.models import db

app = typer.Typer()
symbol_app = typer.Typer()
scraping_url_app = typer.Typer()

app.add_typer(symbol_app, name="symbol")
app.add_typer(scraping_url_app, name="scraping-url")


# Symbol commands
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
        new_symbol = add_symbol(symbol, name, asset_class, sector, industry, exchange)
        rprint(f"[green]Symbol {new_symbol.symbol} added successfully.[/green]")
    except Exception as e:
        rprint(f"[red]Error adding symbol: {str(e)}[/red]")


@symbol_app.command("list")
def cli_list_symbols():
    """List all symbols in the database."""
    symbols = list_symbols()
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
        update_symbol(symbol, name, asset_class, sector, industry, exchange)
        rprint(f"[green]Symbol {symbol} updated successfully.[/green]")
    except Exception as e:
        rprint(f"[red]Error updating symbol: {str(e)}[/red]")


@symbol_app.command("delete")
def cli_delete_symbol(symbol: str = typer.Argument(..., help="Symbol to delete")):
    """Delete a symbol from the database."""
    try:
        if delete_symbol(symbol):
            rprint(f"[green]Symbol {symbol} deleted successfully.[/green]")
        else:
            rprint(f"[red]Symbol {symbol} not found.[/red]")
    except Exception as e:
        rprint(f"[red]Error deleting symbol: {str(e)}[/red]")


# Scraping URL commands
@scraping_url_app.command("add")
def cli_add_scraping_url(
    symbol: str = typer.Option(..., prompt=True),
    url: str = typer.Option(..., prompt=True),
    source: str = typer.Option(..., prompt=True),
):
    """Add a new scraping URL for a symbol."""
    try:
        new_url = add_scraping_url(symbol, url, source)
        rprint(
            f"[green]Scraping URL for {new_url.symbol.symbol} added successfully.[/green]"
        )
    except Exception as e:
        rprint(f"[red]Error adding scraping URL: {str(e)}[/red]")


@scraping_url_app.command("list")
def cli_list_scraping_urls(
    symbol: Optional[str] = typer.Option(None, help="Filter by symbol"),
):
    """List scraping URLs, optionally filtered by symbol."""
    urls = list_scraping_urls(symbol)
    table = Table(title="Scraping URLs")
    table.add_column("ID", style="cyan")
    table.add_column("Symbol", style="magenta")
    table.add_column("URL", style="green")
    table.add_column("Source", style="yellow")
    table.add_column("Is Active", style="blue")
    table.add_column("Last Scraped", style="red")

    for url in urls:
        table.add_row(
            str(url.id),
            url.symbol.symbol,
            url.url,
            url.source,
            str(url.is_active),
            str(url.last_scraped_at) if url.last_scraped_at else "Never",
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
        update_scraping_url(id, url, source, is_active)
        rprint(f"[green]Scraping URL (ID: {id}) updated successfully.[/green]")
    except Exception as e:
        rprint(f"[red]Error updating scraping URL: {str(e)}[/red]")


@scraping_url_app.command("delete")
def cli_delete_scraping_url(
    id: int = typer.Argument(..., help="ID of the scraping URL to delete"),
):
    """Delete a scraping URL."""
    try:
        if delete_scraping_url(id):
            rprint(f"[green]Scraping URL (ID: {id}) deleted successfully.[/green]")
        else:
            rprint(f"[red]Scraping URL with ID {id} not found.[/red]")
    except Exception as e:
        rprint(f"[red]Error deleting scraping URL: {str(e)}[/red]")


if __name__ == "__main__":
    db.connect()
    app()
    db.close()
