from typing import Optional

import typer
from rich import print as rprint
from rich.table import Table

from praice.data_handling.models import ScrapingUrl, Symbol, db

app = typer.Typer()


@app.command()
def add_symbol(
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
        with db.atomic():
            new_symbol = Symbol.create(
                symbol=symbol,
                name=name,
                asset_class=asset_class,
                sector=sector,
                industry=industry,
                exchange=exchange,
            )
        rprint(f"[green]Symbol {new_symbol.symbol} added successfully.[/green]")
    except Exception as e:
        rprint(f"[red]Error adding symbol: {str(e)}[/red]")


@app.command()
def list_symbols():
    """List all symbols in the database."""
    symbols = Symbol.select()
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


@app.command()
def update_symbol(
    symbol: str = typer.Argument(..., help="Symbol to update"),
    name: Optional[str] = typer.Option(None),
    asset_class: Optional[str] = typer.Option(None),
    sector: Optional[str] = typer.Option(None),
    industry: Optional[str] = typer.Option(None),
    exchange: Optional[str] = typer.Option(None),
):
    """Update an existing symbol in the database."""
    try:
        sym = Symbol.get(Symbol.symbol == symbol)
        if name:
            sym.name = name
        if asset_class:
            sym.asset_class = asset_class
        if sector is not None:
            sym.sector = sector
        if industry is not None:
            sym.industry = industry
        if exchange is not None:
            sym.exchange = exchange
        sym.save()
        rprint(f"[green]Symbol {symbol} updated successfully.[/green]")
    except Symbol.DoesNotExist:
        rprint(f"[red]Symbol {symbol} not found.[/red]")
    except Exception as e:
        rprint(f"[red]Error updating symbol: {str(e)}[/red]")


@app.command()
def delete_symbol(symbol: str = typer.Argument(..., help="Symbol to delete")):
    """Delete a symbol from the database."""
    try:
        sym = Symbol.get(Symbol.symbol == symbol)
        sym.delete_instance()
        rprint(f"[green]Symbol {symbol} deleted successfully.[/green]")
    except Symbol.DoesNotExist:
        rprint(f"[red]Symbol {symbol} not found.[/red]")
    except Exception as e:
        rprint(f"[red]Error deleting symbol: {str(e)}[/red]")


@app.command()
def add_scraping_url(
    symbol: str = typer.Option(..., prompt=True),
    url: str = typer.Option(..., prompt=True),
    source: str = typer.Option(..., prompt=True),
):
    """Add a new scraping URL for a symbol."""
    try:
        with db.atomic():
            symbol_obj = Symbol.get(Symbol.symbol == symbol)
            _ = ScrapingUrl.create(symbol=symbol_obj, url=url, source=source)
        rprint(f"[green]Scraping URL for {symbol} added successfully.[/green]")
    except Symbol.DoesNotExist:
        rprint(f"[red]Symbol {symbol} not found.[/red]")
    except Exception as e:
        rprint(f"[red]Error adding scraping URL: {str(e)}[/red]")


@app.command()
def list_scraping_urls(
    symbol: Optional[str] = typer.Option(None, help="Filter by symbol"),
):
    """List scraping URLs, optionally filtered by symbol."""
    query = ScrapingUrl.select(ScrapingUrl, Symbol).join(Symbol)
    if symbol:
        query = query.where(Symbol.symbol == symbol)

    table = Table(title="Scraping URLs")
    table.add_column("ID", style="cyan")
    table.add_column("Symbol", style="magenta")
    table.add_column("URL", style="green")
    table.add_column("Source", style="yellow")
    table.add_column("Is Active", style="blue")
    table.add_column("Last Scraped", style="red")

    for url in query:
        table.add_row(
            str(url.id),
            url.symbol.symbol,
            url.url,
            url.source,
            str(url.is_active),
            str(url.last_scraped_at) if url.last_scraped_at else "Never",
        )

    rprint(table)


@app.command()
def update_scraping_url(
    id: int = typer.Argument(..., help="ID of the scraping URL to update"),
    url: Optional[str] = typer.Option(None),
    source: Optional[str] = typer.Option(None),
    is_active: Optional[bool] = typer.Option(None),
):
    """Update an existing scraping URL."""
    try:
        scraping_url = ScrapingUrl.get_by_id(id)
        if url:
            scraping_url.url = url
        if source:
            scraping_url.source = source
        if is_active is not None:
            scraping_url.is_active = is_active
        scraping_url.save()
        rprint(f"[green]Scraping URL (ID: {id}) updated successfully.[/green]")
    except ScrapingUrl.DoesNotExist:
        rprint(f"[red]Scraping URL with ID {id} not found.[/red]")
    except Exception as e:
        rprint(f"[red]Error updating scraping URL: {str(e)}[/red]")


@app.command()
def delete_scraping_url(
    id: int = typer.Argument(..., help="ID of the scraping URL to delete"),
):
    """Delete a scraping URL."""
    try:
        scraping_url = ScrapingUrl.get_by_id(id)
        scraping_url.delete_instance()
        rprint(f"[green]Scraping URL (ID: {id}) deleted successfully.[/green]")
    except ScrapingUrl.DoesNotExist:
        rprint(f"[red]Scraping URL with ID {id} not found.[/red]")
    except Exception as e:
        rprint(f"[red]Error deleting scraping URL: {str(e)}[/red]")


if __name__ == "__main__":
    db.connect()
    app()
    db.close()
