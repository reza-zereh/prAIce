from datetime import UTC, datetime
from enum import Enum

from peewee import (
    AutoField,
    BigIntegerField,
    BooleanField,
    CharField,
    DateField,
    DateTimeField,
    DecimalField,
    ForeignKeyField,
    Model,
    PostgresqlDatabase,
    TextField,
)

from praice.config import settings

db = PostgresqlDatabase(
    database=settings.DB_NAME,
    user=settings.DB_USERNAME,
    password=settings.DB_PASSWORD,
    host=settings.DB_HOST,
    port=settings.DB_PORT,
)


class BaseModel(Model):
    """
    BaseModel class.

    Attributes:
        Meta: A nested class that defines the metadata for the BaseModel class.
            database (Database): The database connection for the BaseModel class.
    """

    class Meta:
        database = db


class AssetClass(Enum):
    """Enum class representing different asset classes."""

    STOCK = "stock"
    COMMODITY = "commodity"
    CURRENCY = "currency"
    CRYPTO = "crypto"
    ETF = "etf"
    INDEX = "index"
    BOND = "bond"
    MUTUALFUND = "mutualfund"
    FUTURES = "futures"
    OPTION = "option"


class Symbol(BaseModel):
    """
    Represents a symbol in the system.

    Attributes:
        id (int): The unique identifier of the symbol.
        symbol (str): The symbol string (uppercase).
        name (str): The name of the symbol.
        asset_class (str): The asset class of the symbol (lowercase).
        sector (str, optional): The sector of the symbol (title).
        industry (str, optional): The industry of the symbol (title).
        exchange (str, optional): The exchange of the symbol (uppercase).
        is_active (bool): Indicates if the symbol is active.
        created_at (datetime): The date and time when the symbol was created.
        updated_at (datetime): The date and time when the symbol was last updated.
    """

    id = AutoField(primary_key=True)
    symbol = CharField(max_length=10, unique=True, index=True)
    name = CharField(max_length=255)
    asset_class = CharField(
        max_length=50, choices=[(e.value, e.name) for e in AssetClass]
    )
    sector = CharField(max_length=100, null=True)
    industry = CharField(max_length=100, null=True)
    exchange = CharField(max_length=50, null=True)
    is_active = BooleanField(default=True)
    created_at = DateTimeField(default=lambda: datetime.now(UTC))
    updated_at = DateTimeField(default=lambda: datetime.now(UTC))

    class Meta:
        table_name = "symbols"

    def save(self, *args, **kwargs):
        """
        Save the symbol instance.

        Returns:
            The saved symbol instance.
        Raises:
            ValueError: If the asset_class is invalid.
        """

        self.updated_at = datetime.now(UTC)
        self.symbol = self.symbol.upper()
        if self.name:
            self.name = self.name.title()
        if self.exchange:
            self.exchange = self.exchange.upper()
        if self.sector:
            self.sector = self.sector.title()
        if self.industry:
            self.industry = self.industry.title()
        self.asset_class = self.asset_class.lower()

        if self.asset_class not in [e.value for e in AssetClass]:
            raise ValueError(
                f"Invalid asset_class: {self.asset_class}. "
                f"Must be one of {[e.value for e in AssetClass]}"
            )

        return super(Symbol, self).save(*args, **kwargs)

    @classmethod
    def create(cls, **query):
        """
        Create a new symbol instance.

        This method applies the necessary transformations before creating the instance.

        Args:
            **query: The attributes for creating the new symbol.

        Returns:
            The newly created Symbol instance.
        """
        if "symbol" in query:
            query["symbol"] = query["symbol"].upper()
        if "name" in query and query["name"]:
            query["name"] = query["name"].title()
        if "exchange" in query and query["exchange"]:
            query["exchange"] = query["exchange"].upper()
        if "sector" in query and query["sector"]:
            query["sector"] = query["sector"].title()
        if "industry" in query and query["industry"]:
            query["industry"] = query["industry"].title()
        if "asset_class" in query:
            query["asset_class"] = query["asset_class"].lower()
            if query["asset_class"] not in [e.value for e in AssetClass]:
                raise ValueError(
                    f"Invalid asset_class: {query['asset_class']}. "
                    f"Must be one of {[e.value for e in AssetClass]}"
                )
        return super(Symbol, cls).create(**query)


class News(BaseModel):
    """
    Represents a news article.

    Attributes:
        id (int): The unique identifier of the news article.
        url (str): The URL of the news article.
        title (str): The title of the news article.
        content (str): The content of the news article.
        source (str): The source of the news article.
        published_at (datetime): The date and time when the news article was published.
        scraped_at (datetime): The date and time when the news article was scraped.
    """

    id = AutoField(primary_key=True)
    url = CharField(max_length=512, unique=True)
    title = CharField(max_length=255)
    content = TextField(null=True)
    source = CharField(max_length=100)
    published_at = DateTimeField(null=True)
    scraped_at = DateTimeField(default=lambda: datetime.now(UTC))

    class Meta:
        table_name = "news"


class NewsSymbol(BaseModel):
    """
    Represents a mapping between a news article and a symbol.

    Attributes:
        news (ForeignKeyField): The foreign key to the News model, representing the news article.
        symbol (ForeignKeyField): The foreign key to the Symbol model, representing the symbol.
    """

    news = ForeignKeyField(News, backref="news_symbols", on_delete="CASCADE")
    symbol = ForeignKeyField(Symbol, backref="news_symbols", on_delete="CASCADE")

    class Meta:
        table_name = "news_symbols"
        indexes = ((("news", "symbol"), True),)


class ScrapingUrl(BaseModel):
    """
    Represents a scraping URL.

    Attributes:
        id (int): The ID of the scraping URL.
        symbol (Symbol): The symbol associated with the scraping URL.
        url (str): The URL to be scraped.
        source (str): The source of the scraping URL.
        is_active (bool): Indicates if the scraping URL is active.
        created_at (datetime): The timestamp when the scraping URL was created.
        updated_at (datetime): The timestamp when the scraping URL was last updated.
        last_scraped_at (datetime): The timestamp when the scraping URL was last scraped.
    """

    ...
    id = AutoField(primary_key=True)
    symbol = ForeignKeyField(Symbol, backref="scraping_urls")
    url = CharField(max_length=512, unique=True)
    source = CharField(max_length=100)
    is_active = BooleanField(default=True)
    created_at = DateTimeField(default=lambda: datetime.now(UTC))
    updated_at = DateTimeField(default=lambda: datetime.now(UTC))
    last_scraped_at = DateTimeField(null=True)

    class Meta:
        table_name = "scraping_urls"
        indexes = ((("symbol", "source"), True),)

    def save(self, *args, **kwargs):
        """
        Save the object and update the 'updated_at' field with the current datetime in UTC.

        Returns:
            The saved object.
        """
        self.updated_at = datetime.now(UTC)
        return super(ScrapingUrl, self).save(*args, **kwargs)


class HistoricalPrice(BaseModel):
    id = AutoField(primary_key=True)
    symbol = ForeignKeyField(Symbol, backref="historical_prices")
    date = DateField()
    open = DecimalField(max_digits=10, decimal_places=2)
    high = DecimalField(max_digits=10, decimal_places=2)
    low = DecimalField(max_digits=10, decimal_places=2)
    close = DecimalField(max_digits=10, decimal_places=2)
    volume = BigIntegerField()
    dividends = DecimalField(max_digits=10, decimal_places=2)
    stock_splits = DecimalField(max_digits=10, decimal_places=2)

    class Meta:
        table_name = "historical_prices"
        indexes = (
            (("symbol", "date"), True),  # Unique index
        )


def create_tables():
    """
    Creates tables in the database.

    This function uses the `db` connection to create tables in the database. It takes no arguments.
    """
    with db:
        db.create_tables([Symbol, News, NewsSymbol, ScrapingUrl, HistoricalPrice])


if __name__ == "__main__":
    create_tables()
