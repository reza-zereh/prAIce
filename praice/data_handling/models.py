from datetime import UTC, datetime

from peewee import (
    AutoField,
    BooleanField,
    CharField,
    DateTimeField,
    Model,
    PostgresqlDatabase,
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


class Symbol(BaseModel):
    """
    Represents a symbol in the system.

    Attributes:
        id (int): The unique identifier of the symbol.
        symbol (str): The symbol string.
        name (str): The name of the symbol.
        asset_class (str): The asset class of the symbol (e.g., 'stock', 'commodity', 'currency', etc.).
        sector (str, optional): The sector of the symbol.
        industry (str, optional): The industry of the symbol.
        exchange (str, optional): The exchange of the symbol.
        is_active (bool): Indicates if the symbol is active.
        created_at (datetime): The date and time when the symbol was created.
        updated_at (datetime): The date and time when the symbol was last updated.
    """

    id = AutoField(primary_key=True)
    symbol = CharField(max_length=10, unique=True, index=True)
    name = CharField(max_length=255)
    asset_class = CharField(
        max_length=50
    )  # e.g., 'stock', 'commodity', 'currency', etc.
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

        This method updates the `updated_at` attribute of the symbol instance with the current datetime in UTC.
        It then calls the `save` method of the parent class to save the symbol instance.

        Returns:
            The result of calling the `save` method of the parent class.

        """
        self.updated_at = datetime.now(UTC)
        return super(Symbol, self).save(*args, **kwargs)


def create_tables():
    """
    Creates tables in the database.

    This function uses the `db` connection to create tables in the database. It takes no arguments.
    """
    with db:
        db.create_tables([Symbol])


if __name__ == "__main__":
    create_tables()
