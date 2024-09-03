# NOTE: You need to manually manage the order of migrations and ensure they're only run once.

# from peewee import IntegerField, TextField, DecimalField
# from playhouse.migrate import migrate
from playhouse.migrate import PostgresqlMigrator

from praice.data_handling.models import db

migrator = PostgresqlMigrator(db)


def run_migrations():
    """Define your migrations here."""

    # Example migration: Add a new column to the news table
    # migrate(
    #     migrator.add_column(
    #         "news_symbols",
    #         "sentiment_score",
    #         DecimalField(max_digits=10, decimal_places=5, null=True),
    #     ),
    # )

    pass


if __name__ == "__main__":
    db.connect()
    run_migrations()
    db.close()
