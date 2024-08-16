from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from praice.constants import PATHS


class Settings(BaseSettings):
    """
    Configuration settings for the application.

    Attributes:
        DB_HOST (str): The host of the database.
        DB_PORT (int): The port of the database.
        DB_NAME (str): The name of the database.
        DB_USERNAME (str): The username for the database.
        DB_PASSWORD (str): The password for the database.
        LOG_LEVEL (str): The logging level. Default is "INFO".
    """

    # Database settings
    DB_HOST: str
    DB_PORT: int
    DB_NAME: str
    DB_USERNAME: str
    DB_PASSWORD: str

    # Other settings
    LOG_LEVEL: str = Field("INFO", description="Logging level")

    model_config = SettingsConfigDict(
        env_file=PATHS["app"] / ".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )


@lru_cache
def get_settings():
    """
    Retrieves the settings object.

    Returns:
        Settings: The settings object.
    """
    return Settings()


# Create a global instance of the settings
settings = get_settings()
