from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Environment-driven settings. Override any field with an env var of the same name."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+psycopg://taskqueue:taskqueue@localhost:5432/taskqueue"
    redis_url: str = "redis://localhost:6379/0"

    default_queue: str = "default"
    default_priority: int = 5  # 0-9, higher runs first
    default_max_attempts: int = 3


@lru_cache
def get_settings() -> Settings:
    return Settings()
