"""
Конфигурация приложения.

Здесь:
- читаем переменные окружения
- задаём дефолты
- централизуем настройки

Важно:
- НЕ хардкодим секреты
- всё через env
"""

import os


class Settings:
    """
    Глобальные настройки приложения.
    """

    # Общие настройки
    project_name: str = os.getenv("PROJECT_NAME", "metrigo")
    app_version: str = os.getenv("APP_VERSION", "0.1.0")

    # Postgres
    postgres_host: str = os.getenv("POSTGRES_HOST", "postgres")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_db: str = os.getenv("POSTGRES_DB", "metrigo")
    postgres_user: str = os.getenv("POSTGRES_USER", "metrigo")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "")

    # ClickHouse
    clickhouse_host: str = os.getenv("CH_HOST", "clickhouse")
    clickhouse_port: int = int(os.getenv("CH_PORT", "8123"))
    clickhouse_user: str = os.getenv("CH_USER", "default")
    clickhouse_password: str = os.getenv("CH_PASSWORD", "")
    clickhouse_db: str = os.getenv("CH_DB", "metrigo")


# Singleton (чтобы не создавать каждый раз)
settings = Settings()