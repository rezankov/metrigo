from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    project_name: str = "metrigo"
    app_version: str = "0.1.0"

    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_db: str = "metrigo"
    postgres_user: str = "metrigo"
    postgres_password: str

    clickhouse_host: str = "clickhouse"
    clickhouse_port: int = 8123
    clickhouse_db: str = "metrigo"
    clickhouse_user: str = "default"
    clickhouse_password: str

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()