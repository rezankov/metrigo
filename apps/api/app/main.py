"""
Main entrypoint FastAPI приложения.

Задачи файла:
- Создать FastAPI приложение
- Описать базовые endpoints (health, version)
- Проверка доступности инфраструктуры (Postgres + ClickHouse)

Важно:
- Этот файл НЕ содержит бизнес-логики
- Только инициализация и health-check endpoints
"""

from fastapi import FastAPI

from app.config import settings
from app.db import check_postgres, check_clickhouse


# ------------------------------------------------------------------------------
# Создание приложения
# ------------------------------------------------------------------------------

app = FastAPI(
    title="Metrigo API",
    version=settings.app_version,
)


# ------------------------------------------------------------------------------
# Health endpoints
# ------------------------------------------------------------------------------

@app.get("/health")
def health():
    """
    Базовая проверка API.

    Используется:
    - nginx / load balancer
    - мониторинг
    """
    return {
        "status": "ok",
        "service": "api",
    }


@app.get("/version")
def version():
    """
    Версия приложения.

    Удобно:
    - при деплое
    - при отладке
    """
    return {
        "project": settings.project_name,
        "version": settings.app_version,
    }


@app.get("/health/db")
def health_db():
    """
    Проверка доступности баз данных.

    Что проверяем:
    - Postgres (основные данные, пользователи)
    - ClickHouse (аналитика)

    Важно:
    - НЕ делаем сложных запросов
    - только ping/простые проверки
    """
    postgres_ok = check_postgres()
    clickhouse_ok = check_clickhouse()

    return {
        "status": "ok" if postgres_ok and clickhouse_ok else "error",
        "postgres": postgres_ok,
        "clickhouse": clickhouse_ok,
    }


# ------------------------------------------------------------------------------
# Future endpoints (заготовка)
# ------------------------------------------------------------------------------

# Здесь будут:
# - /auth
# - /users
# - /sellers
# - /reports
# - /ai