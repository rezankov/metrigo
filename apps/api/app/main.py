from fastapi import FastAPI

from app.config import settings
from app.db import check_postgres, check_clickhouse


app = FastAPI(
    title="Metrigo API",
    version=settings.app_version,
)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "api",
    }


@app.get("/version")
def version():
    return {
        "project": settings.project_name,
        "version": settings.app_version,
    }


@app.get("/health/db")
def health_db():
    postgres_ok = check_postgres()
    clickhouse_ok = check_clickhouse()

    return {
        "status": "ok" if postgres_ok and clickhouse_ok else "error",
        "postgres": postgres_ok,
        "clickhouse": clickhouse_ok,
    }


@app.get("/sellers")
def sellers():
    return {
        "items": [],
        "message": "Sellers endpoint placeholder",
    }