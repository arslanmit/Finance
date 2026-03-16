"""FastAPI application factory for the Finance CLI API."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from .routes_datasets import router as datasets_router
from .routes_health import router as health_router
from .routes_jobs import router as jobs_router
from .routes_runs import router as runs_router
from .service import ApiContext
from .settings import ApiSettings
from .storage import SqliteStateStore
from .worker import JobWorker


def create_app(settings: ApiSettings | None = None) -> FastAPI:
    effective_settings = ApiSettings.from_env() if settings is None else settings
    storage = SqliteStateStore(effective_settings.database_path)
    worker = JobWorker()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        storage.initialize()
        worker.start()
        app.state.api_context = ApiContext(
            settings=effective_settings,
            storage=storage,
            worker=worker,
        )
        try:
            yield
        finally:
            worker.stop()

    app = FastAPI(
        title="Finance CLI API",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.include_router(health_router)
    app.include_router(datasets_router)
    app.include_router(runs_router)
    app.include_router(jobs_router)
    return app
