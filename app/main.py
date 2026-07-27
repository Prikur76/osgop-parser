from fastapi import FastAPI
from fastapi.responses import JSONResponse

from app.api.router import api_router
from app.core.config import config
from app.core.exceptions import AppError
from app.core.logging import setup_logging


def create_app() -> FastAPI:
    app = FastAPI(
        title="OSGOP Document Parser",
        version="1.0.0",
        description="Service for extracting structured data from OSGOP PDF documents"
    )

    config.validate()
    setup_logging()

    @app.exception_handler(AppError)
    async def app_error_handler(request, exc: AppError):
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": str(exc), "type": type(exc).__name__}
        )

    @app.get("/health", tags=["service"])
    async def health() -> dict:
        """Проверка работоспособности сервиса."""
        return {"status": "ok"}

    app.include_router(api_router, prefix="/api/v1")

    return app


app = create_app()
