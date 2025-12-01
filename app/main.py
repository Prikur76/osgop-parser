from fastapi import FastAPI
from app.api.router import api_router
from app.core.logging import setup_logging

    
def create_app() -> FastAPI:
    app = FastAPI(
        title="OSGOP Document Parser",
        version="1.0.0",
        description="Service for extracting structured data from OSGOP PDF documents"
    )

    setup_logging()

    app.include_router(api_router, prefix="/api/v1")

    return app


app = create_app()
