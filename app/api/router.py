from fastapi import APIRouter
from app.api.v1.endpoints import parser_router


api_router = APIRouter()
api_router.include_router(parser_router, tags=["parser"])
