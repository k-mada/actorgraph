"""
CORS middleware configuration.
Restricts cross-origin access to known frontend origins.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Allowed frontend origins
ALLOWED_ORIGINS = [
    "http://localhost:3000",
    "http://localhost:5173",
]


def add_cors_middleware(app: FastAPI):
    """Apply CORS middleware with restricted settings."""
    app.add_middleware(
        CORSMiddleware,
        allow_origins=ALLOWED_ORIGINS,
        allow_methods=["GET"],
        allow_headers=["X-API-Key", "Content-Type"],
    )
