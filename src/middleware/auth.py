"""
API Key authentication middleware.
Validates X-API-Key header on all requests except docs endpoints.
"""

import os
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

# Paths that don't require authentication (Swagger UI, OpenAPI schema)
EXEMPT_PATHS = {"/docs", "/openapi.json", "/redoc"}


class APIKeyMiddleware(BaseHTTPMiddleware):
    """Middleware that enforces API key authentication via the X-API-Key header."""

    def __init__(self, app):
        super().__init__(app)
        self.api_key = os.getenv("API_KEY")
        if not self.api_key:
            raise ValueError("API_KEY must be set as an environment variable")

    async def dispatch(self, request: Request, call_next):
        if request.url.path in EXEMPT_PATHS:
            return await call_next(request)

        provided_key = request.headers.get("X-API-Key")
        if not provided_key or provided_key != self.api_key:
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid or missing API key"},
            )

        return await call_next(request)
