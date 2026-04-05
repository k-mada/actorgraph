"""
API Key authentication middleware.
Validates X-API-Key header on all protected routes.
"""

import os
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

# Only paths starting with these prefixes require authentication
PROTECTED_PREFIXES = ("/api/",)


class APIKeyMiddleware(BaseHTTPMiddleware):
    """Middleware that enforces API key authentication via the X-API-Key header."""

    def __init__(self, app):
        super().__init__(app)
        self.api_key = os.getenv("API_KEY")
        if not self.api_key:
            raise ValueError("API_KEY must be set as an environment variable")

    async def dispatch(self, request: Request, call_next):
        if not request.url.path.startswith(PROTECTED_PREFIXES):
            return await call_next(request)

        provided_key = request.headers.get("X-API-Key")
        if not provided_key or provided_key != self.api_key:
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid or missing API key"},
            )

        return await call_next(request)
