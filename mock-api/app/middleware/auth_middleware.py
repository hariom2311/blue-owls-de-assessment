from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from app.auth.token_manager import TokenExpiredError, TokenInvalidError, validate_token

_DATA_PREFIX = "/api/v1/data/"
_SCHEMA_SEGMENT = "/schema/"


class AuthMiddleware(BaseHTTPMiddleware):
    """Validates Bearer tokens on all /api/v1/data/* routes except /schema/."""

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # Only guard data endpoints; schema and non-data routes pass through freely
        if not path.startswith(_DATA_PREFIX) or _SCHEMA_SEGMENT in path:
            return await call_next(request)

        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return JSONResponse(
                status_code=401,
                content={"detail": "Missing or invalid Authorization header"},
            )

        token = auth_header.removeprefix("Bearer ")
        try:
            subject = validate_token(token)
            request.state.token_subject = subject
        except TokenExpiredError:
            return JSONResponse(
                status_code=401,
                content={
                    "detail": "Token has expired. Please re-authenticate at /api/v1/auth/token"
                },
            )
        except TokenInvalidError:
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid token"},
            )

        return await call_next(request)
