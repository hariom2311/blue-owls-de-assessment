import logging
import math
import random
import time
from collections import defaultdict

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from app.config import settings

logger = logging.getLogger("mock-api")

_DATA_PREFIX = "/api/v1/data/"
_SCHEMA_SEGMENT = "/schema/"

# Per-token rate limit state: subject → {"count": int, "blocked_until": float}
# Simple dict — GIL makes individual reads/writes safe without an explicit lock.
_rate_state: dict[str, dict] = defaultdict(lambda: {"count": 0, "blocked_until": 0.0})


class ChaosMiddleware(BaseHTTPMiddleware):
    """
    Injects HTTP failures on authenticated /data endpoints (not /schema).

    Two independent failure modes:
      1. Rate limiting (429) — deterministic, per token:
           Every RATE_LIMIT_REQUESTS requests from the same token triggers a 429.
           The token is blocked until Retry-After seconds have elapsed; any request
           during the cooldown also gets a 429.
      2. Random server errors (500) — stochastic:
           Each non-rate-limited request rolls against FAILURE_RATE for a 500.
    """

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        if not path.startswith(_DATA_PREFIX) or _SCHEMA_SEGMENT in path:
            return await call_next(request)

        subject: str = getattr(request.state, "token_subject", "unknown")
        state = _rate_state[subject]
        now = time.monotonic()

        # ── 1. Check if token is currently in cooldown ────────────────────────
        if now < state["blocked_until"]:
            retry_after = math.ceil(state["blocked_until"] - now)
            logger.warning(
                "rate_limit_blocked",
                extra={"endpoint": path, "token_subject": subject, "retry_after": retry_after},
            )
            request.state.chaos_injected = True
            request.state.chaos_type = "rate_limit_exceeded"
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded. Please retry after the specified interval."},
                headers={"Retry-After": str(retry_after)},
            )

        # ── 2. Increment counter; trigger rate limit every N requests ─────────
        state["count"] += 1
        if state["count"] % settings.rate_limit_requests == 0:
            state["blocked_until"] = now + settings.rate_limit_retry_after
            logger.warning(
                "rate_limit_triggered",
                extra={
                    "endpoint": path,
                    "token_subject": subject,
                    "request_count": state["count"],
                    "blocked_for_seconds": settings.rate_limit_retry_after,
                },
            )
            request.state.chaos_injected = True
            request.state.chaos_type = "rate_limit_exceeded"
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded. Please retry after the specified interval."},
                headers={"Retry-After": str(settings.rate_limit_retry_after)},
            )

        # ── 3. Random 500 chaos ───────────────────────────────────────────────
        if random.random() < settings.failure_rate:
            logger.warning(
                "chaos_500_injected",
                extra={"endpoint": path, "token_subject": subject},
            )
            request.state.chaos_injected = True
            request.state.chaos_type = "internal_server_error"
            return JSONResponse(
                status_code=500,
                content={"detail": "Internal server error. Please retry."},
            )

        return await call_next(request)
