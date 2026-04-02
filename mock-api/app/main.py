from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.data.loader import load_all
from app.logging_config import RequestLoggingMiddleware, setup_logging
from app.auth.router import router as auth_router
from app.data.router import router as data_router
from app.middleware.auth_middleware import AuthMiddleware
from app.middleware.chaos import ChaosMiddleware

logger = setup_logging(settings.log_level)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Blue Owls Mock Data API")
    load_all()
    logger.info(
        f"Server ready — port {settings.api_port} | "
        f"chaos rate {settings.failure_rate} | "
        f"token TTL {settings.token_expiry_minutes}m"
    )
    yield
    logger.info("Shutting down Blue Owls Mock Data API")


app = FastAPI(
    title="Blue Owls Mock Data API",
    description=(
        "Serves the Olist Brazilian E-Commerce dataset through authenticated, "
        "paginated REST endpoints with configurable chaos injection."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# ── Middleware stack (outermost → innermost = first to run → last to run) ──────
#
# Starlette evaluates middlewares in reverse-add order: the last add_middleware
# call becomes the outermost wrapper and therefore runs first.
#
# Desired execution order per request:
#   1. CORS          — always first, handles preflight
#   2. RequestLogging — captures timing for every request
#   3. Auth          — validates Bearer token on /data routes; returns 401 early
#   4. Chaos         — injects 500/429 only after auth succeeds
#   5. Route handler
#
# add_middleware order (last added = outermost):
app.add_middleware(ChaosMiddleware)           # 4 — innermost, runs after auth
app.add_middleware(AuthMiddleware)            # 3
app.add_middleware(RequestLoggingMiddleware, logger=logger)  # 2
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])  # 1 — outermost

# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(auth_router, prefix="/api/v1/auth", tags=["auth"])
app.include_router(data_router, prefix="/api/v1/data", tags=["data"])


@app.get("/api/v1/health", tags=["health"])
async def health():
    return {"status": "healthy", "timestamp": datetime.now(timezone.utc).isoformat()}
