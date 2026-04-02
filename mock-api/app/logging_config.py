import json
import logging
import time
from datetime import datetime, timezone

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_data: dict = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        if hasattr(record, "extra"):
            log_data.update(record.extra)
        return json.dumps(log_data)


def setup_logging(log_level: str = "INFO") -> logging.Logger:
    handler = logging.StreamHandler()
    handler.setFormatter(JSONFormatter())

    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    # Avoid duplicate handlers on reload
    root_logger.handlers.clear()
    root_logger.addHandler(handler)

    return logging.getLogger("mock-api")


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Logs every request as structured JSON matching the spec format."""

    def __init__(self, app, logger: logging.Logger):
        super().__init__(app)
        self._logger = logger

    async def dispatch(self, request: Request, call_next):
        start = time.monotonic()
        response = await call_next(request)
        elapsed_ms = round((time.monotonic() - start) * 1000)

        chaos_injected: bool = getattr(request.state, "chaos_injected", False)
        chaos_type: str | None = getattr(request.state, "chaos_type", None)
        token_subject: str | None = getattr(request.state, "token_subject", None)
        records_returned: int = getattr(request.state, "records_returned", 0)

        log_entry: dict = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "method": request.method,
            "path": request.url.path,
            "query_params": dict(request.query_params),
            "status_code": response.status_code,
            "response_time_ms": elapsed_ms,
            "token_subject": token_subject,
            "chaos_injected": chaos_injected,
            "records_returned": records_returned,
        }
        if chaos_injected and chaos_type:
            log_entry["chaos_type"] = chaos_type

        self._logger.info(json.dumps(log_entry))
        return response
