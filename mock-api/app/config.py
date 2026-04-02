from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    api_port: int = Field(default=8000, alias="API_PORT")
    token_expiry_minutes: int = Field(default=15, alias="TOKEN_EXPIRY_MINUTES")
    failure_rate: float = Field(default=0.12, alias="FAILURE_RATE")
    rate_limit_requests: int = Field(default=10, alias="RATE_LIMIT_REQUESTS")
    rate_limit_retry_after: int = Field(default=2, alias="RATE_LIMIT_RETRY_AFTER")
    default_page_size: int = Field(default=1000, alias="DEFAULT_PAGE_SIZE")
    max_page_size: int = Field(default=5000, alias="MAX_PAGE_SIZE")
    api_username: str = Field(default="candidate", alias="API_USERNAME")
    api_password: str = Field(default="blue-owls-2026", alias="API_PASSWORD")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    data_dir: str = Field(default="./data", alias="DATA_DIR")
    jwt_secret: str = Field(
        default="blue-owls-secret-key-change-in-prod", alias="JWT_SECRET"
    )

    model_config = {"env_file": ".env", "populate_by_name": True}


settings = Settings()
