from datetime import datetime, timedelta, timezone

from jose import JWTError, jwt

from app.config import settings

ALGORITHM = "HS256"


class TokenExpiredError(Exception):
    pass


class TokenInvalidError(Exception):
    pass


def create_token(username: str) -> tuple[str, int]:
    """Returns (encoded_jwt, expires_in_seconds)."""
    expiry_seconds = settings.token_expiry_minutes * 60
    expire = datetime.now(timezone.utc) + timedelta(minutes=settings.token_expiry_minutes)
    payload = {
        "sub": username,
        "iat": datetime.now(timezone.utc),
        "exp": expire,
    }
    token = jwt.encode(payload, settings.jwt_secret, algorithm=ALGORITHM)
    return token, expiry_seconds


def validate_token(token: str) -> str:
    """Decodes the token and returns the subject. Raises TokenExpiredError or TokenInvalidError."""
    try:
        payload = jwt.decode(token, settings.jwt_secret, algorithms=[ALGORITHM])
        subject: str = payload["sub"]
        return subject
    except jwt.ExpiredSignatureError:
        raise TokenExpiredError()
    except JWTError:
        raise TokenInvalidError()
