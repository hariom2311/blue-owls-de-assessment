from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.auth.models import TokenRequest, TokenResponse
from app.auth.token_manager import create_token
from app.config import settings

router = APIRouter()


@router.post("/token", response_model=TokenResponse)
async def get_token(request: TokenRequest):
    if request.username != settings.api_username or request.password != settings.api_password:
        return JSONResponse(status_code=401, content={"detail": "Invalid credentials"})

    token, expires_in = create_token(request.username)
    return TokenResponse(access_token=token, token_type="bearer", expires_in=expires_in)
