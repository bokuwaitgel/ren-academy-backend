from fastapi import HTTPException, status
from pydantic import ValidationError

from src.api.api_routes import register
from src.database.mongodb import MongoDB
from src.database.repositories.user_repository import UserRepository
from src.services.auth_service import AuthService
from schemas.auth import UserCreate, UserLogin, RefreshTokenRequest


def _auth_service() -> AuthService:
    db = MongoDB.get_db()
    return AuthService(UserRepository(db))


def _extract_token(payload: dict, field: str = "access_token") -> str:
    token = payload.get(field) or payload.get("token")
    auth_header = payload.get("authorization")
    if not token and isinstance(auth_header, str) and auth_header.lower().startswith("bearer "):
        token = auth_header.split(" ", 1)[1].strip()
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing token")
    return str(token)


@register(name="auth/register", method="POST", required_keys=["username", "email", "password"], optional_keys={"role": "candidate"})
async def auth_register(data: dict):
    try:
        payload = UserCreate(**data)
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=exc.errors())
    return await _auth_service().register(payload)


@register(name="auth/login", method="POST", required_keys=["email", "password"])
async def auth_login(data: dict):
    try:
        payload = UserLogin(**data)
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=exc.errors())
    return await _auth_service().login(payload.email, payload.password)


@register(name="auth/refresh", method="POST", required_keys=["refresh_token"])
async def auth_refresh(data: dict):
    try:
        payload = RefreshTokenRequest(**data)
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=exc.errors())
    return await _auth_service().refresh(payload.refresh_token)


@register(name="auth/me", method="GET", required_keys=[])
async def auth_me(data: dict):
    token = _extract_token(data, field="access_token")
    return await _auth_service().get_current_user(token)


@register(name="auth/users", method="GET", required_keys=[], optional_keys={"page": 1, "page_size": 20})
async def auth_list_users(data: dict):
    token = _extract_token(data, field="access_token")
    user = await _auth_service().get_current_user(token)
    if user.role not in {"admin", "super_admin", "super-admin"}:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required")

    page = int(data.get("page", 1))
    page_size = int(data.get("page_size", 20))
    return await _auth_service().list_users(page=page, page_size=page_size)
