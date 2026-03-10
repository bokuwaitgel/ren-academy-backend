from src.database.repositories.user_repository import UserRepository
from src.services.security import (
    hash_password,
    verify_password,
    create_access_token,
    create_refresh_token,
    decode_token,
)
from schemas.auth import UserCreate, UserResponse, TokenResponse
from fastapi import HTTPException, status
from typing import Optional


class AuthService:
    def __init__(self, user_repo: UserRepository):
        self.user_repo = user_repo

    async def register(self, payload: UserCreate) -> UserResponse:
        # Check duplicates
        if await self.user_repo.find_by_email(payload.email):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Email already registered",
            )
        if await self.user_repo.find_by_username(payload.username):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Username already taken",
            )

        hashed = hash_password(payload.password)
        user = await self.user_repo.create(payload.username, payload.email, hashed, payload.role)
        return UserResponse(**self.user_repo.serialize(user))

    async def login(self, email: str, password: str) -> TokenResponse:
        user = await self.user_repo.find_by_email(email)
        if not user or not verify_password(password, user["hashed_password"]):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid email or password",
            )
        if not user.get("is_active", True):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Account is deactivated",
            )

        token_data = {
            "sub": str(user["_id"]),
            "email": user["email"],
            "role": user.get("role", "candidate"),
        }
        access_token = create_access_token(token_data)
        refresh_token = create_refresh_token(token_data)
        return TokenResponse(access_token=access_token, refresh_token=refresh_token)

    async def refresh(self, refresh_token: str) -> TokenResponse:
        payload = decode_token(refresh_token)
        if not payload or payload.get("type") != "refresh":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired refresh token",
            )

        user_id: str = payload.get("sub") or ""
        if not user_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token payload",
            )
        user = await self.user_repo.find_by_id(user_id)
        if not user or not user.get("is_active", True):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found or deactivated",
            )

        token_data = {
            "sub": str(user["_id"]),
            "email": user["email"],
            "role": user.get("role", "candidate"),
        }
        new_access = create_access_token(token_data)
        new_refresh = create_refresh_token(token_data)
        return TokenResponse(access_token=new_access, refresh_token=new_refresh)

    async def get_current_user(self, token: str) -> UserResponse:
        payload = decode_token(token)
        if not payload or payload.get("type") != "access":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired access token",
                headers={"WWW-Authenticate": "Bearer"},
            )

        user_id: str = payload.get("sub") or ""
        if not user_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token payload",
            )
        user = await self.user_repo.find_by_id(user_id)
        if not user or not user.get("is_active", True):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found or deactivated",
            )
        return UserResponse(**self.user_repo.serialize(user))

    async def list_users(self, page: int = 1, page_size: int = 20) -> list[UserResponse]:
        skip = (page - 1) * page_size
        users = await self.user_repo.list_users(skip=skip, limit=page_size)
        return [UserResponse(**self.user_repo.serialize(u)) for u in users]
