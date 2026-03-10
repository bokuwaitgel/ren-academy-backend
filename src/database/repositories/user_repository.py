from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId
from datetime import datetime, timezone
from typing import Optional


class UserRepository:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.collection = db["users"]

    async def create_indexes(self):
        await self.collection.create_index("email", unique=True)
        await self.collection.create_index("username", unique=True)

    async def find_by_email(self, email: str) -> Optional[dict]:
        return await self.collection.find_one({"email": email})

    async def find_by_id(self, user_id: str) -> Optional[dict]:
        try:
            return await self.collection.find_one({"_id": ObjectId(user_id)})
        except Exception:
            return None

    async def find_by_username(self, username: str) -> Optional[dict]:
        return await self.collection.find_one({"username": username})

    async def create(self, username: str, email: str, hashed_password: str, role: str = "candidate") -> dict:
        user_doc = {
            "username": username,
            "email": email,
            "hashed_password": hashed_password,
            "role": role,
            "is_active": True,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
        }
        result = await self.collection.insert_one(user_doc)
        user_doc["_id"] = result.inserted_id
        return user_doc

    async def update_password(self, user_id: str, hashed_password: str) -> bool:
        result = await self.collection.update_one(
            {"_id": ObjectId(user_id)},
            {"$set": {"hashed_password": hashed_password, "updated_at": datetime.now(timezone.utc)}},
        )
        return result.modified_count == 1

    async def deactivate(self, user_id: str) -> bool:
        result = await self.collection.update_one(
            {"_id": ObjectId(user_id)},
            {"$set": {"is_active": False, "updated_at": datetime.now(timezone.utc)}},
        )
        return result.modified_count == 1

    async def list_users(self, skip: int = 0, limit: int = 20) -> list[dict]:
        cursor = self.collection.find().skip(skip).limit(limit)
        return [doc async for doc in cursor]

    @staticmethod
    def serialize(user: dict) -> dict:
        """Convert MongoDB document to a serializable dict."""
        return {
            "id": str(user["_id"]),
            "username": user["username"],
            "email": user["email"],
            "role": user.get("role", "candidate"),
            "is_active": user.get("is_active", True),
            "created_at": user.get("created_at", datetime.utcnow()),
        }
