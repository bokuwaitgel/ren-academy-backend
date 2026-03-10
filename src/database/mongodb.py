from motor.motor_asyncio import AsyncIOMotorClient
from typing import Optional
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_DB", "mongodb://localhost:27017")
DATABASE_NAME = os.getenv("DATABASE_NAME", "app_db")


class MongoDB:
    client: Optional[AsyncIOMotorClient] = None
    db = None

    @classmethod
    async def connect(cls):
        cls.client = AsyncIOMotorClient(MONGO_URI)
        cls.db = cls.client[DATABASE_NAME]
        print(f"[MongoDB] Connected to '{DATABASE_NAME}'")

    @classmethod
    async def disconnect(cls):
        if cls.client:
            cls.client.close()
            print("[MongoDB] Disconnected")

    @classmethod
    def get_db(cls):
        if cls.db is None:
            raise RuntimeError("MongoDB is not connected. Call MongoDB.connect() first.")
        return cls.db


async def get_database():
    return MongoDB.get_db()
