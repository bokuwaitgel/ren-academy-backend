from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId
from datetime import datetime, timedelta, timezone
from typing import Optional, List


def _oid(id_str: str) -> ObjectId:
    return ObjectId(id_str)


def _serialize(doc: dict) -> dict:
    doc["id"] = str(doc.pop("_id"))
    return doc


# ─────────────────────────────────────────────
# Questions
# ─────────────────────────────────────────────

class QuestionRepository:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.col = db["questions"]

    async def create(self, data: dict) -> dict:
        data["created_at"] = datetime.now(timezone.utc)
        result = await self.col.insert_one(data)
        data["_id"] = result.inserted_id
        return _serialize(data)

    async def find_by_id(self, qid: str) -> Optional[dict]:
        doc = await self.col.find_one({"_id": _oid(qid)})
        return _serialize(doc) if doc else None

    async def find_many(self, ids: List[str]) -> List[dict]:
        oids = [_oid(i) for i in ids]
        cursor = self.col.find({"_id": {"$in": oids}})
        return [_serialize(d) async for d in cursor]

    async def find_all(self, skip: int = 0, limit: int = 20, section: Optional[str] = None) -> List[dict]:
        query = {}
        if section:
            query["section"] = section
        cursor = self.col.find(query).sort("created_at", -1).skip(skip).limit(limit)
        return [_serialize(d) async for d in cursor]

    async def count(self, section: Optional[str] = None) -> int:
        query = {}
        if section:
            query["section"] = section
        return await self.col.count_documents(query)

    async def update(self, qid: str, data: dict) -> Optional[dict]:
        data["updated_at"] = datetime.now(timezone.utc)
        await self.col.update_one({"_id": _oid(qid)}, {"$set": data})
        return await self.find_by_id(qid)

    async def delete(self, qid: str) -> bool:
        result = await self.col.delete_one({"_id": _oid(qid)})
        return result.deleted_count == 1


# ─────────────────────────────────────────────
# Listening Sections
# ─────────────────────────────────────────────

class ListeningSectionRepository:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.col = db["listening_sections"]

    async def create(self, data: dict) -> dict:
        data["created_at"] = datetime.now(timezone.utc)
        result = await self.col.insert_one(data)
        data["_id"] = result.inserted_id
        return _serialize(data)

    async def find_by_id(self, sid: str) -> Optional[dict]:
        doc = await self.col.find_one({"_id": _oid(sid)})
        return _serialize(doc) if doc else None

    async def find_many(self, ids: List[str]) -> List[dict]:
        oids = [_oid(i) for i in ids]
        cursor = self.col.find({"_id": {"$in": oids}})
        return [_serialize(d) async for d in cursor]

    async def find_all(self, skip: int = 0, limit: int = 20) -> List[dict]:
        cursor = self.col.find({}).sort("created_at", -1).skip(skip).limit(limit)
        return [_serialize(d) async for d in cursor]

    async def count(self) -> int:
        return await self.col.count_documents({})

    async def update(self, sid: str, data: dict) -> Optional[dict]:
        data["updated_at"] = datetime.now(timezone.utc)
        await self.col.update_one({"_id": _oid(sid)}, {"$set": data})
        return await self.find_by_id(sid)

    async def delete(self, sid: str) -> bool:
        result = await self.col.delete_one({"_id": _oid(sid)})
        return result.deleted_count == 1


# ─────────────────────────────────────────────
# Reading Passages
# ─────────────────────────────────────────────

class PassageRepository:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.col = db["reading_passages"]

    async def create(self, data: dict) -> dict:
        data["created_at"] = datetime.now(timezone.utc)
        result = await self.col.insert_one(data)
        data["_id"] = result.inserted_id
        return _serialize(data)

    async def find_by_id(self, pid: str) -> Optional[dict]:
        doc = await self.col.find_one({"_id": _oid(pid)})
        return _serialize(doc) if doc else None

    async def find_many(self, ids: List[str]) -> List[dict]:
        oids = [_oid(i) for i in ids]
        cursor = self.col.find({"_id": {"$in": oids}})
        return [_serialize(d) async for d in cursor]

    async def find_all(self, skip: int = 0, limit: int = 20) -> List[dict]:
        cursor = self.col.find({}).sort("created_at", -1).skip(skip).limit(limit)
        return [_serialize(d) async for d in cursor]

    async def count(self) -> int:
        return await self.col.count_documents({})

    async def update(self, pid: str, data: dict) -> Optional[dict]:
        data["updated_at"] = datetime.now(timezone.utc)
        await self.col.update_one({"_id": _oid(pid)}, {"$set": data})
        return await self.find_by_id(pid)

    async def delete(self, pid: str) -> bool:
        result = await self.col.delete_one({"_id": _oid(pid)})
        return result.deleted_count == 1


# ─────────────────────────────────────────────
# Writing Tasks
# ─────────────────────────────────────────────

class WritingTaskRepository:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.col = db["writing_tasks"]

    async def create(self, data: dict) -> dict:
        data["created_at"] = datetime.now(timezone.utc)
        result = await self.col.insert_one(data)
        data["_id"] = result.inserted_id
        return _serialize(data)

    async def find_by_id(self, tid: str) -> Optional[dict]:
        doc = await self.col.find_one({"_id": _oid(tid)})
        return _serialize(doc) if doc else None

    async def find_many(self, ids: List[str]) -> List[dict]:
        oids = [_oid(i) for i in ids]
        cursor = self.col.find({"_id": {"$in": oids}})
        return [_serialize(d) async for d in cursor]

    async def find_all(self, skip: int = 0, limit: int = 20) -> List[dict]:
        cursor = self.col.find({}).sort("created_at", -1).skip(skip).limit(limit)
        return [_serialize(d) async for d in cursor]

    async def count(self) -> int:
        return await self.col.count_documents({})

    async def update(self, tid: str, data: dict) -> Optional[dict]:
        data["updated_at"] = datetime.now(timezone.utc)
        await self.col.update_one({"_id": _oid(tid)}, {"$set": data})
        return await self.find_by_id(tid)

    async def delete(self, tid: str) -> bool:
        result = await self.col.delete_one({"_id": _oid(tid)})
        return result.deleted_count == 1


# ─────────────────────────────────────────────
# Speaking Parts
# ─────────────────────────────────────────────

class SpeakingPartRepository:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.col = db["speaking_parts"]

    async def create(self, data: dict) -> dict:
        data["created_at"] = datetime.now(timezone.utc)
        result = await self.col.insert_one(data)
        data["_id"] = result.inserted_id
        return _serialize(data)

    async def find_by_id(self, pid: str) -> Optional[dict]:
        doc = await self.col.find_one({"_id": _oid(pid)})
        return _serialize(doc) if doc else None

    async def find_many(self, ids: List[str]) -> List[dict]:
        oids = [_oid(i) for i in ids]
        cursor = self.col.find({"_id": {"$in": oids}})
        return [_serialize(d) async for d in cursor]

    async def find_all(self, skip: int = 0, limit: int = 20) -> List[dict]:
        cursor = self.col.find({}).sort("created_at", -1).skip(skip).limit(limit)
        return [_serialize(d) async for d in cursor]

    async def count(self) -> int:
        return await self.col.count_documents({})

    async def update(self, pid: str, data: dict) -> Optional[dict]:
        data["updated_at"] = datetime.utcnow()
        await self.col.update_one({"_id": _oid(pid)}, {"$set": data})
        return await self.find_by_id(pid)

    async def delete(self, pid: str) -> bool:
        result = await self.col.delete_one({"_id": _oid(pid)})
        return result.deleted_count == 1


# ─────────────────────────────────────────────
# Tests (exam papers)
# ─────────────────────────────────────────────

class TestRepository:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.col = db["tests"]

    async def create_indexes(self):
        await self.col.create_index("test_type")
        await self.col.create_index([("test_type", 1), ("created_at", -1)])

    async def create(self, data: dict) -> dict:
        data["created_at"] = datetime.now(timezone.utc)
        result = await self.col.insert_one(data)
        data["_id"] = result.inserted_id
        return _serialize(data)

    async def find_by_id(self, tid: str) -> Optional[dict]:
        doc = await self.col.find_one({"_id": _oid(tid)})
        return _serialize(doc) if doc else None

    async def find_all(self, skip: int = 0, limit: int = 20, test_type: Optional[str] = None) -> List[dict]:
        query = {}
        if test_type:
            query["test_type"] = test_type
        cursor = self.col.find(query).sort("created_at", -1).skip(skip).limit(limit)
        return [_serialize(d) async for d in cursor]

    async def count(self, test_type: Optional[str] = None) -> int:
        query = {}
        if test_type:
            query["test_type"] = test_type
        return await self.col.count_documents(query)

    async def find_one_by_component_id(self, field_name: str, component_id: str, test_type: Optional[str] = None) -> Optional[dict]:
        query = {field_name: component_id}
        if test_type:
            query["test_type"] = test_type
        doc = await self.col.find_one(query)
        return _serialize(doc) if doc else None

    async def update(self, tid: str, data: dict) -> Optional[dict]:
        data["updated_at"] = datetime.now(timezone.utc)
        await self.col.update_one({"_id": _oid(tid)}, {"$set": data})
        return await self.find_by_id(tid)

    async def delete(self, tid: str) -> bool:
        result = await self.col.delete_one({"_id": _oid(tid)})
        return result.deleted_count == 1


# ─────────────────────────────────────────────
# Test Sessions (user attempts)
# ─────────────────────────────────────────────

class TestSessionRepository:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.col = db["test_sessions"]

    async def create_indexes(self):
        await self.col.create_index("user_id")
        await self.col.create_index("test_id")
        await self.col.create_index("test_type")
        await self.col.create_index([("user_id", 1), ("test_type", 1), ("created_at", -1)])

    async def create(self, data: dict) -> dict:
        data["created_at"] = datetime.now(timezone.utc)
        result = await self.col.insert_one(data)
        data["_id"] = result.inserted_id
        return _serialize(data)

    async def find_by_id(self, sid: str) -> Optional[dict]:
        doc = await self.col.find_one({"_id": _oid(sid)})
        return _serialize(doc) if doc else None

    async def find_by_user(self, user_id: str, skip: int = 0, limit: int = 20, test_type: Optional[str] = None) -> List[dict]:
        query = {"user_id": user_id}
        if test_type:
            query["test_type"] = test_type
        cursor = self.col.find(query).sort("created_at", -1).skip(skip).limit(limit)
        return [_serialize(d) async for d in cursor]

    async def count_by_user(self, user_id: str, test_type: Optional[str] = None) -> int:
        query = {"user_id": user_id}
        if test_type:
            query["test_type"] = test_type
        return await self.col.count_documents(query)

    async def update(self, sid: str, data: dict) -> dict:
        data["updated_at"] = datetime.now(timezone.utc)
        await self.col.update_one({"_id": _oid(sid)}, {"$set": data})
        doc = await self.find_by_id(sid)
        if doc is None:
            raise RuntimeError(f"Session {sid} not found after update")
        return doc
