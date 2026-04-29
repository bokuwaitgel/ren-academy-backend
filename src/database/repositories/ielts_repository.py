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

    # ── Section/part builder helpers ──────────────────────────

    # Maps module name → (doc_field, array_field, number_key)
    _MODULE_MAP = {
        "listening": ("listening", "sections", "section_number"),
        "reading":   ("reading",   "sections", "section_number"),
        "writing":   ("writing",   "tasks",    "task_number"),
        "speaking":  ("speaking",  "parts",    "part_number"),
    }

    # Maps SectionPart value → (module, number)
    _SECTION_PART_MAP = {
        "listening_section_1": ("listening", 1),
        "listening_section_2": ("listening", 2),
        "listening_section_3": ("listening", 3),
        "listening_section_4": ("listening", 4),
        "reading_passage_1":   ("reading",   1),
        "reading_passage_2":   ("reading",   2),
        "reading_passage_3":   ("reading",   3),
        "writing_task_1":      ("writing",   1),
        "writing_task_2":      ("writing",   2),
        "speaking_part_1":     ("speaking",  1),
        "speaking_part_2":     ("speaking",  2),
        "speaking_part_3":     ("speaking",  3),
    }

    async def add_section(self, tid: str, module: str, section_data: dict) -> Optional[dict]:
        """Push a new section/part/task into the appropriate module array."""
        doc_field, array_field, _ = self._MODULE_MAP[module]
        await self.col.update_one(
            {"_id": _oid(tid)},
            {
                "$push": {f"{doc_field}.{array_field}": section_data},
                "$set": {"updated_at": datetime.now(timezone.utc)},
            },
        )
        return await self.find_by_id(tid)

    async def update_section(self, tid: str, module: str, number: int, update_fields: dict) -> Optional[dict]:
        """Update fields on a specific section/part/task by its number."""
        doc_field, array_field, number_key = self._MODULE_MAP[module]
        set_payload = {f"{doc_field}.{array_field}.$[elem].{k}": v for k, v in update_fields.items()}
        set_payload["updated_at"] = datetime.now(timezone.utc)
        await self.col.update_one(
            {"_id": _oid(tid)},
            {"$set": set_payload},
            array_filters=[{f"elem.{number_key}": number}],
        )
        return await self.find_by_id(tid)

    async def remove_section(self, tid: str, module: str, number: int) -> Optional[dict]:
        """Pull a section/part/task from the module array.
        If the array becomes empty after removal, unset the whole module field
        so TestOut validation (min_length=1 on sections/tasks/parts) doesn't fail."""
        doc_field, array_field, number_key = self._MODULE_MAP[module]
        await self.col.update_one(
            {"_id": _oid(tid)},
            {
                "$pull": {f"{doc_field}.{array_field}": {number_key: number}},
                "$set": {"updated_at": datetime.now(timezone.utc)},
            },
        )
        # If the module array is now empty, remove the whole module field
        doc = await self.find_by_id(tid)
        if doc:
            module_data = doc.get(doc_field)
            if isinstance(module_data, dict) and not module_data.get(array_field):
                await self.col.update_one(
                    {"_id": _oid(tid)},
                    {"$unset": {doc_field: ""}, "$set": {"updated_at": datetime.now(timezone.utc)}},
                )
                return await self.find_by_id(tid)
        return doc

    async def add_question_to_section(self, tid: str, section_part: str, question_id: str) -> Optional[dict]:
        """Push a question_id into the matching section's question_ids list."""
        module, number = self._SECTION_PART_MAP[section_part]
        doc_field, array_field, number_key = self._MODULE_MAP[module]
        # writing tasks don't have question_ids
        if module == "writing":
            raise ValueError("Writing tasks do not have question_ids")
        await self.col.update_one(
            {"_id": _oid(tid), f"{doc_field}.{array_field}.{number_key}": number},
            {
                "$addToSet": {f"{doc_field}.{array_field}.$.question_ids": question_id},
                "$set": {"updated_at": datetime.now(timezone.utc)},
            },
        )
        return await self.find_by_id(tid)

    async def remove_question_from_section(self, tid: str, section_part: str, question_id: str) -> Optional[dict]:
        """Pull a question_id from the matching section's question_ids list."""
        module, number = self._SECTION_PART_MAP[section_part]
        doc_field, array_field, number_key = self._MODULE_MAP[module]
        if module == "writing":
            raise ValueError("Writing tasks do not have question_ids")
        await self.col.update_one(
            {"_id": _oid(tid), f"{doc_field}.{array_field}.{number_key}": number},
            {
                "$pull": {f"{doc_field}.{array_field}.$.question_ids": question_id},
                "$set": {"updated_at": datetime.now(timezone.utc)},
            },
        )
        return await self.find_by_id(tid)


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

    async def find_active(
        self,
        user_id: str,
        test_id: str,
        mode: str,
        practice_section: Optional[str] = None,
    ) -> Optional[dict]:
        """Return the user's in-progress session for this test/mode, if any."""
        query: dict = {
            "user_id": user_id,
            "test_id": test_id,
            "mode": mode,
            "status": "in_progress",
        }
        if practice_section is not None:
            query["practice_section"] = practice_section
        doc = await self.col.find_one(query, sort=[("created_at", -1)])
        return _serialize(doc) if doc else None

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


# ─────────────────────────────────────────────
# Orders (test purchases via QPay)
# ─────────────────────────────────────────────

class OrderRepository:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.col = db["orders"]

    async def create_indexes(self):
        await self.col.create_index("user_id")
        await self.col.create_index("test_id")
        await self.col.create_index("status")
        await self.col.create_index("qpay_invoice_id")
        await self.col.create_index([("user_id", 1), ("test_id", 1), ("status", 1)])
        await self.col.create_index([("created_at", -1)])

    async def create(self, data: dict) -> dict:
        data["created_at"] = datetime.now(timezone.utc)
        result = await self.col.insert_one(data)
        data["_id"] = result.inserted_id
        return _serialize(data)

    async def find_by_id(self, oid: str) -> Optional[dict]:
        doc = await self.col.find_one({"_id": _oid(oid)})
        return _serialize(doc) if doc else None

    async def find_by_qpay_invoice(self, invoice_id: str) -> Optional[dict]:
        doc = await self.col.find_one({"qpay_invoice_id": invoice_id})
        return _serialize(doc) if doc else None

    async def find_paid_for_user_test(
        self,
        user_id: str,
        test_id: str,
        purchase_mode: Optional[str] = None,
        purchase_section: Optional[str] = None,
    ) -> Optional[dict]:
        query: dict = {"user_id": user_id, "test_id": test_id, "status": "paid"}
        if purchase_mode is not None:
            query["purchase_mode"] = purchase_mode
        if purchase_mode == "practice":
            query["purchase_section"] = purchase_section
        doc = await self.col.find_one(query, sort=[("paid_at", -1)])
        return _serialize(doc) if doc else None

    async def find_paid_unconsumed_for_user_test(
        self,
        user_id: str,
        test_id: str,
        purchase_mode: Optional[str] = None,
        purchase_section: Optional[str] = None,
    ) -> Optional[dict]:
        """Return the user's most recent paid order that has not yet been spent on a session."""
        query: dict = {
            "user_id": user_id,
            "test_id": test_id,
            "status": "paid",
            "$or": [
                {"consumed_session_id": {"$exists": False}},
                {"consumed_session_id": None},
            ],
        }
        if purchase_mode is not None:
            query["purchase_mode"] = purchase_mode
        if purchase_mode == "practice":
            query["purchase_section"] = purchase_section

        doc = await self.col.find_one(
            query,
            sort=[("paid_at", -1)],
        )
        return _serialize(doc) if doc else None

    async def mark_consumed(self, oid: str, session_id: str) -> Optional[dict]:
        """Bind a paid order to the session it was used to start. One order = one session."""
        await self.col.update_one(
            {"_id": _oid(oid)},
            {"$set": {
                "consumed_session_id": session_id,
                "consumed_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            }},
        )
        return await self.find_by_id(oid)

    async def find_active_pending(
        self,
        user_id: str,
        test_id: str,
        purchase_mode: Optional[str] = None,
        purchase_section: Optional[str] = None,
    ) -> Optional[dict]:
        """Return the user's most recent pending order for this test, if any."""
        query: dict = {"user_id": user_id, "test_id": test_id, "status": "pending"}
        if purchase_mode is not None:
            query["purchase_mode"] = purchase_mode
        if purchase_mode == "practice":
            query["purchase_section"] = purchase_section
        doc = await self.col.find_one(
            query,
            sort=[("created_at", -1)],
        )
        return _serialize(doc) if doc else None

    async def list(
        self,
        skip: int = 0,
        limit: int = 20,
        *,
        status: Optional[str] = None,
        user_id: Optional[str] = None,
        test_id: Optional[str] = None,
    ) -> List[dict]:
        query: dict = {}
        if status:  query["status"]  = status
        if user_id: query["user_id"] = user_id
        if test_id: query["test_id"] = test_id
        cursor = self.col.find(query).sort("created_at", -1).skip(skip).limit(limit)
        return [_serialize(d) async for d in cursor]

    async def count(
        self,
        *,
        status: Optional[str] = None,
        user_id: Optional[str] = None,
        test_id: Optional[str] = None,
    ) -> int:
        query: dict = {}
        if status:  query["status"]  = status
        if user_id: query["user_id"] = user_id
        if test_id: query["test_id"] = test_id
        return await self.col.count_documents(query)

    async def update(self, oid: str, data: dict) -> Optional[dict]:
        data["updated_at"] = datetime.now(timezone.utc)
        await self.col.update_one({"_id": _oid(oid)}, {"$set": data})
        return await self.find_by_id(oid)


# ─────────────────────────────────────────────
# Speaking Practice Sessions
# ─────────────────────────────────────────────

class SpeakingPracticeRepository:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.col = db["speaking_practice_sessions"]

    async def create(self, data: dict) -> dict:
        data["created_at"] = datetime.now(timezone.utc)
        data.setdefault("answers", [])
        data.setdefault("status", "in_progress")
        result = await self.col.insert_one(data)
        data["_id"] = result.inserted_id
        return _serialize(data)

    async def find_by_id(self, sid: str) -> Optional[dict]:
        doc = await self.col.find_one({"_id": _oid(sid)})
        return _serialize(doc) if doc else None

    async def find_by_user(self, user_id: str, skip: int = 0, limit: int = 20) -> List[dict]:
        cursor = self.col.find({"user_id": user_id}).sort("created_at", -1).skip(skip).limit(limit)
        return [_serialize(d) async for d in cursor]

    async def push_answer(self, sid: str, answer: dict) -> Optional[dict]:
        """Push or replace an answer by question_id + index."""
        answer["submitted_at"] = datetime.now(timezone.utc).isoformat()
        pull_filter: dict = {"index": answer["index"]}
        if answer.get("question_id"):
            pull_filter["question_id"] = answer["question_id"]
        await self.col.update_one(
            {"_id": _oid(sid)},
            {
                "$pull": {"answers": pull_filter},
                "$set": {"updated_at": datetime.now(timezone.utc)},
            },
        )
        await self.col.update_one(
            {"_id": _oid(sid)},
            {"$push": {"answers": answer}},
        )
        return await self.find_by_id(sid)

    async def complete(self, sid: str, extra: dict | None = None) -> Optional[dict]:
        fields = {"status": "completed", "updated_at": datetime.now(timezone.utc)}
        if extra:
            fields.update(extra)
        await self.col.update_one({"_id": _oid(sid)}, {"$set": fields})
        return await self.find_by_id(sid)
