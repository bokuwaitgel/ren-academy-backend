"""
Repositories for the partner / promo-code system.

Collections:
  - partners
  - promo_campaigns
  - promo_codes      (unique index on `code`)
  - promo_redemptions
"""
from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def _oid(id_str: str) -> ObjectId:
    return ObjectId(id_str)


def _serialize(doc: Optional[dict]) -> Optional[dict]:
    if doc is None:
        return None
    doc["id"] = str(doc.pop("_id"))
    return doc


# ─────────────────────────────────────────────
# Partners
# ─────────────────────────────────────────────

class PartnerRepository:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.col = db["partners"]

    async def create_indexes(self):
        await self.col.create_index("name")
        await self.col.create_index("owner_user_id")
        await self.col.create_index("status")
        await self.col.create_index([("created_at", -1)])

    async def create(self, data: dict) -> dict:
        now = datetime.now(timezone.utc)
        data["created_at"] = now
        data["updated_at"] = now
        result = await self.col.insert_one(data)
        data["_id"] = result.inserted_id
        return _serialize(data)  # type: ignore[return-value]

    async def find_by_id(self, pid: str) -> Optional[dict]:
        try:
            doc = await self.col.find_one({"_id": _oid(pid)})
        except Exception:
            return None
        return _serialize(doc)

    async def find_by_owner_user_id(self, user_id: str) -> Optional[dict]:
        doc = await self.col.find_one({"owner_user_id": user_id})
        return _serialize(doc)

    async def list(self, skip: int = 0, limit: int = 20,
                   *, status: Optional[str] = None, search: Optional[str] = None) -> List[dict]:
        query: Dict[str, Any] = {}
        if status:
            query["status"] = status
        if search:
            query["name"] = {"$regex": search, "$options": "i"}
        cursor = self.col.find(query).sort("created_at", -1).skip(skip).limit(limit)
        return [_serialize(d) for d in [d async for d in cursor]]  # type: ignore[misc]

    async def count(self, *, status: Optional[str] = None, search: Optional[str] = None) -> int:
        query: Dict[str, Any] = {}
        if status:
            query["status"] = status
        if search:
            query["name"] = {"$regex": search, "$options": "i"}
        return await self.col.count_documents(query)

    async def update(self, pid: str, data: dict) -> Optional[dict]:
        data["updated_at"] = datetime.now(timezone.utc)
        await self.col.update_one({"_id": _oid(pid)}, {"$set": data})
        return await self.find_by_id(pid)

    async def delete(self, pid: str) -> bool:
        result = await self.col.delete_one({"_id": _oid(pid)})
        return result.deleted_count == 1


# ─────────────────────────────────────────────
# Campaigns
# ─────────────────────────────────────────────

class CampaignRepository:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.col = db["promo_campaigns"]

    async def create_indexes(self):
        await self.col.create_index("partner_id")
        await self.col.create_index("status")
        await self.col.create_index([("partner_id", 1), ("created_at", -1)])

    async def create(self, data: dict) -> dict:
        now = datetime.now(timezone.utc)
        data["created_at"] = now
        data["updated_at"] = now
        data.setdefault("used_count", 0)
        result = await self.col.insert_one(data)
        data["_id"] = result.inserted_id
        return _serialize(data)  # type: ignore[return-value]

    async def find_by_id(self, cid: str) -> Optional[dict]:
        try:
            doc = await self.col.find_one({"_id": _oid(cid)})
        except Exception:
            return None
        return _serialize(doc)

    async def list_by_partner(self, partner_id: str, skip: int = 0, limit: int = 50) -> List[dict]:
        cursor = self.col.find({"partner_id": partner_id}).sort("created_at", -1).skip(skip).limit(limit)
        return [_serialize(d) for d in [d async for d in cursor]]  # type: ignore[misc]

    async def count_by_partner(self, partner_id: str, *, status: Optional[str] = None) -> int:
        query: Dict[str, Any] = {"partner_id": partner_id}
        if status:
            query["status"] = status
        return await self.col.count_documents(query)

    async def update(self, cid: str, data: dict) -> Optional[dict]:
        data["updated_at"] = datetime.now(timezone.utc)
        await self.col.update_one({"_id": _oid(cid)}, {"$set": data})
        return await self.find_by_id(cid)

    async def increment_used(self, cid: str, delta: int = 1) -> None:
        await self.col.update_one(
            {"_id": _oid(cid)},
            {"$inc": {"used_count": delta},
             "$set": {"updated_at": datetime.now(timezone.utc)}},
        )


# ─────────────────────────────────────────────
# Promo Codes
# ─────────────────────────────────────────────

class PromoCodeRepository:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.col = db["promo_codes"]

    async def create_indexes(self):
        await self.col.create_index("code", unique=True)
        await self.col.create_index("campaign_id")
        await self.col.create_index("partner_id")
        await self.col.create_index("status")
        await self.col.create_index("order_id")
        await self.col.create_index("reserved_at")
        await self.col.create_index([("partner_id", 1), ("status", 1)])
        await self.col.create_index([("campaign_id", 1), ("status", 1), ("reserved_by", 1)])

    async def bulk_insert(self, docs: List[dict]) -> int:
        if not docs:
            return 0
        result = await self.col.insert_many(docs, ordered=False)
        return len(result.inserted_ids)

    async def find_by_code(self, code: str) -> Optional[dict]:
        doc = await self.col.find_one({"code": code.upper()})
        return _serialize(doc)

    async def find_by_id(self, cid: str) -> Optional[dict]:
        try:
            doc = await self.col.find_one({"_id": _oid(cid)})
        except Exception:
            return None
        return _serialize(doc)

    async def reserve(self, code: str, *, order_id: str, user_id: str) -> Optional[dict]:
        """Atomically claim an active code for an order. Returns the updated doc or None."""
        now = datetime.now(timezone.utc)
        doc = await self.col.find_one_and_update(
            {"code": code.upper(), "status": "active"},
            {"$set": {
                "status":       "reserved",
                "order_id":     order_id,
                "reserved_by":  user_id,
                "reserved_at":  now,
                "updated_at":   now,
            }},
            return_document=True,
        )
        return _serialize(doc)

    async def commit_used(self, code_id: str, *, user_id: str) -> Optional[dict]:
        now = datetime.now(timezone.utc)
        doc = await self.col.find_one_and_update(
            {"_id": _oid(code_id), "status": "reserved"},
            {"$set": {
                "status":          "used",
                "used_by_user_id": user_id,
                "used_at":         now,
                "updated_at":      now,
            }},
            return_document=True,
        )
        return _serialize(doc)

    async def release_by_order(self, order_id: str) -> Optional[dict]:
        """Return a reserved code back to active when the order is cancelled/failed."""
        now = datetime.now(timezone.utc)
        doc = await self.col.find_one_and_update(
            {"order_id": order_id, "status": "reserved"},
            {"$set": {"status": "active", "updated_at": now},
             "$unset": {"order_id": "", "reserved_by": "", "reserved_at": ""}},
            return_document=True,
        )
        return _serialize(doc)

    async def revoke(self, code_id: str) -> Optional[dict]:
        now = datetime.now(timezone.utc)
        doc = await self.col.find_one_and_update(
            {"_id": _oid(code_id), "status": {"$in": ["active", "reserved"]}},
            {"$set": {"status": "revoked", "updated_at": now}},
            return_document=True,
        )
        return _serialize(doc)

    async def list(
        self,
        *,
        campaign_id: Optional[str] = None,
        partner_id: Optional[str] = None,
        status: Optional[str] = None,
        skip: int = 0,
        limit: int = 50,
    ) -> List[dict]:
        query: Dict[str, Any] = {}
        if campaign_id: query["campaign_id"] = campaign_id
        if partner_id:  query["partner_id"]  = partner_id
        if status:      query["status"]      = status
        cursor = self.col.find(query).sort("created_at", -1).skip(skip).limit(limit)
        return [_serialize(d) for d in [d async for d in cursor]]  # type: ignore[misc]

    async def count(
        self,
        *,
        campaign_id: Optional[str] = None,
        partner_id: Optional[str] = None,
        status: Optional[str] = None,
    ) -> int:
        query: Dict[str, Any] = {}
        if campaign_id: query["campaign_id"] = campaign_id
        if partner_id:  query["partner_id"]  = partner_id
        if status:      query["status"]      = status
        return await self.col.count_documents(query)

    async def list_all_by_campaign(self, campaign_id: str) -> List[dict]:
        """Used for CSV / XLSX export."""
        cursor = self.col.find({"campaign_id": campaign_id}).sort("created_at", 1)
        return [_serialize(d) for d in [d async for d in cursor]]  # type: ignore[misc]

    async def expire_due(self, *, before: datetime) -> int:
        """Mark all active/reserved codes from campaigns whose valid_until has passed."""
        result = await self.col.update_many(
            {"status": {"$in": ["active", "reserved"]},
             "campaign_valid_until": {"$ne": None, "$lt": before}},
            {"$set": {"status": "expired", "updated_at": datetime.now(timezone.utc)}},
        )
        return result.modified_count

    async def release_stale(self, *, before: datetime) -> int:
        """Return reservations back to `active` if they have been held since `before`
        without being committed. Triggered by the background reaper."""
        now = datetime.now(timezone.utc)
        result = await self.col.update_many(
            {"status": "reserved", "reserved_at": {"$lt": before}},
            {"$set": {"status": "active", "updated_at": now},
             "$unset": {"order_id": "", "reserved_by": "", "reserved_at": ""}},
        )
        return result.modified_count

    async def user_has_reservation_in_campaign(
        self,
        *,
        user_id: str,
        campaign_id: str,
        code: Optional[str] = None,
    ) -> bool:
        """True if `user_id` currently holds a `reserved` code in `campaign_id`.
        Pass `code` to exclude that specific code (used when re-previewing the same
        code the user already holds — that case is handled by invoice reuse)."""
        query: Dict[str, Any] = {
            "campaign_id": campaign_id,
            "status":      "reserved",
            "reserved_by": user_id,
        }
        if code:
            query["code"] = {"$ne": code.upper()}
        doc = await self.col.find_one(query)
        return doc is not None


# ─────────────────────────────────────────────
# Redemptions
# ─────────────────────────────────────────────

class RedemptionRepository:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.col = db["promo_redemptions"]

    async def create_indexes(self):
        await self.col.create_index("partner_id")
        await self.col.create_index("campaign_id")
        await self.col.create_index("user_id")
        await self.col.create_index("order_id", unique=True)
        await self.col.create_index([("partner_id", 1), ("created_at", -1)])

    async def create(self, data: dict) -> dict:
        data["created_at"] = datetime.now(timezone.utc)
        result = await self.col.insert_one(data)
        data["_id"] = result.inserted_id
        return _serialize(data)  # type: ignore[return-value]

    async def find_by_order(self, order_id: str) -> Optional[dict]:
        doc = await self.col.find_one({"order_id": order_id})
        return _serialize(doc)

    async def user_used_campaign(self, *, user_id: str, campaign_id: str) -> bool:
        doc = await self.col.find_one({"user_id": user_id, "campaign_id": campaign_id})
        return doc is not None

    async def list(
        self,
        *,
        partner_id: Optional[str] = None,
        campaign_id: Optional[str] = None,
        skip: int = 0,
        limit: int = 50,
    ) -> List[dict]:
        query: Dict[str, Any] = {}
        if partner_id:  query["partner_id"]  = partner_id
        if campaign_id: query["campaign_id"] = campaign_id
        cursor = self.col.find(query).sort("created_at", -1).skip(skip).limit(limit)
        return [_serialize(d) for d in [d async for d in cursor]]  # type: ignore[misc]

    async def count(
        self,
        *,
        partner_id: Optional[str] = None,
        campaign_id: Optional[str] = None,
        since: Optional[datetime] = None,
    ) -> int:
        query: Dict[str, Any] = {}
        if partner_id:  query["partner_id"]  = partner_id
        if campaign_id: query["campaign_id"] = campaign_id
        if since:       query["created_at"]  = {"$gte": since}
        return await self.col.count_documents(query)

    async def sum_amounts(
        self,
        *,
        partner_id: str,
        since: Optional[datetime] = None,
    ) -> Dict[str, float]:
        match: Dict[str, Any] = {"partner_id": partner_id}
        if since:
            match["created_at"] = {"$gte": since}
        pipeline = [
            {"$match": match},
            {"$group": {
                "_id": None,
                "gross":     {"$sum": "$amount_original"},
                "paid":      {"$sum": "$amount_paid"},
                "discount":  {"$sum": "$amount_discounted"},
            }},
        ]
        async for row in self.col.aggregate(pipeline):
            return {
                "gross":    float(row.get("gross")    or 0),
                "paid":     float(row.get("paid")     or 0),
                "discount": float(row.get("discount") or 0),
            }
        return {"gross": 0.0, "paid": 0.0, "discount": 0.0}
