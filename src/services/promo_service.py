"""
PromoService — orchestrates partner/promo-code lifecycle.

Responsibilities:
  * Generate unique single-use codes for a campaign.
  * Validate a code at checkout (preview only).
  * Atomically reserve a code against an order, commit on payment, release on cancel.
  * Build partner-portal summary aggregates.

Race-safety: code state transitions use Mongo `findOneAndUpdate` with a status
filter so two simultaneous redemptions of the last code race for the same row;
exactly one wins.
"""
from __future__ import annotations

import secrets
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException, status
from pymongo.errors import DuplicateKeyError

from schemas.partners import (
    CampaignStatus,
    CampaignType,
    PartnerStatus,
    PromoCodeStatus,
)
from src.database.repositories.partner_repository import (
    CampaignRepository,
    PartnerRepository,
    PromoCodeRepository,
    RedemptionRepository,
)


# Avoid ambiguous chars: 0/O 1/I/L
_CODE_ALPHABET = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(dt: Optional[datetime]) -> Optional[datetime]:
    """Mongo returns naive datetimes; treat them as UTC for comparison."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _random_body(length: int = 8) -> str:
    return "".join(secrets.choice(_CODE_ALPHABET) for _ in range(length))


class PromoService:
    def __init__(
        self,
        *,
        partner_repo: PartnerRepository,
        campaign_repo: CampaignRepository,
        code_repo: PromoCodeRepository,
        redemption_repo: RedemptionRepository,
    ):
        self.partner_repo    = partner_repo
        self.campaign_repo   = campaign_repo
        self.code_repo       = code_repo
        self.redemption_repo = redemption_repo

    # ── Code generation ──────────────────────────────────────

    async def generate_codes_for_campaign(
        self,
        campaign: Dict[str, Any],
        n: int,
    ) -> int:
        """Insert n unique codes for a campaign. Retries on duplicate-key collisions."""
        prefix = (campaign.get("prefix") or "").upper()
        valid_until = campaign.get("valid_until")
        partner_id = campaign["partner_id"]
        campaign_id = campaign["id"]

        inserted = 0
        attempts = 0
        max_attempts = n * 5  # generous retry budget for duplicates
        batch: List[dict] = []
        seen: set[str] = set()

        while inserted < n and attempts < max_attempts:
            attempts += 1
            body = _random_body(8)
            code = f"{prefix}{body}"
            if code in seen:
                continue
            seen.add(code)
            batch.append({
                "code":                  code,
                "campaign_id":           campaign_id,
                "partner_id":            partner_id,
                "status":                PromoCodeStatus.ACTIVE.value,
                "campaign_valid_until":  valid_until,
                "created_at":            _now(),
            })
            if len(batch) >= 200 or len(batch) + inserted >= n:
                try:
                    written = await self.code_repo.bulk_insert(batch)
                    inserted += written
                except DuplicateKeyError:
                    # On collision, fall back to per-row insert with retries.
                    for doc in batch:
                        for _ in range(5):
                            try:
                                await self.code_repo.bulk_insert([doc])
                                inserted += 1
                                break
                            except DuplicateKeyError:
                                doc["code"] = f"{prefix}{_random_body(8)}"
                                continue
                batch = []

        if inserted < n:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Could only mint {inserted}/{n} unique codes — try a different prefix or shorter total",
            )
        return inserted

    # ── Validate (preview only) ──────────────────────────────

    async def preview_redeem(
        self,
        *,
        code: str,
        user_id: str,
        original_amount: float,
        currency: str = "MNT",
    ) -> Dict[str, Any]:
        """Pure read — no state mutations. Returns a structured result."""
        result: Dict[str, Any] = {
            "valid":           False,
            "reason":          None,
            "code_type":       None,
            "discount_pct":    None,
            "amount_original": original_amount,
            "amount_after":    original_amount,
            "amount_discounted": 0.0,
            "currency":        currency,
            "free":            False,
        }

        code_doc = await self.code_repo.find_by_code(code.strip())
        if not code_doc:
            result["reason"] = "Код олдсонгүй"
            return result
        if code_doc["status"] != PromoCodeStatus.ACTIVE.value:
            result["reason"] = f"Код {code_doc['status']} төлөвт байна"
            return result

        campaign = await self.campaign_repo.find_by_id(code_doc["campaign_id"])
        if not campaign:
            result["reason"] = "Кампанит ажил олдсонгүй"
            return result
        if campaign.get("status") != CampaignStatus.ACTIVE.value:
            result["reason"] = "Кампанит ажил идэвхгүй байна"
            return result

        partner = await self.partner_repo.find_by_id(campaign["partner_id"])
        if not partner or partner.get("status") != PartnerStatus.ACTIVE.value:
            result["reason"] = "Хамтрагч идэвхгүй"
            return result

        now = _now()
        valid_from = _as_utc(campaign.get("valid_from"))
        valid_until = _as_utc(campaign.get("valid_until"))
        if valid_from and now < valid_from:
            result["reason"] = "Кодын хүчинтэй хугацаа эхлээгүй байна"
            return result
        if valid_until and now > valid_until:
            result["reason"] = "Кодын хугацаа дууссан"
            return result

        if await self.redemption_repo.user_used_campaign(
            user_id=user_id, campaign_id=campaign["id"],
        ):
            result["reason"] = "Та энэ кампанит ажилд аль хэдийн нэг код ашигласан байна"
            return result

        code_type = campaign["code_type"]
        result["code_type"] = code_type

        if code_type == CampaignType.FREE.value:
            max_uses = campaign.get("max_uses")
            if max_uses is not None and campaign.get("used_count", 0) >= int(max_uses):
                result["reason"] = "Үнэгүй кодын дээд тоонд хүрсэн"
                return result
            result["valid"] = True
            result["free"] = True
            result["discount_pct"] = 100.0
            result["amount_after"] = 0.0
            result["amount_discounted"] = original_amount
            return result

        # discount
        pct = float(campaign.get("discount_pct") or 0.0)
        if pct <= 0 or pct > 100:
            result["reason"] = "Кодын хөнгөлөлтийн хувь буруу"
            return result
        discount = round(original_amount * (pct / 100.0), 2)
        after = max(0.0, round(original_amount - discount, 2))
        result["valid"] = True
        result["discount_pct"] = pct
        result["amount_discounted"] = discount
        result["amount_after"] = after
        result["free"] = after <= 0
        return result

    # ── Reserve / commit / release ───────────────────────────

    async def reserve_code(
        self,
        *,
        code: str,
        order_id: str,
        user_id: str,
    ) -> Dict[str, Any]:
        """Atomically mark a code as reserved against `order_id`. Returns the code doc."""
        reserved = await self.code_repo.reserve(code, order_id=order_id, user_id=user_id)
        if not reserved:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Promo code is no longer available",
            )
        return reserved

    async def commit_redemption(
        self,
        *,
        order: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Called when an order transitions to paid. Idempotent on order_id."""
        code_id = order.get("promo_code_id")
        if not code_id:
            return None

        # Idempotency: bail if already recorded.
        if await self.redemption_repo.find_by_order(order["id"]):
            return None

        code_doc = await self.code_repo.find_by_id(code_id)
        if not code_doc:
            return None

        if code_doc["status"] == PromoCodeStatus.RESERVED.value:
            await self.code_repo.commit_used(code_id, user_id=order["user_id"])
        # else: already used (idempotent retry) — fall through to write redemption row

        await self.campaign_repo.increment_used(code_doc["campaign_id"], delta=1)

        return await self.redemption_repo.create({
            "code_id":           code_id,
            "code":              code_doc["code"],
            "campaign_id":       code_doc["campaign_id"],
            "partner_id":        code_doc["partner_id"],
            "user_id":           order["user_id"],
            "order_id":          order["id"],
            "test_id":           order.get("test_id"),
            "amount_original":   float(order.get("original_amount") or order.get("amount") or 0),
            "amount_paid":       float(order.get("amount") or 0),
            "amount_discounted": float(order.get("discount_amount") or 0),
        })

    async def release_code_for_order(self, order_id: str) -> None:
        """Return a reserved code to the active pool when an order is cancelled/failed."""
        await self.code_repo.release_by_order(order_id)

    # ── Partner-portal aggregates ────────────────────────────

    async def partner_summary(self, partner_id: str) -> Dict[str, Any]:
        partner = await self.partner_repo.find_by_id(partner_id)
        if not partner:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Partner not found")

        total      = await self.code_repo.count(partner_id=partner_id)
        used       = await self.code_repo.count(partner_id=partner_id, status=PromoCodeStatus.USED.value)
        reserved   = await self.code_repo.count(partner_id=partner_id, status=PromoCodeStatus.RESERVED.value)
        active     = await self.code_repo.count(partner_id=partner_id, status=PromoCodeStatus.ACTIVE.value)
        expired    = await self.code_repo.count(partner_id=partner_id, status=PromoCodeStatus.EXPIRED.value)
        revoked    = await self.code_repo.count(partner_id=partner_id, status=PromoCodeStatus.REVOKED.value)
        campaigns  = await self.campaign_repo.count_by_partner(partner_id)

        now = _now()
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        red_total = await self.redemption_repo.count(partner_id=partner_id)
        red_month = await self.redemption_repo.count(partner_id=partner_id, since=month_start)

        sales_total = await self.redemption_repo.sum_amounts(partner_id=partner_id)
        sales_month = await self.redemption_repo.sum_amounts(partner_id=partner_id, since=month_start)

        share_pct = float(partner.get("profit_share_pct") or 0.0)
        payout_total = round(sales_total["paid"] * share_pct / 100.0, 2)
        payout_month = round(sales_month["paid"] * share_pct / 100.0, 2)

        return {
            "partner_id":        partner_id,
            "total_codes":       total,
            "used_codes":        used,
            "reserved_codes":    reserved,
            "active_codes":      active,
            "expired_codes":     expired,
            "revoked_codes":     revoked,
            "total_campaigns":   campaigns,
            "redemptions_total": red_total,
            "redemptions_month": red_month,
            "gross_sales_total": sales_total["paid"],
            "gross_sales_month": sales_month["paid"],
            "payout_total":      payout_total,
            "payout_month":      payout_month,
            "profit_share_pct":  share_pct,
        }

    # ── Maintenance ──────────────────────────────────────────

    async def expire_due_codes(self) -> int:
        return await self.code_repo.expire_due(before=_now())
