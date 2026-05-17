"""
Partner / promo-code API endpoints.

Admin-facing (super-admin only):
  admin/partners/list
  admin/partners/create
  admin/partners/get
  admin/partners/update
  admin/partners/delete
  admin/partners/campaigns/list
  admin/partners/campaigns/create        — atomically mints N unique codes
  admin/partners/campaigns/update
  admin/partners/codes/list
  admin/partners/codes/revoke
  admin/partners/codes/export            — CSV (string body)
  admin/partners/redemptions/list

Partner-facing (role=partner):
  partner/me
  partner/summary
  partner/campaigns/list
  partner/codes/list
  partner/codes/export
  partner/redemptions/list
"""
from datetime import datetime, timezone
from io import StringIO
import csv

from fastapi import HTTPException, status
from pydantic import ValidationError

from src.api.api_routes import register
from src.database.mongodb import MongoDB
from src.database.repositories.partner_repository import (
    CampaignRepository,
    PartnerRepository,
    PromoCodeRepository,
    RedemptionRepository,
)
from src.database.repositories.ielts_repository import OrderRepository, TestRepository
from src.database.repositories.user_repository import UserRepository
from src.services.auth_service import AuthService
from src.services.promo_service import PromoService

from schemas.partners import (
    CampaignCreateRequest,
    CampaignStatus,
    CampaignType,
    CampaignUpdateRequest,
    PartnerCreateRequest,
    PartnerStatus,
    PartnerUpdateRequest,
    PromoCodeStatus,
)


# ─────────────────────────────────────────────
# DI helpers
# ─────────────────────────────────────────────

def _repos():
    db = MongoDB.get_db()
    return {
        "partner":    PartnerRepository(db),
        "campaign":   CampaignRepository(db),
        "code":       PromoCodeRepository(db),
        "redemption": RedemptionRepository(db),
        "order":      OrderRepository(db),
        "test":       TestRepository(db),
        "user":       UserRepository(db),
    }


def _promo_service(repos: dict) -> PromoService:
    return PromoService(
        partner_repo=repos["partner"],
        campaign_repo=repos["campaign"],
        code_repo=repos["code"],
        redemption_repo=repos["redemption"],
    )


def _extract_token(payload: dict) -> str:
    token = payload.get("access_token") or payload.get("token")
    auth_header = payload.get("authorization")
    if not token and isinstance(auth_header, str) and auth_header.lower().startswith("bearer "):
        token = auth_header.split(" ", 1)[1].strip()
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing token")
    return str(token)


async def _current_user(payload: dict):
    db = MongoDB.get_db()
    auth_svc = AuthService(UserRepository(db))
    return await auth_svc.get_current_user(_extract_token(payload))


async def _require_super_admin(payload: dict):
    user = await _current_user(payload)
    if user.role not in {"super_admin", "super-admin"}:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Super-admin access required")
    return user


async def _require_admin(payload: dict):
    user = await _current_user(payload)
    if user.role not in {"admin", "super_admin", "super-admin"}:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required")
    return user


async def _require_partner(payload: dict):
    user = await _current_user(payload)
    if user.role != "partner":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Partner access required")
    repos = _repos()
    partner = await repos["partner"].find_by_owner_user_id(user.id)
    if not partner:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No partner profile linked to this account")
    if partner.get("status") != PartnerStatus.ACTIVE.value:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Partner account is suspended")
    return user, partner, repos


def _clean_meta(data: dict):
    for key in ("authorization", "access_token", "token"):
        data.pop(key, None)


def _paginate(items, total, page, page_size):
    return {
        "items":       items,
        "total":       total,
        "page":        page,
        "page_size":   page_size,
        "total_pages": max(1, (total + page_size - 1) // page_size),
    }


async def _enrich_code_users(repos, codes):
    """Attach used_by_username for each code with a used_by_user_id."""
    user_repo = repos["user"]
    for c in codes:
        uid = c.get("used_by_user_id")
        if not uid:
            continue
        try:
            u = await user_repo.find_by_id(uid)
            if u:
                c["used_by_username"] = u.get("username")
        except Exception:
            pass
    return codes


# ═════════════════════════════════════════════
#  ADMIN — PARTNERS (super-admin only)
# ═════════════════════════════════════════════

@register(
    name="admin/partners/list",
    method="GET",
    required_keys=[],
    optional_keys={"page": 1, "page_size": 20, "status": None, "search": None},
    summary="List partners",
    tags=["Partners"],
)
async def admin_partners_list(data: dict):
    await _require_admin(data)
    repos = _repos()
    page = int(data.get("page") or 1)
    page_size = int(data.get("page_size") or 20)
    items = await repos["partner"].list(
        skip=(page - 1) * page_size, limit=page_size,
        status=data.get("status"), search=data.get("search"),
    )
    # Decorate with counters
    for p in items:
        p["total_codes"]      = await repos["code"].count(partner_id=p["id"])
        p["used_codes"]       = await repos["code"].count(partner_id=p["id"], status=PromoCodeStatus.USED.value)
        p["active_campaigns"] = await repos["campaign"].count_by_partner(p["id"], status=CampaignStatus.ACTIVE.value)
    total = await repos["partner"].count(status=data.get("status"), search=data.get("search"))
    return _paginate(items, total, page, page_size)


@register(
    name="admin/partners/get",
    method="GET",
    required_keys=["partner_id"],
    summary="Get partner detail",
    tags=["Partners"],
)
async def admin_partners_get(data: dict):
    await _require_admin(data)
    repos = _repos()
    partner = await repos["partner"].find_by_id(data["partner_id"])
    if not partner:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Partner not found")
    partner["total_codes"]      = await repos["code"].count(partner_id=partner["id"])
    partner["used_codes"]       = await repos["code"].count(partner_id=partner["id"], status=PromoCodeStatus.USED.value)
    partner["active_campaigns"] = await repos["campaign"].count_by_partner(partner["id"], status=CampaignStatus.ACTIVE.value)
    return partner


@register(
    name="admin/partners/create",
    method="POST",
    required_keys=["name"],
    optional_keys={"contact_email": None, "contact_phone": None, "contract_note": None,
                   "profit_share_pct": 0.0, "owner_user_id": None},
    summary="Create partner",
    tags=["Partners"],
)
async def admin_partners_create(data: dict):
    await _require_super_admin(data)
    _clean_meta(data)
    try:
        req = PartnerCreateRequest(**data)
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=exc.errors())
    repos = _repos()

    # Validate optional owner user
    if req.owner_user_id:
        u = await repos["user"].find_by_id(req.owner_user_id)
        if not u:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="owner_user_id not found")
        if await repos["partner"].find_by_owner_user_id(req.owner_user_id):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="User already owns another partner")

    return await repos["partner"].create({
        "name":             req.name,
        "contact_email":    req.contact_email,
        "contact_phone":    req.contact_phone,
        "contract_note":    req.contract_note,
        "profit_share_pct": float(req.profit_share_pct),
        "owner_user_id":    req.owner_user_id,
        "status":           PartnerStatus.ACTIVE.value,
    })


@register(
    name="admin/partners/update",
    method="PUT",
    required_keys=["partner_id"],
    optional_keys={"name": None, "contact_email": None, "contact_phone": None, "contract_note": None,
                   "profit_share_pct": None, "owner_user_id": None, "status": None},
    summary="Update partner",
    tags=["Partners"],
)
async def admin_partners_update(data: dict):
    await _require_super_admin(data)
    pid = data.pop("partner_id")
    _clean_meta(data)
    try:
        req = PartnerUpdateRequest(**data)
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=exc.errors())
    repos = _repos()
    update = {k: v for k, v in req.model_dump(exclude_none=True).items()}
    if "status" in update and hasattr(update["status"], "value"):
        update["status"] = update["status"].value
    result = await repos["partner"].update(pid, update)
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Partner not found")
    return result


@register(
    name="admin/partners/delete",
    method="DELETE",
    required_keys=["partner_id"],
    summary="Delete partner (only if no codes have been used)",
    tags=["Partners"],
)
async def admin_partners_delete(data: dict):
    await _require_super_admin(data)
    repos = _repos()
    pid = data["partner_id"]
    used = await repos["code"].count(partner_id=pid, status=PromoCodeStatus.USED.value)
    if used > 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Cannot delete partner with redeemed codes; suspend instead")
    ok = await repos["partner"].delete(pid)
    if not ok:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Partner not found")
    return {"status": "deleted"}


# ═════════════════════════════════════════════
#  ADMIN — CAMPAIGNS
# ═════════════════════════════════════════════

@register(
    name="admin/partners/campaigns/list",
    method="GET",
    required_keys=["partner_id"],
    optional_keys={"page": 1, "page_size": 50},
    summary="List campaigns for a partner",
    tags=["Partners"],
)
async def admin_campaigns_list(data: dict):
    await _require_admin(data)
    repos = _repos()
    pid = data["partner_id"]
    page = int(data.get("page") or 1)
    page_size = int(data.get("page_size") or 50)
    items = await repos["campaign"].list_by_partner(pid, skip=(page-1)*page_size, limit=page_size)
    total = await repos["campaign"].count_by_partner(pid)
    return _paginate(items, total, page, page_size)


@register(
    name="admin/partners/campaigns/create",
    method="POST",
    required_keys=["partner_id", "name", "code_type", "total_codes"],
    optional_keys={"discount_pct": None, "max_uses": None, "prefix": None,
                   "valid_from": None, "valid_until": None},
    summary="Create a campaign and mint its unique codes",
    tags=["Partners"],
)
async def admin_campaigns_create(data: dict):
    user = await _require_super_admin(data)
    pid = data.pop("partner_id")
    _clean_meta(data)
    try:
        req = CampaignCreateRequest(**data)
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=exc.errors())

    if req.code_type == CampaignType.DISCOUNT and not req.discount_pct:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="discount_pct is required for discount campaigns")
    if req.code_type == CampaignType.FREE and not req.max_uses:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="max_uses is required for free campaigns")

    repos = _repos()
    partner = await repos["partner"].find_by_id(pid)
    if not partner:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Partner not found")

    prefix = (req.prefix or "").upper()
    if prefix and not all(c.isalnum() or c in "-_" for c in prefix):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="prefix must be alphanumeric / dash / underscore")

    campaign = await repos["campaign"].create({
        "partner_id":   pid,
        "name":         req.name,
        "code_type":    req.code_type.value,
        "discount_pct": float(req.discount_pct) if req.discount_pct else None,
        "max_uses":     int(req.max_uses) if req.max_uses else None,
        "total_codes":  int(req.total_codes),
        "prefix":       prefix or None,
        "valid_from":   req.valid_from,
        "valid_until":  req.valid_until,
        "status":       CampaignStatus.ACTIVE.value,
        "created_by":   user.id,
    })

    promo = _promo_service(repos)
    minted = await promo.generate_codes_for_campaign(campaign, int(req.total_codes))
    campaign["minted"] = minted
    return campaign


@register(
    name="admin/partners/campaigns/update",
    method="PUT",
    required_keys=["campaign_id"],
    optional_keys={"name": None, "valid_from": None, "valid_until": None, "status": None},
    summary="Update campaign metadata",
    tags=["Partners"],
)
async def admin_campaigns_update(data: dict):
    await _require_super_admin(data)
    cid = data.pop("campaign_id")
    _clean_meta(data)
    try:
        req = CampaignUpdateRequest(**data)
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=exc.errors())
    repos = _repos()
    update = {k: v for k, v in req.model_dump(exclude_none=True).items()}
    if "status" in update and hasattr(update["status"], "value"):
        update["status"] = update["status"].value
    result = await repos["campaign"].update(cid, update)
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Campaign not found")
    return result


# ═════════════════════════════════════════════
#  ADMIN — CODES
# ═════════════════════════════════════════════

@register(
    name="admin/partners/codes/list",
    method="GET",
    required_keys=[],
    optional_keys={"campaign_id": None, "partner_id": None, "status": None, "page": 1, "page_size": 50},
    summary="List promo codes (filter by campaign or partner)",
    tags=["Partners"],
)
async def admin_codes_list(data: dict):
    await _require_admin(data)
    repos = _repos()
    page = int(data.get("page") or 1)
    page_size = int(data.get("page_size") or 50)
    items = await repos["code"].list(
        campaign_id=data.get("campaign_id"),
        partner_id=data.get("partner_id"),
        status=data.get("status"),
        skip=(page-1)*page_size, limit=page_size,
    )
    items = await _enrich_code_users(repos, items)
    total = await repos["code"].count(
        campaign_id=data.get("campaign_id"),
        partner_id=data.get("partner_id"),
        status=data.get("status"),
    )
    return _paginate(items, total, page, page_size)


@register(
    name="admin/partners/codes/revoke",
    method="POST",
    required_keys=["code_id"],
    summary="Revoke a promo code (only active or reserved codes)",
    tags=["Partners"],
)
async def admin_codes_revoke(data: dict):
    await _require_super_admin(data)
    repos = _repos()
    doc = await repos["code"].revoke(data["code_id"])
    if not doc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Code not found or already used")
    return doc


@register(
    name="admin/partners/codes/export",
    method="GET",
    required_keys=["campaign_id"],
    summary="Export all codes of a campaign as CSV",
    tags=["Partners"],
)
async def admin_codes_export(data: dict):
    await _require_admin(data)
    repos = _repos()
    campaign = await repos["campaign"].find_by_id(data["campaign_id"])
    if not campaign:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Campaign not found")
    items = await repos["code"].list_all_by_campaign(data["campaign_id"])

    buf = StringIO()
    writer = csv.writer(buf)
    writer.writerow(["code", "status", "used_by_user_id", "used_at", "created_at"])
    for c in items:
        writer.writerow([
            c.get("code"),
            c.get("status"),
            c.get("used_by_user_id") or "",
            c.get("used_at").isoformat() if c.get("used_at") else "",
            c.get("created_at").isoformat() if c.get("created_at") else "",
        ])
    return {
        "filename": f"{campaign.get('name', 'campaign')}_codes.csv",
        "content_type": "text/csv",
        "body": buf.getvalue(),
        "row_count": len(items),
    }


@register(
    name="admin/partners/redemptions/list",
    method="GET",
    required_keys=[],
    optional_keys={"partner_id": None, "campaign_id": None, "page": 1, "page_size": 50},
    summary="List redemption events",
    tags=["Partners"],
)
async def admin_redemptions_list(data: dict):
    await _require_admin(data)
    repos = _repos()
    page = int(data.get("page") or 1)
    page_size = int(data.get("page_size") or 50)
    items = await repos["redemption"].list(
        partner_id=data.get("partner_id"),
        campaign_id=data.get("campaign_id"),
        skip=(page-1)*page_size, limit=page_size,
    )
    total = await repos["redemption"].count(
        partner_id=data.get("partner_id"),
        campaign_id=data.get("campaign_id"),
    )
    # Enrich with usernames + test titles
    for r in items:
        try:
            u = await repos["user"].find_by_id(r["user_id"])
            if u:
                r["username"] = u.get("username")
        except Exception:
            pass
        tid = r.get("test_id")
        if tid:
            try:
                t = await repos["test"].find_by_id(str(tid))
                if t:
                    r["test_title"] = t.get("title")
            except Exception:
                pass
    return _paginate(items, total, page, page_size)


# ═════════════════════════════════════════════
#  PARTNER — self-service (role=partner)
# ═════════════════════════════════════════════

@register(
    name="partner/me",
    method="GET",
    required_keys=[],
    summary="Return current partner profile",
    tags=["Partner Portal"],
)
async def partner_me(data: dict):
    user, partner, _ = await _require_partner(data)
    return {"user": {"id": user.id, "username": user.username, "email": user.email},
            "partner": partner}


@register(
    name="partner/summary",
    method="GET",
    required_keys=[],
    summary="Aggregate summary for the partner dashboard",
    tags=["Partner Portal"],
)
async def partner_summary(data: dict):
    _, partner, repos = await _require_partner(data)
    promo = _promo_service(repos)
    return await promo.partner_summary(partner["id"])


@register(
    name="partner/campaigns/list",
    method="GET",
    required_keys=[],
    optional_keys={"page": 1, "page_size": 50},
    summary="List own campaigns",
    tags=["Partner Portal"],
)
async def partner_campaigns_list(data: dict):
    _, partner, repos = await _require_partner(data)
    page = int(data.get("page") or 1)
    page_size = int(data.get("page_size") or 50)
    items = await repos["campaign"].list_by_partner(partner["id"], skip=(page-1)*page_size, limit=page_size)
    total = await repos["campaign"].count_by_partner(partner["id"])
    return _paginate(items, total, page, page_size)


@register(
    name="partner/codes/list",
    method="GET",
    required_keys=[],
    optional_keys={"campaign_id": None, "status": None, "page": 1, "page_size": 50},
    summary="List own codes",
    tags=["Partner Portal"],
)
async def partner_codes_list(data: dict):
    _, partner, repos = await _require_partner(data)
    page = int(data.get("page") or 1)
    page_size = int(data.get("page_size") or 50)
    items = await repos["code"].list(
        partner_id=partner["id"],
        campaign_id=data.get("campaign_id"),
        status=data.get("status"),
        skip=(page-1)*page_size, limit=page_size,
    )
    items = await _enrich_code_users(repos, items)
    total = await repos["code"].count(
        partner_id=partner["id"],
        campaign_id=data.get("campaign_id"),
        status=data.get("status"),
    )
    return _paginate(items, total, page, page_size)


@register(
    name="partner/codes/export",
    method="GET",
    required_keys=["campaign_id"],
    summary="Export own campaign codes as CSV",
    tags=["Partner Portal"],
)
async def partner_codes_export(data: dict):
    _, partner, repos = await _require_partner(data)
    campaign = await repos["campaign"].find_by_id(data["campaign_id"])
    if not campaign or campaign.get("partner_id") != partner["id"]:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Campaign not found")
    items = await repos["code"].list_all_by_campaign(data["campaign_id"])
    buf = StringIO()
    writer = csv.writer(buf)
    writer.writerow(["code", "status", "used_at"])
    for c in items:
        writer.writerow([
            c.get("code"),
            c.get("status"),
            c.get("used_at").isoformat() if c.get("used_at") else "",
        ])
    return {
        "filename": f"{campaign.get('name', 'campaign')}_codes.csv",
        "content_type": "text/csv",
        "body": buf.getvalue(),
        "row_count": len(items),
    }


@register(
    name="partner/redemptions/list",
    method="GET",
    required_keys=[],
    optional_keys={"campaign_id": None, "page": 1, "page_size": 50},
    summary="List own redemptions (user nickname + test only — no PII)",
    tags=["Partner Portal"],
)
async def partner_redemptions_list(data: dict):
    _, partner, repos = await _require_partner(data)
    page = int(data.get("page") or 1)
    page_size = int(data.get("page_size") or 50)
    items = await repos["redemption"].list(
        partner_id=partner["id"],
        campaign_id=data.get("campaign_id"),
        skip=(page-1)*page_size, limit=page_size,
    )
    # Strip to non-PII fields and add nickname / test title
    out = []
    for r in items:
        username = None
        try:
            u = await repos["user"].find_by_id(r["user_id"])
            if u:
                username = u.get("username")
        except Exception:
            pass
        test_title = None
        if r.get("test_id"):
            try:
                t = await repos["test"].find_by_id(str(r["test_id"]))
                if t:
                    test_title = t.get("title")
            except Exception:
                pass
        out.append({
            "id":                r["id"],
            "code":              r.get("code"),
            "campaign_id":       r["campaign_id"],
            "username":          username,           # nickname only — no email/phone
            "test_title":        test_title,
            "amount_original":   r.get("amount_original"),
            "amount_paid":       r.get("amount_paid"),
            "amount_discounted": r.get("amount_discounted"),
            "created_at":        r.get("created_at"),
        })
    total = await repos["redemption"].count(
        partner_id=partner["id"],
        campaign_id=data.get("campaign_id"),
    )
    return _paginate(out, total, page, page_size)
