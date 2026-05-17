"""
Partner / promo-code schemas.

A Partner is an external organisation (school, company, course) that REN Academy
contracts with to distribute promo codes to its own members. Each contract is
represented by one or more PromoCampaigns, which in turn own a fixed pool of
unique single-use PromoCodes.

Lifecycle:
  PromoCode:    active → reserved(order_id) → used        (happy path)
                active → expired                          (valid_until passed)
                active → revoked                          (admin action)
                reserved → active                         (order cancelled / failed)

  Redemption is recorded only when the underlying order reaches `paid`.
"""
from enum import Enum
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, EmailStr, Field


# ─────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────

class PartnerStatus(str, Enum):
    ACTIVE    = "active"
    SUSPENDED = "suspended"


class CampaignType(str, Enum):
    DISCOUNT = "discount"   # percent-off
    FREE     = "free"       # 100% free, capped by max_uses


class CampaignStatus(str, Enum):
    ACTIVE   = "active"
    PAUSED   = "paused"
    FINISHED = "finished"


class PromoCodeStatus(str, Enum):
    ACTIVE   = "active"
    RESERVED = "reserved"
    USED     = "used"
    EXPIRED  = "expired"
    REVOKED  = "revoked"


# ─────────────────────────────────────────────
# Partner
# ─────────────────────────────────────────────

class PartnerCreateRequest(BaseModel):
    name:               str = Field(..., min_length=2, max_length=120)
    contact_email:      Optional[EmailStr] = None
    contact_phone:      Optional[str] = Field(default=None, max_length=40)
    contract_note:      Optional[str] = Field(default=None, max_length=2000)
    profit_share_pct:   float = Field(default=0.0, ge=0.0, le=100.0)
    owner_user_id:      Optional[str] = Field(default=None, description="Existing user account to grant partner-portal access")


class PartnerUpdateRequest(BaseModel):
    name:             Optional[str] = Field(default=None, min_length=2, max_length=120)
    contact_email:    Optional[EmailStr] = None
    contact_phone:    Optional[str] = Field(default=None, max_length=40)
    contract_note:    Optional[str] = Field(default=None, max_length=2000)
    profit_share_pct: Optional[float] = Field(default=None, ge=0.0, le=100.0)
    owner_user_id:    Optional[str] = None
    status:           Optional[PartnerStatus] = None


class PartnerOut(BaseModel):
    id:               str
    name:             str
    contact_email:    Optional[str] = None
    contact_phone:    Optional[str] = None
    contract_note:    Optional[str] = None
    profit_share_pct: float
    owner_user_id:    Optional[str] = None
    status:           PartnerStatus
    created_at:       datetime
    updated_at:       Optional[datetime] = None

    # Computed counters (best-effort, populated by repository/service)
    total_codes:      Optional[int] = None
    used_codes:       Optional[int] = None
    active_campaigns: Optional[int] = None


# ─────────────────────────────────────────────
# Campaign
# ─────────────────────────────────────────────

class CampaignCreateRequest(BaseModel):
    name:          str = Field(..., min_length=2, max_length=120)
    code_type:     CampaignType
    discount_pct:  Optional[float] = Field(default=None, ge=1.0, le=100.0,
                                           description="Required for discount; ignored for free")
    max_uses:      Optional[int] = Field(default=None, ge=1, le=100000,
                                         description="Hard cap for free campaigns; advisory for discount")
    total_codes:   int = Field(..., ge=1, le=10000, description="Number of unique codes to mint")
    prefix:        Optional[str] = Field(default=None, max_length=12,
                                          description="Uppercase prefix prepended to every code, e.g. 'BRG-30-'")
    valid_from:    Optional[datetime] = None
    valid_until:   Optional[datetime] = None


class CampaignUpdateRequest(BaseModel):
    name:        Optional[str] = Field(default=None, min_length=2, max_length=120)
    valid_from:  Optional[datetime] = None
    valid_until: Optional[datetime] = None
    status:      Optional[CampaignStatus] = None


class CampaignOut(BaseModel):
    id:           str
    partner_id:   str
    name:         str
    code_type:    CampaignType
    discount_pct: Optional[float] = None
    max_uses:     Optional[int] = None
    total_codes:  int
    used_count:   int = 0
    prefix:       Optional[str] = None
    valid_from:   Optional[datetime] = None
    valid_until:  Optional[datetime] = None
    status:       CampaignStatus
    created_by:   Optional[str] = None
    created_at:   datetime
    updated_at:   Optional[datetime] = None


# ─────────────────────────────────────────────
# Promo Code
# ─────────────────────────────────────────────

class PromoCodeOut(BaseModel):
    id:               str
    campaign_id:      str
    partner_id:       str
    code:             str
    status:           PromoCodeStatus
    used_by_user_id:  Optional[str] = None
    used_by_username: Optional[str] = None
    used_at:          Optional[datetime] = None
    order_id:         Optional[str] = None
    reserved_at:      Optional[datetime] = None
    created_at:       datetime


# ─────────────────────────────────────────────
# Redemption
# ─────────────────────────────────────────────

class RedemptionOut(BaseModel):
    id:                str
    code_id:           str
    code:              Optional[str] = None
    campaign_id:       str
    partner_id:        str
    user_id:           str
    username:          Optional[str] = None
    order_id:          str
    test_id:           Optional[str] = None
    test_title:        Optional[str] = None
    amount_original:   float
    amount_paid:       float
    amount_discounted: float
    created_at:        datetime


# ─────────────────────────────────────────────
# Validate / Preview
# ─────────────────────────────────────────────

class PromoValidateRequest(BaseModel):
    code:    str = Field(..., min_length=3, max_length=64)
    test_id: str
    mode:    str = Field(default="full_test")
    section: Optional[str] = None


class PromoValidateResponse(BaseModel):
    valid:              bool
    reason:             Optional[str] = None
    code_type:          Optional[CampaignType] = None
    discount_pct:       Optional[float] = None
    amount_original:    Optional[float] = None
    amount_after:       Optional[float] = None
    amount_discounted:  Optional[float] = None
    currency:           Optional[str] = None
    free:               bool = False


# ─────────────────────────────────────────────
# Partner-portal summary
# ─────────────────────────────────────────────

class PartnerSummary(BaseModel):
    partner_id:        str
    total_codes:       int
    used_codes:        int
    reserved_codes:    int
    active_codes:      int
    expired_codes:     int
    revoked_codes:     int
    total_campaigns:   int
    redemptions_total: int
    redemptions_month: int
    gross_sales_total: float
    gross_sales_month: float
    payout_total:      float
    payout_month:      float
    profit_share_pct:  float
