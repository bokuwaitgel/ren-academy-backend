"""
Payment / order schemas.

An Order represents a user's intent to purchase a single test. Its lifecycle:
  pending → paid (via QPay callback or manual admin action)
          → cancelled (admin or user before payment)
          → refunded (admin after payment)
          → failed (terminal error)
"""
from enum import Enum
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class OrderStatus(str, Enum):
    PENDING   = "pending"
    PAID      = "paid"
    CANCELLED = "cancelled"
    REFUNDED  = "refunded"
    FAILED    = "failed"


class OrderCreateRequest(BaseModel):
    test_id: str = Field(..., description="Test the user wants to buy")
    mode: str = Field(default="full_test", description="Purchase scope: full_test or practice")
    section: Optional[str] = Field(default=None, description="Required when mode is practice")


class InvoicePayload(BaseModel):
    """Snapshot of QPay invoice fields useful to the frontend (QR / deeplinks)."""
    invoice_id: str
    qr_text:    Optional[str] = None
    qr_image:   Optional[str] = None
    qPay_shortUrl: Optional[str] = None
    urls:       Optional[List[Dict[str, Any]]] = None


class OrderOut(BaseModel):
    id:               str
    user_id:          str
    test_id:          str
    purchase_mode:    Optional[str] = None
    purchase_section: Optional[str] = None
    test_title:       Optional[str] = None
    amount:           float
    currency:         str
    status:           OrderStatus
    qpay_invoice_id:  Optional[str] = None
    qpay_payment_id:  Optional[str] = None
    invoice:          Optional[InvoicePayload] = None
    paid_at:          Optional[datetime] = None
    cancelled_at:     Optional[datetime] = None
    refunded_at:      Optional[datetime] = None
    manual:           bool = False
    manual_note:      Optional[str] = None
    manual_admin_id:  Optional[str] = None
    created_at:       datetime
    updated_at:       Optional[datetime] = None

    model_config = {"from_attributes": True}


class AdminOrderListFilters(BaseModel):
    page:      int = 1
    page_size: int = 20
    status:    Optional[OrderStatus] = None
    user_id:   Optional[str] = None
    test_id:   Optional[str] = None


class AdminMarkPaidRequest(BaseModel):
    order_id: str
    note:     Optional[str] = Field(default=None, max_length=500)


class AdminOrderActionRequest(BaseModel):
    """Used for cancel / refund / recheck — same shape, semantics differ."""
    order_id: str
    note:     Optional[str] = Field(default=None, max_length=500)


class CheckOrderResponse(BaseModel):
    order:     OrderOut
    paid_now:  bool = Field(default=False, description="True if this call flipped the order to paid")
