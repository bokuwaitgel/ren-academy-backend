"""
Payment API endpoints.

User-facing:
  payments/create-invoice  POST  — start paying for a test (creates order + QPay invoice)
  payments/check           GET   — poll order status (also forces a QPay sync)
  payments/my-orders       GET   — user's order history
  payments/qpay-callback   GET/POST — webhook QPay calls when invoice is paid

Admin-facing:
  admin/payments/list      GET
  admin/payments/get       GET
  admin/payments/cancel    POST
  admin/payments/refund    POST
  admin/payments/mark-paid POST
  admin/payments/recheck   POST
"""
from fastapi import HTTPException, status
from pydantic import ValidationError

from src.api.api_routes import register
from src.database.mongodb import MongoDB
from src.database.repositories.ielts_repository import OrderRepository, TestRepository
from src.database.repositories.user_repository import UserRepository
from src.services.auth_service import AuthService
from src.services.payment_service import PaymentService

from schemas.payments import OrderCreateRequest


# ── Helpers ───────────────────────────────────

def _services():
    db = MongoDB.get_db()
    return (
        PaymentService(
            order_repo=OrderRepository(db),
            test_repo =TestRepository(db),
        ),
        AuthService(UserRepository(db)),
    )


def _extract_token(payload: dict) -> str:
    token = payload.get("access_token") or payload.get("token")
    auth_header = payload.get("authorization")
    if not token and isinstance(auth_header, str) and auth_header.lower().startswith("bearer "):
        token = auth_header.split(" ", 1)[1].strip()
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing token")
    return str(token)


async def _require_auth(payload: dict):
    _, auth_svc = _services()
    user = await auth_svc.get_current_user(_extract_token(payload))
    return user


async def _require_admin(payload: dict):
    user = await _require_auth(payload)
    if user.role not in {"admin", "super_admin", "super-admin"}:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required")
    return user


def _clean_meta(data: dict):
    for key in ("authorization", "access_token", "token"):
        data.pop(key, None)


# ═════════════════════════════════════════════
#  USER ENDPOINTS
# ═════════════════════════════════════════════

@register(
    name="payments/create-invoice",
    method="POST",
    required_keys=["test_id"],
    summary="Create payment invoice for a test",
    description="Create an order + QPay invoice for the given test. Returns the order with QR / deeplinks.",
    tags=["Payments"],
)
async def payments_create_invoice(data: dict):
    user = await _require_auth(data)
    _clean_meta(data)
    try:
        payload = OrderCreateRequest(**data)
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=exc.errors())
    svc, _ = _services()
    return await svc.create_order(user_id=user.id, test_id=payload.test_id)


@register(
    name="payments/check",
    method="GET",
    required_keys=["order_id"],
    summary="Check / poll order payment status",
    description="Sync with QPay and return the latest order status. Frontends should poll this while showing the QR.",
    tags=["Payments"],
)
async def payments_check(data: dict):
    user = await _require_auth(data)
    svc, _ = _services()
    order = await svc.get_order(data["order_id"])
    if order["user_id"] != user.id and user.role not in {"admin", "super_admin", "super-admin", "examiner"}:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not your order")
    if order.get("status") == "paid":
        return {"order": order, "paid_now": False}
    order, paid_now = await svc.check_and_sync(data["order_id"])
    return {"order": order, "paid_now": paid_now}


@register(
    name="payments/my-orders",
    method="GET",
    required_keys=[],
    optional_keys={"page": 1, "page_size": 20},
    summary="List my orders",
    description="Authenticated user's order history.",
    tags=["Payments"],
)
async def payments_my_orders(data: dict):
    user = await _require_auth(data)
    svc, _ = _services()
    return await svc.list_my_orders(
        user_id=user.id,
        page=int(data.get("page", 1)),
        page_size=min(int(data.get("page_size", 20)), 100),
    )


@register(
    name="payments/qpay-callback",
    method="GET",
    required_keys=["order_id"],
    summary="QPay webhook (GET)",
    description="Webhook QPay calls when an invoice is paid. We re-verify against QPay before flipping the order.",
    tags=["Payments"],
)
async def payments_qpay_callback_get(data: dict):
    # QPay calls this URL with the order_id we stuffed into the callback. We re-check
    # against QPay rather than trusting the callback alone, then return 200 either way
    # so QPay doesn't retry forever on transient errors.
    svc, _ = _services()
    try:
        _, paid_now = await svc.check_and_sync(data["order_id"])
        return {"ok": True, "paid_now": paid_now}
    except HTTPException as e:
        return {"ok": False, "detail": str(e.detail)}


# ═════════════════════════════════════════════
#  ADMIN ENDPOINTS
# ═════════════════════════════════════════════

@register(
    name="admin/payments/list",
    method="GET",
    required_keys=[],
    optional_keys={"page": 1, "page_size": 20, "status": None, "user_id": None, "test_id": None},
    summary="List orders (admin)",
    description="List all orders with optional filters.",
    tags=["Admin"],
)
async def admin_payments_list(data: dict):
    await _require_admin(data)
    svc, _ = _services()
    return await svc.admin_list_orders(
        page=int(data.get("page", 1)),
        page_size=min(int(data.get("page_size", 20)), 100),
        status_=data.get("status"),
        user_id=data.get("user_id"),
        test_id=data.get("test_id"),
    )


@register(
    name="admin/payments/get",
    method="GET",
    required_keys=["order_id"],
    summary="Get order (admin)",
    description="Get full order details.",
    tags=["Admin"],
)
async def admin_payments_get(data: dict):
    await _require_admin(data)
    svc, _ = _services()
    return await svc.get_order(data["order_id"])


@register(
    name="admin/payments/recheck",
    method="POST",
    required_keys=["order_id"],
    summary="Recheck payment with QPay (admin)",
    description="Force a fresh QPay payment lookup. Useful if the callback was missed.",
    tags=["Admin"],
)
async def admin_payments_recheck(data: dict):
    await _require_admin(data)
    svc, _ = _services()
    order, paid_now = await svc.check_and_sync(data["order_id"])
    return {"order": order, "paid_now": paid_now}


@register(
    name="admin/payments/cancel",
    method="POST",
    required_keys=["order_id"],
    optional_keys={"note": None},
    summary="Cancel order (admin)",
    description="Cancel a pending order. Also cancels the QPay invoice.",
    tags=["Admin"],
)
async def admin_payments_cancel(data: dict):
    await _require_admin(data)
    svc, _ = _services()
    return await svc.admin_cancel_order(data["order_id"], note=data.get("note"))


@register(
    name="admin/payments/refund",
    method="POST",
    required_keys=["order_id"],
    optional_keys={"note": None},
    summary="Refund order (admin)",
    description="Refund a paid order via QPay.",
    tags=["Admin"],
)
async def admin_payments_refund(data: dict):
    await _require_admin(data)
    svc, _ = _services()
    return await svc.admin_refund_order(data["order_id"], note=data.get("note"))


@register(
    name="admin/payments/mark-paid",
    method="POST",
    required_keys=["order_id"],
    optional_keys={"note": None},
    summary="Mark order paid manually (admin)",
    description="Mark an order paid without going through QPay (e.g. cash / bank transfer received offline).",
    tags=["Admin"],
)
async def admin_payments_mark_paid(data: dict):
    admin = await _require_admin(data)
    svc, _ = _services()
    return await svc.admin_mark_paid(data["order_id"], admin_id=admin.id, note=data.get("note"))
