"""
PaymentService — orchestrates order lifecycle + QPay calls.

Order lifecycle:
  pending → paid       (QPay callback or admin mark-paid or admin recheck)
          → cancelled  (user-cancel before payment, or admin cancel)
          → refunded   (admin refund after payment)
          → failed     (terminal error)

Free tests (test.price == 0) bypass this service entirely.
"""
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException, status

from src.database.repositories.ielts_repository import (
    OrderRepository,
    TestRepository,
    TestSessionRepository,
)
from src.services.qpay_client import QPayClient, QPayError
from schemas.payments import OrderStatus


BACKEND_PUBLIC_URL = os.getenv("BACKEND_PUBLIC_URL", "http://localhost:8000").rstrip("/")


class PaymentService:
    def __init__(
        self,
        *,
        order_repo: OrderRepository,
        test_repo:  TestRepository,
        session_repo: Optional[TestSessionRepository] = None,
        qpay:       Optional[QPayClient] = None,
    ):
        self.order_repo   = order_repo
        self.test_repo    = test_repo
        self.session_repo = session_repo
        self.qpay         = qpay or QPayClient.instance()

    # ── Public read helpers ───────────────────────────────────

    async def user_has_paid(
        self,
        user_id: str,
        test_id: str,
        mode: str = "full_test",
        section: Optional[str] = None,
    ) -> bool:
        """Cheap lookup used to gate start_test. Only unspent paid orders count."""
        existing = await self.order_repo.find_paid_unconsumed_for_user_test(
            user_id,
            test_id,
            purchase_mode=mode,
            purchase_section=section if mode == "practice" else None,
        )
        return existing is not None

    def _resolve_price_for_scope(self, test: Dict[str, Any], mode: str, section: Optional[str]) -> float:
        if mode == "practice":
            section_prices = test.get("section_prices") or {}
            default_price = float(test.get("price") or 0)
            if not isinstance(section_prices, dict):
                return default_price
            if section:
                section_value = section_prices.get(section) if section in section_prices else default_price
                return float(section_value if section_value is not None else default_price)
            values = [float(v or 0) for v in section_prices.values()]
            return min(values) if values else default_price
        return float(test.get("price") or 0)

    async def access_status(
        self,
        *,
        user_id: str,
        test_id: str,
        mode: str = "full_test",
        section: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Returns the user's access state for a single test:
          - free            : test has price 0
          - in_progress     : user has an unfinished session — they can resume it
          - unstarted_paid  : user paid and hasn't started yet — they can start
          - unowned         : user must buy
        """
        test = await self.test_repo.find_by_id(test_id)
        if not test:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test not found")

        if mode not in {"full_test", "practice"}:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid mode")
        if mode == "practice" and section:
            if section not in {"listening", "reading", "writing", "speaking"}:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid practice section")

        price = self._resolve_price_for_scope(test, mode, section)
        if price <= 0:
            return {
                "test_id": test_id,
                "status": "free",
                "mode": mode,
                "practice_section": section if mode == "practice" else None,
                "price": 0,
                "currency": test.get("currency") or "MNT",
            }

        # In-progress session takes precedence — the user can resume regardless of orders.
        if self.session_repo is not None:
            query: Dict[str, Any] = {
                "user_id": user_id,
                "test_id": test_id,
                "status": "in_progress",
                "mode": mode,
            }
            if mode == "practice" and section:
                query["practice_section"] = section
            in_progress = await self.session_repo.col.find_one(query)
            if in_progress:
                return {
                    "test_id": test_id,
                    "status": "in_progress",
                    "session_id": str(in_progress["_id"]),
                    "mode": in_progress.get("mode"),
                    "practice_section": in_progress.get("practice_section"),
                    "price": price,
                    "currency": test.get("currency") or "MNT",
                }

        unspent = await self.order_repo.find_paid_unconsumed_for_user_test(
            user_id,
            test_id,
            purchase_mode=mode,
            purchase_section=section if mode == "practice" else None,
        )
        if unspent:
            return {
                "test_id": test_id,
                "status": "unstarted_paid",
                "order_id": unspent["id"],
                "mode": mode,
                "practice_section": (
                    section if (mode == "practice" and section) else unspent.get("purchase_section")
                ),
                "price": price,
                "currency": test.get("currency") or "MNT",
            }

        return {
            "test_id": test_id,
            "status": "unowned",
            "mode": mode,
            "practice_section": section if mode == "practice" else None,
            "price": price,
            "currency": test.get("currency") or "MNT",
        }

    async def access_statuses(
        self,
        *,
        user_id: str,
        test_ids: List[str],
        mode: str = "full_test",
        section: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Bulk version of access_status for the dashboard."""
        unique = [t for t in dict.fromkeys(test_ids) if t]
        items: List[Dict[str, Any]] = []
        for tid in unique:
            try:
                items.append(await self.access_status(user_id=user_id, test_id=tid, mode=mode, section=section))
            except HTTPException:
                # Skip missing tests rather than fail the whole batch.
                continue
        return {"items": items}

    async def list_my_orders(self, user_id: str, page: int = 1, page_size: int = 20) -> Dict[str, Any]:
        skip = (page - 1) * page_size
        items = await self.order_repo.list(skip=skip, limit=page_size, user_id=user_id)
        items = await self._enrich_with_test_titles(items)
        total = await self.order_repo.count(user_id=user_id)
        return _paginate(items, total, page, page_size)

    async def admin_list_orders(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
        status_: Optional[str] = None,
        user_id: Optional[str] = None,
        test_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        skip = (page - 1) * page_size
        items = await self.order_repo.list(
            skip=skip, limit=page_size,
            status=status_, user_id=user_id, test_id=test_id,
        )
        items = await self._enrich_with_test_titles(items)
        total = await self.order_repo.count(status=status_, user_id=user_id, test_id=test_id)
        return _paginate(items, total, page, page_size)

    async def get_order(self, order_id: str) -> Dict[str, Any]:
        order = await self.order_repo.find_by_id(order_id)
        if not order:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Order not found")
        order = (await self._enrich_with_test_titles([order]))[0]
        return order

    # ── Create + check ────────────────────────────────────────

    async def create_order(
        self,
        *,
        user_id: str,
        test_id: str,
        mode: str = "full_test",
        section: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a pending order + QPay invoice. Returns the order with invoice payload attached."""
        test = await self.test_repo.find_by_id(test_id)
        if not test:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test not found")
        if not test.get("is_published"):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Test is not published")

        if mode not in {"full_test", "practice"}:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid mode")
        if mode == "practice":
            if not section:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="'section' is required for practice mode")
            if section not in {"listening", "reading", "writing", "speaking"}:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid practice section")

        amount   = self._resolve_price_for_scope(test, mode, section)
        currency = test.get("currency") or "MNT"

        if amount <= 0:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="This test is free — no payment required")

        # Already own an unused entitlement? Don't double-bill. Once that entitlement
        # has been spent on a session (consumed_session_id set) we let the user buy again.
        existing_unspent = await self.order_repo.find_paid_unconsumed_for_user_test(
            user_id,
            test_id,
            purchase_mode=mode,
            purchase_section=section if mode == "practice" else None,
        )
        if existing_unspent:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="You already have an unused session for this test",
            )

        # If the user already has an in-progress session for this test they can resume it for free.
        if self.session_repo is not None:
            query: Dict[str, Any] = {
                "user_id": user_id,
                "test_id": test_id,
                "status": "in_progress",
                "mode": mode,
            }
            if mode == "practice":
                query["practice_section"] = section
            in_progress = await self.session_repo.col.find_one(query)
            if in_progress:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="You have an in-progress session for this test — continue it instead",
                )

        # Reuse an existing pending order so the user doesn't get a fresh QR every click.
        existing_pending = await self.order_repo.find_active_pending(
            user_id,
            test_id,
            purchase_mode=mode,
            purchase_section=section if mode == "practice" else None,
        )
        if existing_pending and existing_pending.get("qpay_invoice_id"):
            return existing_pending

        # 1) Create the order shell so we have an order_id to reference in QPay.
        order = await self.order_repo.create({
            "user_id":   user_id,
            "test_id":   test_id,
            "purchase_mode": mode,
            "purchase_section": section if mode == "practice" else None,
            "amount":    amount,
            "currency":  currency,
            "status":    OrderStatus.PENDING.value,
            "manual":    False,
        })

        # 2) Create the QPay invoice. callback_url points back to our webhook with order_id.
        callback_url = f"{BACKEND_PUBLIC_URL}/api/payments/qpay-callback?order_id={order['id']}"
        try:
            invoice = await self.qpay.create_invoice(
                sender_invoice_no   = order["id"],
                invoice_description = (
                    f"{(test.get('title') or 'Test purchase')}"
                    + (f" - {section.capitalize()} practice" if mode == "practice" and section else "")
                )[:200],
                amount              = amount,
                callback_url        = callback_url,
            )
        except QPayError as e:
            await self.order_repo.update(order["id"], {
                "status": OrderStatus.FAILED.value,
                "error":  str(e),
            })
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail={
                    "message": f"QPay invoice creation failed: {e}",
                    "qpay": e.payload,
                },
            )

        # 3) Persist invoice fields on the order.
        order = await self.order_repo.update(order["id"], {
            "qpay_invoice_id": invoice.get("invoice_id"),
            "invoice": {
                "invoice_id":    invoice.get("invoice_id"),
                "qr_text":       invoice.get("qr_text"),
                "qr_image":      invoice.get("qr_image"),
                "qPay_shortUrl": invoice.get("qPay_shortUrl"),
                "urls":          invoice.get("urls"),
            },
        })
        return order  # type: ignore[return-value]

    async def check_and_sync(self, order_id: str) -> Tuple[Dict[str, Any], bool]:
        """Ask QPay if this invoice is paid; flip order to paid if so. Returns (order, paid_now)."""
        order = await self.order_repo.find_by_id(order_id)
        if not order:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Order not found")
        if order.get("status") == OrderStatus.PAID.value:
            return order, False
        invoice_id = order.get("qpay_invoice_id")
        if not invoice_id:
            return order, False

        try:
            result = await self.qpay.check_payment(invoice_id)
        except QPayError as e:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail={
                    "message": f"QPay check failed: {e}",
                    "qpay": e.payload,
                },
            )

        rows = result.get("rows") or []
        # QPay returns multiple rows in case of partial pays — find a successful one.
        successful = next(
            (r for r in rows if str(r.get("payment_status", "")).upper() in {"PAID", "SUCCESS"}),
            None,
        )
        if not successful:
            return order, False

        order = await self._mark_paid(order_id, qpay_payment_id=successful.get("payment_id"))
        return order, True

    # ── Admin actions ────────────────────────────────────────

    async def admin_mark_paid(self, order_id: str, *, admin_id: str, note: Optional[str]) -> Dict[str, Any]:
        order = await self.order_repo.find_by_id(order_id)
        if not order:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Order not found")
        if order.get("status") == OrderStatus.PAID.value:
            return order
        return await self._mark_paid(order_id, manual=True, admin_id=admin_id, note=note)

    async def admin_cancel_order(self, order_id: str, *, note: Optional[str]) -> Dict[str, Any]:
        order = await self.order_repo.find_by_id(order_id)
        if not order:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Order not found")
        if order.get("status") not in {OrderStatus.PENDING.value, OrderStatus.FAILED.value}:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Cannot cancel order in status '{order.get('status')}'",
            )

        invoice_id = order.get("qpay_invoice_id")
        qpay_err: Optional[str] = None
        if invoice_id:
            try:
                await self.qpay.cancel_invoice(invoice_id)
            except QPayError as e:
                # QPay sometimes 404s on already-cancelled invoices — log but proceed.
                qpay_err = str(e)

        update: Dict[str, Any] = {
            "status":       OrderStatus.CANCELLED.value,
            "cancelled_at": datetime.now(timezone.utc),
        }
        if note:     update["cancel_note"] = note
        if qpay_err: update["qpay_error"]  = qpay_err
        return await self.order_repo.update(order_id, update)  # type: ignore[return-value]

    async def admin_refund_order(self, order_id: str, *, note: Optional[str]) -> Dict[str, Any]:
        order = await self.order_repo.find_by_id(order_id)
        if not order:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Order not found")
        if order.get("status") != OrderStatus.PAID.value:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Only paid orders can be refunded")

        # Manual orders have no QPay payment — just flip status.
        payment_id = order.get("qpay_payment_id")
        qpay_err: Optional[str] = None
        if payment_id and not order.get("manual"):
            try:
                await self.qpay.refund_payment(payment_id)
            except QPayError as e:
                qpay_err = str(e)
                # Don't swallow — refunds should fail loudly so the admin can retry.
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail={
                        "message": f"QPay refund failed: {qpay_err}",
                        "qpay": e.payload,
                    },
                )

        update: Dict[str, Any] = {
            "status":      OrderStatus.REFUNDED.value,
            "refunded_at": datetime.now(timezone.utc),
        }
        if note: update["refund_note"] = note
        return await self.order_repo.update(order_id, update)  # type: ignore[return-value]

    # ── Internal ─────────────────────────────────────────────

    async def _mark_paid(
        self,
        order_id: str,
        *,
        qpay_payment_id: Optional[str] = None,
        manual: bool = False,
        admin_id: Optional[str] = None,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        update: Dict[str, Any] = {
            "status":  OrderStatus.PAID.value,
            "paid_at": datetime.now(timezone.utc),
            "manual":  manual,
        }
        if qpay_payment_id: update["qpay_payment_id"] = qpay_payment_id
        if admin_id:        update["manual_admin_id"] = admin_id
        if note:            update["manual_note"]     = note
        result = await self.order_repo.update(order_id, update)
        if result is None:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to mark order paid")
        return result

    async def _enrich_with_test_titles(self, orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Attach test_title to each order for nicer admin/user displays."""
        if not orders:
            return orders
        unique_test_ids = list({o.get("test_id") for o in orders if o.get("test_id")})
        title_map: Dict[str, str] = {}
        for tid in unique_test_ids:
            if tid is None:
                continue
            try:
                doc = await self.test_repo.find_by_id(str(tid))
                if doc:
                    title_map[tid] = doc.get("title", "")
            except Exception:
                pass
        for o in orders:
            o["test_title"] = title_map.get(o.get("test_id", ""), None)
        return orders


def _paginate(items: List[Dict[str, Any]], total: int, page: int, page_size: int) -> Dict[str, Any]:
    return {
        "items":       items,
        "total":       total,
        "page":        page,
        "page_size":   page_size,
        "total_pages": max(1, (total + page_size - 1) // page_size),
    }
