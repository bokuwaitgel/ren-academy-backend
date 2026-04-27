"""
QPay v2 merchant API client.

Wraps token auth (with cached access_token + auto-refresh), invoice
creation/cancellation, and payment lookups against https://merchant.qpay.mn/v2.
"""
import os
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional
from urllib.parse import urljoin

import httpx
from dotenv import load_dotenv

load_dotenv()


QPAY_BASE_URL    = os.getenv("QPAY_BASE_URL", "https://merchant.qpay.mn").rstrip("/")
QPAY_CLIENT_ID   = os.getenv("QPAY_CLIENT_ID", "TEST_MERCHANT")
QPAY_CLIENT_SEC  = os.getenv("QPAY_CLIENT_SECRET", "WBDUzy8n")
QPAY_INVOICE_CODE = os.getenv("QPAY_INVOICE_CODE", "TEST_INVOICE")
QPAY_AUTH_PATH = os.getenv("QPAY_AUTH_PATH", "/v2/auth/token")


class QPayError(Exception):
    def __init__(self, message: str, status_code: int = 502, payload: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload


class QPayClient:
    """Singleton-ish client. Use QPayClient.instance()."""

    _instance: Optional["QPayClient"] = None

    def __init__(self):
        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None
        self._expires_at: Optional[datetime] = None
        self._lock = asyncio.Lock()

    @classmethod
    def instance(cls) -> "QPayClient":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    # ── Token lifecycle ───────────────────────────────────────

    @staticmethod
    def _api_url(path: str) -> str:
        """Build a full API URL while avoiding duplicated /v2 prefixes.

        Supports either base URL style:
        - https://merchant.qpay.mn
        - https://merchant.qpay.mn/v2
        """
        normalized_path = path if path.startswith("/") else f"/{path}"
        if QPAY_BASE_URL.endswith("/v2") and normalized_path.startswith("/v2/"):
            normalized_path = normalized_path[3:]
        return f"{QPAY_BASE_URL}{normalized_path}"

    async def _login(self) -> None:
        """Authenticate against QPay with HTTP Basic credentials.

        Some QPay environments return redirects on auth endpoints.
        We preserve POST + Basic auth on redirects and try fallback paths.
        """
        candidate_paths = [QPAY_AUTH_PATH, "/v2/auth/token", "/auth/token"]
        tried_urls: list[str] = []
        res: Optional[httpx.Response] = None

        async with httpx.AsyncClient(timeout=20.0, follow_redirects=False) as client:
            for path in candidate_paths:
                url = self._api_url(path)
                if url in tried_urls:
                    continue
                tried_urls.append(url)

                res = await client.post(url, auth=(QPAY_CLIENT_ID, QPAY_CLIENT_SEC))

                # Follow one explicit redirect manually to preserve POST + Basic auth.
                if res.status_code in {301, 302, 307, 308}:
                    location = res.headers.get("location")
                    if location:
                        redirect_url = urljoin(url, location)
                        if redirect_url not in tried_urls:
                            tried_urls.append(redirect_url)
                        res = await client.post(
                            redirect_url,
                            auth=(QPAY_CLIENT_ID, QPAY_CLIENT_SEC),
                        )

                if res.status_code == 200:
                    break

        if res is None or res.status_code != 200:
            status_code = res.status_code if res is not None else 0
            location = res.headers.get("location") if res is not None else None
            payload = _safe_json(res) if res is not None else None
            raise QPayError(
                f"QPay auth failed ({status_code})",
                status_code=502,
                payload={
                    "response": payload,
                    "location": location,
                    "tried_urls": tried_urls,
                },
            )

        body = _safe_json(res)
        if not isinstance(body, dict) or not body.get("access_token"):
            raise QPayError(
                "QPay auth response is not valid JSON token payload",
                status_code=502,
                payload={
                    "status_code": res.status_code,
                    "content_type": res.headers.get("content-type"),
                    "location": res.headers.get("location"),
                    "tried_urls": tried_urls,
                    "response_preview": _response_excerpt(res),
                },
            )

        self._access_token  = body.get("access_token")
        self._refresh_token = body.get("refresh_token")
        # QPay returns expires_in (seconds) — be conservative, refresh 60s early
        expires_in = int(body.get("expires_in") or 3600)
        self._expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in - 60)

    async def _ensure_token(self) -> str:
        async with self._lock:
            if (
                not self._access_token
                or not self._expires_at
                or datetime.now(timezone.utc) >= self._expires_at
            ):
                await self._login()
            assert self._access_token  # for type checker
            return self._access_token

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: Optional[Dict[str, Any]] = None,
        retry_on_401: bool = True,
    ) -> Dict[str, Any]:
        token = await self._ensure_token()
        url = self._api_url(path)
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        async with httpx.AsyncClient(timeout=20.0) as client:
            res = await client.request(method, url, json=json, headers=headers)

        if res.status_code == 401 and retry_on_401:
            # Force re-login on next call, then retry once.
            self._access_token = None
            self._expires_at = None
            return await self._request(method, path, json=json, retry_on_401=False)

        if res.status_code >= 400:
            raise QPayError(
                f"QPay {method} {path} failed ({res.status_code})",
                status_code=502,
                payload=_safe_json(res),
            )

        return _safe_json(res) or {}

    # ── Invoice ──────────────────────────────────────────────

    async def create_invoice(
        self,
        *,
        sender_invoice_no: str,
        invoice_description: str,
        amount: float,
        callback_url: str,
        invoice_receiver_code: str = "terminal",
    ) -> Dict[str, Any]:
        """Create a simple invoice. Returns QPay invoice payload (invoice_id, qr_text, qr_image, urls, ...)."""
        body = {
            "invoice_code":          QPAY_INVOICE_CODE,
            "sender_invoice_no":     sender_invoice_no,
            "invoice_receiver_code": invoice_receiver_code,
            "invoice_description":   invoice_description,
            "amount":                amount,
            "callback_url":          callback_url,
        }
        return await self._request("POST", "/v2/invoice", json=body)

    async def cancel_invoice(self, invoice_id: str) -> Dict[str, Any]:
        return await self._request("DELETE", f"/v2/invoice/{invoice_id}")

    # ── Payment ──────────────────────────────────────────────

    async def check_payment(self, invoice_id: str) -> Dict[str, Any]:
        """POST /v2/payment/check — query by INVOICE id. Returns {count, paid_amount, rows: [...]}."""
        body = {
            "object_type": "INVOICE",
            "object_id":   invoice_id,
            "offset":      {"page_number": 1, "page_limit": 100},
        }
        return await self._request("POST", "/v2/payment/check", json=body)

    async def get_payment(self, payment_id: str) -> Dict[str, Any]:
        return await self._request("GET", f"/v2/payment/{payment_id}")

    async def cancel_payment(self, payment_id: str, note: str = "cancelled") -> Dict[str, Any]:
        return await self._request(
            "DELETE",
            f"/v2/payment/cancel/{payment_id}",
            json={"note": note},
        )

    async def refund_payment(self, payment_id: str) -> Dict[str, Any]:
        return await self._request("DELETE", f"/v2/payment/refund/{payment_id}")


def _safe_json(res: httpx.Response) -> Optional[Dict[str, Any]]:
    try:
        return res.json()
    except ValueError:
        return None


def _response_excerpt(res: httpx.Response, limit: int = 500) -> str:
    text = (res.text or "").strip()
    if len(text) > limit:
        return text[:limit] + "..."
    return text
