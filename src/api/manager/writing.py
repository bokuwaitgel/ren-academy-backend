"""
Writing evaluation endpoints — AI-powered IELTS writing assessment.
"""

from datetime import datetime, timezone

from fastapi import HTTPException, status

from src.api.api_routes import register
from src.database.mongodb import MongoDB
from src.database.repositories.user_repository import UserRepository
from src.services.auth_service import AuthService
from src.agent.writing_agent import get_writing_agent


# ── Helpers ───────────────────────────────────

def _auth_service() -> AuthService:
    db = MongoDB.get_db()
    return AuthService(UserRepository(db))


def _extract_token(payload: dict) -> str:
    token = payload.get("access_token") or payload.get("token")
    auth_header = payload.get("authorization")
    if not token and isinstance(auth_header, str) and auth_header.lower().startswith("bearer "):
        token = auth_header.split(" ", 1)[1].strip()
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing token")
    return str(token)


async def _require_auth(payload: dict):
    token = _extract_token(payload)
    return await _auth_service().get_current_user(token)


# ═════════════════════════════════════════════
#  WRITING EVALUATION
# ═════════════════════════════════════════════

@register(
    name="writing/evaluate",
    method="POST",
    required_keys=["content"],
    optional_keys={
        "prompt": "",
        "task_type": "",
        "question_id": None,
    },
    summary="Evaluate writing with AI",
    description=(
        "Submit any IELTS writing content for detailed AI evaluation using pydantic_ai + Gemini.\n\n"
        "Returns:\n"
        "- Band scores for all 4 IELTS criteria + overall\n"
        "- Error counts (grammar, vocabulary, sentence)\n"
        "- AI detection percentage\n"
        "- Writing level classification\n"
        "- Word and sentence corrections\n"
        "- Fully improved version of the essay\n"
        "- Actionable feedback and motivation"
    ),
    tags=["Writing"],
)
async def writing_evaluate(data: dict) -> dict:
    user = await _require_auth(data)

    content: str = data.get("content", "").strip()
    if not content:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Writing content cannot be empty",
        )
    if len(content) < 50:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Writing content is too short for a meaningful evaluation (minimum 50 characters)",
        )

    prompt: str = data.get("prompt", "") or ""
    task_type: str = data.get("task_type", "") or ""
    question_id: str | None = data.get("question_id")

    agent = get_writing_agent()
    try:
        result = await agent.analyze(
            content=content,
            prompt=prompt,
            task_type=task_type,
            question_id=question_id or "",
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"AI evaluation failed: {exc}",
        )

    response = result.model_dump()
    response["evaluated_at"] = datetime.now(timezone.utc).isoformat()
    response["user_id"] = user.id
    if question_id:
        response["question_id"] = question_id

    return response
