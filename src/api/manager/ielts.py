"""
IELTS API endpoints — question CRUD, test management, user test-taking.
"""

from fastapi import HTTPException, status
from pydantic import ValidationError

from src.api.api_routes import register
from src.database.mongodb import MongoDB
from src.database.repositories.ielts_repository import (
    QuestionRepository,
    TestRepository,
    TestSessionRepository,
)
from src.services.auth_service import AuthService
from src.services.ielts_service import IeltsService
from src.database.repositories.user_repository import UserRepository
from schemas.ielts import (
    QuestionCreate,
    QuestionUpdate,
    SectionAnswers,
    TestCreate,
    TestUpdate,
)


# ── Helpers ───────────────────────────────────

def _ielts_service() -> IeltsService:
    db = MongoDB.get_db()
    return IeltsService(
        question_repo=QuestionRepository(db),
        test_repo=TestRepository(db),
        session_repo=TestSessionRepository(db),
    )


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


async def _require_admin(payload: dict):
    user = await _require_auth(payload)
    if user.role != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required")
    return user


async def _require_admin_or_examiner(payload: dict):
    user = await _require_auth(payload)
    if user.role not in {"admin", "examiner"}:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin or examiner access required")
    return user


# ═════════════════════════════════════════════
#  QUESTION CRUD (admin/examiner only)
# ═════════════════════════════════════════════

@register(
    name="questions/create",
    method="POST",
    required_keys=["title", "section", "section_part", "test_type", "module_type", "type", "instruction"],
    optional_keys={
        "tags": [], "context": None, "passage": None, "audio_url": None, "image_url": None,
        "options": None, "correct_option": None, "correct_options": None,
        "form_fields": None, "table_cells": None, "flow_steps": None,
        "sentences": None, "summary_items": None, "short_items": None,
        "map_word_box": None, "map_slots": None, "matching_items": None,
        "heading_options": None, "heading_items": None, "tfng_items": None,
        "pick_items": None, "writing_prompt": None, "cue_card": None,
        "speaking_questions": None,
    },
    summary="Create question",
    description="Create a new IELTS question (admin/examiner only).",
    tags=["Questions"],
)
async def question_create(data: dict):
    await _require_admin_or_examiner(data)
    _clean_meta(data)
    try:
        payload = QuestionCreate(**data)
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=exc.errors())
    return await _ielts_service().create_question(payload)


@register(
    name="questions/list",
    method="GET",
    required_keys=[],
    optional_keys={"page": 1, "page_size": 20, "section": None, "section_part": None, "type": None, "module_type": None, "search": None},
    summary="List questions",
    description="List and filter IELTS questions with pagination.",
    tags=["Questions"],
)
async def question_list(data: dict):
    await _require_admin_or_examiner(data)
    svc = _ielts_service()
    return await svc.list_questions(
        page=int(data.get("page", 1)),
        page_size=min(int(data.get("page_size", 20)), 100),
        section=data.get("section"),
        section_part=data.get("section_part"),
        question_type=data.get("type"),
        module_type=data.get("module_type"),
        search=data.get("search"),
    )


@register(
    name="questions/get",
    method="GET",
    required_keys=["question_id"],
    summary="Get question",
    description="Get a single question by ID.",
    tags=["Questions"],
)
async def question_get(data: dict):
    await _require_admin_or_examiner(data)
    return await _ielts_service().get_question(data["question_id"])


@register(
    name="questions/update",
    method="PUT",
    required_keys=["question_id"],
    optional_keys={
        "title": None, "instruction": None, "context": None, "passage": None,
        "audio_url": None, "image_url": None, "tags": None,
        "options": None, "correct_option": None, "correct_options": None,
        "form_fields": None, "table_cells": None, "flow_steps": None,
        "sentences": None, "summary_items": None, "short_items": None,
        "map_word_box": None, "map_slots": None, "matching_items": None,
        "heading_options": None, "heading_items": None, "tfng_items": None,
        "pick_items": None, "writing_prompt": None, "cue_card": None,
        "speaking_questions": None,
    },
    summary="Update question",
    description="Partially update a question (admin/examiner only).",
    tags=["Questions"],
)
async def question_update(data: dict):
    await _require_admin_or_examiner(data)
    qid = data.pop("question_id")
    _clean_meta(data)
    try:
        payload = QuestionUpdate(**data)
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=exc.errors())
    return await _ielts_service().update_question(qid, payload)


@register(
    name="questions/delete",
    method="DELETE",
    required_keys=["question_id"],
    summary="Delete question",
    description="Delete a question by ID (admin only).",
    tags=["Questions"],
)
async def question_delete(data: dict):
    await _require_admin(data)
    return await _ielts_service().delete_question(data["question_id"])


@register(
    name="questions/bulk-create",
    method="POST",
    required_keys=["questions"],
    summary="Bulk create questions",
    description="Create multiple questions at once (admin/examiner only).",
    tags=["Questions"],
)
async def question_bulk_create(data: dict):
    await _require_admin_or_examiner(data)
    raw_list = data.get("questions", [])
    if not isinstance(raw_list, list) or not raw_list:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="'questions' must be a non-empty list")
    try:
        payloads = [QuestionCreate(**q) for q in raw_list]
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=exc.errors())
    return await _ielts_service().bulk_create_questions(payloads)


# ═════════════════════════════════════════════
#  TEST CRUD (admin/examiner only)
# ═════════════════════════════════════════════

@register(
    name="tests/create",
    method="POST",
    required_keys=["title", "sections"],
    optional_keys={
        "description": None, "test_type": "ielts", "module_type": "academic",
        "is_published": False, "time_limit_minutes": 164, "tags": [],
    },
    summary="Create test",
    description="Create a new exam paper composed of questions (admin/examiner).",
    tags=["Tests"],
)
async def test_create(data: dict):
    await _require_admin_or_examiner(data)
    _clean_meta(data)
    try:
        payload = TestCreate(**data)
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=exc.errors())
    return await _ielts_service().create_test(payload)


@register(
    name="tests/list",
    method="GET",
    required_keys=[],
    optional_keys={"page": 1, "page_size": 20, "test_type": None, "published_only": False},
    summary="List tests",
    description="List tests with pagination. Candidates see published only.",
    tags=["Tests"],
)
async def test_list(data: dict):
    user = await _require_auth(data)
    published_only = data.get("published_only", False)
    # Candidates can only see published tests
    if user.role == "candidate":
        published_only = True
    svc = _ielts_service()
    return await svc.list_tests(
        page=int(data.get("page", 1)),
        page_size=min(int(data.get("page_size", 20)), 100),
        test_type=data.get("test_type"),
        published_only=published_only,
    )


@register(
    name="tests/get",
    method="GET",
    required_keys=["test_id"],
    summary="Get test",
    description="Get a single test by ID.",
    tags=["Tests"],
)
async def test_get(data: dict):
    user = await _require_auth(data)
    test = await _ielts_service().get_test(data["test_id"])
    if user.role == "candidate" and not test.is_published:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test not found")
    return test


@register(
    name="tests/update",
    method="PUT",
    required_keys=["test_id"],
    optional_keys={
        "title": None, "description": None, "sections": None,
        "is_published": None, "time_limit_minutes": None, "tags": None,
    },
    summary="Update test",
    description="Partially update a test (admin/examiner only).",
    tags=["Tests"],
)
async def test_update(data: dict):
    await _require_admin_or_examiner(data)
    tid = data.pop("test_id")
    _clean_meta(data)
    try:
        payload = TestUpdate(**data)
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=exc.errors())
    return await _ielts_service().update_test(tid, payload)


@register(
    name="tests/delete",
    method="DELETE",
    required_keys=["test_id"],
    summary="Delete test",
    description="Delete a test by ID (admin only).",
    tags=["Tests"],
)
async def test_delete(data: dict):
    await _require_admin(data)
    return await _ielts_service().delete_test(data["test_id"])


@register(
    name="tests/publish",
    method="POST",
    required_keys=["test_id", "is_published"],
    summary="Publish/unpublish test",
    description="Set test visibility (admin/examiner only).",
    tags=["Tests"],
)
async def test_publish(data: dict):
    await _require_admin_or_examiner(data)
    return await _ielts_service().publish_test(data["test_id"], bool(data["is_published"]))


# ═════════════════════════════════════════════
#  TEST-TAKING (candidates)
# ═════════════════════════════════════════════

@register(
    name="sessions/start",
    method="POST",
    required_keys=["test_id"],
    summary="Start test session",
    description="Start a new test session for the authenticated user.",
    tags=["Sessions"],
)
async def session_start(data: dict):
    user = await _require_auth(data)
    return await _ielts_service().start_test(user.id, data["test_id"])


@register(
    name="sessions/questions",
    method="GET",
    required_keys=["session_id"],
    summary="Get session questions",
    description="Get all questions for an active session (answers stripped).",
    tags=["Sessions"],
)
async def session_questions(data: dict):
    user = await _require_auth(data)
    return await _ielts_service().get_test_questions_for_session(user.id, data["session_id"])


@register(
    name="sessions/submit",
    method="POST",
    required_keys=["session_id", "sections"],
    summary="Submit answers",
    description="Submit answers for one or more sections. Can be called multiple times.",
    tags=["Sessions"],
)
async def session_submit(data: dict):
    user = await _require_auth(data)
    raw_sections = data.get("sections", [])
    try:
        sections = [SectionAnswers(**s) if isinstance(s, dict) else s for s in raw_sections]
    except (ValidationError, TypeError) as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc))
    return await _ielts_service().submit_answers(user.id, data["session_id"], sections)


@register(
    name="sessions/finalize",
    method="POST",
    required_keys=["session_id"],
    summary="Finalize session",
    description="Finish the test, auto-score listening/reading, and get results.",
    tags=["Sessions"],
)
async def session_finalize(data: dict):
    user = await _require_auth(data)
    return await _ielts_service().finalize_session(user.id, data["session_id"])


@register(
    name="sessions/get",
    method="GET",
    required_keys=["session_id"],
    summary="Get session",
    description="Get session details for the authenticated user.",
    tags=["Sessions"],
)
async def session_get(data: dict):
    user = await _require_auth(data)
    return await _ielts_service().get_session(data["session_id"], user_id=user.id)


@register(
    name="sessions/list",
    method="GET",
    required_keys=[],
    optional_keys={"page": 1, "page_size": 20, "test_type": None},
    summary="List my sessions",
    description="List all test sessions for the authenticated user.",
    tags=["Sessions"],
)
async def session_list(data: dict):
    user = await _require_auth(data)
    return await _ielts_service().list_user_sessions(
        user.id,
        page=int(data.get("page", 1)),
        page_size=min(int(data.get("page_size", 20)), 100),
        test_type=data.get("test_type"),
    )


@register(
    name="sessions/result",
    method="GET",
    required_keys=["session_id"],
    summary="Get session result",
    description="Get scored results for a submitted/graded session.",
    tags=["Sessions"],
)
async def session_result(data: dict):
    user = await _require_auth(data)
    return await _ielts_service().get_session_result(data["session_id"], user_id=user.id)


# ── Utility ───────────────────────────────────

def _clean_meta(data: dict):
    """Remove framework keys injected by the route system."""
    for key in ("authorization", "access_token", "token"):
        data.pop(key, None)
