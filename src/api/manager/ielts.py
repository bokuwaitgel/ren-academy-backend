"""
IELTS API endpoints — question CRUD, test management, user test-taking.
"""

from fastapi import HTTPException, status
from pydantic import ValidationError

from src.api.api_routes import register
from src.database.mongodb import MongoDB
from src.database.repositories.ielts_repository import (
    OrderRepository,
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
    SubmitSectionRequest,
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
        order_repo=OrderRepository(db),
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
    required_keys=["title"],
    optional_keys={
        "description": None, "test_type": "ielts", "module_type": "academic",
        "is_published": False, "tags": [],
        "price": 0.0, "currency": "MNT",
        "listening": None, "reading": None, "writing": None, "speaking": None,
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
    published_only = True
    svc = _ielts_service()
    return await svc.list_tests(
        page=int(data.get("page", 1)),
        page_size=min(int(data.get("page_size", 20)), 100),
        test_type=data.get("test_type"),
        published_only=published_only,
    )


@register(
    name="tests/lists",
    method="GET",
    required_keys=[],
    optional_keys={"page": 1, "page_size": 20, "test_type": None, "published_only": False},
    summary="List tests",
    description="List tests with pagination. Candidates see published only.",
    tags=["Tests"],
)
async def test_lists(data: dict):
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
    name="tests/section",
    method="GET",
    required_keys=["section"],
    summary="List tests that have a section",
    description="Returns a minimal list (test_id, test_title, module_type) of all tests that contain the given section.",
    tags=["Tests"],
)
async def test_get_section(data: dict):
    user = await _require_auth(data)
    return await _ielts_service().get_section_across_tests(
        section=data["section"],
        strip_answers=user.role == "candidate",
        published_only=user.role == "candidate",
    )


@register(
    name="tests/section/detail",
    method="GET",
    required_keys=["test_id", "section"],
    summary="Get full section content for a test",
    description="Returns the full content (sub-sections/parts/questions) for a specific section of a specific test.",
    tags=["Tests"],
)
async def test_get_section_detail(data: dict):
    user = await _require_auth(data)
    test = await _ielts_service().get_test(data["test_id"])
    if user.role == "candidate" and not test.is_published:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test not found")
    return await _ielts_service().get_test_section_detail(
        test_id=data["test_id"],
        section=data["section"],
        strip_answers=user.role == "candidate",
    )


@register(
    name="tests/update",
    method="PUT",
    required_keys=["test_id"],
    optional_keys={
        "title": None, "description": None,
        "is_published": None, "tags": None,
        "price": None, "currency": None,
        "listening": None, "reading": None, "writing": None, "speaking": None,
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
#  TEST BUILDER — section/part management
# ═════════════════════════════════════════════

@register(
    name="tests/section/add",
    method="POST",
    required_keys=["test_id", "module"],
    optional_keys={
        "section_number": None,
        "audio_url": None,
        "passage": None,
        "task_number": None,
        "description": None,
        "image_url": None,
        "part_number": None,
    },
    summary="Add section/part to test",
    description=(
        "Add a section or part to an existing test.\n\n"
        "- **listening**: provide `section_number` (1-4) and `audio_url`\n"
        "- **reading**: provide `section_number` (1-3) and `passage`\n"
        "- **writing**: provide `task_number` (1-2) and `description`; optional `image_url`\n"
        "- **speaking**: provide `part_number` (1-3)"
    ),
    tags=["Tests"],
)
async def test_section_add(data: dict):
    await _require_admin_or_examiner(data)
    test_id = data.pop("test_id")
    module = data.pop("module")
    _clean_meta(data)
    return await _ielts_service().add_section_to_test(test_id, module, data)


@register(
    name="tests/section/update",
    method="PUT",
    required_keys=["test_id", "module", "number"],
    optional_keys={"audio_url": None, "passage": None, "description": None, "image_url": None},
    summary="Update section/part metadata",
    description="Update the audio_url, passage, description, or image_url of an existing section.",
    tags=["Tests"],
)
async def test_section_update(data: dict):
    await _require_admin_or_examiner(data)
    test_id = data.pop("test_id")
    module = data.pop("module")
    number = int(data.pop("number"))
    _clean_meta(data)
    update_fields = {k: v for k, v in data.items() if v is not None}
    return await _ielts_service().update_test_section(test_id, module, number, update_fields)


@register(
    name="tests/section/remove",
    method="DELETE",
    required_keys=["test_id", "module", "number"],
    summary="Remove section/part from test",
    description="Remove a listening section, reading passage, writing task, or speaking part from a test.",
    tags=["Tests"],
)
async def test_section_remove(data: dict):
    await _require_admin_or_examiner(data)
    return await _ielts_service().remove_section_from_test(data["test_id"], data["module"], int(data["number"]))


@register(
    name="tests/section/question/add",
    method="POST",
    required_keys=["test_id", "section_part", "question_id"],
    summary="Add question to test section",
    description=(
        "Add an existing question to a specific section/part of a test.\n\n"
        "`section_part` values: `listening_section_1..4`, `reading_passage_1..3`, `speaking_part_1..3`\n\n"
        "Writing tasks do not use this endpoint — they reference questions via the question's `section_part`."
    ),
    tags=["Tests"],
)
async def test_section_question_add(data: dict):
    await _require_admin_or_examiner(data)
    return await _ielts_service().add_question_to_test_section(
        data["test_id"], data["section_part"], data["question_id"]
    )


@register(
    name="tests/section/question/remove",
    method="POST",
    required_keys=["test_id", "section_part", "question_id"],
    summary="Remove question from test section",
    description="Remove a question from a specific section/part of a test.",
    tags=["Tests"],
)
async def test_section_question_remove(data: dict):
    await _require_admin_or_examiner(data)
    return await _ielts_service().remove_question_from_test_section(
        data["test_id"], data["section_part"], data["question_id"]
    )


# ═════════════════════════════════════════════
#  TEST-TAKING (candidates)
# ═════════════════════════════════════════════

@register(
    name="sessions/start",
    method="POST",
    required_keys=["test_id"],
    optional_keys={
        "mode": "full_test",   # "full_test" | "practice"
        "section": None,       # required when mode == "practice"
    },
    summary="Start test session",
    description=(
        "Start a new test session. "
        "Use mode='full_test' for a complete simulated IELTS exam (all 4 sections in order). "
        "Use mode='practice' with a 'section' value to practice a single section."
    ),
    tags=["Sessions"],
)
async def session_start(data: dict):
    user = await _require_auth(data)
    return await _ielts_service().start_test(
        user_id=user.id,
        test_id=data["test_id"],
        mode=data.get("mode", "full_test"),
        section=data.get("section"),
    )


@register(
    name="sessions/section/start",
    method="POST",
    required_keys=["session_id"],
    summary="Start current section",
    description=(
        "Explicitly start the current section's timer. "
        "For FULL_TEST mode — call this when the candidate is ready to begin the section. "
        "The timer auto-starts on the first question fetch as well."
    ),
    tags=["Sessions"],
)
async def session_section_start(data: dict):
    user = await _require_auth(data)
    return await _ielts_service().start_section(user.id, data["session_id"])


@register(
    name="sessions/section/submit",
    method="POST",
    required_keys=["session_id", "section"],
    optional_keys={"answers": []},
    summary="Submit section answers",
    description=(
        "Submit answers for the current section and advance to the next one. "
        "For FULL_TEST: automatically moves current_section to the next section. "
        "For PRACTICE: marks the session ready for finalization. "
        "Call /sessions/finalize after all sections are submitted."
    ),
    tags=["Sessions"],
)
async def session_section_submit(data: dict):
    user = await _require_auth(data)
    raw_answers = data.get("answers", [])
    try:
        from schemas.ielts import AnswerSubmission
        answers = [AnswerSubmission(**a) if isinstance(a, dict) else a for a in raw_answers]
    except (ValidationError, TypeError) as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc))
    return await _ielts_service().submit_section_answers(
        user_id=user.id,
        session_id=data["session_id"],
        section=data["section"],
        answers=answers,
    )


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


@register(
    name="sessions/grade",
    method="POST",
    required_keys=["session_id", "section", "band_score"],
    optional_keys={"details": None},
    summary="Grade writing/speaking section",
    description=(
        "Admin or examiner manually sets the band score for a writing or speaking section. "
        "section must be 'writing' or 'speaking'. band_score must be 0–9 in 0.5 increments."
    ),
    tags=["Sessions"],
)
async def session_grade(data: dict):
    await _require_admin_or_examiner(data)
    return await _ielts_service().grade_writing_speaking(
        session_id=data["session_id"],
        section=data["section"],
        band_score=float(data["band_score"]),
        details=data.get("details"),
    )


@register(
    name="sessions/writing/ai-grade",
    method="POST",
    required_keys=["session_id"],
    summary="AI-grade writing section",
    description=(
        "Calls the AI writing evaluator on the session's stored writing answers "
        "and saves the resulting band score back to the session. "
        "Returns the full AI evaluation result plus the updated session."
    ),
    tags=["Sessions"],
)
async def session_writing_ai_grade(data: dict):
    from src.agent.writing_agent import get_writing_agent

    user = await _require_auth(data)
    svc = _ielts_service()

    session = await svc.get_session(data["session_id"], user_id=user.id if user.role == "candidate" else None)
    session_dict = session.model_dump() if hasattr(session, "model_dump") else dict(session)

    writing_answers: dict = (session_dict.get("answers") or {}).get("writing", {})
    if not writing_answers:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No writing answers found in this session",
        )

    # Fetch test to get task descriptions
    test = await svc.test_repo.find_by_id(session_dict["test_id"])
    tasks_meta = {}
    if test:
        for task in (test.get("writing") or {}).get("tasks", []):
            tasks_meta[f"task_{task['task_number']}"] = task.get("description", "")

    agent = get_writing_agent()
    evaluations = {}
    band_scores = []

    for answer_key, essay_text in writing_answers.items():
        if not essay_text or not str(essay_text).strip():
            continue
        task_num = answer_key.replace("task_", "")
        task_type = f"Task {task_num}" if task_num.isdigit() else ""
        prompt = tasks_meta.get(answer_key, "")
        try:
            result = await agent.analyze(
                content=str(essay_text),
                prompt=prompt,
                task_type=task_type,
            )
            evaluations[answer_key] = result.model_dump()
            band_scores.append(result.overall_score)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"AI evaluation failed for {answer_key}: {exc}",
            )

    if not band_scores:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No valid writing answers to evaluate",
        )

    # Average band scores across tasks, round to nearest 0.5
    avg_band = sum(band_scores) / len(band_scores)
    final_band = round(avg_band * 2) / 2

    updated_session = await svc.grade_writing_speaking(
        session_id=data["session_id"],
        section="writing",
        band_score=final_band,
        details={"ai_evaluations": evaluations},
    )

    return {
        "session": updated_session,
        "writing_band": final_band,
        "evaluations": evaluations,
    }


# ── Utility ───────────────────────────────────

def _clean_meta(data: dict):
    """Remove framework keys injected by the route system."""
    for key in ("authorization", "access_token", "token"):
        data.pop(key, None)
