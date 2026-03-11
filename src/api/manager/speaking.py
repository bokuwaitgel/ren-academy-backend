"""
Speaking evaluation endpoints — AI-powered IELTS speaking assessment.
"""

from datetime import datetime, timezone

import httpx
from fastapi import HTTPException, status

from src.api.api_routes import register
from src.database.mongodb import MongoDB
from src.database.repositories.ielts_repository import QuestionRepository, SpeakingPracticeRepository, TestSessionRepository, TestRepository
from src.database.repositories.user_repository import UserRepository
from src.services.auth_service import AuthService
from src.agent.speaking_agent import get_speaking_agent
import base64 as _base64

from src.services.s3_service import S3StorageService


def _decode_base64(data: str) -> bytes:
    raw = data.strip()
    if "," in raw and "base64" in raw[:64].lower():
        raw = raw.split(",", 1)[1]
    try:
        return _base64.b64decode(raw, validate=True)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid base64 audio: {exc}")


# ── Helpers ───────────────────────────────────

def _auth_service() -> AuthService:
    db = MongoDB.get_db()
    return AuthService(UserRepository(db))


def _question_repo() -> QuestionRepository:
    db = MongoDB.get_db()
    return QuestionRepository(db)


def _practice_repo() -> SpeakingPracticeRepository:
    db = MongoDB.get_db()
    return SpeakingPracticeRepository(db)


def _test_repo() -> TestRepository:
    db = MongoDB.get_db()
    return TestRepository(db)


def _session_repo() -> TestSessionRepository:
    db = MongoDB.get_db()
    return TestSessionRepository(db)


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


async def _fetch_audio_bytes(audio_url: str) -> bytes:
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(audio_url)
            resp.raise_for_status()
            return resp.content
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to fetch audio from URL: {exc}",
        )


# ═════════════════════════════════════════════
#  SPEAKING EVALUATION — transcript
# ═════════════════════════════════════════════

@register(
    name="speaking/evaluate",
    method="POST",
    required_keys=["transcript"],
    optional_keys={
        "question": "",
        "part": "",
        "question_id": None,
    },
    summary="Evaluate speaking transcript with AI",
    description=(
        "Submit an IELTS speaking transcript for AI evaluation using pydantic_ai + Gemini.\n\n"
        "Evaluated against all 4 official IELTS Speaking band descriptors:\n"
        "- Fluency & Coherence\n"
        "- Lexical Resource\n"
        "- Grammatical Range & Accuracy\n"
        "- Pronunciation (estimated from text cues)\n\n"
        "Returns band scores, per-criterion feedback, sample improvements, "
        "strengths, areas for improvement, and an overall motivating summary."
    ),
    tags=["Speaking"],
)
async def speaking_evaluate(data: dict) -> dict:
    user = await _require_auth(data)

    transcript: str = (data.get("transcript") or "").strip()
    if not transcript:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Transcript cannot be empty",
        )
    if len(transcript) < 20:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Transcript is too short for meaningful evaluation (minimum 20 characters)",
        )

    question: str = data.get("question", "") or ""
    part: str = data.get("part", "") or ""
    question_id: str | None = data.get("question_id")

    agent = get_speaking_agent()
    try:
        result = await agent.analyze(
            content=transcript,
            question=question,
            part=part,
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


# ═════════════════════════════════════════════
#  SPEAKING EVALUATION — audio URL
# ═════════════════════════════════════════════

@register(
    name="speaking/evaluate/audio",
    method="POST",
    required_keys=["audio_url"],
    optional_keys={
        "question": "",
        "part": "",
        "question_id": None,
        "media_type": "audio/webm",
    },
    summary="Evaluate speaking audio with AI",
    description=(
        "Submit a URL pointing to a recorded IELTS speaking response (e.g. from S3) for AI evaluation.\n\n"
        "The audio is fetched server-side and sent to Gemini for multimodal analysis.\n"
        "Pronunciation is assessed directly from the audio, giving a more accurate score than transcript-only evaluation.\n\n"
        "Evaluated against all 4 official IELTS Speaking band descriptors:\n"
        "- Fluency & Coherence\n"
        "- Lexical Resource\n"
        "- Grammatical Range & Accuracy\n"
        "- Pronunciation"
    ),
    tags=["Speaking"],
)
async def speaking_evaluate_audio(data: dict) -> dict:
    user = await _require_auth(data)

    audio_url: str = (data.get("audio_url") or "").strip()
    if not audio_url:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="audio_url cannot be empty",
        )

    question: str = data.get("question", "") or ""
    part: str = data.get("part", "") or ""
    question_id: str | None = data.get("question_id")
    media_type: str = data.get("media_type", "audio/webm") or "audio/webm"

    audio_bytes = await _fetch_audio_bytes(audio_url)
    if len(audio_bytes) < 1000:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Audio file is too small or empty",
        )

    agent = get_speaking_agent()
    try:
        result = await agent.analyze(
            content=audio_bytes,
            media_type=media_type,
            question=question,
            part=part,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"AI evaluation failed: {exc}",
        )

    response = result.model_dump()
    response["evaluated_at"] = datetime.now(timezone.utc).isoformat()
    response["user_id"] = user.id
    response["audio_url"] = audio_url
    if question_id:
        response["question_id"] = question_id

    return response


# ═════════════════════════════════════════════
#  SPEAKING EVALUATION — base64 audio (no URL)
# ═════════════════════════════════════════════

@register(
    name="speaking/evaluate/base64",
    method="POST",
    required_keys=["file_content_base64"],
    optional_keys={
        "question": "",
        "part": "",
        "question_id": None,
        "content_type": "audio/webm",
    },
    summary="Evaluate speaking audio from base64 data",
    description=(
        "Submit base64-encoded audio for AI speaking evaluation — no URL or session required.\n\n"
        "Use this when you already have the raw audio (e.g. from a recorder) and want a standalone evaluation.\n"
        "The audio is decoded and sent directly to Gemini for multimodal analysis.\n\n"
        "Evaluated against all 4 official IELTS Speaking band descriptors:\n"
        "- Fluency & Coherence\n"
        "- Lexical Resource\n"
        "- Grammatical Range & Accuracy\n"
        "- Pronunciation"
    ),
    tags=["Speaking"],
)
async def speaking_evaluate_base64(data: dict) -> dict:
    user = await _require_auth(data)

    file_content_base64: str = (data.get("file_content_base64") or "").strip()
    if not file_content_base64:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="file_content_base64 cannot be empty",
        )

    question: str = data.get("question", "") or ""
    part: str = data.get("part", "") or ""
    question_id: str | None = data.get("question_id")
    content_type: str = data.get("content_type", "audio/webm") or "audio/webm"

    audio_bytes = _decode_base64(file_content_base64)
    if len(audio_bytes) < 1000:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Audio file is too small or empty",
        )

    agent = get_speaking_agent()
    try:
        result = await agent.analyze(
            content=audio_bytes,
            media_type=content_type,
            question=question,
            part=part,
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


# ═════════════════════════════════════════════
#  SPEAKING QUESTION BY INDEX (Part 1 & 3)
# ═════════════════════════════════════════════

_INTERVIEW_TYPES = {"speaking_interview", "speaking_discussion"}

@register(
    name="speaking/question/item",
    method="GET",
    required_keys=["question_id", "index"],
    summary="Get a single speaking question by index",
    description=(
        "Returns one question from a Part 1 (interview) or Part 3 (discussion) question set by 0-based index.\n\n"
        "Use this to step through questions one by one on the frontend (e.g. ?q=0, ?q=1).\n"
        "Only works for speaking_interview and speaking_discussion types."
    ),
    tags=["Speaking"],
)
async def speaking_question_item(data: dict) -> dict:
    await _require_auth(data)

    question_id: str = str(data["question_id"]).strip()
    try:
        index = int(data["index"])
    except (TypeError, ValueError):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="index must be an integer")

    doc = await _question_repo().find_by_id(question_id)
    if not doc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Question not found")

    if doc.get("type") not in _INTERVIEW_TYPES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="This endpoint only supports speaking_interview (Part 1) and speaking_discussion (Part 3)",
        )

    questions: list = doc.get("speaking_questions") or []
    if not questions:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No speaking questions found in this document")

    if index < 0 or index >= len(questions):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Index {index} out of range — this set has {len(questions)} question(s) (0–{len(questions) - 1})",
        )

    return {
        "question_id": question_id,
        "index": index,
        "total": len(questions),
        "part": "Part 1" if doc.get("type") == "speaking_interview" else "Part 3",
        "item": questions[index],
    }


# ═════════════════════════════════════════════
#  SPEAKING SESSION (Part 1 & 3)
# ═════════════════════════════════════════════

@register(
    name="speaking/start",
    method="POST",
    required_keys=["question_id"],
    summary="Start a speaking practice session",
    description="Creates a new practice session for a Part 1 or Part 3 question set. Returns a session_id to use for subsequent answer submissions.",
    tags=["Speaking"],
)
async def speaking_practice_start(data: dict) -> dict:
    user = await _require_auth(data)

    question_id: str = str(data["question_id"]).strip()
    doc = await _question_repo().find_by_id(question_id)
    if not doc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Question not found")
    if doc.get("type") not in _INTERVIEW_TYPES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Practice sessions only supported for speaking_interview (Part 1) and speaking_discussion (Part 3)",
        )

    questions: list = doc.get("speaking_questions") or []
    if not questions:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No speaking questions in this document")

    part = "Part 1" if doc.get("type") == "speaking_interview" else "Part 3"
    session = await _practice_repo().create({
        "user_id": user.id,
        "question_id": question_id,
        "part": part,
        "total": len(questions),
    })
    return {"session_id": session["id"], "question_id": question_id, "part": part, "total": len(questions)}


@register(
    name="speaking/upload",
    method="POST",
    required_keys=["session_id", "index", "file_content_base64"],
    optional_keys={"content_type": "audio/webm"},
    summary="Upload audio for one question (no evaluation)",
    description=(
        "Upload base64-encoded audio for a single question and get back an audio_url.\n\n"
        "Use this if you want to upload first and evaluate later via speaking/evaluate/audio.\n"
        "For upload + evaluate in one call, use speaking/submit instead."
    ),
    tags=["Speaking"],
)
async def speaking_upload(data: dict) -> dict:
    user = await _require_auth(data)

    session_id: str = str(data["session_id"]).strip()
    file_content_base64: str = str(data["file_content_base64"]).strip()
    content_type: str = data.get("content_type", "audio/webm") or "audio/webm"

    try:
        index = int(data["index"])
    except (TypeError, ValueError):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="index must be an integer")

    session = await _practice_repo().find_by_id(session_id)
    if not session:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Practice session not found")
    if session["user_id"] != user.id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not your session")

    total = session["total"]
    if index < 0 or index >= total:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Index {index} out of range (0–{total - 1})",
        )

    audio_bytes = _decode_base64(file_content_base64)
    if len(audio_bytes) < 1000:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Audio file is too small or empty")

    ext = content_type.split("/")[-1].split(";")[0].strip() or "webm"
    s3_result = S3StorageService().upload_bytes(
        module_type="practice",
        test_id=session_id,
        section="speaking",
        file_name=f"q{index}.{ext}",
        file_bytes=audio_bytes,
        content_type=content_type,
        base_prefix=f"sessions/{user.id}",
        sub_path=str(index),
    )
    q_doc = await _question_repo().find_by_id(session["question_id"])
    questions: list = (q_doc.get("speaking_questions") or []) if q_doc else []
    question_text: str = questions[index]["question"] if index < len(questions) else ""

    return {
        "audio_url": s3_result["url"],
        "session_id": session_id,
        "index": index,
        "question": question_text,
        "part": session["part"],
        "total": session["total"],
    }


@register(
    name="speaking/submit",
    method="POST",
    required_keys=["session_id", "index", "file_content_base64"],
    optional_keys={"content_type": "audio/webm"},
    summary="Submit audio for one practice question",
    description=(
        "One-step endpoint: upload audio + AI evaluate + save result for a single question.\n\n"
        "Send the recorded audio as base64 with the question index (0-based).\n"
        "S3 path: sessions/{user_id}/practice/{session_id}/q{index}.webm\n"
        "Auto-completes the session when all questions are answered."
    ),
    tags=["Speaking"],
)
async def speaking_practice_submit(data: dict) -> dict:
    user = await _require_auth(data)

    session_id: str = str(data["session_id"]).strip()
    file_content_base64: str = str(data["file_content_base64"]).strip()
    content_type: str = data.get("content_type", "audio/webm") or "audio/webm"

    try:
        index = int(data["index"])
    except (TypeError, ValueError):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="index must be an integer")

    session = await _practice_repo().find_by_id(session_id)
    if not session:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Practice session not found")
    if session["user_id"] != user.id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not your session")
    if session["status"] == "completed":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Session is already completed")

    total = session["total"]
    if index < 0 or index >= total:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Index {index} out of range (0–{total - 1})",
        )

    # Decode bytes once — reuse for S3 upload and AI evaluation (no re-fetch)
    audio_bytes = _decode_base64(file_content_base64)
    if len(audio_bytes) < 1000:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Audio file is too small or empty")

    # Upload to S3
    ext = content_type.split("/")[-1].split(";")[0].strip() or "webm"
    s3_result = S3StorageService().upload_bytes(
        module_type="practice",
        test_id=session_id,
        section="speaking",
        file_name=f"q{index}.{ext}",
        file_bytes=audio_bytes,
        content_type=content_type,
        base_prefix=f"sessions/{user.id}",
        sub_path=str(index),
    )
    audio_url: str = s3_result["url"]

    # Fetch question text for AI context
    q_doc = await _question_repo().find_by_id(session["question_id"])
    questions: list = (q_doc.get("speaking_questions") or []) if q_doc else []
    question_text: str = questions[index]["question"] if index < len(questions) else ""

    agent = get_speaking_agent()
    try:
        result = await agent.analyze(
            content=audio_bytes,
            media_type=content_type,
            question=question_text,
            part=session["part"],
        )
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=f"AI evaluation failed: {exc}")

    evaluation = result.model_dump()

    # Save answer
    updated = await _practice_repo().push_answer(session_id, {
        "index": index,
        "question": question_text,
        "audio_url": audio_url,
        "evaluation": evaluation,
    })
    if updated is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session not found after saving answer")

    answered_indices = {a["index"] for a in (updated.get("answers") or [])}
    if len(answered_indices) >= total:
        completed = await _practice_repo().complete(session_id)
        if completed is not None:
            updated = completed

    return {
        "session_id": session_id,
        "index": index,
        "question": question_text,
        "part": session["part"],
        "audio_url": audio_url,
        "status": updated["status"],
        "answered": len(answered_indices),
        "total": total,
        "evaluation": evaluation,
    }


@register(
    name="speaking/result",
    method="GET",
    required_keys=["session_id"],
    summary="Get full practice session result",
    description="Returns the complete practice session including all submitted answers and their AI evaluations.",
    tags=["Speaking"],
)
async def speaking_practice_result(data: dict) -> dict:
    user = await _require_auth(data)

    session_id: str = str(data["session_id"]).strip()
    session = await _practice_repo().find_by_id(session_id)
    if not session:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Practice session not found")
    if session["user_id"] != user.id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not your session")

    # Sort answers by index
    answers = sorted(session.get("answers") or [], key=lambda a: a["index"])
    session["answers"] = answers
    return session


@register(
    name="speaking/sessions",
    method="GET",
    required_keys=[],
    optional_keys={"page": 1, "page_size": 20},
    summary="List user's speaking practice sessions",
    description="Returns a paginated list of the authenticated user's speaking practice sessions, most recent first.",
    tags=["Speaking"],
)
async def speaking_practice_sessions(data: dict) -> dict:
    user = await _require_auth(data)

    try:
        page = max(1, int(data.get("page", 1)))
        page_size = min(max(1, int(data.get("page_size", 20))), 100)
    except (TypeError, ValueError):
        page, page_size = 1, 20

    skip = (page - 1) * page_size
    sessions = await _practice_repo().find_by_user(user.id, skip=skip, limit=page_size)
    return {"page": page, "page_size": page_size, "items": sessions}


# ═════════════════════════════════════════════
#  SPEAKING SESSION (full test) — submit all audio
# ═════════════════════════════════════════════

@register(
    name="speaking/session/submit",
    method="POST",
    required_keys=["session_id", "parts"],
    optional_keys={"content_type": "audio/webm"},
    summary="Submit all speaking audio for a test session",
    description=(
        "Submit audio answers for all speaking parts of a test session in one call.\n\n"
        "Each part contains a question_id and an array of audio answers (base64-encoded).\n"
        "For each audio:\n"
        "1. Upload to S3\n"
        "2. Evaluate with AI (Gemini multimodal)\n"
        "3. Save audio_url + evaluation to the test session\n\n"
        "Expected body:\n"
        '```json\n'
        '{\n'
        '  "session_id": "...",\n'
        '  "parts": [\n'
        '    {\n'
        '      "part_number": 1,\n'
        '      "question_id": "...",\n'
        '      "answers": [\n'
        '        {"index": 0, "file_content_base64": "..."},\n'
        '        {"index": 1, "file_content_base64": "..."}\n'
        '      ]\n'
        '    },\n'
        '    {\n'
        '      "part_number": 2,\n'
        '      "question_id": "...",\n'
        '      "answers": [\n'
        '        {"file_content_base64": "..."}\n'
        '      ]\n'
        '    }\n'
        '  ]\n'
        '}\n'
        '```\n'
        "Auto-marks speaking section as completed and saves evaluations."
    ),
    tags=["Speaking"],
)
async def speaking_session_submit(data: dict) -> dict:
    user = await _require_auth(data)

    session_id: str = str(data["session_id"]).strip()
    parts: list = data.get("parts") or []
    content_type: str = data.get("content_type", "audio/webm") or "audio/webm"

    if not parts:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="parts cannot be empty")

    # Validate session
    session = await _session_repo().find_by_id(session_id)
    if not session:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test session not found")
    if session["user_id"] != user.id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not your session")

    # Load test to get speaking part structure
    test = await _test_repo().find_by_id(session["test_id"])
    if not test:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test not found")

    speaking_module = test.get("speaking")
    if not speaking_module:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Test has no speaking section")

    agent = get_speaking_agent()
    ext = content_type.split("/")[-1].split(";")[0].strip() or "webm"

    all_results: list = []
    session_answers: dict = session.get("answers", {}) or {}
    if "speaking" not in session_answers:
        session_answers["speaking"] = {}

    for part_data in parts:
        part_number = int(part_data.get("part_number", 0))
        question_id: str = str(part_data.get("question_id", "")).strip()
        answers: list = part_data.get("answers") or []

        if not question_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Part {part_number}: question_id is required",
            )

        # Load question document
        q_doc = await _question_repo().find_by_id(question_id)
        if not q_doc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Part {part_number}: question {question_id} not found",
            )

        q_type = q_doc.get("type", "")
        part_label = f"Part {part_number}"

        # Determine question texts for AI context
        if q_type == "speaking_cue_card":
            cue_card = q_doc.get("cue_card") or {}
            question_texts = [cue_card.get("topic", "")]
        else:
            speaking_questions = q_doc.get("speaking_questions") or []
            question_texts = [sq.get("question", "") for sq in speaking_questions]

        part_results: list = []

        for ans in answers:
            index = int(ans.get("index", 0))
            file_b64: str = str(ans.get("file_content_base64", "")).strip()

            if not file_b64:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Part {part_number}, index {index}: file_content_base64 is required",
                )

            audio_bytes = _decode_base64(file_b64)
            if len(audio_bytes) < 1000:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Part {part_number}, index {index}: audio file is too small or empty",
                )

            # Upload to S3
            s3_result = S3StorageService().upload_bytes(
                module_type="responses",
                test_id=session_id,
                section="speaking",
                file_name=f"part{part_number}_q{index}.{ext}",
                file_bytes=audio_bytes,
                content_type=content_type,
                base_prefix=f"sessions/{user.id}",
                sub_path=f"{question_id}/{index}",
            )
            audio_url: str = s3_result["url"]

            # AI evaluation
            question_text = question_texts[index] if index < len(question_texts) else ""
            try:
                result = await agent.analyze(
                    content=audio_bytes,
                    media_type=content_type,
                    question=question_text,
                    part=part_label,
                )
                evaluation = result.model_dump()
            except Exception as exc:
                evaluation = {"error": str(exc)}

            # Store as session answer
            answer_key = f"{question_id}_{index}"
            session_answers["speaking"][answer_key] = {
                "question_id": question_id,
                "part_number": part_number,
                "index": index,
                "question": question_text,
                "audio_url": audio_url,
                "evaluation": evaluation,
                "submitted_at": datetime.now(timezone.utc).isoformat(),
            }

            part_results.append({
                "question_id": question_id,
                "part_number": part_number,
                "index": index,
                "question": question_text,
                "audio_url": audio_url,
                "evaluation": evaluation,
            })

        all_results.extend(part_results)

    # Mark speaking section as completed in session_sections
    session_sections = session.get("session_sections", [])
    now = datetime.now(timezone.utc)
    for sec in session_sections:
        if sec.get("section") == "speaking":
            if sec.get("status") in ("not_started", "NOT_STARTED"):
                sec["started_at"] = now
            sec["status"] = "COMPLETED"
            sec["completed_at"] = now
            started = sec.get("started_at")
            if started:
                if isinstance(started, str):
                    started = datetime.fromisoformat(started)
                if started.tzinfo is None:
                    started = started.replace(tzinfo=timezone.utc)
                sec["time_spent_seconds"] = int((now - started).total_seconds())
            break

    # Advance current_section to next NOT_STARTED section
    next_section = None
    for sec in session_sections:
        if sec.get("status") in ("not_started", "NOT_STARTED"):
            next_section = sec["section"]
            break

    # Save to test session
    await _session_repo().update(session_id, {
        "answers": session_answers,
        "session_sections": session_sections,
        "current_section": next_section,
    })

    return {
        "session_id": session_id,
        "test_id": session["test_id"],
        "total_submitted": len(all_results),
        "results": all_results,
    }


# ═════════════════════════════════════════════
#  SPEAKING SESSION (full test) — submit via audio URLs
# ═════════════════════════════════════════════

@register(
    name="speaking/session/submit/url",
    method="POST",
    required_keys=["session_id", "parts"],
    optional_keys={"media_type": "audio/webm"},
    summary="Submit speaking audio URLs for a test session",
    description=(
        "Submit already-uploaded audio URLs for all speaking parts of a test session.\n\n"
        "Use this when audio has already been uploaded to S3 (e.g. via storage/session/upload-speaking-response).\n"
        "For each audio URL:\n"
        "1. Fetch audio from URL\n"
        "2. Evaluate with AI (Gemini multimodal)\n"
        "3. Save audio_url + evaluation to the test session\n\n"
        "Expected body:\n"
        '```json\n'
        '{\n'
        '  "session_id": "...",\n'
        '  "parts": [\n'
        '    {\n'
        '      "part_number": 1,\n'
        '      "question_id": "...",\n'
        '      "answers": [\n'
        '        {"index": 0, "audio_url": "https://..."},\n'
        '        {"index": 1, "audio_url": "https://..."}\n'
        '      ]\n'
        '    },\n'
        '    {\n'
        '      "part_number": 2,\n'
        '      "question_id": "...",\n'
        '      "answers": [\n'
        '        {"audio_url": "https://..."}\n'
        '      ]\n'
        '    }\n'
        '  ]\n'
        '}\n'
        '```\n'
        "Auto-marks speaking section as completed and saves evaluations."
    ),
    tags=["Speaking"],
)
async def speaking_session_submit_url(data: dict) -> dict:
    user = await _require_auth(data)

    session_id: str = str(data["session_id"]).strip()
    parts: list = data.get("parts") or []
    media_type: str = data.get("media_type", "audio/webm") or "audio/webm"

    if not parts:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="parts cannot be empty")

    # Validate session
    session = await _session_repo().find_by_id(session_id)
    if not session:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test session not found")
    if session["user_id"] != user.id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not your session")

    # Load test to get speaking part structure
    test = await _test_repo().find_by_id(session["test_id"])
    if not test:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test not found")

    speaking_module = test.get("speaking")
    if not speaking_module:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Test has no speaking section")

    agent = get_speaking_agent()

    all_results: list = []
    session_answers: dict = session.get("answers", {}) or {}
    if "speaking" not in session_answers:
        session_answers["speaking"] = {}

    for part_data in parts:
        part_number = int(part_data.get("part_number", 0))
        question_id: str = str(part_data.get("question_id", "")).strip()
        answers: list = part_data.get("answers") or []

        if not question_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Part {part_number}: question_id is required",
            )

        # Load question document
        q_doc = await _question_repo().find_by_id(question_id)
        if not q_doc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Part {part_number}: question {question_id} not found",
            )

        q_type = q_doc.get("type", "")
        part_label = f"Part {part_number}"

        # Determine question texts for AI context
        if q_type == "speaking_cue_card":
            cue_card = q_doc.get("cue_card") or {}
            question_texts = [cue_card.get("topic", "")]
        else:
            speaking_questions = q_doc.get("speaking_questions") or []
            question_texts = [sq.get("question", "") for sq in speaking_questions]

        part_results: list = []

        for ans in answers:
            index = int(ans.get("index", 0))
            audio_url: str = str(ans.get("audio_url", "")).strip()

            if not audio_url:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Part {part_number}, index {index}: audio_url is required",
                )

            # Fetch audio from URL
            audio_bytes = await _fetch_audio_bytes(audio_url)
            if len(audio_bytes) < 1000:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Part {part_number}, index {index}: audio file is too small or empty",
                )

            # AI evaluation
            question_text = question_texts[index] if index < len(question_texts) else ""
            try:
                result = await agent.analyze(
                    content=audio_bytes,
                    media_type=media_type,
                    question=question_text,
                    part=part_label,
                )
                evaluation = result.model_dump()
            except Exception as exc:
                evaluation = {"error": str(exc)}

            # Store as session answer
            answer_key = f"{question_id}_{index}"
            session_answers["speaking"][answer_key] = {
                "question_id": question_id,
                "part_number": part_number,
                "index": index,
                "question": question_text,
                "audio_url": audio_url,
                "evaluation": evaluation,
                "submitted_at": datetime.now(timezone.utc).isoformat(),
            }

            part_results.append({
                "question_id": question_id,
                "part_number": part_number,
                "index": index,
                "question": question_text,
                "audio_url": audio_url,
                "evaluation": evaluation,
            })

        all_results.extend(part_results)

    # Mark speaking section as completed in session_sections
    session_sections = session.get("session_sections", [])
    now = datetime.now(timezone.utc)
    for sec in session_sections:
        if sec.get("section") == "speaking":
            if sec.get("status") in ("not_started", "NOT_STARTED"):
                sec["started_at"] = now
            sec["status"] = "COMPLETED"
            sec["completed_at"] = now
            started = sec.get("started_at")
            if started:
                if isinstance(started, str):
                    started = datetime.fromisoformat(started)
                if started.tzinfo is None:
                    started = started.replace(tzinfo=timezone.utc)
                sec["time_spent_seconds"] = int((now - started).total_seconds())
            break

    # Advance current_section to next NOT_STARTED section
    next_section = None
    for sec in session_sections:
        if sec.get("status") in ("not_started", "NOT_STARTED"):
            next_section = sec["section"]
            break

    # Save to test session
    await _session_repo().update(session_id, {
        "answers": session_answers,
        "session_sections": session_sections,
        "current_section": next_section,
    })

    return {
        "session_id": session_id,
        "test_id": session["test_id"],
        "total_submitted": len(all_results),
        "results": all_results,
    }
