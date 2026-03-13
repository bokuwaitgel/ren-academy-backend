"""
Speaking evaluation endpoints — AI-powered IELTS speaking assessment.
"""

import base64 as _base64

import httpx
from fastapi import HTTPException, status

from src.api.api_routes import register
from src.database.mongodb import MongoDB
from src.database.repositories.ielts_repository import TestSessionRepository
from src.database.repositories.user_repository import UserRepository
from src.services.auth_service import AuthService
from src.services.s3_service import S3StorageService
from src.agent.speaking_agent import get_speaking_agent


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _round_half(value: float) -> float:
    return round(value * 2) / 2


def _calculate_speaking_section_score(all_results: list) -> dict | None:
    valid_evals = [
        r["evaluation"] for r in all_results
        if isinstance(r.get("evaluation"), dict) and "error" not in r["evaluation"]
    ]
    if not valid_evals:
        return None

    def _avg(vals: list[float]) -> float | None:
        return _round_half(sum(vals) / len(vals)) if vals else None

    fluency_avg = _avg([e["fluency_coherence"] for e in valid_evals if "fluency_coherence" in e])
    lexical_avg = _avg([e["lexical_resource"] for e in valid_evals if "lexical_resource" in e])
    grammar_avg = _avg([e["grammar_accuracy"] for e in valid_evals if "grammar_accuracy" in e])
    pronun_avg  = _avg([e["pronunciation"] for e in valid_evals if "pronunciation" in e])

    criteria_vals = [v for v in [fluency_avg, lexical_avg, grammar_avg, pronun_avg] if v is not None]
    overall = _avg(criteria_vals) if criteria_vals else _avg(
        [e["overall_score"] for e in valid_evals if "overall_score" in e]
    )

    return {
        "band_score": overall,
        "criteria": {
            "fluency_coherence": fluency_avg,
            "lexical_resource": lexical_avg,
            "grammar_accuracy": grammar_avg,
            "pronunciation": pronun_avg,
        },
        "answer_count": len(valid_evals),
        "answer_details": all_results,
    }


def _auth_service() -> AuthService:
    db = MongoDB.get_db()
    return AuthService(UserRepository(db))


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


def _decode_base64(data: str) -> bytes:
    raw = data.strip()
    if "," in raw and "base64" in raw[:64].lower():
        raw = raw.split(",", 1)[1]
    try:
        return _base64.b64decode(raw, validate=True)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid base64 audio: {exc}")


# ═════════════════════════════════════════════
#  SPEAKING SESSION — get uploaded audio URLs
# ═════════════════════════════════════════════

@register(
    name="speaking/session/urls",
    method="GET",
    required_keys=["session_id"],
    summary="Get speaking uploaded audio URLs",
    description="Returns all uploaded audio URLs stored in the session's speaking answers.",
    tags=["Speaking"],
)
async def speaking_session_urls(data: dict) -> dict:
    user = await _require_auth(data)

    session_id: str = str(data["session_id"]).strip()
    session = await _session_repo().find_by_id(session_id)
    if not session:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session not found")
    if session["user_id"] != user.id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not your session")

    speaking_answers: dict = (session.get("answers") or {}).get("speaking", {})

    return {
        "session_id": session_id,
        "speaking": speaking_answers,
    }


# ═════════════════════════════════════════════
#  SPEAKING AUDIO UPLOAD
# ═════════════════════════════════════════════

@register(
    name="speaking/upload",
    method="POST",
    required_keys=["session_id", "question_id", "index", "file_content_base64", "part"],
    optional_keys={"content_type": "audio/webm", "question": ""},
    summary="Upload speaking audio and save URL to session",
    description=(
        "Upload base64-encoded audio to S3 and save the URL directly into the session's speaking answers.\n\n"
        "part: 1, 2, or 3\n"
        "question: the question text (required for Part 1 & 3, ignored for Part 2)\n"
        "S3 path: speaking/{mode}/{session_id}_{test_id}_{user_id}_{question_id}_{index}.{ext}"
    ),
    tags=["Speaking"],
)
async def speaking_upload(data: dict) -> dict:
    user = await _require_auth(data)

    session_id: str = str(data["session_id"]).strip()
    question_id: str = str(data["question_id"]).strip()
    file_content_base64: str = str(data["file_content_base64"]).strip()
    content_type: str = data.get("content_type", "audio/webm") or "audio/webm"
    question_text: str = str(data.get("question", "") or "")

    try:
        index = int(data["index"])
    except (TypeError, ValueError):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="index must be an integer")

    try:
        part = int(data["part"])
        if part not in (1, 2, 3):
            raise ValueError
    except (TypeError, ValueError):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="part must be 1, 2, or 3")

    session = await _session_repo().find_by_id(session_id)
    if not session:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session not found")
    if session["user_id"] != user.id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not your session")

    audio_bytes = _decode_base64(file_content_base64)
    if len(audio_bytes) < 1000:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Audio file is too small or empty")

    ext = content_type.split("/")[-1].split(";")[0].strip() or "webm"
    s3_result = S3StorageService().upload_speaking_audio(
        mode=session.get("mode", "full_test"),
        session_id=session_id,
        user_id=user.id,
        question_id=question_id,
        index=index,
        file_bytes=audio_bytes,
        ext=ext,
        content_type=content_type,
        test_id=session.get("test_id"),
    )

    audio_url: str = s3_result["url"]

    # Save URL directly into session.answers.speaking
    session_answers: dict = dict(session.get("answers") or {})
    speaking_answers: dict = dict(session_answers.get("speaking") or {})

    if part == 2:
        speaking_answers[question_id] = {"part": part, "audio_url": audio_url}
    else:
        existing = speaking_answers.get(question_id, {"part": part, "responses": []})
        responses: list = list(existing.get("responses", []))
        # Replace if same index, otherwise append
        entry = {"question": question_text, "audio_url": audio_url}
        if index < len(responses):
            responses[index] = entry
        else:
            responses.append(entry)
        speaking_answers[question_id] = {"part": part, "responses": responses}

    session_answers["speaking"] = speaking_answers
    await _session_repo().update(session_id, {"answers": session_answers})

    return {
        "audio_url": audio_url,
        "session_id": session_id,
        "question_id": question_id,
        "part": part,
        "index": index,
    }


# ═════════════════════════════════════════════
#  SPEAKING SESSION — evaluate from stored answers
# ═════════════════════════════════════════════

@register(
    name="speaking/session/evaluate",
    method="POST",
    required_keys=["session_id"],
    optional_keys={"media_type": "audio/webm"},
    summary="Evaluate stored speaking answers for a test session",
    description=(
        "Reads the speaking answers stored in the test session (submitted via sessions/section/submit) "
        "and evaluates each audio URL with AI.\n\n"
        "Expected answer format per question_id:\n"
        "- Part 1 & 3: {\"part\": 1, \"responses\": [{\"question\": \"...\", \"audio_url\": \"...\"}]}\n"
        "- Part 2:     {\"part\": 2, \"audio_url\": \"...\"}\n\n"
        "Saves evaluations back into the session and updates the speaking band score."
    ),
    tags=["Speaking"],
)
async def speaking_session_evaluate(data: dict) -> dict:
    user = await _require_auth(data)

    session_id: str = str(data["session_id"]).strip()
    media_type: str = data.get("media_type", "audio/webm") or "audio/webm"

    session = await _session_repo().find_by_id(session_id)
    if not session:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test session not found")
    if session["user_id"] != user.id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not your session")

    session_answers: dict = session.get("answers", {}) or {}
    speaking_answers: dict = session_answers.get("speaking", {})

    if not speaking_answers:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No speaking answers found. Submit answers first via sessions/section/submit.",
        )

    agent = get_speaking_agent()
    all_results: list = []

    for question_id, answer in speaking_answers.items():
        part_number = answer.get("part", 0)
        part_label = f"Part {part_number}"

        if part_number == 2:
            audio_url: str = str(answer.get("audio_url", "")).strip()
            if not audio_url:
                continue
            audio_bytes = await _fetch_audio_bytes(audio_url)
            try:
                result = await agent.analyze(
                    content=audio_bytes,
                    media_type=media_type,
                    question="",
                    part=part_label,
                )
                evaluation = result.model_dump()
            except Exception as exc:
                evaluation = {"error": str(exc)}

            answer["evaluation"] = evaluation
            all_results.append({
                "question_id": question_id,
                "part_number": part_number,
                "audio_url": audio_url,
                "evaluation": evaluation,
            })

        else:
            responses: list = answer.get("responses", [])
            evaluated_responses = []
            for resp in responses:
                audio_url = str(resp.get("audio_url", "")).strip()
                question_text = resp.get("question", "")
                if not audio_url:
                    evaluated_responses.append(resp)
                    continue
                audio_bytes = await _fetch_audio_bytes(audio_url)
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

                evaluated_responses.append({**resp, "evaluation": evaluation})
                all_results.append({
                    "question_id": question_id,
                    "part_number": part_number,
                    "question": question_text,
                    "audio_url": audio_url,
                    "evaluation": evaluation,
                })

            answer["responses"] = evaluated_responses

    speaking_score = _calculate_speaking_section_score(all_results)

    section_scores = [
        s for s in (session.get("section_scores") or [])
        if s.get("section") != "speaking"
    ]
    if speaking_score and speaking_score.get("band_score") is not None:
        section_scores.append({
            "section": "speaking",
            "raw_score": speaking_score["answer_count"],
            "max_score": len(all_results),
            "band_score": speaking_score["band_score"],
            "details": speaking_score,
        })

    session_answers["speaking"] = speaking_answers
    update: dict = {"answers": session_answers, "section_scores": section_scores}

    bands = [s["band_score"] for s in section_scores if s.get("band_score") is not None]
    if bands:
        update["overall_band"] = _round_half(sum(bands) / len(bands))

    await _session_repo().update(session_id, update)

    return {
        "session_id": session_id,
        "test_id": session["test_id"],
        "total_evaluated": len(all_results),
        "speaking_score": speaking_score,
        "results": all_results,
    }
