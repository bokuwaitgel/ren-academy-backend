from fastapi import HTTPException, status
from pydantic import ValidationError

from schemas.auth import UserResponse
from schemas.storage import S3QuestionFileUploadRequest, S3StructureCreateRequest
from src.api.api_routes import register
from src.database.mongodb import MongoDB
from src.database.repositories.ielts_repository import TestSessionRepository
from src.database.repositories.user_repository import UserRepository
from src.services.auth_service import AuthService
from src.services.s3_service import S3StorageService


def _session_repo() -> TestSessionRepository:
    return TestSessionRepository(MongoDB.get_db())


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


async def _current_user(payload: dict) -> UserResponse:
    token = _extract_token(payload)
    return await _auth_service().get_current_user(token)


def _require_roles(user: UserResponse, *roles: str):
    # super_admin always has access regardless of the requested roles
    allowed = set(roles) | {"super_admin", "super-admin"}
    if user.role not in allowed:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Access denied. Required role: {', '.join(roles)}",
        )


@register(
    name="storage/admin/s3/create-question-structure",
    method="POST",
    required_keys=["module_type", "test_id"],
    optional_keys={"sections": None, "base_prefix": "questions"},
)
async def create_question_structure(data: dict):
    user = await _current_user(data)
    _require_roles(user, "admin")
    payload_data = {k: v for k, v in data.items() if k != "access_token"}
    try:
        payload = S3StructureCreateRequest(**payload_data)
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=exc.errors())

    return S3StorageService().create_question_bucket_structure(
        module_type=payload.module_type,
        test_id=payload.test_id,
        sections=payload.sections,
        base_prefix=payload.base_prefix,
    )


@register(
    name="storage/admin/s3/upload-question-file",
    method="POST",
    required_keys=["module_type", "test_id", "section", "file_name", "file_content_base64"],
    optional_keys={"content_type": None, "base_prefix": "questions", "sub_path": None},
)
async def upload_question_file(data: dict):
    user = await _current_user(data)
    _require_roles(user, "admin")
    payload_data = {k: v for k, v in data.items() if k != "access_token"}
    try:
        payload = S3QuestionFileUploadRequest(**payload_data)
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=exc.errors())

    return S3StorageService().upload_question_file(
        module_type=payload.module_type,
        test_id=payload.test_id,
        section=payload.section,
        file_name=payload.file_name,
        file_content_base64=payload.file_content_base64,
        content_type=payload.content_type,
        base_prefix=payload.base_prefix,
        sub_path=payload.sub_path,
    )


@register(
    name="storage/admin/s3/upload-listening-audio",
    method="POST",
    required_keys=["module_type", "test_id", "file_name", "file_content_base64"],
    optional_keys={"content_type": "audio/mpeg", "base_prefix": "questions"},
    summary="Upload Listening Audio",
    description="Uploads listening section audio to S3 under listening/audio.",
    tags=["Storage"],
)
async def upload_listening_audio(data: dict):
    user = await _current_user(data)
    _require_roles(user, "admin")
    return S3StorageService().upload_question_file(
        module_type=str(data["module_type"]),
        test_id=str(data["test_id"]),
        section="listening",
        file_name=str(data["file_name"]),
        file_content_base64=str(data["file_content_base64"]),
        content_type=str(data.get("content_type")) if data.get("content_type") else None,
        base_prefix=str(data.get("base_prefix", "questions")),
        sub_path="audio",
    )


@register(
    name="storage/admin/s3/upload-reading-image",
    method="POST",
    required_keys=["module_type", "test_id", "file_name", "file_content_base64"],
    optional_keys={"content_type": "image/jpeg", "base_prefix": "questions"},
    summary="Upload Reading Image",
    description="Uploads reading image assets to S3 under reading/images.",
    tags=["Storage"],
)
async def upload_reading_image(data: dict):
    user = await _current_user(data)
    _require_roles(user, "admin")
    return S3StorageService().upload_question_file(
        module_type=str(data["module_type"]),
        test_id=str(data["test_id"]),
        section="reading",
        file_name=str(data["file_name"]),
        file_content_base64=str(data["file_content_base64"]),
        content_type=str(data.get("content_type")) if data.get("content_type") else None,
        base_prefix=str(data.get("base_prefix", "questions")),
        sub_path="images",
    )


@register(
    name="storage/session/upload-speaking-response",
    method="POST",
    required_keys=["session_id", "question_id", "file_name", "file_content_base64", "part"],
    optional_keys={"content_type": "audio/webm", "question": ""},
    summary="Upload Speaking Response Audio",
    description=(
        "Upload a candidate's spoken response audio for a speaking question and save the URL into the session.\n\n"
        "part: 1, 2, or 3\n"
        "question: question text (used for Part 1 & 3, ignored for Part 2)"
    ),
    tags=["Storage"],
)
async def upload_speaking_response(data: dict):
    user = await _current_user(data)

    session_id = str(data["session_id"])
    question_id = str(data["question_id"])
    question_text = str(data.get("question", "") or "")

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

    url_data = S3StorageService().upload_question_file(
        module_type="responses",
        test_id=session_id,
        section="speaking",
        file_name=str(data["file_name"]),
        file_content_base64=str(data["file_content_base64"]),
        content_type=str(data.get("content_type", "audio/webm")),
        base_prefix=f"sessions/{user.id}",
        sub_path=question_id,
    )
    audio_url: str = url_data["url"]

    session_answers: dict = dict(session.get("answers") or {})
    speaking_answers: dict = dict(session_answers.get("speaking") or {})

    if part == 2:
        speaking_answers[question_id] = {"part": part, "audio_url": audio_url}
    else:
        existing = speaking_answers.get(question_id, {"part": part, "responses": []})
        responses: list = list(existing.get("responses", []))
        responses.append({"question": question_text, "audio_url": audio_url})
        speaking_answers[question_id] = {"part": part, "responses": responses}

    session_answers["speaking"] = speaking_answers
    await _session_repo().update(session_id, {"answers": session_answers})

    return {"audio_url": audio_url, "question_id": question_id, "part": part}


@register(
    name="storage/admin/s3/upload-speaking-audio",
    method="POST",
    required_keys=["module_type", "test_id", "file_name", "file_content_base64"],
    optional_keys={"content_type": "audio/mpeg", "base_prefix": "questions", "sub_path": "audio"},
    summary="Upload Speaking Audio",
    description="Uploads speaking section audio (prompts or examples) to S3 under speaking/audio.",
    tags=["Storage"],
)
async def upload_speaking_audio(data: dict):
    user = await _current_user(data)
    _require_roles(user, "admin")
    return S3StorageService().upload_question_file(
        module_type=str(data["module_type"]),
        test_id=str(data["test_id"]),
        section="speaking",
        file_name=str(data["file_name"]),
        file_content_base64=str(data["file_content_base64"]),
        content_type=str(data.get("content_type")) if data.get("content_type") else None,
        base_prefix=str(data.get("base_prefix", "questions")),
        sub_path=str(data.get("sub_path", "audio")),
    )
