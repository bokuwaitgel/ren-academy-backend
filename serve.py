
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, File, Form, Header, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from typing import Any, Dict, List, cast
import re

from pydantic import BaseModel, Field, create_model
import os
import uvicorn

from src.api.api_routes import ENDPOINTS
from src.api import manager as _endpoints
from src.agent.speaking_agent import get_speaking_agent
from src.services.auth_service import AuthService
from src.services.s3_service import S3StorageService
from src.database.mongodb import MongoDB
from src.database.repositories.user_repository import UserRepository
from src.database.repositories.ielts_repository import (
    QuestionRepository,
    SpeakingPracticeRepository,
    TestRepository,
    TestSessionRepository,
)

load_dotenv()


@asynccontextmanager
async def lifespan(_: FastAPI):
    await MongoDB.connect()
    db = MongoDB.get_db()
    await UserRepository(db).create_indexes()
    await TestRepository(db).create_indexes()
    await TestSessionRepository(db).create_indexes()
    # Ensure question indexes
    q_repo = QuestionRepository(db)
    await q_repo.col.create_index("section")
    await q_repo.col.create_index("type")
    await q_repo.col.create_index("module_type")
    await q_repo.col.create_index([("created_at", -1)])
    try:
        yield
    finally:
        await MongoDB.disconnect()


app = FastAPI(title="Ren Academy API", version="1.0.0", lifespan=lifespan)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
        tags=app.openapi_tags,
    )

    openapi_schema.setdefault("components", {})
    openapi_schema["components"].setdefault("securitySchemes", {})
    openapi_schema["components"]["securitySchemes"]["BearerAuth"] = {
        "type": "http",
        "scheme": "bearer",
        "bearerFormat": "JWT",
        "description": "Paste access token: Bearer <token>",
    }
    openapi_schema["security"] = [{"BearerAuth": []}]

    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi

def _route_tags(name: str) -> List[str]:
    first = name.split("/", 1)[0].lower()
    tag_by_prefix = {
        "auth": "Auth",
        "storage": "Storage",
        "health": "System",
        "project-info": "System",
    }
    return [tag_by_prefix.get(first, "System")]


def _normalize_tags(tags: List[str]) -> List[str]:
    normalized: List[str] = []
    seen: set[str] = set()
    for tag in tags:
        if tag in seen:
            continue
        seen.add(tag)
        normalized.append(tag)
    return normalized


def _route_summary(route_name: str, method: str) -> str:
    parts = [p for p in route_name.split("/") if p]
    if not parts:
        return f"{method.title()} Endpoint"
    action_map = {
        "list": "List",
        "get": "Get",
        "start": "Start",
        "submit": "Submit",
        "finalize": "Finalize",
        "create": "Create",
        "refresh": "Refresh",
        "login": "Login",
        "register": "Register",
    }
    action = action_map.get(parts[-1], method.title())
    domain = parts[0].capitalize()
    target = " ".join(part.replace("-", " ") for part in parts[1:-1])
    target = target.title().strip()
    if target:
        return f"{action} {domain} {target}".strip()
    if len(parts) > 1:
        return f"{action} {domain} {parts[1].replace('-', ' ').title()}".strip()
    return f"{action} {domain}".strip()


def _route_description(route_name: str, required_keys: set[str], optional_defaults: Dict[str, Any]) -> str:
    required = ", ".join(sorted(required_keys)) if required_keys else "None"
    optional = ", ".join(sorted(optional_defaults.keys())) if optional_defaults else "None"
    return f"Route: /api/{route_name}. Required fields: {required}. Optional fields: {optional}."


def _infer_field_type_and_description(key: str, default: Any, required: bool) -> tuple[Any, str]:
    key_lower = key.lower()
    typed_fields: dict[str, tuple[Any, str]] = {
        "page": (int, "Page number starting from 1."),
        "page_size": (int, "Number of items per page (max 100)."),
        "skip": (int, "Legacy offset for pagination."),
        "limit": (int, "Legacy page size for pagination."),
        "email": (str, "User email address."),
        "username": (str, "Username."),
        "password": (str, "User password."),
        "refresh_token": (str, "Refresh token."),
        "access_token": (str, "Access token."),
        "role": (str, "User role."),
    }

    if key_lower in typed_fields:
        return typed_fields[key_lower]
    if key_lower.endswith("_id"):
        return str, "Resource ID."
    if key_lower.endswith("_ids"):
        return list[str], "List of resource IDs."
    if isinstance(default, bool):
        return bool, "Boolean field."
    if isinstance(default, int):
        return int, "Integer field."
    if isinstance(default, float):
        return float, "Numeric field."
    if isinstance(default, str):
        return str, "Text field."
    if isinstance(default, list):
        return list[Any], "Array field."
    if isinstance(default, dict):
        return dict[str, Any], "Object field."
    if required:
        return Any, "Required field."
    return Any, "Optional field."


def _make_get_dispatch(handler):
    async def get_dispatch(request: Request, authorization: str | None = Header(default=None, alias="Authorization")):
        payload: Dict[str, Any] = dict(request.query_params)
        if authorization:
            payload["authorization"] = authorization
        return await handler(payload)

    return get_dispatch


def _model_name(route_name: str) -> str:
    parts = [part for part in re.split(r"[^a-zA-Z0-9]+", route_name) if part]
    joined = "".join(part.capitalize() for part in parts)
    return f"{joined or 'Dynamic'}Request"


def _build_request_model(route_name: str, required_keys: set[str], optional_defaults: Dict[str, Any]) -> type[BaseModel]:
    fields: Dict[str, tuple[Any, Any]] = {}

    for key in sorted(required_keys):
        field_type, field_description = _infer_field_type_and_description(key, None, True)
        fields[key] = (field_type, Field(..., description=field_description))

    for key, default in optional_defaults.items():
        if key in required_keys:
            continue
        field_type, field_description = _infer_field_type_and_description(key, default, False)
        fields[key] = (field_type, Field(default=default, description=field_description))

    if not fields:
        fields["payload"] = (Dict[str, Any] | None, Field(default=None, description="Optional payload object"))

    return create_model(_model_name(route_name), **cast(dict[str, Any], fields))


def _make_body_dispatch(handler, request_model: type[BaseModel]):
    async def body_dispatch(
        data,
        authorization: str | None = Header(default=None, alias="Authorization"),
    ):
        payload = data.model_dump() if isinstance(data, BaseModel) else data
        if isinstance(payload, dict) and set(payload.keys()) == {"payload"} and payload.get("payload") is None:
            payload = {}
        if authorization:
            payload["authorization"] = authorization
        return await handler(payload)

    body_dispatch.__annotations__["data"] = request_model

    return body_dispatch


def _add_dynamic_route(
    method: str,
    path: str,
    route_name: str,
    handler,
    required_keys: set[str],
    optional_defaults: Dict[str, Any],
    include_in_schema: bool = True,
    explicit_summary: str | None = None,
    explicit_description: str | None = None,
    explicit_tags: List[str] | None = None,
):
    endpoint_name = route_name.replace("/", "_")
    summary = explicit_summary or _route_summary(route_name, method)
    description = explicit_description or _route_description(route_name, required_keys, optional_defaults)
    tags = _normalize_tags(explicit_tags or _route_tags(route_name))

    if method == "GET":
        get_dispatch = _make_get_dispatch(handler)
        app.add_api_route(
            path,
            get_dispatch,
            methods=["GET"],
            summary=summary,
            description=description,
            name=endpoint_name,
            include_in_schema=include_in_schema,
        )
    elif method == "POST":
        request_model = _build_request_model(route_name, required_keys, optional_defaults)
        post_dispatch = _make_body_dispatch(handler, request_model)
        app.add_api_route(
            path,
            post_dispatch,
            methods=["POST"],
            summary=summary,
            description=description,
            name=endpoint_name,
            include_in_schema=include_in_schema,
        )
    elif method == "PUT":
        request_model = _build_request_model(route_name, required_keys, optional_defaults)
        put_dispatch = _make_body_dispatch(handler, request_model)
        app.add_api_route(
            path,
            put_dispatch,
            methods=["PUT"],
            summary=summary,
            description=description,
            name=endpoint_name,
            include_in_schema=include_in_schema,
        )
    elif method == "DELETE":
        request_model = _build_request_model(route_name, required_keys, optional_defaults)
        delete_dispatch = _make_body_dispatch(handler, request_model)
        app.add_api_route(
            path,
            delete_dispatch,
            methods=["DELETE"],
            summary=summary,
            description=description,
            name=endpoint_name,
            include_in_schema=include_in_schema,
        )
    else:
        raise ValueError(f"Unsupported HTTP method: {method}")

# Dynamically add routes based on registered handlers
for name, info in ENDPOINTS.items():
    method = info["method"]
    handler = info["handler"]
    required_keys = info.get("required_keys", set())
    optional_defaults = info.get("optional_defaults", {})
    summary = info.get("summary")
    description = info.get("description")
    tags = info.get("tags")

    # Primary namespaced route (shown in Swagger)
    _add_dynamic_route(
        method,
        f"/api/{name}",
        name,
        handler,
        required_keys,
        optional_defaults,
        include_in_schema=True,
        explicit_summary=summary,
        explicit_description=description,
        explicit_tags=tags,
    )

@app.post(
    "/api/storage/session/upload-speaking-response/file",
    tags=["Storage"],
    summary="Upload Speaking Response Audio (File Upload)",
    description="Upload a candidate's spoken response as a multipart file. Returns the S3 URL to submit as the answer.",
)
async def upload_speaking_response_file(
    session_id: str = Form(...),
    question_id: str = Form(...),
    file: UploadFile = File(...),
    content_type: str = Form(default="audio/webm"),
    authorization: str | None = Header(default=None, alias="Authorization"),
):
    db = MongoDB.get_db()
    auth_svc = AuthService(UserRepository(db))

    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing token")
    token = authorization.split(" ", 1)[1].strip()
    user = await auth_svc.get_current_user(token)

    file_bytes = await file.read()
    resolved_ct = content_type or file.content_type or "audio/webm"
    file_name = file.filename or f"{question_id}.webm"

    url_data = S3StorageService().upload_bytes(
        module_type="responses",
        test_id=str(session_id),
        section="speaking",
        file_name=file_name,
        file_bytes=file_bytes,
        content_type=resolved_ct,
        base_prefix=f"sessions/{user.id}",
        sub_path=str(question_id),
    )
    return {"audio_url": url_data["url"], "question_id": question_id}


@app.post(
    "/api/speaking/upload/file",
    tags=["Speaking"],
    summary="Upload speaking audio file (multipart)",
    description="Upload raw audio bytes for a single practice question. Returns audio_url. No AI evaluation.",
)
async def speaking_upload_file(
    session_id: str = Form(...),
    index: int = Form(...),
    file: UploadFile = File(...),
    content_type: str = Form(default="audio/webm"),
    authorization: str | None = Header(default=None, alias="Authorization"),
):
    db = MongoDB.get_db()
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing token")
    user = await AuthService(UserRepository(db)).get_current_user(authorization.split(" ", 1)[1].strip())

    session = await SpeakingPracticeRepository(db).find_by_id(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Practice session not found")
    if session["user_id"] != user.id:
        raise HTTPException(status_code=403, detail="Not your session")
    if index < 0 or index >= session["total"]:
        raise HTTPException(status_code=400, detail=f"Index {index} out of range (0–{session['total'] - 1})")

    file_bytes = await file.read()
    if len(file_bytes) < 1000:
        raise HTTPException(status_code=400, detail="Audio file is too small or empty")

    resolved_ct = content_type or file.content_type or "audio/webm"
    ext = resolved_ct.split("/")[-1].split(";")[0].strip() or "webm"

    q_doc = await QuestionRepository(db).find_by_id(session["question_id"])
    questions: list = (q_doc.get("speaking_questions") or []) if q_doc else []
    question_text: str = questions[index]["question"] if index < len(questions) else ""

    s3_result = S3StorageService().upload_bytes(
        module_type="practice",
        test_id=session_id,
        section="speaking",
        file_name=f"q{index}.{ext}",
        file_bytes=file_bytes,
        content_type=resolved_ct,
        base_prefix=f"sessions/{user.id}",
        sub_path=str(index),
    )
    return {
        "audio_url": s3_result["url"],
        "session_id": session_id,
        "index": index,
        "question": question_text,
        "part": session["part"],
        "total": session["total"],
    }


@app.post(
    "/api/speaking/submit/file",
    tags=["Speaking"],
    summary="Submit speaking audio file (multipart) + evaluate",
    description="Upload raw audio bytes for a single practice question, evaluate with AI, and save result. One-step multipart version of speaking/submit.",
)
async def speaking_submit_file(
    session_id: str = Form(...),
    index: int = Form(...),
    file: UploadFile = File(...),
    content_type: str = Form(default="audio/webm"),
    authorization: str | None = Header(default=None, alias="Authorization"),
):
    db = MongoDB.get_db()
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing token")
    user = await AuthService(UserRepository(db)).get_current_user(authorization.split(" ", 1)[1].strip())

    practice_repo = SpeakingPracticeRepository(db)
    session = await practice_repo.find_by_id(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Practice session not found")
    if session["user_id"] != user.id:
        raise HTTPException(status_code=403, detail="Not your session")
    if session["status"] == "completed":
        raise HTTPException(status_code=400, detail="Session is already completed")

    total = session["total"]
    if index < 0 or index >= total:
        raise HTTPException(status_code=400, detail=f"Index {index} out of range (0–{total - 1})")

    file_bytes = await file.read()
    if len(file_bytes) < 1000:
        raise HTTPException(status_code=400, detail="Audio file is too small or empty")

    resolved_ct = content_type or file.content_type or "audio/webm"
    ext = resolved_ct.split("/")[-1].split(";")[0].strip() or "webm"

    # Upload to S3
    s3_result = S3StorageService().upload_bytes(
        module_type="practice",
        test_id=session_id,
        section="speaking",
        file_name=f"q{index}.{ext}",
        file_bytes=file_bytes,
        content_type=resolved_ct,
        base_prefix=f"sessions/{user.id}",
        sub_path=str(index),
    )
    audio_url = s3_result["url"]

    # Fetch question text
    q_doc = await QuestionRepository(db).find_by_id(session["question_id"])
    questions: list = (q_doc.get("speaking_questions") or []) if q_doc else []
    question_text: str = questions[index]["question"] if index < len(questions) else ""

    # Evaluate with AI (reuse file_bytes — no re-fetch)
    try:
        result = await get_speaking_agent().analyze(
            content=file_bytes,
            media_type=resolved_ct,
            question=question_text,
            part=session["part"],
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"AI evaluation failed: {exc}")

    evaluation = result.model_dump()

    updated = await practice_repo.push_answer(session_id, {
        "index": index,
        "question": question_text,
        "audio_url": audio_url,
        "evaluation": evaluation,
    })

    answered_indices = {a["index"] for a in (updated.get("answers") or [])}
    if len(answered_indices) >= total:
        updated = await practice_repo.complete(session_id)

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


@app.get("/")
async def root():
    return {
        "message": "Welcome to the Ren Academy API service.",
        "docs": "/docs",
        "redoc": "/redoc",
        "openapi": "/openapi.json",
        "auth_base": "/api/auth",
    }

# start  api


def main():
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8000"))
    reload_enabled = os.getenv("RELOAD", "true").lower() in {"1", "true", "yes"}
    uvicorn.run("serve:app", host=host, port=port, reload=reload_enabled)


if __name__ == "__main__":
    main()
