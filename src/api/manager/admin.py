"""
Admin Panel API endpoints — dashboard, user management, session oversight, grading.
"""

from datetime import datetime, timezone

from bson import ObjectId  # type: ignore[import-untyped]
from fastapi import HTTPException, status

from src.api.api_routes import register
from src.database.mongodb import MongoDB
from src.database.repositories.ielts_repository import (
    QuestionRepository,
    SpeakingPracticeRepository,
    TestRepository,
    TestSessionRepository,
)
from src.database.repositories.user_repository import UserRepository
from src.services.auth_service import AuthService
from src.services.ielts_service import IeltsService


# ── Helpers ───────────────────────────────────

def _get_services():
    db = MongoDB.get_db()
    return (
        IeltsService(
            question_repo=QuestionRepository(db),
            test_repo=TestRepository(db),
            session_repo=TestSessionRepository(db),
        ),
        AuthService(UserRepository(db)),
        UserRepository(db),
    )


def _extract_token(payload: dict) -> str:
    token = payload.get("access_token") or payload.get("token")
    auth_header = payload.get("authorization")
    if not token and isinstance(auth_header, str) and auth_header.lower().startswith("bearer "):
        token = auth_header.split(" ", 1)[1].strip()
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing token")
    return str(token)


async def _require_admin(payload: dict):
    token = _extract_token(payload)
    ielts_svc, auth_svc, user_repo = _get_services()
    user = await auth_svc.get_current_user(token)
    if user.role not in {"admin", "super_admin", "super-admin"}:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required")
    return user, ielts_svc, auth_svc, user_repo


async def _require_super_admin(payload: dict):
    token = _extract_token(payload)
    ielts_svc, auth_svc, user_repo = _get_services()
    user = await auth_svc.get_current_user(token)
    if user.role not in {"super_admin", "super-admin"}:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Super-admin access required")
    return user, ielts_svc, auth_svc, user_repo


async def _require_admin_or_examiner(payload: dict):
    token = _extract_token(payload)
    ielts_svc, auth_svc, user_repo = _get_services()
    user = await auth_svc.get_current_user(token)
    if user.role not in {"admin", "examiner", "super_admin", "super-admin"}:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin or examiner access required")
    return user, ielts_svc, auth_svc, user_repo


# ═════════════════════════════════════════════
#  DASHBOARD
# ═════════════════════════════════════════════

@register(
    name="admin/dashboard",
    method="GET",
    required_keys=[],
    summary="Admin dashboard",
    description="Get aggregated statistics for the admin dashboard.",
    tags=["Admin"],
)
async def admin_dashboard(data: dict):
    user, ielts_svc, auth_svc, user_repo = await _require_admin(data)
    return await ielts_svc.get_dashboard_stats(user_repo)


# ═════════════════════════════════════════════
#  USER MANAGEMENT
# ═════════════════════════════════════════════

@register(
    name="admin/users/list",
    method="GET",
    required_keys=[],
    optional_keys={"page": 1, "page_size": 20, "role": None, "search": None},
    summary="List all users (admin)",
    description="List all users with filtering and pagination.",
    tags=["Admin"],
)
async def admin_users_list(data: dict):
    user, ielts_svc, auth_svc, user_repo = await _require_admin(data)
    page = int(data.get("page", 1))
    page_size = min(int(data.get("page_size", 20)), 100)
    role = data.get("role")
    search = data.get("search")
    skip = (page - 1) * page_size

    query = {}
    if role:
        query["role"] = role
    if search:
        query["$or"] = [
            {"username": {"$regex": search, "$options": "i"}},
            {"email": {"$regex": search, "$options": "i"}},
        ]

    total = await user_repo.collection.count_documents(query)
    cursor = user_repo.collection.find(query).sort("created_at", -1).skip(skip).limit(page_size)
    users = [UserRepository.serialize(doc) async for doc in cursor]
    total_pages = max(1, (total + page_size - 1) // page_size)

    return {
        "items": users,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
    }


@register(
    name="admin/users/get",
    method="GET",
    required_keys=["user_id"],
    summary="Get user details (admin)",
    description="Get full details of a specific user.",
    tags=["Admin"],
)
async def admin_users_get(data: dict):
    user, ielts_svc, auth_svc, user_repo = await _require_admin(data)
    target = await user_repo.find_by_id(data["user_id"])
    if not target:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    serialized = UserRepository.serialize(target)

    # Include session history summary
    sessions = await ielts_svc.session_repo.find_by_user(data["user_id"], skip=0, limit=100)
    serialized["total_sessions"] = len(sessions)
    completed = [s for s in sessions if s.get("status") in {"submitted", "graded"}]
    serialized["completed_sessions"] = len(completed)
    bands = [s["overall_band"] for s in completed if s.get("overall_band") is not None]
    serialized["average_band"] = round(sum(bands) / len(bands) * 2) / 2 if bands else None
    serialized["recent_sessions"] = sessions[:5]

    return serialized


@register(
    name="admin/users/update",
    method="PUT",
    required_keys=["user_id"],
    optional_keys={"role": None, "is_active": None},
    summary="Update user (admin)",
    description="Update a user's role or active status.",
    tags=["Admin"],
)
async def admin_users_update(data: dict):
    admin_user, ielts_svc, auth_svc, user_repo = await _require_admin(data)
    target_id = data["user_id"]

    # Prevent admin from deactivating themselves
    if target_id == admin_user.id and data.get("is_active") is False:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot deactivate yourself")

    target = await user_repo.find_by_id(target_id)
    if not target:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    update_fields = {}
    if data.get("role") is not None:
        valid_roles = {"candidate", "examiner", "admin", "super_admin", "super-admin"}
        if data["role"] not in valid_roles:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid role. Must be one of: {', '.join(valid_roles)}",
            )
        update_fields["role"] = data["role"]
    if data.get("is_active") is not None:
        update_fields["is_active"] = bool(data["is_active"])

    if not update_fields:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No fields to update")

    update_fields["updated_at"] = datetime.now(timezone.utc)
    await user_repo.collection.update_one(
        {"_id": ObjectId(target_id)},
        {"$set": update_fields},
    )

    updated = await user_repo.find_by_id(target_id)
    if not updated:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found after update")
    return UserRepository.serialize(updated)


@register(
    name="admin/users/deactivate",
    method="POST",
    required_keys=["user_id"],
    summary="Deactivate user (admin)",
    description="Deactivate a user account.",
    tags=["Admin"],
)
async def admin_users_deactivate(data: dict):
    admin_user, ielts_svc, auth_svc, user_repo = await _require_admin(data)
    target_id = data["user_id"]
    if target_id == admin_user.id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot deactivate yourself")
    success = await user_repo.deactivate(target_id)
    if not success:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return {"status": "deactivated", "user_id": target_id}


# ═════════════════════════════════════════════
#  SESSION MANAGEMENT
# ═════════════════════════════════════════════

@register(
    name="admin/sessions/list",
    method="GET",
    required_keys=[],
    optional_keys={"page": 1, "page_size": 20, "status": None, "user_id": None},
    summary="List all sessions (admin)",
    description="List all test sessions with filtering.",
    tags=["Admin"],
)
async def admin_sessions_list(data: dict):
    user, ielts_svc, auth_svc, user_repo = await _require_admin_or_examiner(data)
    return await ielts_svc.list_all_sessions(
        page=int(data.get("page", 1)),
        page_size=min(int(data.get("page_size", 20)), 100),
        status_filter=data.get("status"),
        user_id=data.get("user_id"),
    )


@register(
    name="admin/sessions/get",
    method="GET",
    required_keys=["session_id"],
    summary="Get session details (admin)",
    description="Get full session details including answers (admin/examiner).",
    tags=["Admin"],
)
async def admin_sessions_get(data: dict):
    user, ielts_svc, auth_svc, user_repo = await _require_admin_or_examiner(data)
    return await ielts_svc.get_session(data["session_id"])


@register(
    name="admin/sessions/result",
    method="GET",
    required_keys=["session_id"],
    summary="Get session result (admin)",
    description="Get the scored results for any session.",
    tags=["Admin"],
)
async def admin_sessions_result(data: dict):
    user, ielts_svc, auth_svc, user_repo = await _require_admin_or_examiner(data)
    return await ielts_svc.get_session_result(data["session_id"])


@register(
    name="admin/sessions/grade",
    method="POST",
    required_keys=["session_id", "section", "band_score"],
    optional_keys={"details": None},
    summary="Grade writing/speaking",
    description="Manually set band score for writing or speaking sections (examiner/admin).",
    tags=["Admin"],
)
async def admin_sessions_grade(data: dict):
    user, ielts_svc, auth_svc, user_repo = await _require_admin_or_examiner(data)
    section = data["section"]
    if section not in {"writing", "speaking"}:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Can only grade writing or speaking sections")
    band_score = float(data["band_score"])
    if band_score < 0 or band_score > 9 or (band_score * 2) != int(band_score * 2):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Band score must be 0-9 in 0.5 increments")
    return await ielts_svc.grade_writing_speaking(
        session_id=data["session_id"],
        section=section,
        band_score=band_score,
        details=data.get("details"),
    )


@register(
    name="admin/sessions/delete",
    method="DELETE",
    required_keys=["session_id"],
    summary="Delete session (super-admin)",
    description="Permanently delete a test session. Only super-admins can perform this action.",
    tags=["Admin"],
)
async def admin_sessions_delete(data: dict):
    user, ielts_svc, auth_svc, user_repo = await _require_super_admin(data)
    session_id = data["session_id"]
    db = MongoDB.get_db()
    result = await db["test_sessions"].delete_one({"_id": ObjectId(session_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session not found")
    return {"status": "deleted", "session_id": session_id}


# ═════════════════════════════════════════════
#  QUESTION & TEST ANALYTICS
# ═════════════════════════════════════════════

@register(
    name="admin/analytics/questions",
    method="GET",
    required_keys=[],
    optional_keys={"section": None, "module_type": None},
    summary="Question analytics",
    description="Get question distribution analytics.",
    tags=["Admin"],
)
async def admin_analytics_questions(data: dict):
    user, ielts_svc, auth_svc, user_repo = await _require_admin(data)
    db = MongoDB.get_db()
    q_col = db["questions"]

    # Distribution by section
    pipeline_section = [{"$group": {"_id": "$section", "count": {"$sum": 1}}}]
    by_section = {}
    async for doc in q_col.aggregate(pipeline_section):
        by_section[doc["_id"] or "unknown"] = doc["count"]

    # Distribution by type
    pipeline_type = [{"$group": {"_id": "$type", "count": {"$sum": 1}}}]
    by_type = {}
    async for doc in q_col.aggregate(pipeline_type):
        by_type[doc["_id"] or "unknown"] = doc["count"]

    # Distribution by module
    pipeline_module = [{"$group": {"_id": "$module_type", "count": {"$sum": 1}}}]
    by_module = {}
    async for doc in q_col.aggregate(pipeline_module):
        by_module[doc["_id"] or "unknown"] = doc["count"]

    total = await q_col.count_documents({})

    return {
        "total_questions": total,
        "by_section": by_section,
        "by_type": by_type,
        "by_module": by_module,
    }


@register(
    name="admin/analytics/tests",
    method="GET",
    required_keys=[],
    summary="Test analytics",
    description="Get test and session analytics.",
    tags=["Admin"],
)
async def admin_analytics_tests(data: dict):
    user, ielts_svc, auth_svc, user_repo = await _require_admin(data)
    db = MongoDB.get_db()
    t_col = db["tests"]
    s_col = db["test_sessions"]

    total_tests = await t_col.count_documents({})
    published_tests = await t_col.count_documents({"is_published": True})
    draft_tests = total_tests - published_tests

    total_sessions = await s_col.count_documents({})

    # Band distribution
    pipeline_bands = [
        {"$match": {"overall_band": {"$ne": None}}},
        {"$group": {"_id": "$overall_band", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
    ]
    band_distribution = {}
    async for doc in s_col.aggregate(pipeline_bands):
        band_distribution[str(doc["_id"])] = doc["count"]

    # Most attempted tests
    pipeline_popular = [
        {"$group": {"_id": "$test_id", "attempts": {"$sum": 1}}},
        {"$sort": {"attempts": -1}},
        {"$limit": 10},
    ]
    popular_tests = []
    async for doc in s_col.aggregate(pipeline_popular):
        test_doc = await t_col.find_one({"_id": ObjectId(doc["_id"])}) if doc["_id"] else None
        popular_tests.append({
            "test_id": doc["_id"],
            "title": test_doc.get("title", "Unknown") if test_doc else "Deleted",
            "attempts": doc["attempts"],
        })

    return {
        "total_tests": total_tests,
        "published_tests": published_tests,
        "draft_tests": draft_tests,
        "total_sessions": total_sessions,
        "band_distribution": band_distribution,
        "most_popular_tests": popular_tests,
    }


# ═════════════════════════════════════════════
#  SPEAKING PRACTICE SESSION MANAGEMENT
# ═════════════════════════════════════════════

@register(
    name="admin/speaking-practice/list",
    method="GET",
    required_keys=[],
    optional_keys={"page": 1, "page_size": 20, "user_id": None, "status": None},
    summary="List speaking practice sessions (admin)",
    description="List all speaking practice sessions with optional filters.",
    tags=["Admin"],
)
async def admin_speaking_practice_list(data: dict):
    user, ielts_svc, auth_svc, user_repo = await _require_admin_or_examiner(data)
    db = MongoDB.get_db()
    repo = SpeakingPracticeRepository(db)

    page = max(1, int(data.get("page", 1)))
    page_size = min(max(1, int(data.get("page_size", 20))), 100)
    skip = (page - 1) * page_size

    query: dict = {}
    if data.get("user_id"):
        query["user_id"] = data["user_id"]
    if data.get("status"):
        query["status"] = data["status"]

    total = await repo.col.count_documents(query)
    cursor = repo.col.find(query).sort("created_at", -1).skip(skip).limit(page_size)
    items = []
    async for doc in cursor:
        doc["id"] = str(doc.pop("_id"))
        items.append(doc)

    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": max(1, (total + page_size - 1) // page_size),
    }


@register(
    name="admin/speaking-practice/get",
    method="GET",
    required_keys=["session_id"],
    summary="Get speaking practice session (admin)",
    description="Get full speaking practice session including all answers and evaluations.",
    tags=["Admin"],
)
async def admin_speaking_practice_get(data: dict):
    user, ielts_svc, auth_svc, user_repo = await _require_admin_or_examiner(data)
    db = MongoDB.get_db()
    repo = SpeakingPracticeRepository(db)

    session = await repo.find_by_id(data["session_id"])
    if not session:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Practice session not found")

    # Sort answers by index
    session["answers"] = sorted(session.get("answers") or [], key=lambda a: a["index"])
    return session
