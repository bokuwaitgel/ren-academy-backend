"""
IELTS Service — business logic for questions, tests, sessions, and scoring.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, status

from schemas.enums import (
    ModuleType,
    QuestionType,
    SectionType,
    TestStatus,
    WritingQuestionType,
    SpeakingQuestionType,
)
from schemas.ielts import (
    AnswerSubmission,
    PaginatedResponse,
    QuestionCreate,
    QuestionOut,
    QuestionSafe,
    QuestionUpdate,
    SectionAnswers,
    SectionConfig,
    SectionScore,
    SessionResult,
    TestCreate,
    TestOut,
    TestSessionOut,
    TestUpdate,
)

# Import scoring utilities — schemas/ielts/ dir is shadowed by schemas/ielts.py,
# so we use importlib to load from the sub-directory.
import importlib.util as _ilu, pathlib as _pl, types as _types
_scoring_path = _pl.Path(__file__).resolve().parents[2] / "schemas" / "ielts" / "scoring.py"
_spec = _ilu.spec_from_file_location("schemas_ielts_scoring", str(_scoring_path))
assert _spec is not None and _spec.loader is not None, "Cannot locate schemas/ielts/scoring.py"
_scoring: _types.ModuleType = _ilu.module_from_spec(_spec)
_spec.loader.exec_module(_scoring)  # type: ignore[union-attr]

raw_to_band_listening = _scoring.raw_to_band_listening
raw_to_band_reading = _scoring.raw_to_band_reading
criteria_average_to_band = _scoring.criteria_average_to_band
calculate_overall_band = _scoring.calculate_overall_band
from src.database.repositories.ielts_repository import (
    QuestionRepository,
    TestRepository,
    TestSessionRepository,
)


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _strip_answers(question: dict) -> dict:
    """Remove answer fields so candidates don't see correct answers."""
    ANSWER_KEYS = {
        "correct_option", "correct_options",
    }
    safe = {k: v for k, v in question.items() if k not in ANSWER_KEYS}

    # Strip answers from nested items
    def _strip_nested(items: list, keys_to_remove: set) -> list:
        if not items:
            return items
        return [{k: v for k, v in item.items() if k not in keys_to_remove} for item in items]

    if safe.get("form_fields"):
        safe["form_fields"] = _strip_nested(safe["form_fields"], {"answer"})
    if safe.get("table_cells"):
        safe["table_cells"] = _strip_nested(safe["table_cells"], {"answer"})
    if safe.get("flow_steps"):
        safe["flow_steps"] = _strip_nested(safe["flow_steps"], {"answer"})
    if safe.get("sentences"):
        safe["sentences"] = _strip_nested(safe["sentences"], {"answer"})
    if safe.get("summary_items"):
        safe["summary_items"] = [{k: v for k, v in item.items() if k not in {"answer"}} for item in safe["summary_items"]]
    if safe.get("short_items"):
        safe["short_items"] = _strip_nested(safe["short_items"], {"answer"})
    if safe.get("map_slots"):
        safe["map_slots"] = _strip_nested(safe["map_slots"], {"answer"})
    if safe.get("matching_items"):
        safe["matching_items"] = _strip_nested(safe["matching_items"], {"answer"})
    if safe.get("heading_items"):
        safe["heading_items"] = _strip_nested(safe["heading_items"], {"answer"})
    if safe.get("tfng_items"):
        safe["tfng_items"] = _strip_nested(safe["tfng_items"], {"answer"})
    if safe.get("pick_items"):
        safe["pick_items"] = [{k: v for k, v in item.items() if k not in {"answers"}} for item in safe["pick_items"]]

    # Strip sample answers from writing/speaking
    if safe.get("writing_prompt") and isinstance(safe["writing_prompt"], dict):
        safe["writing_prompt"] = {k: v for k, v in safe["writing_prompt"].items() if k != "sample_answer"}
    if safe.get("cue_card") and isinstance(safe["cue_card"], dict):
        safe["cue_card"] = {k: v for k, v in safe["cue_card"].items() if k != "sample_answer"}
    if safe.get("speaking_questions"):
        safe["speaking_questions"] = [
            {k: v for k, v in sq.items() if k not in {"sample_answer", "band_tip"}}
            for sq in safe["speaking_questions"]
        ]

    return safe


def _score_question(question: dict, user_answer: Any) -> tuple[int, int]:
    """
    Score a single question.
    Returns (points_earned, max_points).
    """
    q_type = question.get("type", "")

    # Multiple choice — 1 point
    if q_type == QuestionType.MULTIPLE_CHOICE:
        correct = question.get("correct_option", "")
        earned = 1 if str(user_answer).strip().upper() == str(correct).strip().upper() else 0
        return earned, 1

    # Multiple select — 1 point per correct option
    if q_type == QuestionType.MULTIPLE_SELECT:
        correct_set = set(str(c).strip().upper() for c in (question.get("correct_options") or []))
        if not correct_set:
            return 0, 0
        user_set = set()
        if isinstance(user_answer, list):
            user_set = set(str(a).strip().upper() for a in user_answer)
        earned = len(correct_set & user_set)
        return earned, len(correct_set)

    # TRUE/FALSE/NOT GIVEN & YES/NO/NOT GIVEN
    if q_type in {QuestionType.TRUE_FALSE_NOT_GIVEN, QuestionType.YES_NO_NOT_GIVEN}:
        items = question.get("tfng_items") or []
        if not items:
            return 0, 0
        answers_map = {}
        if isinstance(user_answer, dict):
            answers_map = {str(k): str(v).strip().upper() for k, v in user_answer.items()}
        elif isinstance(user_answer, list):
            answers_map = {str(i): str(v).strip().upper() for i, v in enumerate(user_answer)}
        earned = 0
        for i, item in enumerate(items):
            correct = str(item.get("answer", "")).strip().upper()
            given = answers_map.get(str(i), "")
            if given == correct:
                earned += 1
        return earned, len(items)

    # Form completion
    if q_type == QuestionType.FORM_COMPLETION:
        items = question.get("form_fields") or []
        return _score_fill_items(items, "answer", user_answer)

    # Table completion
    if q_type == QuestionType.TABLE_COMPLETION:
        items = question.get("table_cells") or []
        return _score_fill_items(items, "answer", user_answer)

    # Flow chart completion
    if q_type == QuestionType.FLOW_CHART_COMPLETION:
        items = question.get("flow_steps") or []
        blank_items = [s for s in items if s.get("is_blank")]
        return _score_fill_items(blank_items, "answer", user_answer)

    # Sentence / note completion
    if q_type in {QuestionType.SENTENCE_COMPLETION, QuestionType.NOTE_COMPLETION}:
        items = question.get("sentences") or []
        return _score_fill_items(items, "answer", user_answer)

    # Summary completion
    if q_type == QuestionType.SUMMARY_COMPLETION:
        items = question.get("summary_items") or []
        return _score_fill_items(items, "answer", user_answer)

    # Short answer
    if q_type == QuestionType.SHORT_ANSWER:
        items = question.get("short_items") or []
        return _score_fill_items(items, "answer", user_answer)

    # Map / plan / diagram labelling
    if q_type in {QuestionType.MAP_LABELLING, QuestionType.PLAN_LABELLING, QuestionType.DIAGRAM_LABELLING}:
        items = question.get("map_slots") or []
        return _score_fill_items(items, "answer", user_answer)

    # Matching / matching features
    if q_type in {QuestionType.MATCHING, QuestionType.MATCHING_FEATURES, QuestionType.MATCHING_INFORMATION}:
        items = question.get("matching_items") or []
        return _score_fill_items(items, "answer", user_answer)

    # Matching headings
    if q_type == QuestionType.MATCHING_HEADINGS:
        items = question.get("heading_items") or []
        return _score_fill_items(items, "answer", user_answer)

    # Pick from list
    if q_type == QuestionType.PICK_FROM_LIST:
        items = question.get("pick_items") or []
        if not items:
            return 0, 0
        earned = 0
        total = 0
        answers_map = {}
        if isinstance(user_answer, dict):
            answers_map = user_answer
        elif isinstance(user_answer, list):
            answers_map = {str(i): v for i, v in enumerate(user_answer)}
        for i, item in enumerate(items):
            correct_set = set(str(a).strip().upper() for a in (item.get("answers") or []))
            total += len(correct_set)
            user_val = answers_map.get(str(i), [])
            if isinstance(user_val, list):
                user_set = set(str(a).strip().upper() for a in user_val)
            else:
                user_set = {str(user_val).strip().upper()}
            earned += len(correct_set & user_set)
        return earned, total

    # Writing & speaking are not auto-scored (return 0/0 — scored by examiner)
    return 0, 0


def _score_fill_items(items: list, answer_key: str, user_answer: Any) -> tuple[int, int]:
    """Score fill-in-the-blank style items."""
    if not items:
        return 0, 0
    answers_map: Dict[str, str] = {}
    if isinstance(user_answer, dict):
        answers_map = {str(k): str(v).strip().lower() for k, v in user_answer.items()}
    elif isinstance(user_answer, list):
        answers_map = {str(i): str(v).strip().lower() for i, v in enumerate(user_answer)}
    earned = 0
    for i, item in enumerate(items):
        correct = str(item.get(answer_key, "")).strip().lower()
        given = answers_map.get(str(i), "")
        if given == correct:
            earned += 1
    return earned, len(items)


# ─────────────────────────────────────────────
# Service class
# ─────────────────────────────────────────────

class IeltsService:
    def __init__(
        self,
        question_repo: QuestionRepository,
        test_repo: TestRepository,
        session_repo: TestSessionRepository,
    ):
        self.question_repo = question_repo
        self.test_repo = test_repo
        self.session_repo = session_repo

    # ── Question CRUD ─────────────────────────

    async def create_question(self, payload: QuestionCreate) -> QuestionOut:
        data = payload.model_dump()
        data["updated_at"] = datetime.now(timezone.utc)
        doc = await self.question_repo.create(data)
        return QuestionOut(**doc)

    async def get_question(self, question_id: str) -> QuestionOut:
        doc = await self.question_repo.find_by_id(question_id)
        if not doc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Question not found")
        return QuestionOut(**doc)

    async def list_questions(
        self,
        page: int = 1,
        page_size: int = 20,
        section: Optional[str] = None,
        section_part: Optional[str] = None,
        question_type: Optional[str] = None,
        module_type: Optional[str] = None,
        search: Optional[str] = None,
    ) -> PaginatedResponse:
        skip = (page - 1) * page_size
        # Build filter
        query: Dict[str, Any] = {}
        if section:
            query["section"] = section
        if section_part:
            query["section_part"] = section_part
        if question_type:
            query["type"] = question_type
        if module_type:
            query["module_type"] = module_type

        # Use repository for filtered query
        total = await self._count_questions(query)
        docs = await self._find_questions(query, skip, page_size, search)
        total_pages = max(1, (total + page_size - 1) // page_size)
        return PaginatedResponse(
            items=[QuestionOut(**d) for d in docs],
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
        )

    async def _count_questions(self, query: dict) -> int:
        return await self.question_repo.col.count_documents(query)

    async def _find_questions(self, query: dict, skip: int, limit: int, search: Optional[str] = None) -> List[dict]:
        if search:
            query["$or"] = [
                {"title": {"$regex": search, "$options": "i"}},
                {"instruction": {"$regex": search, "$options": "i"}},
                {"tags": {"$regex": search, "$options": "i"}},
            ]
        cursor = self.question_repo.col.find(query).sort("created_at", -1).skip(skip).limit(limit)
        from src.database.repositories.ielts_repository import _serialize
        return [_serialize(d) async for d in cursor]

    async def update_question(self, question_id: str, payload: QuestionUpdate) -> QuestionOut:
        existing = await self.question_repo.find_by_id(question_id)
        if not existing:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Question not found")
        update_data = payload.model_dump(exclude_unset=True)
        if not update_data:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No fields to update")
        doc = await self.question_repo.update(question_id, update_data)
        if not doc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Question not found after update")
        return QuestionOut(**doc)  # type: ignore[arg-type]

    async def delete_question(self, question_id: str) -> dict:
        existing = await self.question_repo.find_by_id(question_id)
        if not existing:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Question not found")
        deleted = await self.question_repo.delete(question_id)
        if not deleted:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to delete question")
        return {"status": "deleted", "question_id": question_id}

    async def bulk_create_questions(self, questions: List[QuestionCreate]) -> List[QuestionOut]:
        results = []
        for q in questions:
            doc = await self.create_question(q)
            results.append(doc)
        return results

    # ── Test CRUD ─────────────────────────────

    async def create_test(self, payload: TestCreate) -> TestOut:
        # Validate all referenced question IDs exist
        all_qids = []
        for sec in payload.sections:
            all_qids.extend(sec.question_ids)

        unique_qids = list(set(all_qids))
        existing_questions = await self.question_repo.find_many(unique_qids)
        existing_ids = {q["id"] for q in existing_questions}
        missing = [qid for qid in unique_qids if qid not in existing_ids]
        if missing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Question IDs not found: {missing}",
            )

        data = payload.model_dump()
        data["question_count"] = len(all_qids)
        data["updated_at"] = datetime.now(timezone.utc)
        doc = await self.test_repo.create(data)
        return TestOut(**doc)

    async def get_test(self, test_id: str) -> TestOut:
        doc = await self.test_repo.find_by_id(test_id)
        if not doc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test not found")
        return TestOut(**doc)

    async def list_tests(
        self,
        page: int = 1,
        page_size: int = 20,
        test_type: Optional[str] = None,
        published_only: bool = False,
    ) -> PaginatedResponse:
        skip = (page - 1) * page_size
        query: Dict[str, Any] = {}
        if test_type:
            query["test_type"] = test_type
        if published_only:
            query["is_published"] = True
        total = await self.test_repo.col.count_documents(query)
        cursor = self.test_repo.col.find(query).sort("created_at", -1).skip(skip).limit(page_size)
        from src.database.repositories.ielts_repository import _serialize
        docs = [_serialize(d) async for d in cursor]
        total_pages = max(1, (total + page_size - 1) // page_size)
        return PaginatedResponse(
            items=[TestOut(**d) for d in docs],
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
        )

    async def update_test(self, test_id: str, payload: TestUpdate) -> TestOut:
        existing = await self.test_repo.find_by_id(test_id)
        if not existing:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test not found")
        update_data = payload.model_dump(exclude_unset=True)
        if not update_data:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No fields to update")

        # Re-validate question IDs if sections changed
        if "sections" in update_data:
            all_qids = []
            for sec in update_data["sections"]:
                all_qids.extend(sec["question_ids"])
            unique_qids = list(set(all_qids))
            existing_questions = await self.question_repo.find_many(unique_qids)
            existing_ids = {q["id"] for q in existing_questions}
            missing = [qid for qid in unique_qids if qid not in existing_ids]
            if missing:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Question IDs not found: {missing}",
                )
            update_data["question_count"] = len(all_qids)

        doc = await self.test_repo.update(test_id, update_data)
        if not doc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test not found after update")
        return TestOut(**doc)  # type: ignore[arg-type]

    async def delete_test(self, test_id: str) -> dict:
        existing = await self.test_repo.find_by_id(test_id)
        if not existing:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test not found")
        deleted = await self.test_repo.delete(test_id)
        if not deleted:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to delete test")
        return {"status": "deleted", "test_id": test_id}

    async def publish_test(self, test_id: str, is_published: bool) -> TestOut:
        existing = await self.test_repo.find_by_id(test_id)
        if not existing:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test not found")
        doc = await self.test_repo.update(test_id, {"is_published": is_published})
        if not doc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test not found after update")
        return TestOut(**doc)  # type: ignore[arg-type]

    # ── Test-taking (sessions) ────────────────

    async def start_test(self, user_id: str, test_id: str) -> TestSessionOut:
        test = await self.test_repo.find_by_id(test_id)
        if not test:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test not found")
        if not test.get("is_published"):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Test is not published yet")

        # Check for existing in-progress session
        existing = await self.session_repo.col.find_one({
            "user_id": user_id,
            "test_id": test_id,
            "status": TestStatus.IN_PROGRESS.value,
        })
        if existing:
            from src.database.repositories.ielts_repository import _serialize
            return TestSessionOut(**_serialize(existing))

        session_data = {
            "test_id": test_id,
            "user_id": user_id,
            "test_type": test.get("test_type", "ielts"),
            "module_type": test.get("module_type", "academic"),
            "status": TestStatus.IN_PROGRESS.value,
            "answers": {},
            "section_scores": [],
            "overall_band": None,
            "started_at": datetime.now(timezone.utc),
            "finished_at": None,
            "time_spent_seconds": None,
            "updated_at": datetime.now(timezone.utc),
        }
        doc = await self.session_repo.create(session_data)
        return TestSessionOut(**doc)

    async def get_test_questions_for_session(self, user_id: str, session_id: str) -> dict:
        """Return test questions with answers stripped for the candidate."""
        session = await self._get_session_or_fail(session_id)
        if session["user_id"] != user_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not your session")
        if session["status"] not in {TestStatus.IN_PROGRESS.value, TestStatus.NOT_STARTED.value}:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Session is not in progress")

        test = await self.test_repo.find_by_id(session["test_id"])
        if not test:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test not found")

        sections_out = []
        for section_cfg in test.get("sections", []):
            q_ids = section_cfg.get("question_ids", [])
            questions = await self.question_repo.find_many(q_ids)
            safe_questions = [_strip_answers(q) for q in questions]
            sections_out.append({
                "section": section_cfg["section"],
                "section_part": section_cfg["section_part"],
                "time_limit_minutes": section_cfg.get("time_limit_minutes"),
                "questions": safe_questions,
            })

        return {
            "session_id": session_id,
            "test_id": session["test_id"],
            "test_title": test.get("title"),
            "time_limit_minutes": test.get("time_limit_minutes"),
            "sections": sections_out,
        }

    async def submit_answers(self, user_id: str, session_id: str, sections: List[SectionAnswers]) -> TestSessionOut:
        session = await self._get_session_or_fail(session_id)
        if session["user_id"] != user_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not your session")
        if session["status"] != TestStatus.IN_PROGRESS.value:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Session is not in progress")

        existing_answers = session.get("answers", {})
        for section in sections:
            section_key = section.section.value
            if section_key not in existing_answers:
                existing_answers[section_key] = {}
            for ans in section.answers:
                existing_answers[section_key][ans.question_id] = ans.answer

        doc = await self.session_repo.update(session_id, {"answers": existing_answers})
        return TestSessionOut(**doc)

    async def finalize_session(self, user_id: str, session_id: str) -> SessionResult:
        session = await self._get_session_or_fail(session_id)
        if session["user_id"] != user_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not your session")
        if session["status"] != TestStatus.IN_PROGRESS.value:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Session is not in progress")

        test = await self.test_repo.find_by_id(session["test_id"])
        if not test:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test not found")

        answers = session.get("answers", {})
        is_academic = test.get("module_type", "academic") == ModuleType.ACADEMIC.value

        section_scores: List[SectionScore] = []
        band_by_section: Dict[str, float] = {}

        for section_cfg in test.get("sections", []):
            section_type = section_cfg["section"]
            q_ids = section_cfg.get("question_ids", [])
            questions = await self.question_repo.find_many(q_ids)

            section_answers = answers.get(section_type, {})
            total_earned = 0
            total_max = 0

            for q in questions:
                user_ans = section_answers.get(q["id"])
                earned, max_pts = _score_question(q, user_ans)
                total_earned += earned
                total_max += max_pts

            # Calculate band score
            if section_type == SectionType.LISTENING.value:
                band = raw_to_band_listening(total_earned)
            elif section_type == SectionType.READING.value:
                band = raw_to_band_reading(total_earned, is_academic=is_academic)
            elif section_type in {SectionType.WRITING.value, SectionType.SPEAKING.value}:
                # Writing & speaking: not auto-scored — placeholder band 0
                band = 0.0
            else:
                band = 0.0

            band_by_section[section_type] = band
            section_scores.append(SectionScore(
                section=SectionType(section_type),
                raw_score=total_earned,
                max_score=total_max,
                band_score=band,
            ))

        # Overall band (if all 4 sections present)
        overall = None
        if all(s.value in band_by_section for s in [SectionType.LISTENING, SectionType.READING, SectionType.WRITING, SectionType.SPEAKING]):
            overall = calculate_overall_band(
                band_by_section[SectionType.LISTENING.value],
                band_by_section[SectionType.READING.value],
                band_by_section[SectionType.WRITING.value],
                band_by_section[SectionType.SPEAKING.value],
            )

        finished_at = datetime.now(timezone.utc)
        started_at = session.get("started_at") or session.get("created_at")
        time_spent = None
        if started_at:
            if isinstance(started_at, str):
                started_at = datetime.fromisoformat(started_at)
            time_spent = int((finished_at - started_at).total_seconds())

        update_data = {
            "status": TestStatus.SUBMITTED.value,
            "section_scores": [s.model_dump() for s in section_scores],
            "overall_band": overall,
            "finished_at": finished_at,
            "time_spent_seconds": time_spent,
        }
        await self.session_repo.update(session_id, update_data)

        return SessionResult(
            session_id=session_id,
            test_id=session["test_id"],
            user_id=user_id,
            status=TestStatus.SUBMITTED,
            section_scores=section_scores,
            overall_band=overall,
            started_at=started_at or finished_at,
            finished_at=finished_at,
            time_spent_seconds=time_spent,
        )

    async def get_session(self, session_id: str, user_id: Optional[str] = None) -> TestSessionOut:
        session = await self._get_session_or_fail(session_id)
        if user_id and session["user_id"] != user_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not your session")
        return TestSessionOut(**session)

    async def list_user_sessions(
        self,
        user_id: str,
        page: int = 1,
        page_size: int = 20,
        test_type: Optional[str] = None,
    ) -> PaginatedResponse:
        skip = (page - 1) * page_size
        total = await self.session_repo.count_by_user(user_id, test_type=test_type)
        docs = await self.session_repo.find_by_user(user_id, skip=skip, limit=page_size, test_type=test_type)
        total_pages = max(1, (total + page_size - 1) // page_size)
        return PaginatedResponse(
            items=[TestSessionOut(**d) for d in docs],
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
        )

    async def get_session_result(self, session_id: str, user_id: Optional[str] = None) -> SessionResult:
        session = await self._get_session_or_fail(session_id)
        if user_id and session["user_id"] != user_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not your session")
        if session["status"] not in {TestStatus.SUBMITTED.value, TestStatus.GRADED.value}:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Session not yet submitted or graded")

        section_scores = [SectionScore(**s) for s in (session.get("section_scores") or [])]
        sa: datetime = session.get("started_at") or session.get("created_at") or datetime.now(timezone.utc)
        return SessionResult(
            session_id=session_id,
            test_id=session["test_id"],
            user_id=session["user_id"],
            status=TestStatus(session["status"]),
            section_scores=section_scores,
            overall_band=session.get("overall_band"),
            started_at=sa,
            finished_at=session.get("finished_at"),
            time_spent_seconds=session.get("time_spent_seconds"),
        )

    # ── Admin helpers ─────────────────────────

    async def list_all_sessions(
        self,
        page: int = 1,
        page_size: int = 20,
        status_filter: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> PaginatedResponse:
        skip = (page - 1) * page_size
        query: Dict[str, Any] = {}
        if status_filter:
            query["status"] = status_filter
        if user_id:
            query["user_id"] = user_id
        total = await self.session_repo.col.count_documents(query)
        cursor = self.session_repo.col.find(query).sort("created_at", -1).skip(skip).limit(page_size)
        from src.database.repositories.ielts_repository import _serialize
        docs = [_serialize(d) async for d in cursor]
        total_pages = max(1, (total + page_size - 1) // page_size)
        return PaginatedResponse(
            items=[TestSessionOut(**d) for d in docs],
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
        )

    async def grade_writing_speaking(
        self,
        session_id: str,
        section: str,
        band_score: float,
        details: Optional[Dict[str, Any]] = None,
    ) -> TestSessionOut:
        """Examiner manually sets band score for writing/speaking."""
        session = await self._get_session_or_fail(session_id)
        if session["status"] not in {TestStatus.SUBMITTED.value, TestStatus.GRADED.value}:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Session must be submitted first")

        section_scores = session.get("section_scores", [])
        updated = False
        for sc in section_scores:
            if sc.get("section") == section:
                sc["band_score"] = band_score
                if details:
                    sc["details"] = details
                updated = True
                break

        if not updated:
            section_scores.append({
                "section": section,
                "raw_score": 0,
                "max_score": 0,
                "band_score": band_score,
                "details": details,
            })

        # Recalculate overall
        band_map = {sc["section"]: sc["band_score"] for sc in section_scores}
        overall = None
        if all(s.value in band_map for s in [SectionType.LISTENING, SectionType.READING, SectionType.WRITING, SectionType.SPEAKING]):
            overall = calculate_overall_band(
                band_map[SectionType.LISTENING.value],
                band_map[SectionType.READING.value],
                band_map[SectionType.WRITING.value],
                band_map[SectionType.SPEAKING.value],
            )

        update_data: Dict[str, Any] = {
            "section_scores": section_scores,
            "overall_band": overall,
            "status": TestStatus.GRADED.value,
        }
        doc = await self.session_repo.update(session_id, update_data)
        return TestSessionOut(**doc)

    async def get_dashboard_stats(self, user_repo) -> dict:
        """Aggregate stats for admin dashboard."""
        from src.database.repositories.ielts_repository import _serialize

        total_questions = await self.question_repo.col.count_documents({})
        total_tests = await self.test_repo.col.count_documents({})
        total_sessions = await self.session_repo.col.count_documents({})
        active_sessions = await self.session_repo.col.count_documents({"status": TestStatus.IN_PROGRESS.value})
        completed_sessions = await self.session_repo.col.count_documents({
            "status": {"$in": [TestStatus.SUBMITTED.value, TestStatus.GRADED.value]}
        })

        # Total users
        total_users = await user_repo.collection.count_documents({})

        # Users by role
        pipeline_roles = [
            {"$group": {"_id": "$role", "count": {"$sum": 1}}}
        ]
        users_by_role = {}
        async for doc in user_repo.collection.aggregate(pipeline_roles):
            users_by_role[doc["_id"] or "candidate"] = doc["count"]

        # Sessions by status
        pipeline_status = [
            {"$group": {"_id": "$status", "count": {"$sum": 1}}}
        ]
        sessions_by_status = {}
        async for doc in self.session_repo.col.aggregate(pipeline_status):
            sessions_by_status[doc["_id"] or "unknown"] = doc["count"]

        # Average band
        pipeline_avg = [
            {"$match": {"overall_band": {"$ne": None}}},
            {"$group": {"_id": None, "avg_band": {"$avg": "$overall_band"}}},
        ]
        avg_band = None
        async for doc in self.session_repo.col.aggregate(pipeline_avg):
            avg_band = round(doc["avg_band"] * 2) / 2 if doc.get("avg_band") else None

        return {
            "total_users": total_users,
            "total_questions": total_questions,
            "total_tests": total_tests,
            "total_sessions": total_sessions,
            "active_sessions": active_sessions,
            "completed_sessions": completed_sessions,
            "average_band": avg_band,
            "users_by_role": users_by_role,
            "sessions_by_status": sessions_by_status,
        }

    # ── Private helpers ───────────────────────

    async def _get_session_or_fail(self, session_id: str) -> dict:
        doc = await self.session_repo.find_by_id(session_id)
        if not doc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session not found")
        return doc
