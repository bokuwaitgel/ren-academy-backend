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
    SectionStatus,
    SessionMode,
    TestStatus,
    WritingQuestionType,
    SpeakingQuestionType,
)
from schemas.ielts import (
    AnswerSubmission,
    IELTS_SECTION_ORDER,
    IELTS_SECTION_TIME_SECONDS,
    PaginatedResponse,
    QuestionCreate,
    QuestionOut,
    QuestionSafe,
    QuestionUpdate,
    SectionAnswers,
    SectionScore,
    SessionResult,
    SubmitSectionRequest,
    TestCreate,
    TestOut,
    TestSummary,
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


def _calculate_speaking_score_from_answers(speaking_answers: dict) -> tuple[float | None, dict | None]:
    """
    Extract all AI evaluations stored in answers["speaking"] and compute band + details.
    Returns (band_score, details_dict) or (None, None) if nothing evaluatable.
    """
    all_results = []
    for qid, answer in (speaking_answers or {}).items():
        part_number = answer.get("part", 0)
        # Part 2: single evaluation at top level
        if part_number == 2:
            evaluation = answer.get("evaluation")
            if evaluation and isinstance(evaluation, dict) and "error" not in evaluation:
                all_results.append({
                    "question_id": qid,
                    "part_number": part_number,
                    "audio_url": answer.get("audio_url", ""),
                    "evaluation": evaluation,
                })
        else:
            # Part 1 / Part 3: responses list
            for resp in answer.get("responses", []):
                evaluation = resp.get("evaluation")
                if evaluation and isinstance(evaluation, dict) and "error" not in evaluation:
                    all_results.append({
                        "question_id": qid,
                        "part_number": part_number,
                        "question": resp.get("question", ""),
                        "audio_url": resp.get("audio_url", ""),
                        "evaluation": evaluation,
                    })

    if not all_results:
        return None, None

    def _round_half_local(v: float) -> float:
        return round(v * 2) / 2

    def _avg_rounded(vals: list) -> float | None:
        clean = [x for x in vals if x is not None]
        return _round_half_local(sum(clean) / len(clean)) if clean else None

    valid_evals = [r["evaluation"] for r in all_results]
    fluency = _avg_rounded([e.get("fluency_coherence") for e in valid_evals])
    lexical = _avg_rounded([e.get("lexical_resource") for e in valid_evals])
    grammar = _avg_rounded([e.get("grammar_accuracy") for e in valid_evals])
    pronun  = _avg_rounded([e.get("pronunciation") for e in valid_evals])

    criteria_vals = [v for v in [fluency, lexical, grammar, pronun] if v is not None]
    if criteria_vals:
        overall = _avg_rounded(criteria_vals)
    else:
        overall = _avg_rounded([e.get("overall_score") for e in valid_evals])

    details = {
        "band_score": overall,
        "criteria": {
            "fluency_coherence": fluency,
            "lexical_resource": lexical,
            "grammar_accuracy": grammar,
            "pronunciation": pronun,
        },
        "answer_count": len(all_results),
        "answer_details": all_results,
    }
    return overall, details


def _extract_correct_answer(q: dict) -> Any:
    """Extract the displayable correct answer from a question."""
    q_type = q.get("type", "")
    if q_type == QuestionType.MULTIPLE_CHOICE:
        return q.get("correct_option")
    if q_type == QuestionType.MULTIPLE_SELECT:
        return q.get("correct_options")
    # Item-based types: return {index: answer} mapping
    for field in [
        "tfng_items", "form_fields", "table_cells", "flow_steps",
        "sentences", "summary_items", "short_items", "map_slots",
        "matching_items", "heading_items",
    ]:
        items = q.get(field) or []
        if items:
            return {str(i): item.get("answer") for i, item in enumerate(items) if item.get("answer") is not None}
    return None


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
# Module structure helpers
# ─────────────────────────────────────────────

def _get_available_sections(test: dict) -> List[str]:
    """Returns which of the 4 IELTS sections are present in the test, in standard order."""
    return [s for s in IELTS_SECTION_ORDER if test.get(s)]


def _get_question_ids_for_section(test: dict, section: str) -> List[str]:
    """Returns all question IDs for a given section from the module structure."""
    ids: List[str] = []
    if section == "listening":
        for sec in (test.get("listening") or {}).get("sections", []):
            ids.extend(sec.get("question_ids", []))
    elif section == "reading":
        for sec in (test.get("reading") or {}).get("sections", []):
            ids.extend(sec.get("question_ids", []))
    elif section == "speaking":
        for part in (test.get("speaking") or {}).get("parts", []):
            ids.extend(part.get("question_ids", []))
    return ids


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
        all_qids: List[str] = []
        if payload.listening:
            for sec in payload.listening.sections:
                all_qids.extend(sec.question_ids)
        if payload.reading:
            for sec in payload.reading.sections:
                all_qids.extend(sec.question_ids)
        if payload.speaking:
            for part in payload.speaking.parts:
                all_qids.extend(part.question_ids)

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
            items=[TestSummary(
                **d,
                has_listening=bool(d.get("listening")),
                has_reading=bool(d.get("reading")),
                has_writing=bool(d.get("writing")),
                has_speaking=bool(d.get("speaking")),
            ) for d in docs],
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

        # Re-validate question IDs if any module changed
        all_qids: List[str] = []
        for mod_key, sub_key in [("listening", "sections"), ("reading", "sections"), ("speaking", "parts")]:
            module = update_data.get(mod_key)
            if module is None:
                continue
            for item in module.get(sub_key, []):
                all_qids.extend(item.get("question_ids", []))
        if all_qids:
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

    async def start_test(
        self,
        user_id: str,
        test_id: str,
        mode: str = SessionMode.FULL_TEST.value,
        section: Optional[str] = None,
    ) -> TestSessionOut:
        test = await self.test_repo.find_by_id(test_id)
        if not test:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test not found")
        if not test.get("is_published"):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Test is not published yet")

        test_section_values = _get_available_sections(test)

        if mode == SessionMode.PRACTICE.value:
            if not section:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="'section' is required for practice mode",
                )
            if section not in test_section_values:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Section '{section}' not found in this test",
                )

        # Return existing in-progress session if one exists for this user/test/mode
        existing = await self.session_repo.find_active(
            user_id=user_id,
            test_id=test_id,
            mode=mode,
            practice_section=section if mode == SessionMode.PRACTICE.value else None,
        )
        if existing:
            return TestSessionOut(**existing)

        # Build session_sections based on mode
        if mode == SessionMode.FULL_TEST.value:
            # All sections present in the test, ordered by IELTS standard order
            session_sections = []
            order_index = 0
            for sec_val in IELTS_SECTION_ORDER:
                if sec_val not in test_section_values:
                    continue
                time_limit_secs = IELTS_SECTION_TIME_SECONDS.get(sec_val)
                session_sections.append({
                    "section": sec_val,
                    "order_index": order_index,
                    "status": SectionStatus.NOT_STARTED.value,
                    "time_limit_seconds": time_limit_secs,
                    "started_at": None,
                    "completed_at": None,
                    "time_spent_seconds": None,
                })
                order_index += 1
            current_section = session_sections[0]["section"] if session_sections else None
        else:
            # PRACTICE — single section only
            time_limit_secs = IELTS_SECTION_TIME_SECONDS.get(section) if section else None
            session_sections = [{
                "section": section,
                "order_index": 0,
                "status": SectionStatus.NOT_STARTED.value,
                "time_limit_seconds": time_limit_secs,
                "started_at": None,
                "completed_at": None,
                "time_spent_seconds": None,
            }]
            current_section = section

        session_data: Dict[str, Any] = {
            "test_id": test_id,
            "user_id": user_id,
            "test_type": test.get("test_type", "ielts"),
            "module_type": test.get("module_type", "academic"),
            "mode": mode,
            "practice_section": section if mode == SessionMode.PRACTICE.value else None,
            "status": TestStatus.IN_PROGRESS.value,
            "current_section": current_section,
            "session_sections": session_sections,
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

    async def start_section(self, user_id: str, session_id: str) -> TestSessionOut:
        """
        Mark the current section as IN_PROGRESS (starts the timer).
        For FULL_TEST only — PRACTICE sessions auto-start on question fetch.
        """
        session = await self._get_session_or_fail(session_id)
        if session["user_id"] != user_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not your session")
        if session["status"] != TestStatus.IN_PROGRESS.value:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Session is not in progress")

        current_section = session.get("current_section")
        if not current_section:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="All sections are completed")

        session_sections = session.get("session_sections", [])
        updated = False
        for sec in session_sections:
            if sec["section"] == current_section:
                if sec["status"] == SectionStatus.IN_PROGRESS.value:
                    # Already started — idempotent, just return
                    return TestSessionOut(**session)
                if sec["status"] != SectionStatus.NOT_STARTED.value:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Section '{current_section}' is already {sec['status']}",
                    )
                sec["status"] = SectionStatus.IN_PROGRESS.value
                sec["started_at"] = datetime.now(timezone.utc)
                updated = True
                break

        if not updated:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Current section not found in session")

        doc = await self.session_repo.update(session_id, {"session_sections": session_sections})
        return TestSessionOut(**doc)

    async def submit_section_answers(
        self,
        user_id: str,
        session_id: str,
        section: str,
        answers: List[AnswerSubmission],
    ) -> TestSessionOut:
        """
        Save answers for the given section, mark it COMPLETED, and advance
        current_section to the next NOT_STARTED section (FULL_TEST) or None (PRACTICE).
        """
        session = await self._get_session_or_fail(session_id)
        if session["user_id"] != user_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not your session")
        if session["status"] != TestStatus.IN_PROGRESS.value:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Session is not in progress")

        current_section = session.get("current_section")
        if section != current_section:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Cannot submit '{section}' — current section is '{current_section}'",
            )

        # Save answers
        existing_answers: Dict[str, Any] = session.get("answers", {})
        if section not in existing_answers:
            existing_answers[section] = {}
        for ans in answers:
            existing_answers[section][ans.question_id] = ans.answer

        # Mark section COMPLETED and record timing
        now = datetime.now(timezone.utc)
        session_sections = session.get("session_sections", [])
        for sec in session_sections:
            if sec["section"] == section:
                # Auto-start if not already started (covers PRACTICE flow)
                if sec["status"] == SectionStatus.NOT_STARTED.value:
                    sec["started_at"] = now
                sec["status"] = SectionStatus.COMPLETED.value
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
        next_section: Optional[str] = None
        for sec in session_sections:
            if sec["status"] == SectionStatus.NOT_STARTED.value:
                next_section = sec["section"]
                break

        update_data: Dict[str, Any] = {
            "answers": existing_answers,
            "session_sections": session_sections,
            "current_section": next_section,
        }

        # For listening/reading: score immediately on submit and save per-question details
        if section in {SectionType.LISTENING.value, SectionType.READING.value}:
            test = await self.test_repo.find_by_id(session["test_id"])
            if test:
                is_academic = test.get("module_type", "academic") == ModuleType.ACADEMIC.value
                q_ids = _get_question_ids_for_section(test, section)
                questions = await self.question_repo.find_many(q_ids) if q_ids else []
                section_answers = existing_answers.get(section, {})

                total_earned = 0
                total_max = 0
                answer_details: List[Dict[str, Any]] = []
                for q in questions:
                    user_ans = section_answers.get(q["id"])
                    earned, max_pts = _score_question(q, user_ans)
                    total_earned += earned
                    total_max += max_pts
                    answer_details.append({
                        "question_id": q["id"],
                        "title": q.get("title", ""),
                        "type": q.get("type", ""),
                        "user_answer": user_ans,
                        "correct_answer": _extract_correct_answer(q),
                        "is_correct": earned == max_pts and max_pts > 0,
                        "earned": earned,
                        "max": max_pts,
                    })

                if section == SectionType.LISTENING.value:
                    band = raw_to_band_listening(total_earned)
                else:
                    band = raw_to_band_reading(total_earned, is_academic=is_academic)

                section_score = {
                    "section": section,
                    "raw_score": total_earned,
                    "max_score": total_max,
                    "band_score": band,
                    "details": {"answer_details": answer_details},
                }

                existing_scores: List[Dict[str, Any]] = session.get("section_scores") or []
                existing_scores = [s for s in existing_scores if s.get("section") != section]
                existing_scores.append(section_score)
                update_data["section_scores"] = existing_scores

        doc = await self.session_repo.update(session_id, update_data)
        return TestSessionOut(**doc)

    async def get_test_questions_for_session(self, user_id: str, session_id: str) -> dict:
        """
        Return questions for the current section (answers stripped).
        - FULL_TEST: returns only the current section's questions.
        - PRACTICE: returns the single section's questions.
        Also auto-starts the section timer if it's still NOT_STARTED.
        """
        session = await self._get_session_or_fail(session_id)
        if session["user_id"] != user_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not your session")
        if session["status"] not in {TestStatus.IN_PROGRESS.value, TestStatus.NOT_STARTED.value}:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Session is not in progress")

        current_section = session.get("current_section")
        if not current_section:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="All sections are already completed")

        test = await self.test_repo.find_by_id(session["test_id"])
        if not test:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test not found")

        # Auto-start timer for the current section if NOT_STARTED
        session_sections = session.get("session_sections", [])
        for sec in session_sections:
            if sec["section"] == current_section and sec["status"] == SectionStatus.NOT_STARTED.value:
                sec["status"] = SectionStatus.IN_PROGRESS.value
                sec["started_at"] = datetime.now(timezone.utc)
                await self.session_repo.update(session_id, {"session_sections": session_sections})
                break

        # Build section output with sub-section detail based on module structure
        time_limit_secs = next(
            (s.get("time_limit_seconds") for s in session_sections if s["section"] == current_section),
            None,
        )
        section_out: dict = {"section": current_section, "time_limit_seconds": time_limit_secs}

        if current_section == SectionType.LISTENING.value:
            sub_sections = []
            for sec in (test.get("listening") or {}).get("sections", []):
                q_ids = sec.get("question_ids", [])
                questions = await self.question_repo.find_many(q_ids)
                sub_sections.append({
                    "section_number": sec["section_number"],
                    "audio_url": sec.get("audio_url"),
                    "questions": [_strip_answers(q) for q in questions],
                })
            section_out["sub_sections"] = sub_sections

        elif current_section == SectionType.READING.value:
            sub_sections = []
            for sec in (test.get("reading") or {}).get("sections", []):
                q_ids = sec.get("question_ids", [])
                questions = await self.question_repo.find_many(q_ids)
                sub_sections.append({
                    "section_number": sec["section_number"],
                    "passage": sec.get("passage"),
                    "questions": [_strip_answers(q) for q in questions],
                })
            section_out["sub_sections"] = sub_sections

        elif current_section == SectionType.WRITING.value:
            tasks = []
            for task in (test.get("writing") or {}).get("tasks", []):
                tasks.append({
                    "task_number": task["task_number"],
                    "answer_key": f"task_{task['task_number']}",
                    "description": task.get("description"),
                    "image_url": task.get("image_url"),
                })
            section_out["tasks"] = tasks

        elif current_section == SectionType.SPEAKING.value:
            parts = []
            for part in (test.get("speaking") or {}).get("parts", []):
                q_ids = part.get("question_ids", [])
                questions = await self.question_repo.find_many(q_ids)
                parts.append({
                    "part_number": part["part_number"],
                    "questions": [_strip_answers(q) for q in questions],
                })
            section_out["parts"] = parts

        return {
            "session_id": session_id,
            "test_id": session["test_id"],
            "test_title": test.get("title"),
            "mode": session.get("mode", SessionMode.FULL_TEST.value),
            "current_section": current_section,
            "section": section_out,
        }

    async def get_section_across_tests(self, section: str, strip_answers: bool = True, published_only: bool = True) -> dict:
        """Return the given section's content from every test that has it."""
        valid_sections = {s.value for s in SectionType}
        if section not in valid_sections:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid section '{section}'. Must be one of: {', '.join(sorted(valid_sections))}",
            )

        query: Dict[str, Any] = {section: {"$ne": None}}
        if published_only:
            query["is_published"] = True

        from src.database.repositories.ielts_repository import _serialize
        cursor = self.test_repo.col.find(query).sort("created_at", -1)
        results = []

        async for doc in cursor:
            test = _serialize(doc)
            test_id = test["id"]
            entry: Dict[str, Any] = {
                "test_id": test_id,
                "test_title": test.get("title"),
                "module_type": test.get("module_type"),
            }

            if section == SectionType.LISTENING.value:
                sub_sections = []
                for sec in (test.get("listening") or {}).get("sections", []):
                    questions = await self.question_repo.find_many(sec.get("question_ids", []))
                    sub_sections.append({
                        "section_number": sec["section_number"],
                        "audio_url": sec.get("audio_url"),
                        "questions": [_strip_answers(q) if strip_answers else q for q in questions],
                    })
                entry["sub_sections"] = sub_sections

            elif section == SectionType.READING.value:
                sub_sections = []
                for sec in (test.get("reading") or {}).get("sections", []):
                    questions = await self.question_repo.find_many(sec.get("question_ids", []))
                    sub_sections.append({
                        "section_number": sec["section_number"],
                        "passage": sec.get("passage"),
                        "questions": [_strip_answers(q) if strip_answers else q for q in questions],
                    })
                entry["sub_sections"] = sub_sections

            elif section == SectionType.WRITING.value:
                tasks = []
                for task in (test.get("writing") or {}).get("tasks", []):
                    tasks.append({
                        "task_number": task["task_number"],
                        "answer_key": f"task_{task['task_number']}",
                        "description": task.get("description"),
                        "image_url": task.get("image_url"),
                    })
                entry["tasks"] = tasks

            elif section == SectionType.SPEAKING.value:
                parts = []
                for part in (test.get("speaking") or {}).get("parts", []):
                    questions = await self.question_repo.find_many(part.get("question_ids", []))
                    parts.append({
                        "part_number": part["part_number"],
                        "questions": [_strip_answers(q) if strip_answers else q for q in questions],
                    })
                entry["parts"] = parts

            results.append(entry)

        minimal = [{"test_id": r["test_id"], "test_title": r["test_title"], "module_type": r["module_type"]} for r in results]
        return {"section": section, "total": len(minimal), "items": minimal}

    async def get_test_section_detail(self, test_id: str, section: str, strip_answers: bool = True) -> dict:
        """Return full content for a single section of a specific test."""
        valid_sections = {s.value for s in SectionType}
        if section not in valid_sections:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid section '{section}'. Must be one of: {', '.join(sorted(valid_sections))}",
            )

        test = await self.test_repo.find_by_id(test_id)
        if not test:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test not found")

        entry: Dict[str, Any] = {
            "test_id": test_id,
            "test_title": test.get("title"),
            "module_type": test.get("module_type"),
            "section": section,
        }

        if section == SectionType.LISTENING.value:
            sub_sections = []
            for sec in (test.get("listening") or {}).get("sections", []):
                questions = await self.question_repo.find_many(sec.get("question_ids", []))
                sub_sections.append({
                    "section_number": sec["section_number"],
                    "audio_url": sec.get("audio_url"),
                    "questions": [_strip_answers(q) if strip_answers else q for q in questions],
                })
            entry["sub_sections"] = sub_sections

        elif section == SectionType.READING.value:
            sub_sections = []
            for sec in (test.get("reading") or {}).get("sections", []):
                questions = await self.question_repo.find_many(sec.get("question_ids", []))
                sub_sections.append({
                    "section_number": sec["section_number"],
                    "passage": sec.get("passage"),
                    "questions": [_strip_answers(q) if strip_answers else q for q in questions],
                })
            entry["sub_sections"] = sub_sections

        elif section == SectionType.WRITING.value:
            tasks = []
            for task in (test.get("writing") or {}).get("tasks", []):
                tasks.append({
                    "task_number": task["task_number"],
                    "answer_key": f"task_{task['task_number']}",
                    "description": task.get("description"),
                    "image_url": task.get("image_url"),
                })
            entry["tasks"] = tasks

        elif section == SectionType.SPEAKING.value:
            parts = []
            for part in (test.get("speaking") or {}).get("parts", []):
                questions = await self.question_repo.find_many(part.get("question_ids", []))
                parts.append({
                    "part_number": part["part_number"],
                    "questions": [_strip_answers(q) if strip_answers else q for q in questions],
                })
            entry["parts"] = parts

        return entry

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

        # For FULL_TEST: ensure all sections are completed before finalizing
        mode = session.get("mode", SessionMode.FULL_TEST.value)
        if mode == SessionMode.FULL_TEST.value:
            session_sections = session.get("session_sections", [])
            incomplete = [
                s["section"] for s in session_sections
                if s["status"] != SectionStatus.COMPLETED.value
            ]
            if incomplete:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Cannot finalize: sections not yet completed: {incomplete}",
                )

        test = await self.test_repo.find_by_id(session["test_id"])
        if not test:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test not found")

        answers = session.get("answers", {})
        is_academic = test.get("module_type", "academic") == ModuleType.ACADEMIC.value

        section_scores: List[SectionScore] = []
        band_by_section: Dict[str, float] = {}

        # Preserve any already-graded scores (e.g. speaking AI evaluation)
        existing_scores: Dict[str, Any] = {
            s["section"]: s for s in (session.get("section_scores") or [])
        }

        available_sections = _get_available_sections(test)
        if mode == SessionMode.PRACTICE.value:
            practice_section = session.get("practice_section")
            available_sections = [s for s in available_sections if s == practice_section]

        for section_type in available_sections:
            q_ids = _get_question_ids_for_section(test, section_type)
            questions = await self.question_repo.find_many(q_ids) if q_ids else []

            section_answers = answers.get(section_type, {})
            total_earned = 0
            total_max = 0
            answer_details: List[Dict[str, Any]] = []

            for q in questions:
                user_ans = section_answers.get(q["id"])
                earned, max_pts = _score_question(q, user_ans)
                total_earned += earned
                total_max += max_pts
                if section_type in {SectionType.LISTENING.value, SectionType.READING.value}:
                    answer_details.append({
                        "question_id": q["id"],
                        "title": q.get("title", ""),
                        "type": q.get("type", ""),
                        "user_answer": user_ans,
                        "correct_answer": _extract_correct_answer(q),
                        "earned": earned,
                        "max": max_pts,
                    })

            # Calculate band score
            if section_type == SectionType.LISTENING.value:
                band = raw_to_band_listening(total_earned)
                details: Optional[Dict[str, Any]] = {"answer_details": answer_details}
            elif section_type == SectionType.READING.value:
                band = raw_to_band_reading(total_earned, is_academic=is_academic)
                details = {"answer_details": answer_details}
            elif section_type in {SectionType.WRITING.value, SectionType.SPEAKING.value}:
                # Preserve existing score if already graded
                existing = existing_scores.get(section_type)
                existing_band = float(existing["band_score"]) if existing and existing.get("band_score") else 0.0
                if existing_band > 0:
                    band = existing_band
                    details = existing.get("details") if existing else None
                elif section_type == SectionType.SPEAKING.value:
                    # Auto-calculate from stored evaluations in answers["speaking"]
                    band, details = _calculate_speaking_score_from_answers(answers.get("speaking", {}))
                    band = band or 0.0
                else:
                    band = 0.0
                    details = None
            else:
                band = 0.0
                details = None

            band_by_section[section_type] = band
            section_scores.append(SectionScore(
                section=SectionType(section_type),
                raw_score=total_earned,
                max_score=total_max,
                band_score=band,
                details=details,
            ))

        # Overall band — only calculated for FULL_TEST with all 4 sections
        overall = None
        if mode == SessionMode.FULL_TEST.value and all(
            s.value in band_by_section
            for s in [SectionType.LISTENING, SectionType.READING, SectionType.WRITING, SectionType.SPEAKING]
        ):
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
            if started_at.tzinfo is None:
                started_at = started_at.replace(tzinfo=timezone.utc)
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
            mode=SessionMode(mode),
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
            mode=SessionMode(session.get("mode", SessionMode.FULL_TEST.value)),
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

    # ── Section/Part Builder ──────────────────

    async def add_section_to_test(self, test_id: str, module: str, data: dict) -> TestOut:
        test = await self.test_repo.find_by_id(test_id)
        if not test:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test not found")

        if module == "listening":
            section_number = data.get("section_number")
            if not section_number:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="listening requires section_number (1-4)")
            if not (1 <= section_number <= 4):
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="section_number must be 1-4 for listening")
            section_data = {"section_number": section_number, "audio_url": data.get("audio_url") or "", "question_ids": []}

        elif module == "reading":
            section_number = data.get("section_number")
            if not section_number:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="reading requires section_number (1-3)")
            if not (1 <= section_number <= 3):
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="section_number must be 1-3 for reading")
            section_data = {"section_number": section_number, "passage": data.get("passage") or "", "question_ids": []}

        elif module == "writing":
            task_number = data.get("task_number")
            if not task_number:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="writing requires task_number (1-2)")
            if not (1 <= task_number <= 2):
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="task_number must be 1-2 for writing")
            section_data = {"task_number": task_number, "description": data.get("description") or "", "image_url": data.get("image_url")}

        elif module == "speaking":
            part_number = data.get("part_number")
            if not part_number:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="speaking requires part_number (1-3)")
            if not (1 <= part_number <= 3):
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="part_number must be 1-3 for speaking")
            section_data = {"part_number": part_number, "question_ids": []}

        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="module must be listening, reading, writing, or speaking")

        doc = await self.test_repo.add_section(test_id, module, section_data)
        if not doc:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to add section")
        return TestOut(**doc)

    async def update_test_section(self, test_id: str, module: str, number: int, update_fields: dict) -> TestOut:
        test = await self.test_repo.find_by_id(test_id)
        if not test:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test not found")
        doc = await self.test_repo.update_section(test_id, module, number, update_fields)
        if not doc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test not found after update")
        return TestOut(**doc)

    async def remove_section_from_test(self, test_id: str, module: str, number: int) -> TestOut:
        test = await self.test_repo.find_by_id(test_id)
        if not test:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test not found")
        doc = await self.test_repo.remove_section(test_id, module, number)
        if not doc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test not found after removing section")
        return TestOut(**doc)

    async def add_question_to_test_section(self, test_id: str, section_part: str, question_id: str) -> TestOut:
        test = await self.test_repo.find_by_id(test_id)
        if not test:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test not found")
        question = await self.question_repo.find_by_id(question_id)
        if not question:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Question not found")
        try:
            doc = await self.test_repo.add_question_to_section(test_id, section_part, question_id)
        except ValueError as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
        if not doc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test not found after adding question")
        return TestOut(**doc)

    async def remove_question_from_test_section(self, test_id: str, section_part: str, question_id: str) -> TestOut:
        test = await self.test_repo.find_by_id(test_id)
        if not test:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test not found")
        try:
            doc = await self.test_repo.remove_question_from_section(test_id, section_part, question_id)
        except ValueError as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
        if not doc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test not found after removing question")
        from src.database.repositories.ielts_repository import _serialize
        serialized_doc = _serialize(doc)
        return TestOut(**serialized_doc)

    # ── Private helpers ───────────────────────

    async def _get_session_or_fail(self, session_id: str) -> dict:
        doc = await self.session_repo.find_by_id(session_id)
        if not doc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session not found")
        return doc
