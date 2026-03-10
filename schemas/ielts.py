from pydantic import BaseModel, Field, model_validator, field_validator
from typing import Optional, List, Any, Dict, Literal
from datetime import datetime
from schemas.enums import (
    TestType,
    ModuleType, SectionType, SectionPart, QuestionType, WritingQuestionType, SpeakingQuestionType,
    WritingCriteria, SpeakingCriteria, TestStatus
)


IELTS_TOTAL_DURATION_MINUTES = 164  # 2h 44m

IELTS_BAND_DESCRIPTIONS: Dict[int, str] = {
    9: "Expert user",
    8: "Very good user",
    7: "Good user",
    6: "Competent user",
    5: "Modest user",
    4: "Limited user",
    3: "Extremely limited user",
    2: "Intermittent user",
    1: "Non-user",
    0: "Did not attempt the test",
}


# ─────────────────────────────────────────────
# Question schemas for Listening and Reading
# ─────────────────────────────────────────────

class AnswerOption(BaseModel):
    label: str  = Field(..., examples=["A"])
    text:  str  = Field(..., examples=["Visit the library"])

class FormField(BaseModel):
    label:  str = Field(..., examples=["Full name"])
    prefix: str = Field(default="", examples=["£"])
    answer: str = Field(..., examples=["Johnson"])

class TableCell(BaseModel):
    row_header: str = Field(..., examples=["Monday"])
    col_header: str = Field(..., examples=["Time"])
    answer:     str = Field(..., examples=["9:00 AM"])

class FlowStep(BaseModel):
    step_number: int  = Field(..., ge=1)
    description: str  = Field(..., examples=["Raw materials are collected"])
    answer:      Optional[str] = Field(default=None, examples=["heated"])
    is_blank:    bool = Field(default=False)

class SentenceItem(BaseModel):
    before: str = Field(..., examples=["The process begins with"])
    after:  str = Field(default="", examples=["being filtered."])
    answer: str = Field(..., examples=["water"])

class ShortAnswerItem(BaseModel):
    question: str = Field(..., examples=["What is the maximum weight allowed?"])
    answer:   str = Field(..., examples=["20 kilograms"])

class MapSlot(BaseModel):
    slot_label: str = Field(..., examples=["1"])
    position:   str = Field(..., examples=["top-center"])
    answer:     str = Field(..., examples=["library"])

class MatchingItem(BaseModel):
    item:   str = Field(..., examples=["Roman Gallery"])
    answer: str = Field(..., examples=["C"])

class HeadingItem(BaseModel):
    paragraph_label: str = Field(..., examples=["Paragraph A"])
    answer:          str = Field(..., examples=["i"])

class TFNGItem(BaseModel):
    statement: str                              = Field(..., examples=["The Earth is flat."])
    answer:    str                              = Field(..., examples=["FALSE", "TRUE", "NOT GIVEN"])

    @field_validator("answer")
    @classmethod
    def valid_tfng(cls, v):
        if v.upper() not in {"TRUE", "FALSE", "NOT GIVEN", "YES", "NO"}:
            raise ValueError("answer must be TRUE/FALSE/NOT GIVEN or YES/NO")
        return v.upper()

class PickFromListItem(BaseModel):
    question: str       = Field(..., examples=["Which TWO problems are mentioned?"])
    answers:  List[str] = Field(..., examples=[["B", "D"]])

class WritingPrompt(BaseModel):
    prompt:          str           = Field(..., examples=["Some people believe that..."])
    word_limit:      int           = Field(default=250, ge=50)
    time_limit_mins: int           = Field(default=40, ge=10)
    band_descriptors: Optional[dict] = Field(default=None)
    sample_answer:   Optional[str] = Field(default=None)
    # Task 1 specifics
    chart_type:      Optional[str] = Field(default=None, examples=["bar chart", "line graph", "pie chart", "table", "map", "process diagram"])
    image_url:       Optional[str] = Field(default=None)
    # Letter specifics
    letter_type:     Optional[str] = Field(default=None, examples=["formal", "semi-formal", "informal"])
    letter_situation: Optional[str] = Field(default=None)

class CueCard(BaseModel):
    topic:       str       = Field(..., examples=["Describe a place you have visited."])
    bullet_points: List[str] = Field(..., examples=[["Where it is", "When you went", "What you did"]])
    follow_up:   Optional[str] = Field(default=None, examples=["And explain how you felt about it."])
    prep_time_seconds: int = Field(default=60)
    speak_time_seconds: int = Field(default=120)
    sample_answer: Optional[str] = Field(default=None)

class SpeakingQuestion(BaseModel):
    question:      str           = Field(..., examples=["Do you enjoy cooking?"])
    follow_ups:    List[str]     = Field(default=[], examples=[["Why?", "How often?"]])
    sample_answer: Optional[str] = Field(default=None)
    band_tip:      Optional[str] = Field(default=None)

class SummaryItem(BaseModel):
    before:  str = Field(..., examples=["The company was founded in"])
    after:   str = Field(default="", examples=["by two engineers."])
    answer:  str = Field(..., examples=["1998"])
    word_options: Optional[List[str]] = Field(default=None, examples=[["1998", "2001", "1990"]])


# ── Base Question ──────────────────────────────────────────────

class QuestionBase(BaseModel):
    # ── Core fields ───────────────────────────────────────────
    title:          str             = Field(..., min_length=3, max_length=300)
    section:        SectionType
    section_part:   SectionPart
    test_type:      TestType
    module_type:    ModuleType
    type:           QuestionType
    tags:           List[str]       = Field(default=[])
    context:        Optional[str]   = Field(default=None, examples=["You hear two people discussing..."])
    instruction:    str             = Field(..., min_length=3)
    passage:        Optional[str]   = Field(default=None, description="Reading passage text")
    audio_url:      Optional[str]   = Field(default=None)
    image_url:      Optional[str]   = Field(default=None)

    # ── LISTENING / READING sub-models ────────────────────────
    options:           Optional[List[AnswerOption]]  = None   # MCQ / Matching box
    correct_option:    Optional[str]                 = None   # MCQ single answer
    correct_options:   Optional[List[str]]           = None   # Multi-select answers
    form_fields:       Optional[List[FormField]]     = None
    table_cells:       Optional[List[TableCell]]     = None
    flow_steps:        Optional[List[FlowStep]]      = None
    sentences:         Optional[List[SentenceItem]]  = None
    summary_items:     Optional[List[SummaryItem]]   = None
    short_items:       Optional[List[ShortAnswerItem]] = None
    map_word_box:      Optional[List[str]]           = None
    map_slots:         Optional[List[MapSlot]]       = None
    matching_items:    Optional[List[MatchingItem]]  = None
    heading_options:   Optional[List[AnswerOption]]  = None   # e.g. i–x
    heading_items:     Optional[List[HeadingItem]]   = None
    tfng_items:        Optional[List[TFNGItem]]      = None
    pick_items:        Optional[List[PickFromListItem]] = None

    # ── WRITING sub-models ────────────────────────────────────
    writing_prompt:    Optional[WritingPrompt]       = None

    # ── SPEAKING sub-models ───────────────────────────────────
    cue_card:          Optional[CueCard]             = None
    speaking_questions: Optional[List[SpeakingQuestion]] = None

    @model_validator(mode="after")
    def check_type_data_consistency(self):
        t = self.type
        # MCQ needs options + correct_option
        if t == QuestionType.MULTIPLE_CHOICE:
            if not self.options or not self.correct_option:
                raise ValueError("multiple_choice requires 'options' and 'correct_option'")
        if t == QuestionType.MULTIPLE_SELECT:
            if not self.options or not self.correct_options:
                raise ValueError("multiple_select requires 'options' and 'correct_options'")
        if t in {QuestionType.FORM_COMPLETION} and not self.form_fields:
            raise ValueError(f"{t} requires 'form_fields'")
        if t == QuestionType.TABLE_COMPLETION and not self.table_cells:
            raise ValueError("table_completion requires 'table_cells'")
        if t == QuestionType.FLOW_CHART_COMPLETION and not self.flow_steps:
            raise ValueError("flow_chart_completion requires 'flow_steps'")
        if t in {QuestionType.SENTENCE_COMPLETION, QuestionType.NOTE_COMPLETION} and not self.sentences:
            raise ValueError(f"{t} requires 'sentences'")
        if t == QuestionType.SUMMARY_COMPLETION and not self.summary_items:
            raise ValueError("summary_completion requires 'summary_items'")
        if t == QuestionType.SHORT_ANSWER and not self.short_items:
            raise ValueError("short_answer requires 'short_items'")
        if t in {QuestionType.MAP_LABELLING, QuestionType.PLAN_LABELLING, QuestionType.DIAGRAM_LABELLING}:
            if not self.map_slots:
                raise ValueError(f"{t} requires 'map_slots'")
        if t in {QuestionType.MATCHING, QuestionType.MATCHING_FEATURES} and not self.matching_items:
            raise ValueError(f"{t} requires 'matching_items'")
        if t == QuestionType.MATCHING_HEADINGS and not self.heading_items:
            raise ValueError("matching_headings requires 'heading_items'")
        if t in {QuestionType.TRUE_FALSE_NOT_GIVEN, QuestionType.YES_NO_NOT_GIVEN} and not self.tfng_items:
            raise ValueError(f"{t} requires 'tfng_items'")
        if t == QuestionType.PICK_FROM_LIST and not self.pick_items:
            raise ValueError("pick_from_list requires 'pick_items'")
        if t in {
            WritingQuestionType.GRAPH_DESCRIPTION, WritingQuestionType.LETTER_WRITING,
            WritingQuestionType.PROCESS_DESCRIPTION, WritingQuestionType.MAP_COMPARISON,
            WritingQuestionType.ESSAY_OPINION, WritingQuestionType.ESSAY_DISCUSSION,
            WritingQuestionType.ESSAY_PROBLEM_SOLUTION, WritingQuestionType.ESSAY_ADVANTAGES,
            WritingQuestionType.ESSAY_MIXED
        } and not self.writing_prompt:
            raise ValueError(f"{t} requires 'writing_prompt'")
        if t == SpeakingQuestionType.CUE_CARD and not self.cue_card:
            raise ValueError("speaking_cue_card requires 'cue_card'")
        if t in {SpeakingQuestionType.INTERVIEW, SpeakingQuestionType.  DISCUSSION} and not self.speaking_questions:
            raise ValueError(f"{t} requires 'speaking_questions'")
        return self


class QuestionCreate(QuestionBase):
    pass

class QuestionUpdate(BaseModel):
    title:             Optional[str]            = None
    instruction:       Optional[str]            = None
    context:           Optional[str]            = None
    passage:           Optional[str]            = None
    audio_url:         Optional[str]            = None
    image_url:         Optional[str]            = None
    tags:              Optional[List[str]]      = None
    options:           Optional[List[AnswerOption]]    = None
    correct_option:    Optional[str]            = None
    correct_options:   Optional[List[str]]      = None
    form_fields:       Optional[List[FormField]] = None
    table_cells:       Optional[List[TableCell]] = None
    flow_steps:        Optional[List[FlowStep]]  = None
    sentences:         Optional[List[SentenceItem]] = None
    summary_items:     Optional[List[SummaryItem]]  = None
    short_items:       Optional[List[ShortAnswerItem]] = None
    map_word_box:      Optional[List[str]]      = None
    map_slots:         Optional[List[MapSlot]]  = None
    matching_items:    Optional[List[MatchingItem]] = None
    heading_options:   Optional[List[AnswerOption]] = None
    heading_items:     Optional[List[HeadingItem]]  = None
    tfng_items:        Optional[List[TFNGItem]]     = None
    pick_items:        Optional[List[PickFromListItem]] = None
    writing_prompt:    Optional[WritingPrompt]   = None
    cue_card:          Optional[CueCard]         = None
    speaking_questions: Optional[List[SpeakingQuestion]] = None

class QuestionOut(QuestionBase):
    id:         str
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


# ── Safe question output (strips answers for candidates) ──────

class QuestionSafe(BaseModel):
    """Question view with answers stripped — sent to candidates during a test."""
    id:           str
    title:        str
    section:      SectionType
    section_part: SectionPart
    test_type:    TestType
    module_type:  ModuleType
    type:         QuestionType
    context:      Optional[str]  = None
    instruction:  str
    passage:      Optional[str]  = None
    audio_url:    Optional[str]  = None
    image_url:    Optional[str]  = None
    options:      Optional[List[AnswerOption]] = None
    heading_options: Optional[List[AnswerOption]] = None
    # Writing / Speaking prompts (no sample answers)
    writing_prompt:    Optional[WritingPrompt]       = None
    cue_card:          Optional[CueCard]             = None
    speaking_questions: Optional[List[SpeakingQuestion]] = None
    # Structural items (blanks only, no answers)
    form_fields:       Optional[List[Dict[str, Any]]]   = None
    table_cells:       Optional[List[Dict[str, Any]]]   = None
    flow_steps:        Optional[List[Dict[str, Any]]]   = None
    sentences:         Optional[List[Dict[str, Any]]]   = None
    summary_items:     Optional[List[Dict[str, Any]]]   = None
    short_items:       Optional[List[Dict[str, Any]]]   = None
    map_slots:         Optional[List[Dict[str, Any]]]   = None
    matching_items:    Optional[List[Dict[str, Any]]]   = None
    heading_items:     Optional[List[Dict[str, Any]]]   = None
    tfng_items:        Optional[List[Dict[str, Any]]]   = None
    pick_items:        Optional[List[Dict[str, Any]]]   = None

    model_config = {"from_attributes": True}


# ─────────────────────────────────────────────
# Pagination
# ─────────────────────────────────────────────

class PaginatedResponse(BaseModel):
    items: List[Any]
    total: int
    page: int
    page_size: int
    total_pages: int


# ─────────────────────────────────────────────
# Test (exam paper) schemas
# ─────────────────────────────────────────────

class SectionConfig(BaseModel):
    """Questions grouped under one section of a test."""
    section:      SectionType
    section_part: SectionPart
    question_ids: List[str] = Field(..., min_length=1)
    time_limit_minutes: Optional[int] = None

class TestCreate(BaseModel):
    title:       str = Field(..., min_length=3, max_length=200)
    description: Optional[str] = None
    test_type:   TestType = TestType.IELTS
    module_type: ModuleType = ModuleType.ACADEMIC
    sections:    List[SectionConfig] = Field(..., min_length=1)
    is_published: bool = False
    time_limit_minutes: int = Field(default=IELTS_TOTAL_DURATION_MINUTES, ge=1)
    tags:        List[str] = []

class TestUpdate(BaseModel):
    title:       Optional[str] = None
    description: Optional[str] = None
    sections:    Optional[List[SectionConfig]] = None
    is_published: Optional[bool] = None
    time_limit_minutes: Optional[int] = None
    tags:        Optional[List[str]] = None

class TestOut(BaseModel):
    id:           str
    title:        str
    description:  Optional[str] = None
    test_type:    TestType
    module_type:  ModuleType
    sections:     List[SectionConfig]
    is_published: bool
    time_limit_minutes: int
    tags:         List[str] = []
    question_count: int = 0
    created_at:   datetime
    updated_at:   Optional[datetime] = None

    model_config = {"from_attributes": True}


# ─────────────────────────────────────────────
# Test Session (user attempt) schemas
# ─────────────────────────────────────────────

class AnswerSubmission(BaseModel):
    question_id: str
    answer: Any = Field(..., description="User's answer (string, list, dict depending on question type)")

class SectionAnswers(BaseModel):
    section: SectionType
    answers: List[AnswerSubmission]

class StartTestRequest(BaseModel):
    test_id: str

class SubmitAnswersRequest(BaseModel):
    session_id: str
    sections:   List[SectionAnswers]

class FinalizeSessionRequest(BaseModel):
    session_id: str


class SectionScore(BaseModel):
    section:    SectionType
    raw_score:  int
    max_score:  int
    band_score: float
    details:    Optional[Dict[str, Any]] = None

class SessionResult(BaseModel):
    session_id:     str
    test_id:        str
    user_id:        str
    status:         TestStatus
    section_scores: List[SectionScore] = []
    overall_band:   Optional[float] = None
    started_at:     datetime
    finished_at:    Optional[datetime] = None
    time_spent_seconds: Optional[int] = None

class TestSessionOut(BaseModel):
    id:              str
    test_id:         str
    user_id:         str
    status:          TestStatus
    answers:         Dict[str, Any] = {}
    section_scores:  List[SectionScore] = []
    overall_band:    Optional[float] = None
    started_at:      datetime
    finished_at:     Optional[datetime] = None
    time_spent_seconds: Optional[int] = None
    created_at:      datetime
    updated_at:      Optional[datetime] = None

    model_config = {"from_attributes": True}


# ─────────────────────────────────────────────
# Admin schemas
# ─────────────────────────────────────────────

class AdminDashboardStats(BaseModel):
    total_users:      int
    total_questions:  int
    total_tests:      int
    total_sessions:   int
    active_sessions:  int
    completed_sessions: int
    average_band:     Optional[float] = None
    users_by_role:    Dict[str, int] = {}
    sessions_by_status: Dict[str, int] = {}

class AdminUserUpdate(BaseModel):
    role:      Optional[str] = None
    is_active: Optional[bool] = None

class AdminTestPublish(BaseModel):
    test_id:     str
    is_published: bool
