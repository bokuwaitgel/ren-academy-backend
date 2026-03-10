from enum import Enum


class TestType(str, Enum):
    IELTS = "ielts"


class ModuleType(str, Enum):
    ACADEMIC = "academic"
    GENERAL = "general"


class SectionType(str, Enum):
    LISTENING = "listening"
    READING = "reading"
    WRITING = "writing"
    SPEAKING = "speaking"


class SectionPart(str, Enum):
    # Listening sections
    L_SECTION_1 = "listening_section_1"
    L_SECTION_2 = "listening_section_2"
    L_SECTION_3 = "listening_section_3"
    L_SECTION_4 = "listening_section_4"
    # Reading passage types
    R_PASSAGE_1 = "reading_passage_1"
    R_PASSAGE_2 = "reading_passage_2"
    R_PASSAGE_3 = "reading_passage_3"
    # Writing tasks
    W_TASK_1    = "writing_task_1"
    W_TASK_2    = "writing_task_2"
    # Speaking parts
    S_PART_1    = "speaking_part_1"
    S_PART_2    = "speaking_part_2"
    S_PART_3    = "speaking_part_3"

class QuestionType(str, Enum):
    # ── LISTENING & READING (shared) ──────────────────────────
    MULTIPLE_CHOICE         = "multiple_choice"
    MULTIPLE_SELECT         = "multiple_select"
    FORM_COMPLETION         = "form_completion"
    NOTE_COMPLETION         = "note_completion"
    TABLE_COMPLETION        = "table_completion"
    FLOW_CHART_COMPLETION   = "flow_chart_completion"
    SUMMARY_COMPLETION      = "summary_completion"
    SENTENCE_COMPLETION     = "sentence_completion"
    SHORT_ANSWER            = "short_answer"
    MATCHING                = "matching"
    MATCHING_HEADINGS       = "matching_headings"
    MATCHING_INFORMATION    = "matching_information"
    MATCHING_FEATURES       = "matching_features"
    MAP_LABELLING           = "map_labelling"
    PLAN_LABELLING          = "plan_labelling"
    DIAGRAM_LABELLING       = "diagram_labelling"
    # ── READING only ──────────────────────────────────────────
    TRUE_FALSE_NOT_GIVEN    = "true_false_not_given"
    YES_NO_NOT_GIVEN        = "yes_no_not_given"
    PICK_FROM_LIST          = "pick_from_list"


class WritingQuestionType(str, Enum):
    # ── WRITING ───────────────────────────────────────────────
    GRAPH_DESCRIPTION       = "graph_description"       # Task 1 Academic
    LETTER_WRITING          = "letter_writing"          # Task 1 General
    PROCESS_DESCRIPTION     = "process_description"     # Task 1 Academic
    MAP_COMPARISON          = "map_comparison"          # Task 1 Academic
    ESSAY_OPINION           = "essay_opinion"           # Task 2
    ESSAY_DISCUSSION        = "essay_discussion"        # Task 2
    ESSAY_PROBLEM_SOLUTION  = "essay_problem_solution"  # Task 2
    ESSAY_ADVANTAGES        = "essay_advantages"        # Task 2
    ESSAY_MIXED             = "essay_mixed"             # Task 2


class SpeakingQuestionType(str, Enum):
    # ── SPEAKING ──────────────────────────────────────────────
    INTERVIEW      = "speaking_interview"      # Part 1
    CUE_CARD       = "speaking_cue_card"       # Part 2
    DISCUSSION     = "speaking_discussion"     # Part 3


class WritingCriteria(str, Enum):
    TASK_ACHIEVEMENT = "task_achievement"
    TASK_RESPONSE = "task_response"
    COHERENCE_COHESION = "coherence_cohesion"
    LEXICAL_RESOURCE = "lexical_resource"
    GRAMMATICAL_RANGE_ACCURACY = "grammatical_range_accuracy"


class SpeakingCriteria(str, Enum):
    FLUENCY_COHERENCE = "fluency_coherence"
    LEXICAL_RESOURCE = "lexical_resource"
    GRAMMATICAL_RANGE_ACCURACY = "grammatical_range_accuracy"
    PRONUNCIATION = "pronunciation"


class TestStatus(str, Enum):
    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    SUBMITTED = "submitted"
    GRADED = "graded"


class BandScore(float, Enum):
    BAND_0 = 0.0
    BAND_1 = 1.0
    BAND_2 = 2.0
    BAND_3 = 3.0
    BAND_4 = 4.0
    BAND_4_5 = 4.5
    BAND_5 = 5.0
    BAND_5_5 = 5.5
    BAND_6 = 6.0
    BAND_6_5 = 6.5
    BAND_7 = 7.0
    BAND_7_5 = 7.5
    BAND_8 = 8.0
    BAND_8_5 = 8.5
    BAND_9 = 9.0
