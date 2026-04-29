"""
IELTS Speaking Evaluator — pydantic_ai + Gemini implementation.
Supports audio (binary) or transcript (text) input.
"""

from pydantic import BaseModel, Field
from pydantic_ai import Agent, BinaryContent

from src.agent.agent import IeltsSpeakingAgent


# ─────────────────────────────────────────────
# Result schema
# ─────────────────────────────────────────────

class SpeakingEvaluationResult(BaseModel):
    # Band scores (0–9, 0.5 increments)
    overall_score: float = Field(..., ge=0, le=9, description="Overall IELTS Speaking band score (average of 4 criteria, nearest 0.5)")
    fluency_coherence: float = Field(..., ge=0, le=9, description="Fluency & Coherence band score")
    lexical_resource: float = Field(..., ge=0, le=9, description="Lexical Resource band score")
    grammar_accuracy: float = Field(..., ge=0, le=9, description="Grammatical Range & Accuracy band score")
    pronunciation: float = Field(..., ge=0, le=9, description="Pronunciation band score")

    # Classification
    speaking_level: str = Field(..., description="Beginner | Elementary | Intermediate | Upper-Intermediate | Advanced | Expert")

    # Per-criterion feedback
    fluency_feedback: str = Field(..., description="Specific feedback on Fluency & Coherence (2–3 sentences)")
    lexical_feedback: str = Field(..., description="Specific feedback on Lexical Resource (2–3 sentences)")
    grammar_feedback: str = Field(..., description="Specific feedback on Grammatical Range & Accuracy (2–3 sentences)")
    pronunciation_feedback: str = Field(..., description="Specific feedback on Pronunciation (2–3 sentences)")

    # Error counts
    grammar_errors: int = Field(..., ge=0, description="Number of grammatical errors found in the transcript")
    vocabulary_errors: int = Field(..., ge=0, description="Number of vocabulary or word-choice errors found")

    # Improvements
    sample_improvements: list[str] = Field(..., description="3–5 specific phrases or sentences from the student rewritten at a higher band level")

    # Overall
    strengths: str = Field(..., description="2–3 key strengths of the speaking response")
    areas_for_improvement: str = Field(..., description="2–3 main areas that need improvement with actionable advice")
    overall_feedback: str = Field(..., description="2–3 sentence holistic feedback summary")
    motivation: str = Field(..., description="Short motivating message for the student (1–2 sentences)")

    # Source
    evaluated_from: str = Field(..., description="'audio' if audio was analysed, 'transcript' if only text was provided")


# ─────────────────────────────────────────────
# System prompt
# ─────────────────────────────────────────────

_SYSTEM_PROMPT = """You are a certified IELTS examiner with over 10 years of experience evaluating Academic and General Training Speaking tests.

## Your task
Evaluate the submitted IELTS speaking response and return a structured assessment following the official IELTS Speaking band descriptors below.

## Official IELTS Speaking Band Descriptors

### 1. Fluency and Coherence
- Band 9 – Speaks effortlessly, almost no repetition or self-correction. Any hesitation is content-related, not word-search.
- Band 8 – Generally fluent; occasional repetition. Ideas well-connected and developed.
- Band 7 – Speaks at length without noticeable effort; uses connectives and discourse markers effectively.
- Band 6 – Willing to speak at length but coherence sometimes lost due to repetition, self-correction, or hesitation.
- Band 5 – Maintains flow but uses repetition and limited range of connectives.
- Band 4 – Slow delivery with many long pauses; only basic meaning conveyed on familiar topics.
- Band 3 – Long pauses dominate; cannot connect even simple sentences.
- Band 2 – Almost no communication; long pauses before most words.
- Band 1 – No real communication possible.

### 2. Lexical Resource
- Band 9 – Uses vocabulary with full flexibility and precision on all topics. Natural idiomatic usage.
- Band 8 – Wide resource; skilful use of uncommon items; rare minor errors in word choice or collocation.
- Band 7 – Flexible use; some ability to use idiomatic language; occasional inaccuracies.
- Band 6 – Adequate resource for most topics; attempts paraphrase but with limited success.
- Band 5 – Limited range; uses basic vocabulary; errors in word choice noticeable.
- Band 4 – Basic vocabulary only sufficient for familiar topics; frequent inaccuracies.
- Band 3 – Uses simple vocabulary for personal information; inadequate for unfamiliar topics.
- Band 2 – Only isolated words and memorised phrases; little real lexical resource.
- Band 1 – No usable vocabulary.

### 3. Grammatical Range and Accuracy
- Band 9 – Uses full range of structures naturally and appropriately. Errors extremely rare.
- Band 8 – Wide range; most complex structures accurate; occasional minor errors.
- Band 7 – Good range; frequently error-free; errors do not cause misunderstanding.
- Band 6 – Mix of simple and complex structures; errors in complex structures; meaning rarely obscured.
- Band 5 – Attempts complex structures but with limited success; errors frequently occur.
- Band 4 – Mostly simple structures; many errors that cause misunderstanding.
- Band 3 – Uses memorised phrases; errors in everything except formulaic expressions.
- Band 2 – Only isolated words; no grammatical structure.
- Band 1 – No usable grammar.

### 4. Pronunciation
- Band 9 – Uses full range of phonological features with precision and subtlety; effortless to understand.
- Band 8 – Wide range of features used effectively; accent barely affects intelligibility.
- Band 7 – Uses a range of features; generally easy to understand despite occasional lack of control.
- Band 6 – Uses some features; generally intelligible but L1 accent sometimes affects clarity.
- Band 5 – Intelligibility is at risk due to mispronunciation; some effort required from listener.
- Band 4 – Limited range of features; mispronunciations cause misunderstanding frequently.
- Band 3 – Pronunciation causes major comprehension difficulty throughout.
- Band 2 – Speech is mostly unintelligible.
- Band 1 – No intelligible speech.

## Band Score Rules
- Score each criterion from 0 to 9 in 0.5 increments.
- Overall = average of the 4 criteria, rounded to nearest 0.5.

## Speaking Level Mapping
- Band 1–3   → Beginner
- Band 3.5–4.5 → Elementary
- Band 5–5.5  → Intermediate
- Band 6–6.5  → Upper-Intermediate
- Band 7–7.5  → Advanced
- Band 8–9   → Expert

## Pronunciation Scoring
- If AUDIO is provided: assess pronunciation directly from phonological features heard.
- If only a TRANSCRIPT is provided: estimate based on vocabulary complexity, sentence structure, and observable self-correction markers in the text. Note the limitation in the pronunciation_feedback field.

## Sample Improvements
Choose 3–5 actual phrases from the student's response and rewrite them at a 1–2 band higher level.

## Tone
Be fair, specific, and encouraging. Always refer to the band descriptors when justifying scores."""


# ─────────────────────────────────────────────
# Agent implementation
# ─────────────────────────────────────────────

class GeminiSpeakingAgent(IeltsSpeakingAgent):
    """Concrete IELTS speaking evaluator using pydantic_ai + Gemini."""

    def __init__(self) -> None:
        self._agent: Agent[None, SpeakingEvaluationResult] = Agent(
            "google-gla:gemini-flash-latest",
            output_type=SpeakingEvaluationResult,
            system_prompt=_SYSTEM_PROMPT,
        )

    async def analyze(  # type: ignore[override]
        self,
        content: str | bytes = "",
        media_type: str = "audio/webm",
        question: str = "",
        part: str = "",
    ) -> SpeakingEvaluationResult:
        """
        Evaluate an IELTS speaking response.

        Args:
            content:    Either a transcript string or raw audio bytes.
            media_type: MIME type when content is audio bytes (default 'audio/webm').
            question:   The speaking question/prompt (optional).
            part:       'Part 1', 'Part 2', or 'Part 3' (optional).
        """
        msg_parts: list[str | BinaryContent] = []

        if part:
            msg_parts.append(f"**Speaking Part**: {part}")
        if question:
            msg_parts.append(f"**Question / Prompt**:\n{question}")

        if isinstance(content, bytes) and len(content) > 0:
            msg_parts.append(BinaryContent(data=content, media_type=media_type))
            msg_parts.append("**Instruction**: Evaluate the speaking response in the audio above.")
            evaluated_from_hint = "audio"
        else:
            transcript = content if isinstance(content, str) else ""
            msg_parts.append(f"**Student's transcript**:\n{transcript}")
            msg_parts.append("**Instruction**: Evaluate this transcript. No audio was provided; estimate pronunciation from text cues.")
            evaluated_from_hint = "transcript"

        msg_parts.append(f'\n*Set evaluated_from to "{evaluated_from_hint}".*')

        result = await self._agent.run(msg_parts)
        return result.output


# Singleton — reuse across requests
_agent_instance: GeminiSpeakingAgent | None = None


def get_speaking_agent() -> GeminiSpeakingAgent:
    global _agent_instance
    if _agent_instance is None:
        _agent_instance = GeminiSpeakingAgent()
    return _agent_instance
