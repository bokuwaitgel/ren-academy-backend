"""
IELTS Writing Evaluator — pydantic_ai + Gemini implementation.
"""

import os
from typing import List

from pydantic import BaseModel, Field
from pydantic_ai import Agent

from src.agent.agent import IeltsWritingAgent


# ─────────────────────────────────────────────
# Result schema
# ─────────────────────────────────────────────

class SentenceCorrection(BaseModel):
    original: str = Field(..., description="The original problematic sentence")
    corrected: str = Field(..., description="The corrected sentence")
    explanation: str = Field(..., description="Brief explanation of the correction")


class WritingEvaluationResult(BaseModel):
    # Band scores (0–9, 0.5 increments)
    overall_score: float = Field(..., ge=0, le=9, description="Overall IELTS band score (average of 4 criteria, nearest 0.5)")
    task_achievement: float = Field(..., ge=0, le=9, description="Task Achievement / Task Response band score")
    coherence_cohesion: float = Field(..., ge=0, le=9, description="Coherence & Cohesion band score")
    lexical_resource: float = Field(..., ge=0, le=9, description="Lexical Resource band score")
    grammar_accuracy: float = Field(..., ge=0, le=9, description="Grammatical Range & Accuracy band score")

    # AI detection
    ai_detection: str = Field(..., description="'Human Written' if ai_generation_percentage < 50, else 'AI Generated'")
    ai_generation_percentage: int = Field(..., ge=0, le=100, description="Estimated percentage of AI-generated content")

    # Error counts
    grammar_errors: int = Field(..., ge=0, description="Number of grammatical errors found")
    vocabulary_errors: int = Field(..., ge=0, description="Number of vocabulary/word-choice errors found")
    sentence_errors: int = Field(..., ge=0, description="Number of sentence-level structural errors")

    # Classification
    task_type: str = Field(..., description="'Task 1' or 'Task 2'")
    writing_level: str = Field(..., description="Beginner | Elementary | Intermediate | Upper-Intermediate | Advanced | Expert")

    # Feedback
    ai_suggestions: str = Field(..., description="Specific, actionable improvement tips (2–3 sentences)")
    motivation: str = Field(..., description="Short motivating message for the student (1–2 sentences)")

    # Corrections & improved text
    word_corrections: str = Field(..., description="Full essay with vocabulary corrections applied (keep structure, fix word choices)")
    sentence_corrections: List[SentenceCorrection] = Field(..., description="Up to 5 most important sentence-level corrections")
    improved_version: str = Field(..., description="Fully rewritten, polished version of the essay maintaining the writer's core ideas")
    overall_feedback: str = Field(..., description="2–3 sentence holistic feedback summary")


# ─────────────────────────────────────────────
# System prompt
# ─────────────────────────────────────────────

_SYSTEM_PROMPT = """You are a certified IELTS examiner with over 10 years of experience evaluating Academic and General Training writing tasks.

## Your task
Evaluate the submitted IELTS writing and return a structured assessment.

## Band score criteria (use official IELTS descriptors)
Score each criterion on 0–9 in 0.5 increments:
- **Task Achievement / Task Response**: Does the response address all parts of the task? Is the position clear and supported?
- **Coherence & Cohesion**: Is the essay logically organised? Are cohesive devices used appropriately?
- **Lexical Resource**: Range, accuracy, and appropriacy of vocabulary. Collocations, spelling.
- **Grammatical Range & Accuracy**: Variety of sentence structures, frequency of errors.
- **Overall**: Average of the 4 above, rounded to nearest 0.5.

## Writing level mapping
- Band 1–3 → Beginner
- Band 3.5–4.5 → Elementary
- Band 5–5.5 → Intermediate
- Band 6–6.5 → Upper-Intermediate
- Band 7–7.5 → Advanced
- Band 8–9 → Expert

## AI detection
Estimate likelihood the text was AI-generated (0 = fully human, 100 = fully AI).
Set ai_detection to "Human Written" if percentage < 50, else "AI Generated".

## Corrections
- word_corrections: Return the full original essay with ONLY vocabulary/word-choice fixes (do not restructure sentences).
- sentence_corrections: Pick the 3–5 most impactful sentence-level issues; provide original, corrected, and a brief explanation.
- improved_version: A polished rewrite that preserves the student's ideas but corrects all errors and improves band score.

## Tone
Be fair, specific, and encouraging. Avoid generic advice."""


# ─────────────────────────────────────────────
# Agent implementation
# ─────────────────────────────────────────────

class GeminiWritingAgent(IeltsWritingAgent):
    """Concrete IELTS writing evaluator using pydantic_ai + Gemini."""

    def __init__(self) -> None:
        self._agent: Agent[None, WritingEvaluationResult] = Agent(
            # "google-gla:gemini-2.5-pro",
            "google-gla:gemini-flash-latest",
            output_type=WritingEvaluationResult,
            system_prompt=_SYSTEM_PROMPT,
        )

    async def analyze(  # type: ignore[override]
        self,
        content: str,
        prompt: str = "",
        task_type: str = "",
        question_id: str = "",
    ) -> WritingEvaluationResult:
        """
        Evaluate an IELTS writing submission.

        Args:
            content:     The student's essay text.
            prompt:      The original writing prompt/question (optional but improves accuracy).
            task_type:   "Task 1" or "Task 2" (optional — agent will infer if not provided).
            question_id: For reference only.
        """
        parts = []

        if task_type:
            parts.append(f"**Task type**: {task_type}")
        if prompt:
            parts.append(f"**Writing prompt**:\n{prompt}")

        parts.append(f"**Student's essay**:\n{content}")

        if not task_type and not prompt:
            parts.append("\n*Determine the task type from the essay content.*")

        user_message = "\n\n".join(parts)

        result = await self._agent.run(user_message)
        return result.output


# Singleton — reuse across requests
_agent_instance: GeminiWritingAgent | None = None


def get_writing_agent() -> GeminiWritingAgent:
    global _agent_instance
    if _agent_instance is None:
        _agent_instance = GeminiWritingAgent()
    return _agent_instance
