"""
IELTS Band Score calculation utilities.

Listening & Reading: raw score → band (official conversion tables)
Writing & Speaking:  criteria average → rounded band (nearest 0.5)
Overall: average of 4 bands → rounded to nearest 0.5
"""

# ─────────────────────────────────────────────
# Official Listening raw score → band table
# 40 questions total
# ─────────────────────────────────────────────
_LISTENING_RAW_TO_BAND: dict[int, float] = {
    39: 9.0, 40: 9.0,
    37: 8.5, 38: 8.5,
    35: 8.0, 36: 8.0,
    32: 7.5, 33: 7.5, 34: 7.5,
    30: 7.0, 31: 7.0,
    26: 6.5, 27: 6.5, 28: 6.5, 29: 6.5,
    23: 6.0, 24: 6.0, 25: 6.0,
    18: 5.5, 19: 5.5, 20: 5.5, 21: 5.5, 22: 5.5,
    16: 5.0, 17: 5.0,
    13: 4.5, 14: 4.5, 15: 4.5,
    10: 4.0, 11: 4.0, 12: 4.0,
    8:  3.5, 9:  3.5,
    6:  3.0, 7:  3.0,
    4:  2.5, 5:  2.5,
    3:  2.0,
    2:  1.5,
    1:  1.0,
    0:  0.0,
}

_ACADEMIC_READING_TO_BAND: dict[int, float] = {
    39: 9.0, 40: 9.0,
    37: 8.5, 38: 8.5,
    35: 8.0, 36: 8.0,
    33: 7.5, 34: 7.5,
    30: 7.0, 31: 7.0, 32: 7.0,
    27: 6.5, 28: 6.5, 29: 6.5,
    23: 6.0, 24: 6.0, 25: 6.0, 26: 6.0,
    19: 5.5, 20: 5.5, 21: 5.5, 22: 5.5,
    15: 5.0, 16: 5.0, 17: 5.0, 18: 5.0,
    13: 4.5, 14: 4.5,
    10: 4.0, 11: 4.0, 12: 4.0,
    8:  3.5, 9:  3.5,
    6:  3.0, 7:  3.0,
    4:  2.5, 5:  2.5,
    3:  2.0,
    2:  1.5,
    1:  1.0,
    0:  0.0,
}

_GT_READING_TO_BAND: dict[int, float] = {
    40: 9.0,
    39: 8.5,
    37: 8.0, 38: 8.0,
    36: 7.5,
    34: 7.0, 35: 7.0,
    32: 6.5, 33: 6.5,
    30: 6.0, 31: 6.0,
    27: 5.5, 28: 5.5, 29: 5.5,
    23: 5.0, 24: 5.0, 25: 5.0, 26: 5.0,
    19: 4.5, 20: 4.5, 21: 4.5, 22: 4.5,
    15: 4.0, 16: 4.0, 17: 4.0, 18: 4.0,
    12: 3.5, 13: 3.5, 14: 3.5,
    9:  3.0, 10: 3.0, 11: 3.0,
    6:  2.5, 7:  2.5, 8:  2.5,
    4:  2.0, 5:  2.0,
    2:  1.5, 3:  1.5,
    1:  1.0,
    0:  0.0,
}


def raw_to_band_listening(raw: int) -> float:
    raw = max(0, min(raw, 40))
    for score in range(raw, -1, -1):
        if score in _LISTENING_RAW_TO_BAND:
            return _LISTENING_RAW_TO_BAND[score]
    return 0.0


def raw_to_band_reading(raw: int, is_academic: bool = True) -> float:
    raw = max(0, min(raw, 40))
    table = _ACADEMIC_READING_TO_BAND if is_academic else _GT_READING_TO_BAND
    for score in range(raw, -1, -1):
        if score in table:
            return table[score]
    return 0.0


def criteria_average_to_band(scores: list[float]) -> float:
    if not scores:
        return 0.0
    avg = sum(scores) / len(scores)
    return round(avg * 2) / 2


def calculate_overall_band(
    listening: float,
    reading: float,
    writing: float,
    speaking: float,
) -> float:
    avg = (listening + reading + writing + speaking) / 4
    return round(avg * 2) / 2


BAND_DESCRIPTIONS: dict[float, str] = {
    9.0: "Expert user – Full operational command of the language.",
    8.5: "Very good user – Occasional inaccuracies, handles complex arguments well.",
    8.0: "Very good user – Very good command, rare unsystematic errors.",
    7.5: "Good user – Operational command with occasional inaccuracies.",
    7.0: "Good user – Generally effective command despite some inaccuracies.",
    6.5: "Competent user – Effective command in familiar situations.",
    6.0: "Competent user – Generally effective despite some mistakes.",
    5.5: "Modest user – Partial command, understands overall meaning in most situations.",
    5.0: "Modest user – Partial command, copes with overall meaning.",
    4.5: "Limited user – Basic competence limited to familiar situations.",
    4.0: "Limited user – Frequent problems in understanding and expression.",
    3.5: "Extremely limited user – Conveys and understands general meaning only.",
    3.0: "Extremely limited user – Very frequent breakdowns in communication.",
    2.5: "Intermittent user – Great difficulty understanding spoken and written English.",
    2.0: "Intermittent user – No real communication possible.",
    1.5: "Non user – No ability to use the language except a few isolated words.",
    1.0: "Non user – Essentially no ability to use the language.",
    0.0: "Did not attempt the test.",
}


def get_band_description(band: float) -> str:
    for b in sorted(BAND_DESCRIPTIONS.keys(), reverse=True):
        if band >= b:
            return BAND_DESCRIPTIONS[b]
    return BAND_DESCRIPTIONS[0.0]
