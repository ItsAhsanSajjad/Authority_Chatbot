from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, Tuple


# ----------------------------
# Language detection (simple + deterministic)
# ----------------------------

DEVANAGARI_RE = re.compile(r"[\u0900-\u097F]")   # Hindi/Devanagari (unsupported)
URDU_ARABIC_RE = re.compile(r"[\u0600-\u06FF]")  # Urdu/Arabic script


def detect_language(text: str) -> str:
    """
    Returns: "english" | "urdu" | "roman_urdu" | "unsupported"
    """
    if DEVANAGARI_RE.search(text or ""):
        return "unsupported"
    if URDU_ARABIC_RE.search(text or ""):
        return "urdu"

    t = (text or "").lower()

    # Roman Urdu markers (deterministic heuristic)
    roman_markers = [
        "aoa", "a.o.a", "assalam", "salam", "salaam", "slm",
        "ap", "aap", "kya", "kaise", "kaisay", "kesy", "kese",
        "hain", "hai", "theek", "thik", "shukriya", "jazakallah",
        "haal", "hal"
    ]
    if any(re.search(rf"\b{re.escape(m)}\b", t) for m in roman_markers):
        return "roman_urdu"

    return "english"


# ----------------------------
# Normalize for latin matching
# ----------------------------

def _norm_latin(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9\s\.\-']", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


# ----------------------------
# Greeting / smalltalk patterns
# ----------------------------

_GREETING_PATTERNS_LATIN = [
    r"^(hi|hello|hey|hy)\b",
    r"^(aoa|a\.o\.a|assalam|assalamu|asalam|salam|salaam|slm)\b",
    r"^(good\s+morning|good\s+afternoon|good\s+evening)\b",
]

_SMALLTALK_PATTERNS_LATIN = [
    r"^(how\s+are\s+you|how\s+r\s+you|how\s+ru)\b",
    r"^(kya\s+haal|kya\s+hal|haal\s+chaal|hal\s+chaal)\b",
    r"^(ap\s+kaise|aap\s+kaise|kaise\s+hain|kaisay\s+hain|kesy\s+hain|kese\s+hain)\b",
    r"^(thanks|thank\s+you|thx|shukriya|jazakallah)\b",
]

_GREETING_PATTERNS_URDU = [
    r"^(السلام\s*علیکم|اسلام\s*علیکم|سلام)\b",
]
_SMALLTALK_PATTERNS_URDU = [
    r"^(آپ\s*کیسے\s*ہیں|آپ\s*کیسی\s*ہیں|کیا\s*حال\s*ہے|کیا\s*حال)\b",
    r"^(شکریہ|جزاک\s*اللہ)\b",
]


def _starts_with_any(patterns: list[str], text: str) -> bool:
    for p in patterns:
        if re.search(p, text):
            return True
    return False


def is_smalltalk_or_greeting(text: str) -> bool:
    """
    True if the message (or a remaining fragment) is greeting/smalltalk.
    Deterministic. No LLM.
    """
    raw = (text or "").strip()
    if not raw:
        return False

    # Urdu script
    if URDU_ARABIC_RE.search(raw):
        r = raw.strip()
        return _starts_with_any(_GREETING_PATTERNS_URDU + _SMALLTALK_PATTERNS_URDU, r)

    # Latin/roman/english
    t = _norm_latin(raw)
    return _starts_with_any(_GREETING_PATTERNS_LATIN + _SMALLTALK_PATTERNS_LATIN, t)


def is_greeting_prefix(text: str) -> bool:
    """
    True if the message starts with a greeting token (not necessarily smalltalk-only).
    """
    raw = (text or "").strip()
    if not raw:
        return False

    if URDU_ARABIC_RE.search(raw):
        return _starts_with_any(_GREETING_PATTERNS_URDU, raw.strip())

    t = _norm_latin(raw)
    return _starts_with_any(_GREETING_PATTERNS_LATIN, t)


# ----------------------------
# Greeting + question split
# ----------------------------

@dataclass
class SmalltalkDecision:
    is_greeting_only: bool
    ack: str                # brief greeting acknowledgment (for greet+question)
    response: str           # full deterministic response (for greeting-only)
    remaining_question: str # if greet+question, this is the question
    language: str


def split_greeting_and_question(text: str) -> Tuple[bool, str, str]:
    """
    Returns:
      (has_greeting_prefix, ack, remaining_question)

    - If no greeting prefix: (False, "", original text)
    - If greeting only: (True, ack, "")
    - If greeting + question: (True, ack, question part)
    """
    raw = (text or "").strip()
    if not raw:
        return False, "", ""

    lang = detect_language(raw)

    # Urdu greeting prefix
    if lang == "urdu":
        m = re.search(r"^(السلام\s*علیکم|اسلام\s*علیکم|سلام)\b", raw)
        if not m:
            return False, "", raw
        remaining = raw[m.end():].lstrip(" ,:-—–\n\t")
        ack = "وعلیکم السلام! "
        return True, ack, remaining

    # Latin greeting prefix
    t = _norm_latin(raw)
    greet_prefix_re = re.compile(
        r"^(hi|hello|hey|hy|aoa|a\.o\.a|slm|salam|salaam|assalam|assalamu|asalam|good\s+morning|good\s+afternoon|good\s+evening)\b",
        re.IGNORECASE,
    )
    m = greet_prefix_re.search(t)
    if not m:
        return False, "", raw

    first_token = m.group(0)
    remaining = re.sub(rf"^{re.escape(first_token)}", "", raw, flags=re.IGNORECASE).lstrip(" ,:-\u2014\u2013\n\t")
    # Strip punctuation-only noise (e.g., "Hello!" -> remaining = "!" -> "")
    remaining = re.sub(r"^[!?.,;:\s]+$", "", remaining).strip()

    token = first_token.lower()
    if any(x in token for x in ["aoa", "a.o.a", "slm", "salam", "salaam", "assalam", "assalamu", "asalam"]):
        ack = "Wa Alaikum Assalam! "
    else:
        ack = "Hello! "

    return True, ack, remaining


# ----------------------------
# Deterministic responses (no retrieval)
# ----------------------------

_RESPONSES = {
    "english": "Hello! I am the PERA AI Assistant. How can I help you with PERA-related questions?",
    "roman_urdu": "Hello! Main PERA AI Assistant hoon. Aap PERA se related sawal pooch sakte hain — main help kar doon ga.",
    "urdu": "سلام! میں PERA AI Assistant ہوں۔ آپ PERA سے متعلق سوالات پوچھ سکتے ہیں، میں آپ کی رہنمائی کر دوں گا/گی۔",
    "unsupported": "I currently support only English, Urdu, and Roman Urdu. Please ask in one of these languages.",
}


def decide_smalltalk(text: str) -> Optional[SmalltalkDecision]:
    """
    Main entry:
    - If greeting-only / smalltalk-only -> returns decision with response, is_greeting_only=True
    - If greeting + real question -> returns decision with ack + remaining_question
    - Else -> returns None (normal RAG path)
    """
    raw = (text or "").strip()
    if not raw:
        return SmalltalkDecision(
            is_greeting_only=True,
            ack="",
            response=_RESPONSES["english"],
            remaining_question="",
            language="english",
        )

    lang = detect_language(raw)
    if lang == "unsupported":
        return SmalltalkDecision(
            is_greeting_only=True,
            ack="",
            response=_RESPONSES["unsupported"],
            remaining_question="",
            language="unsupported",
        )

    has_greet, ack, remaining = split_greeting_and_question(raw)

    # ✅ CASE A: Entire message is greeting/smalltalk -> deterministic response
    # Examples: "hi", "hello", "how are you", "aoa", "kya haal", "آپ کیسے ہیں"
    if is_smalltalk_or_greeting(raw):
        # If it also has a remaining part after greeting split, check if that remaining is ALSO smalltalk
        # Example: "hi how are you" / "aoa kya haal" / "السلام علیکم آپ کیسے ہیں"
        if not remaining or not remaining.strip() or is_smalltalk_or_greeting(remaining.strip()):
            return SmalltalkDecision(
                is_greeting_only=True,
                ack="",
                response=_RESPONSES.get(lang, _RESPONSES["english"]),
                remaining_question="",
                language=lang,
            )

    # ✅ CASE B: Greeting + real question -> proceed with RAG on remaining
    # Example: "AOA what is section 3?" -> ack + remaining question
    if has_greet and remaining and remaining.strip():
        # If remaining starts like smalltalk, keep it greeting-only (already handled above),
        # otherwise treat as real question.
        if not is_smalltalk_or_greeting(remaining.strip()):
            return SmalltalkDecision(
                is_greeting_only=False,
                ack=ack,
                response="",
                remaining_question=remaining.strip(),
                language=lang,
            )

    # ✅ CASE C: Non-greeting query -> normal RAG path
    return None
