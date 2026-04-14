from __future__ import annotations

import os
import re
import json
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict

from dotenv import load_dotenv
load_dotenv()

from openai import OpenAI
from index_store import load_index_and_chunks, embed_texts, _normalize_vectors


# =============================================================================
# Active index pointer
# =============================================================================
class ActiveIndexPointer:
    def __init__(self, pointer_path: str = "assets/indexes/ACTIVE.json"):
        self.pointer_path = (pointer_path or "").replace("\\", "/")

    def read_raw(self) -> Optional[str]:
        if not self.pointer_path or not os.path.exists(self.pointer_path):
            return None
        try:
            with open(self.pointer_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            p = (data.get("active_index_dir") or "").strip()
            return p.replace("\\", "/") if p else None
        except Exception:
            return None


_ACTIVE_POINTER = ActiveIndexPointer(os.getenv("INDEX_POINTER_PATH", "assets/indexes/ACTIVE.json"))


def _dir_has_index_files(p: str) -> bool:
    """Directory is 'usable index' if it has faiss.index and non-empty chunks.jsonl."""
    try:
        if not p or not os.path.isdir(p):
            return False
        fa = os.path.join(p, "faiss.index")
        ch = os.path.join(p, "chunks.jsonl")
        return os.path.exists(fa) and os.path.exists(ch) and os.path.getsize(ch) > 0
    except Exception:
        return False


def _resolve_index_dir(index_dir: Optional[str]) -> str:
    """
    Priority:
      1) explicit index_dir arg if usable
      2) ACTIVE.json if usable
      3) env INDEX_DIR if usable
      4) assets/index if usable
      5) assets/index (even if empty)
    """
    if index_dir and str(index_dir).strip():
        p = str(index_dir).strip().replace("\\", "/")
        if _dir_has_index_files(p):
            return p

    ptr = _ACTIVE_POINTER.read_raw()
    if ptr and _dir_has_index_files(ptr):
        return ptr

    env_dir = os.getenv("INDEX_DIR", "").strip().replace("\\", "/")
    if env_dir and _dir_has_index_files(env_dir):
        return env_dir

    if _dir_has_index_files("assets/index"):
        return "assets/index"

    return "assets/index"


# =============================================================================
# Retriever configuration
# =============================================================================
TOP_K = int(os.getenv("RETRIEVER_TOP_K", "40"))

# Primary threshold: used when we have decent semantic signal
SIM_THRESHOLD = float(os.getenv("RETRIEVER_SIM_THRESHOLD", "0.18"))
STRONG_SIM_THRESHOLD = float(os.getenv("RETRIEVER_STRONG_SIM_THRESHOLD", "0.26"))

# Adaptive fallback when scores are low (prevents "no answers")
MIN_SIM_FLOOR = float(os.getenv("RETRIEVER_MIN_SIM_FLOOR", "0.06"))
FALLBACK_KEEP_TOPN = int(os.getenv("RETRIEVER_FALLBACK_KEEP_TOPN", "14"))

MAX_CHUNKS_PER_DOC = int(os.getenv("RETRIEVER_MAX_CHUNKS_PER_DOC", "15"))  # Increased from 6
MAX_DOCS_RETURNED = int(os.getenv("RETRIEVER_MAX_DOCS_RETURNED", "8"))      # Increased from 4

# Soft gating controls
KEEP_TOP_SEMANTIC_PER_DOC = int(os.getenv("RETRIEVER_KEEP_TOP_SEMANTIC_PER_DOC", "2"))
RELATIVE_DOC_SCORE_KEEP = float(os.getenv("RETRIEVER_RELATIVE_DOC_SCORE_KEEP", "0.80"))

QUERY_VARIANTS_ENABLED = os.getenv("RETRIEVER_QUERY_VARIANTS_ENABLED", "1").strip() != "0"
MAX_QUERY_VARIANTS = int(os.getenv("RETRIEVER_MAX_QUERY_VARIANTS", "3"))

LEX_FALLBACK_ENABLED = os.getenv("RETRIEVER_LEX_FALLBACK_ENABLED", "1").strip() != "0"
LEX_FALLBACK_MAX = int(os.getenv("RETRIEVER_LEX_FALLBACK_MAX", "80"))
LEX_FALLBACK_PER_DOC = int(os.getenv("RETRIEVER_LEX_FALLBACK_PER_DOC", "3"))

CRITERIA_DOC_PRIORITIZATION = os.getenv("RETRIEVER_CRITERIA_DOC_PRIORITIZATION", "1").strip() != "0"
CRITERIA_MIN_DOCS = int(os.getenv("RETRIEVER_CRITERIA_MIN_DOCS", "2"))

# Spell correction
SPELL_CORRECTION_ENABLED = os.getenv("RETRIEVER_SPELL_CORRECTION_ENABLED", "1").strip() != "0"
SPELL_MAX_TOKEN_FIXES = int(os.getenv("RETRIEVER_SPELL_MAX_TOKEN_FIXES", "2"))
SPELL_EDIT_DISTANCE = int(os.getenv("RETRIEVER_SPELL_EDIT_DISTANCE", "2"))
MAX_QUERY_VARIANTS_WITH_SPELL = int(os.getenv("RETRIEVER_MAX_QUERY_VARIANTS_WITH_SPELL", "5"))

# LLM rewrite controls
LLM_REWRITE_ENABLED = os.getenv("RETRIEVER_LLM_QUERY_REWRITE_ENABLED", "1").strip() != "0"
LLM_REWRITE_ALWAYS = os.getenv("RETRIEVER_LLM_QUERY_REWRITE_ALWAYS", "1").strip() != "0"
LLM_REWRITE_MODEL = os.getenv("RETRIEVER_LLM_QUERY_REWRITE_MODEL", "gpt-4.1-mini")
LLM_REWRITE_MAX = int(os.getenv("RETRIEVER_LLM_QUERY_REWRITE_MAX", "3"))

# Deterministic reranking weights (semantic dominates; lexical stabilizes)
RERANK_ENABLED = os.getenv("RETRIEVER_RERANK_ENABLED", "1").strip() != "0"
RERANK_ALPHA = float(os.getenv("RETRIEVER_RERANK_ALPHA", "0.75"))  # semantic
RERANK_BETA = float(os.getenv("RETRIEVER_RERANK_BETA", "0.25"))    # lexical

DEBUG = os.getenv("RETRIEVER_DEBUG", "0").strip() != "0"


# =============================================================================
# Keyword extraction / normalization
# =============================================================================
_STOPWORDS = {
    "the", "a", "an", "is", "are", "was", "were", "to", "of", "and", "or",
    "in", "on", "at", "for", "from", "by", "with", "about", "tell", "me",
    "who", "what", "when", "where", "why", "how", "please",
}

_INTENT_STOP = {
    "role", "roles", "duty", "duties", "function", "functions", "responsibility",
    "responsibilities", "tor", "tors", "term", "terms", "reference",
    "criteria", "criterion", "eligibility", "eligible", "qualification", "qualifications",
    "experience", "education", "minimum", "required", "requirement", "requirements",
    "position", "positions", "post", "posts", "job", "jobs", "main", "most",
}

_KEEP_SHORT = {"ai", "ml", "it", "hr", "ppra", "ipo", "cto", "tor", "tors", "dg", "pera", "eo", "io", "sso"}

# ✅ Expanded abbreviations/aliases (your missing production pain points)
_ABBREV_MAP = {
    "cto": "chief technology officer",
    "tor": "terms of reference",
    "tors": "terms of reference",
    "dg": "director general",
    "hr": "human resource",
    "it": "information technology",
    "mgr": "manager",
    "dev": "development",
    "sr": "senior",
    "jr": "junior",

    # roles commonly asked in govt org contexts
    "eo": "enforcement officer",
    "io": "investigation officer",
    "sso": "senior staff officer",
}

_COMMON_MISSPELLINGS = {
    "pira": "pera",
    "perra": "pera",
    "peera": "pera",
    "peraa": "pera",
    "peraah": "pera",
    "pehra": "pera",
}


def _expand_abbrev(s: str) -> str:
    t = (s or "").lower()
    for k, v in _ABBREV_MAP.items():
        t = re.sub(rf"\b{re.escape(k)}\b", v, t)
    return t


def _normalize_text(s: str) -> str:
    """
    Keep Urdu/Arabic script. Normalize punctuation away for matching.
    """
    s = _expand_abbrev(s or "")
    s = s.lower()
    s = re.sub(r"[^a-z0-9\u0600-\u06FF\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    for k, v in _COMMON_MISSPELLINGS.items():
        s = re.sub(rf"\b{re.escape(k)}\b", v, s)
    return s


def _stem_token(t: str) -> str:
    t = (t or "").strip().lower()
    if len(t) <= 3:
        return t
    if t.endswith("'s"):
        t = t[:-2]
    if t.endswith("ies") and len(t) > 4:
        return t[:-3] + "y"
    if t.endswith("es") and len(t) > 4:
        return t[:-2]
    if t.endswith("s") and not t.endswith("ss") and len(t) > 4:
        return t[:-1]
    return t


def _tokenize_for_overlap(s: str) -> List[str]:
    q = _normalize_text(s)
    toks: List[str] = []
    for raw in q.split():
        if not raw:
            continue
        if raw in _STOPWORDS:
            continue
        if len(raw) >= 3 or raw in _KEEP_SHORT:
            toks.append(_stem_token(raw))
    return toks


def _extract_keywords(question: str) -> List[str]:
    toks = _tokenize_for_overlap(question)
    seen = set()
    out: List[str] = []
    for t in toks:
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out[:12]


def _entity_keywords(question: str) -> List[str]:
    toks = _tokenize_for_overlap(question)
    ent: List[str] = []
    seen = set()
    for t in toks:
        if t in _INTENT_STOP:
            continue
        if t in seen:
            continue
        seen.add(t)
        ent.append(t)
    return ent[:10]


def _keyword_overlap_count(keywords: List[str], text: str) -> int:
    if not keywords:
        return 0
    kw_set = set(keywords)
    text_tokens = set(_tokenize_for_overlap(text))
    return len(kw_set.intersection(text_tokens))


def _rows_by_id(rows: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        for k in ("id", "chunk_id"):
            try:
                v = r.get(k, None)
                if v is None:
                    continue
                out[int(v)] = r
                break
            except Exception:
                continue
    return out


def _load_index_rows(index_dir: str):
    """
    Robust against load_index_and_chunks returning:
      (index, rows) OR (index, rows, meta)
    """
    try:
        res = load_index_and_chunks(index_dir=index_dir)  # type: ignore[arg-type]
    except TypeError:
        res = load_index_and_chunks()

    if isinstance(res, tuple):
        if len(res) == 2:
            return res[0], res[1], None
        if len(res) >= 3:
            return res[0], res[1], res[2]
    return None, [], None


# =============================================================================
# Query type detection (tightened)
# =============================================================================
# ✅ Don’t trigger schedule mode just because a digit exists.
_SCHEDULE_PAT = re.compile(r"\b(schedule|scheduled\s+laws|annex(ure)?|appendix)\b", re.I)
_YESNO_PAT = re.compile(r"^(is|are|was|were|do|does|did|can|could|should|will|would|has|have|had)\b", re.I)
_DEF_PAT = re.compile(r"^(what\s+is|define|meaning\s+of)\b|(\bwhat\s+is\b.*\bpera\b)", re.I)

def _is_short_query(q: str) -> bool:
    toks = [t for t in _tokenize_for_overlap(q) if t]
    return len(toks) <= 4

def _is_schedule_query(q: str) -> bool:
    return _SCHEDULE_PAT.search((q or "")) is not None

def _is_yesno_query(q: str) -> bool:
    return _YESNO_PAT.search((q or "").strip().lower()) is not None

def _is_definition_query(q: str) -> bool:
    return _DEF_PAT.search((q or "").strip()) is not None


# =============================================================================
# Spell correction
# =============================================================================
_VOCAB_CACHE: Optional[set] = None
_VOCAB_INDEX_DIR: Optional[str] = None


def _build_vocab_from_rows(rows: List[Dict[str, Any]], limit_rows: int = 50000) -> set:
    vocab = set()
    n = 0
    for r in rows:
        if not r.get("active", True):
            continue
        txt = (r.get("text") or "")
        if not txt:
            continue
        for t in _tokenize_for_overlap(txt):
            if len(t) >= 3 or t in _KEEP_SHORT:
                vocab.add(t)
        n += 1
        if n >= limit_rows:
            break

    vocab.update({
        "pera", "authority", "complaint", "grievance", "hearing", "officer",
        "vision", "mission", "purpose", "objectives", "mandate",
        "terms", "reference", "duties", "responsibilities",
        "manager", "monitoring",
        "schedule", "scheduled", "chairperson", "vice", "secretary",

        # ensure role aliases exist in vocab
        "enforcement", "investigation", "senior", "staff",
    })
    return vocab


def _levenshtein_lte_k(a: str, b: str, k: int) -> Optional[int]:
    if a == b:
        return 0
    la, lb = len(a), len(b)
    if abs(la - lb) > k:
        return None
    if la == 0:
        return lb if lb <= k else None
    if lb == 0:
        return la if la <= k else None

    prev = list(range(lb + 1))
    for i, ca in enumerate(a, start=1):
        cur = [i] + [0] * lb
        row_min = cur[0]
        for j, cb in enumerate(b, start=1):
            cost = 0 if ca == cb else 1
            cur[j] = min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + cost)
            if cur[j] < row_min:
                row_min = cur[j]
        if row_min > k:
            return None
        prev = cur
    return prev[lb] if prev[lb] <= k else None


def _spell_correct_query_variant(question: str, vocab: set) -> Optional[str]:
    if not SPELL_CORRECTION_ENABLED:
        return None

    qn = _normalize_text(question)
    if not qn:
        return None

    toks = qn.split()
    if len(toks) == 0 or len(toks) > 24:
        return None

    corrected: List[str] = []
    changed = 0
    k = max(1, int(SPELL_EDIT_DISTANCE))

    for t in toks:
        if t.isdigit() or (len(t) < 4 and t not in _KEEP_SHORT) or t in _STOPWORDS or t in _KEEP_SHORT:
            corrected.append(t)
            continue

        st = _stem_token(t)
        if t in vocab or st in vocab:
            corrected.append(t)
            continue

        first = t[0]
        lt = len(t)
        best = None
        best_d = k + 1

        # bounded scan: vocab can be huge; first-letter filter keeps it fast
        for w in vocab:
            if not w:
                continue
            if w[0] != first:
                continue
            lw = len(w)
            if abs(lw - lt) > k:
                continue

            d = _levenshtein_lte_k(t, w, k)
            if d is None:
                continue
            if d < best_d:
                best_d = d
                best = w
                if d == 1:
                    break

        if best and best_d <= k:
            corrected.append(best)
            changed += 1
            if changed >= max(1, int(SPELL_MAX_TOKEN_FIXES)):
                corrected.extend(toks[len(corrected):])
                break
        else:
            corrected.append(t)

    if changed == 0:
        return None

    out = " ".join(corrected).strip()
    return out if out and out != qn else None


# =============================================================================
# Intent patterns + expansions
# =============================================================================
_COMPOSITION_PAT = re.compile(r"\b(composition|constitut|constitution|constitute|consist|comprise|members?|authority)\b", re.I)
_CRITERIA_PAT = re.compile(r"\b(criteria|criterion|eligib|qualification|qualify|required|requirement|experience|education|minimum|degree|age|skills?)\b", re.I)
_COMPLAINT_PAT = re.compile(r"\b(complaint|complain|grievance|petition|hearing|hearing officer|appeal)\b", re.I)
_VISION_PAT = re.compile(r"\b(vision|mission|objective|objectives|purpose|aim|aims|mandate)\b", re.I)
_ROLE_PAT = re.compile(r"\b(role|roles|tor|tors|terms of reference|duty|duties|responsibil|function|job description)\b", re.I)

_COMPOSITION_PHRASES = [
    "authority shall consist of",
    "constitution of the authority",
    "members of the authority",
    "the authority shall consist",
    "shall consist of the following members",
]
_CRITERIA_PHRASES = [
    "eligibility criteria",
    "minimum qualification",
    "qualification and experience",
    "required qualification",
    "experience required",
    "education",
    "age limit",
    "skills",
]
_COMPLAINT_PHRASES = [
    "file a complaint",
    "how can i file a complaint",
    "public complaint",
    "complaints and hearings",
    "complaint with pera",
    "hearing officer",
    "what happens after i file a complaint",
]
_VISION_PHRASES = [
    "purpose of the authority",
    "purpose of pera",
    "objectives",
    "functions of the authority",
    "mandate",
    "established to",
    "for the purpose of",
]
_ROLE_PHRASES = [
    "terms of reference",
    "job description",
    "duties and responsibilities",
    "responsibilities include",
    "shall be responsible for",
]

def _intent_extra_keywords(question: str) -> List[str]:
    q = _normalize_text(question)
    extras: List[str] = []

    if _COMPOSITION_PAT.search(q):
        extras.extend(["shall", "consist", "comprise", "constitution", "member", "chairperson", "vice", "secretary", "include", "following"])
    if _CRITERIA_PAT.search(q):
        extras.extend(["eligibility", "criteria", "qualification", "experience", "education", "minimum", "required", "requirement", "age", "degree"])
    if _COMPLAINT_PAT.search(q):
        extras.extend(["complaint", "complaints", "hearing", "hearing officer", "file", "submit", "procedure", "process"])
    if _VISION_PAT.search(q):
        extras.extend(["purpose", "objectives", "functions", "mandate", "aim", "established"])
    if _ROLE_PAT.search(q):
        extras.extend(["terms", "reference", "duties", "responsibilities", "job", "description", "reports", "reporting", "manager"])

    if _is_schedule_query(question):
        extras.extend(["schedule", "scheduled", "annex", "annexure", "appendix", "section"])

    if _is_definition_query(question):
        extras.extend(["pera", "authority", "punjab", "enforcement", "regulatory"])

    out: List[str] = []
    seen = set()
    for e in extras:
        se = _stem_token(e)
        if se in seen:
            continue
        seen.add(se)
        out.append(se)
    return out


def _swap_two_word_title(ent: List[str]) -> str:
    if len(ent) != 2:
        return ""
    a, b = ent[0], ent[1]
    if not a or not b:
        return ""
    return f"{b} {a}".strip()


def _build_query_variants(question: str) -> List[str]:
    q = (question or "").strip()
    if not q:
        return [""]

    variants: List[str] = [q]

    qn = _normalize_text(q)
    if qn and qn != _normalize_text(variants[0]):
        variants.append(qn)

    # Add PERA context if missing
    if qn and "pera" not in qn.split():
        variants.append((q + " in PERA").strip())

    if "pera" in qn.split() or _is_definition_query(q):
        variants.append("Punjab Enforcement and Regulatory Authority PERA")
        variants.append("Punjab Enforcement and Regulation Act PERA")

    if _is_schedule_query(q):
        variants.append("scheduled laws schedule PERA")
        variants.append("Schedule II Punjab Enforcement and Regulation Act")
        variants.append("Schedule III Punjab Enforcement and Regulation Act")

    if _COMPLAINT_PAT.search(qn):
        variants.append("how can i file a complaint with pera procedure")
        variants.append("public complaints and hearings hearing officer process")
    if _VISION_PAT.search(qn):
        variants.append("purpose objectives functions mandate of pera")
        variants.append("objects and purposes for which the authority is established")
    if _ROLE_PAT.search(qn):
        variants.append("terms of reference duties and responsibilities job description in pera")

    ent = _entity_keywords(q)
    ent_phrase = " ".join(ent[:4]).strip()

    if _COMPOSITION_PAT.search(qn):
        variants.append("authority shall consist of the following members chairperson vice chairperson secretary member")
        variants.append("constitution of the authority members of the authority")

    if _CRITERIA_PAT.search(qn):
        variants.append(f"{q} eligibility criteria qualification experience")
        variants.append(f"{q} minimum qualification experience required")

    if _ROLE_PAT.search(qn) and ent_phrase:
        variants.append(f"terms of reference of {ent_phrase} in PERA")
        variants.append(f"duties and responsibilities of {ent_phrase} in PERA")
        variants.append(f"job description of {ent_phrase} in PERA")

    swapped = _swap_two_word_title(ent[:2])
    if swapped:
        variants.append(f"{swapped} job description duties responsibilities in PERA")

    # Manager specific variant: "Manager Development" -> "Manager (Development)"
    if "manager" in qn.split():
        m = re.search(r"manager\s+([a-z]+)", qn)
        if m:
            dept = m.group(1)
            variants.append(f"Manager ({dept})")
            variants.append(f"Manager of {dept}")

    out: List[str] = []
    seen = set()
    for v in variants:
        v = (v or "").strip()
        if not v:
            continue
        k = v.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(v)

    return out[:max(1, MAX_QUERY_VARIANTS)]


def _dedup_and_cap_queries(queries: List[str], cap: int) -> List[str]:
    out: List[str] = []
    seen = set()
    for q in queries:
        q = (q or "").strip()
        if not q:
            continue
        k = q.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(q)
        if len(out) >= cap:
            break
    return out


# =============================================================================
# Lexical fallback helpers
# =============================================================================
def _row_text_for_matching(r: Dict[str, Any]) -> str:
    a = (r.get("text") or "")
    b = (r.get("search_text") or "")
    if b and b not in a:
        return a + "\n" + b
    return a


def _definition_lex_fallback(rows: List[Dict[str, Any]]) -> Dict[int, float]:
    if not LEX_FALLBACK_ENABLED:
        return {}

    phrases = [
        "punjab enforcement and regulatory authority",
        "punjab enforcement and regulation act",
        "enforcement and regulatory authority",
        " p e r a ",
        " pera ",
    ]

    best: Dict[int, float] = {}
    per_doc_counts: Dict[str, int] = defaultdict(int)

    for r in rows:
        if not r.get("active", True):
            continue
        text = _row_text_for_matching(r)
        if not text:
            continue

        tl = " " + (text or "").lower() + " "
        if not any(p in tl for p in phrases):
            continue

        doc_name = r.get("doc_name", "Unknown document")
        if per_doc_counts[doc_name] >= max(1, LEX_FALLBACK_PER_DOC):
            continue

        try:
            cid = int(r.get("id", r.get("chunk_id")))
        except Exception:
            continue

        best[cid] = max(best.get(cid, 0.0), 0.70)
        per_doc_counts[doc_name] += 1

        if len(best) >= max(10, min(LEX_FALLBACK_MAX, 60)):
            break

    return best


def _lexical_fallback_hits(rows: List[Dict[str, Any]], all_keywords: List[str], entity_kw: List[str], question: str) -> Dict[int, float]:
    if not LEX_FALLBACK_ENABLED:
        return {}

    qn = _normalize_text(question)
    want_def = _is_definition_query(question)
    want_comp = _COMPOSITION_PAT.search(qn) is not None
    want_criteria = _CRITERIA_PAT.search(qn) is not None
    want_complaint = _COMPLAINT_PAT.search(qn) is not None
    want_vision = _VISION_PAT.search(qn) is not None
    want_role = _ROLE_PAT.search(qn) is not None
    want_schedule = _is_schedule_query(question)

    if want_def:
        return _definition_lex_fallback(rows)

    if not (want_comp or want_criteria or want_complaint or want_vision or want_role or want_schedule):
        return {}

    if want_schedule:
        phrases = ["scheduled laws", "schedule", "annex", "appendix", "annexure"]
    elif want_comp:
        phrases = _COMPOSITION_PHRASES
    elif want_criteria:
        phrases = _CRITERIA_PHRASES
    elif want_complaint:
        phrases = _COMPLAINT_PHRASES
    elif want_vision:
        phrases = _VISION_PHRASES
    else:
        phrases = _ROLE_PHRASES

    best: Dict[int, float] = {}
    per_doc_counts: Dict[str, int] = defaultdict(int)

    for r in rows:
        if not r.get("active", True):
            continue

        text = _row_text_for_matching(r)
        if not text:
            continue

        tn = _normalize_text(text)
        phrase_hit = any(p in tn for p in phrases)

        overlap_all = _keyword_overlap_count(all_keywords, text)
        overlap_ent = _keyword_overlap_count(entity_kw, text) if entity_kw else 0

        if want_schedule:
            if not phrase_hit and overlap_all < 1:
                continue
        elif want_complaint or want_vision or want_role:
            if not phrase_hit and overlap_all < 1 and overlap_ent < 1:
                continue
        else:
            if entity_kw and overlap_ent < 1 and not phrase_hit:
                continue
            if not phrase_hit and overlap_all < 2:
                continue

        doc_name = r.get("doc_name", "Unknown document")
        if per_doc_counts[doc_name] >= LEX_FALLBACK_PER_DOC:
            continue

        try:
            cid = int(r.get("id", r.get("chunk_id")))
        except Exception:
            continue

        pseudo = 0.40 + min(0.28, 0.05 * overlap_all)
        if phrase_hit:
            pseudo = max(pseudo, 0.62)

        prev = best.get(cid)
        if prev is None or pseudo > prev:
            best[cid] = pseudo
            per_doc_counts[doc_name] += 1

        if len(best) >= LEX_FALLBACK_MAX:
            break

    return best


# =============================================================================
# LLM rewrite (best-effort, never required)
# =============================================================================
def _client() -> OpenAI:
    key = os.getenv("OPENAI_API_KEY", "").strip()
    if not key:
        raise RuntimeError("OPENAI_API_KEY is missing. Ensure .env is present and loaded.")
    return OpenAI(api_key=key)


def _llm_rewrite_queries(question: str) -> List[str]:
    if not (LLM_REWRITE_ENABLED and LLM_REWRITE_ALWAYS):
        return []

    q = (question or "").strip()
    if not q:
        return []

    system = (
        "You rewrite user questions into search-friendly queries for a PERA document chatbot.\n"
        "You must NOT answer.\n"
        "Return ONLY valid JSON: {\"queries\":[\"...\",\"...\"]}\n"
        "Rules:\n"
        "1) Fix spelling (pira/peera/perra -> PERA).\n"
        "2) Expand abbreviations: TOR->terms of reference.\n"
        "3) Add 'PERA' context if missing.\n"
        "4) Produce 1-3 short variants maximum.\n"
        "5) No extra text."
    )

    user = f'User question: "{q}"'

    try:
        resp = _client().chat.completions.create(
            model=LLM_REWRITE_MODEL,
            temperature=0.0,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        ).choices[0].message.content.strip()

    except Exception:
        return []

    try:
        data = json.loads(resp)
        queries = data.get("queries") or []
        if not isinstance(queries, list):
            return []
        cleaned: List[str] = []
        for x in queries:
            if isinstance(x, str) and x.strip():
                cleaned.append(x.strip())
        return _dedup_and_cap_queries(cleaned, cap=max(1, LLM_REWRITE_MAX))
    except Exception:
        return []


def rewrite_contextual_query(current_query: str, last_question: str, last_answer: str) -> str:
    """
    Uses LLM to rewrite a follow-up question into a standalone search query.
    Critical for fixing the 'Regarding: ...' pollution issue.
    """
    q = (current_query or "").strip()
    lq = (last_question or "").strip()
    
    # If no context, return as-is
    if not lq:
        return q

    # If current query is already long/specific, maybe don't rewrite? 
    # But often users say "what about for X", so saftey checks are minimal here.

    system = (
        "You rewrite follow-up questions into standalone search queries.\n"
        "CRITICAL: If the user is continuing a topic (e.g., they asked about CTO, now asking 'can he arrest?'), "
        "you MUST include the subject (CTO) in the rewritten query.\n"
        "Rules:\n"
        "1) Replace ALL pronouns/implicit subjects with the actual entity from history.\n"
        "2) If follow-up mentions an action without subject, ADD the subject from history (e.g., 'arrest kr skta?' → 'CTO arrest kr skta hai?').\n"
        "3) Keep the language (English/Urdu/Roman Urdu) intact.\n"
        "4) Return ONLY the rewritten query. No explanation.\n"
        "5) If already standalone, return unchanged.\n"
    )

    # Truncate last answer to avoid token waste/noise, we mainly need the entities
    la_short = (last_answer or "")[:500] 

    user = (
        f"Last Question: {lq}\n"
        f"Last Answer Snippet: {la_short}...\n"
        f"Current Follow-up: {q}\n"
        "\nRewritten Standalone Query:"
    )

    try:
        resp = _client().chat.completions.create(
            model=LLM_REWRITE_MODEL, # Reuse the same config
            temperature=0.0,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        ).choices[0].message.content.strip()
        
        # Fallback if empty
        if not resp:
            return q
            
        return resp
    except Exception:
        return q



# =============================================================================
# Criteria prioritization (kept)
# =============================================================================
def _criteria_doc_signal_score(doc: Dict[str, Any]) -> float:
    hits = doc.get("hits", []) or []
    if not hits:
        return 0.0
    h0 = hits[0]
    txt = _normalize_text(h0.get("text") or "")
    phrase_hits = sum(1 for p in _CRITERIA_PHRASES if p in txt)
    score = float(h0.get("score", 0.0) or 0.0)
    overlap = int(h0.get("overlap", 0) or 0)
    return (phrase_hits * 10.0) + (overlap * 1.5) + (score * 1.0)


def _apply_criteria_doc_prioritization(question: str, evidence_docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not CRITERIA_DOC_PRIORITIZATION:
        return evidence_docs
    qn = _normalize_text(question)
    if _CRITERIA_PAT.search(qn) is None:
        return evidence_docs

    scored = [(ed, _criteria_doc_signal_score(ed)) for ed in evidence_docs]
    scored.sort(key=lambda x: x[1], reverse=True)

    kept = [x[0] for x in scored[:max(CRITERIA_MIN_DOCS, 1)]]
    kept_names = {d.get("doc_name") for d in kept}

    for ed in evidence_docs:
        if ed.get("doc_name") in kept_names:
            continue
        kept.append(ed)
        kept_names.add(ed.get("doc_name"))
        if len(kept) >= MAX_DOCS_RETURNED:
            break

    return kept[:MAX_DOCS_RETURNED]


# =============================================================================
# Helpers: FAISS safe search + canonical reference path
# =============================================================================
def _canonical_public_path(doc_name: str) -> str:
    dn = (doc_name or "").strip()
    return f"/assets/data/{dn}".replace("\\", "/") if dn else "/assets/data"


def _safe_faiss_k(idx: Any, want_k: int) -> int:
    try:
        ntotal = int(getattr(idx, "ntotal", 0) or 0)
    except Exception:
        ntotal = 0
    if ntotal <= 0:
        return 0
    return max(1, min(int(want_k), ntotal))


def _faiss_dim(idx: Any) -> Optional[int]:
    try:
        d = getattr(idx, "d", None)
        return int(d) if d is not None else None
    except Exception:
        return None


# =============================================================================
# Deterministic reranking (stabilizes phrasing variance)
# =============================================================================
def _rerank_hits(question: str, hits: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not (RERANK_ENABLED and hits):
        return hits

    qn = _normalize_text(question)
    qset = set(qn.split())

    for h in hits:
        txt = (h.get("text") or "")
        stxt = (h.get("search_text") or "")
        tn = _normalize_text(txt + "\n" + stxt)
        tset = set(tn.split())
        ov = len(qset.intersection(tset))
        # cap overlap to keep it bounded
        ov_cap = min(12, ov)

        sem = float(h.get("score", 0.0) or 0.0)
        blend = (RERANK_ALPHA * sem) + (RERANK_BETA * (ov_cap / 12.0))

        h["_lex_ov"] = int(ov)
        h["_blend"] = float(blend)

    hits.sort(
        key=lambda x: (
            float(x.get("_blend", 0.0)),
            int(x.get("_lex_ov", 0)),
            int(x.get("doc_rank", 0) or 0),
            str(x.get("doc_name", "")),
            int(x.get("id", 0) or 0),
        ),
        reverse=True
    )
    return hits


# =============================================================================
# Main retrieval
# =============================================================================
def retrieve(question: str, index_dir: Optional[str] = None) -> Dict[str, Any]:
    resolved_dir = _resolve_index_dir(index_dir)

    empty: Dict[str, Any] = {
        "question": question,
        "has_evidence": False,
        "primary_doc": None,
        "primary_doc_rank": 0,
        "evidence": [],
    }

    question = (question or "").strip()
    if not question:
        return empty

    try:
        idx, rows, meta = _load_index_rows(index_dir=resolved_dir)
        if idx is None or not rows:
            if DEBUG:
                empty["debug"] = {"index_dir": resolved_dir, "note": "Index or chunks missing/empty."}
            return empty

        active_rows = [r for r in rows if r.get("active", True)]
        if not active_rows:
            if DEBUG:
                empty["debug"] = {"index_dir": resolved_dir, "rows": len(rows), "active_rows": 0}
            return empty

        global _VOCAB_CACHE, _VOCAB_INDEX_DIR
        if SPELL_CORRECTION_ENABLED and (_VOCAB_CACHE is None or _VOCAB_INDEX_DIR != resolved_dir):
            _VOCAB_CACHE = _build_vocab_from_rows(active_rows)
            _VOCAB_INDEX_DIR = resolved_dir

        id_to_row = _rows_by_id(rows)

        # Build queries (best effort, never required)
        llm_queries = _llm_rewrite_queries(question) if (LLM_REWRITE_ENABLED and LLM_REWRITE_ALWAYS) else []

        queries: List[str] = []
        if llm_queries:
            queries.extend(llm_queries)

        queries.append(question)

        if QUERY_VARIANTS_ENABLED:
            queries.extend(_build_query_variants(question))

        qn = _normalize_text(question)
        if qn:
            queries.append(qn)

        if SPELL_CORRECTION_ENABLED and _VOCAB_CACHE:
            corrected = _spell_correct_query_variant(question, _VOCAB_CACHE)
            if corrected:
                queries.append(corrected)

        queries = _dedup_and_cap_queries(queries, cap=max(1, MAX_QUERY_VARIANTS_WITH_SPELL))

        entity_kw = _entity_keywords(question)
        base_kw = _extract_keywords(question)
        extras_kw = _intent_extra_keywords(question)
        all_kw = base_kw + [k for k in extras_kw if k not in base_kw]

        # Semantic search
        q_vecs = embed_texts(queries)
        q_vecs = _normalize_vectors(q_vecs)

        # Dim safety
        d_idx = _faiss_dim(idx)
        if d_idx is not None and q_vecs.ndim == 2 and q_vecs.shape[1] != d_idx:
            if DEBUG:
                empty["debug"] = {
                    "index_dir": resolved_dir,
                    "note": "FAISS dim mismatch (index vs query embeddings). Rebuild index.",
                    "faiss_dim": d_idx,
                    "query_dim": int(q_vecs.shape[1]),
                }
            return empty

        k = _safe_faiss_k(idx, TOP_K)
        if k <= 0:
            if DEBUG:
                empty["debug"] = {"index_dir": resolved_dir, "note": "FAISS index has ntotal=0."}
            return empty

        scores_mat, ids_mat = idx.search(q_vecs, k)

        # Collect best score per chunk_id
        best_by_id: Dict[int, float] = {}
        all_scored_pairs: List[Tuple[int, float]] = []

        for qi in range(len(queries)):
            scores = scores_mat[qi].tolist()
            ids = ids_mat[qi].tolist()
            for score, vid in zip(scores, ids):
                if vid == -1:
                    continue
                s = float(score)
                vid_i = int(vid)
                all_scored_pairs.append((vid_i, s))
                prev = best_by_id.get(vid_i)
                if prev is None or s > prev:
                    best_by_id[vid_i] = s

        # Apply SIM_THRESHOLD normally; but if nothing passes, use adaptive fallback top-N
        passed = {cid: sc for cid, sc in best_by_id.items() if sc >= SIM_THRESHOLD}
        adaptive_used = False
        if passed:
            best_by_id = passed
        else:
            adaptive_used = True
            all_scored_pairs.sort(key=lambda x: x[1], reverse=True)
            tmp: Dict[int, float] = {}
            for cid, sc in all_scored_pairs:
                if sc < MIN_SIM_FLOOR:
                    break
                if cid not in tmp:
                    tmp[cid] = sc
                if len(tmp) >= max(6, FALLBACK_KEEP_TOPN):
                    break
            best_by_id = tmp

        # Lex fallback (adds recall)
        lex_best = _lexical_fallback_hits(active_rows, all_kw, entity_kw, question)
        for cid, pseudo in lex_best.items():
            prev = best_by_id.get(cid)
            if prev is None or pseudo > prev:
                best_by_id[cid] = pseudo

        if not best_by_id:
            if DEBUG:
                empty["debug"] = {
                    "index_dir": resolved_dir,
                    "rows": len(rows),
                    "active_rows": len(active_rows),
                    "faiss_dim": d_idx,
                    "queries_used": queries,
                    "llm_queries": llm_queries,
                    "note": "No hits (semantic+lex).",
                }
            return empty

        # Build hit list with provenance (canonical references)
        hits: List[Dict[str, Any]] = []
        for vid_i, score in best_by_id.items():
            r = id_to_row.get(int(vid_i))
            if not r or not r.get("active", True):
                continue

            text_for_overlap = _row_text_for_matching(r)
            overlap_entity = _keyword_overlap_count(entity_kw, text_for_overlap) if entity_kw else 0
            overlap_all = _keyword_overlap_count(all_kw, text_for_overlap)

            overlap = overlap_entity if entity_kw else overlap_all

            doc_name = r.get("doc_name", "Unknown document")
            doc_rank = int(r.get("doc_rank", 0) or 0)

            # canonical public path
            public_path = (r.get("public_path") or "").strip()
            if not public_path:
                public_path = (r.get("path") or "").strip()
            if not public_path:
                public_path = _canonical_public_path(doc_name)
            public_path = public_path.replace("\\", "/")

            loc_kind = (r.get("loc_kind", "") or "").strip()
            loc_start = r.get("loc_start")
            loc_end = r.get("loc_end")

            # Back-compat: if schema uses page but not loc_*
            if (not loc_kind) and (r.get("page") is not None):
                loc_kind = "page"
                loc_start = r.get("page")
                loc_end = r.get("page")

            hits.append({
                "id": int(vid_i),
                "chunk_id": int(vid_i),
                "score": float(score),
                "overlap": int(overlap),
                "doc_name": doc_name,
                "doc_rank": doc_rank,
                "text": (r.get("text") or ""),
                "search_text": (r.get("search_text") or ""),
                "source_type": r.get("source_type", ""),
                "loc_kind": loc_kind,
                "loc_start": loc_start,
                "loc_end": loc_end,
                "page_start": int(loc_start) if loc_start is not None else None, # key fix for UI

                "public_path": public_path,
                "path": public_path,  # backward compatibility

                "overlap_all": int(overlap_all),
                "overlap_entity": int(overlap_entity),

                "reference": {
                    "doc_name": doc_name,
                    "public_path": public_path,
                    "loc_kind": loc_kind,
                    "loc_start": loc_start,
                    "loc_end": loc_end,
                },
            })

        if not hits:
            if DEBUG:
                empty["debug"] = {"index_dir": resolved_dir, "queries_used": queries, "note": "Hits filtered to none."}
            return empty

        # ✅ Deterministic rerank for stability
        hits = _rerank_hits(question, hits)

        # Query modes
        short_mode = _is_short_query(question)
        sched_mode = _is_schedule_query(question)
        yesno_mode = _is_yesno_query(question)
        def_mode = _is_definition_query(question)

        # Soft gating:
        strong_hits = [h for h in hits if float(h["score"]) >= STRONG_SIM_THRESHOLD]

        grouped_all: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for h in hits:
            grouped_all[h["doc_name"]].append(h)

        keep_hits: List[Dict[str, Any]] = []
        for dn, doc_hits in grouped_all.items():
            doc_hits.sort(
                key=lambda x: (
                    float(x.get("_blend", x.get("score", 0.0))),
                    int(x.get("_lex_ov", x.get("overlap", 0))),
                ),
                reverse=True
            )
            keep_hits.extend(doc_hits[:max(1, KEEP_TOP_SEMANTIC_PER_DOC)])

        kept_set = {(h["doc_name"], h["id"]) for h in keep_hits}
        for h in strong_hits:
            kept_set.add((h["doc_name"], h["id"]))

        # Modes: recall-first in short/schedule/yesno/definition
        if short_mode or sched_mode or yesno_mode or def_mode:
            final_hits = hits
        else:
            final_hits = []
            for h in hits:
                key = (h["doc_name"], h["id"])
                if key in kept_set:
                    final_hits.append(h)
                    continue
                sc = float(h.get("score", 0.0) or 0.0)
                ov = int(h.get("overlap_all", 0) or 0)
                if sc >= (SIM_THRESHOLD + 0.04):
                    final_hits.append(h)
                    continue
                if ov >= 1:
                    final_hits.append(h)

            if not final_hits:
                final_hits = keep_hits or hits

        # Group into evidence docs
        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        ranks: Dict[str, int] = {}
        for h in final_hits:
            dn = h["doc_name"]
            grouped[dn].append(h)
            ranks[dn] = max(ranks.get(dn, 0), int(h.get("doc_rank", 0) or 0))

        evidence_docs: List[Dict[str, Any]] = []
        for dn, doc_hits in grouped.items():
            doc_hits.sort(
                key=lambda x: (
                    float(x.get("_blend", x.get("score", 0.0)) or 0.0),
                    int(x.get("_lex_ov", x.get("overlap", 0)) or 0),
                    int(x.get("doc_rank", 0) or 0),
                ),
                reverse=True
            )
            doc_hits = doc_hits[:MAX_CHUNKS_PER_DOC]
            evidence_docs.append({"doc_name": dn, "doc_rank": ranks.get(dn, 0), "hits": doc_hits})

        # =====================================================================
        # PAGE CONTEXT EXPANSION: Get all chunks from nearby pages
        # CONDITIONAL: Only apply for salary/detail queries to avoid noise
        # =====================================================================
        salary_keywords = ["salary", "pay", "sppp", "benefit", "scale", "allowance"]
        detail_keywords = ["detail", "everything", "sab kuch", "all info", "full"]
        q_lower = question.lower()
        needs_expansion = any(kw in q_lower for kw in salary_keywords + detail_keywords)
        
        PAGE_CONTEXT_WINDOW = 2 if needs_expansion else 0  # ±2 pages only for salary/detail
        
        for ed in evidence_docs:
            doc_name = ed.get("doc_name", "")
            existing_hits = ed.get("hits", [])
            
            # Get all page numbers from existing hits
            hit_pages = set()
            for h in existing_hits:
                page = h.get("page_start") or h.get("loc_start")
                if page is not None:
                    try:
                        hit_pages.add(int(page))
                    except (ValueError, TypeError):
                        pass
            
            if not hit_pages:
                continue
            
            # Expand to nearby pages
            pages_to_include = set()
            for p in hit_pages:
                for offset in range(-PAGE_CONTEXT_WINDOW, PAGE_CONTEXT_WINDOW + 1):
                    pages_to_include.add(p + offset)
            
            # Find all chunks from this document on those pages
            seen_ids = {h.get("id") for h in existing_hits}
            for r in active_rows:
                if r.get("doc_name") != doc_name:
                    continue
                
                row_page = r.get("page") or r.get("loc_start")
                if row_page is None:
                    continue
                try:
                    row_page = int(row_page)
                except (ValueError, TypeError):
                    continue
                
                if row_page not in pages_to_include:
                    continue
                
                try:
                    row_id = int(r.get("id", r.get("chunk_id")))
                except (ValueError, TypeError):
                    continue
                
                if row_id in seen_ids:
                    continue
                
                seen_ids.add(row_id)
                
                # Add this chunk as a context hit
                public_path = (r.get("public_path") or r.get("path") or "").strip()
                if not public_path:
                    public_path = _canonical_public_path(doc_name)
                
                existing_hits.append({
                    "id": row_id,
                    "chunk_id": row_id,
                    "score": 0.01,  # Low score since it's context, not a direct match
                    "overlap": 0,
                    "doc_name": doc_name,
                    "text": r.get("text", ""),
                    "page_start": row_page,
                    "loc_start": row_page,
                    "loc_kind": "page",
                    "public_path": public_path,
                    "path": public_path,
                    "_context_expanded": True,  # Mark as expanded
                })
            
            # Sort by page number for logical reading order
            existing_hits.sort(key=lambda x: x.get("page_start") or x.get("loc_start") or 0)
            ed["hits"] = existing_hits

        if not evidence_docs:
            if DEBUG:
                empty["debug"] = {"index_dir": resolved_dir, "note": "No evidence_docs after grouping."}
            return empty

        def best_score(ed: Dict[str, Any]) -> float:
            hs = ed.get("hits", [])
            if not hs:
                return 0.0
            return float(hs[0].get("_blend", hs[0].get("score", 0.0)) or 0.0)

        def best_overlap(ed: Dict[str, Any]) -> int:
            hs = ed.get("hits", [])
            if not hs:
                return 0
            return int(hs[0].get("_lex_ov", hs[0].get("overlap", 0)) or 0)

        evidence_docs.sort(
            key=lambda ed: (best_score(ed), best_overlap(ed), ed.get("doc_rank", 0)),
            reverse=True
        )

        strong_docs = [
            ed for ed in evidence_docs
            if ed.get("hits") and float(ed["hits"][0].get("score", 0.0)) >= STRONG_SIM_THRESHOLD
        ]
        primary = strong_docs[0] if strong_docs else evidence_docs[0]

        best = best_score(primary)
        kept_docs: List[Dict[str, Any]] = []
        for ed in evidence_docs:
            if best <= 0:
                continue
            if best_score(ed) >= best * RELATIVE_DOC_SCORE_KEEP:
                kept_docs.append(ed)

        kept_docs = kept_docs[:MAX_DOCS_RETURNED]
        kept_docs = _apply_criteria_doc_prioritization(question, kept_docs)

        # -----------------------------------------------------------------
        # STRICT SYNC: Filter out documents that have been deleted from disk
        # -----------------------------------------------------------------
        _DATA_DIR_CHECK = os.path.join(os.path.dirname(__file__), "assets", "data")
        final_valid_docs = []
        for ed in kept_docs:
            dn = ed.get("doc_name", "")
            if not dn:
                continue
            
            # Check exact match or appended .pdf
            p1 = os.path.join(_DATA_DIR_CHECK, dn)
            p2 = os.path.join(_DATA_DIR_CHECK, dn + ".pdf")
            
            if os.path.exists(p1) or os.path.exists(p2):
                final_valid_docs.append(ed)
        
        kept_docs = final_valid_docs
        # -----------------------------------------------------------------

        if not kept_docs:
            return empty

        out: Dict[str, Any] = {
            "question": question,
            "has_evidence": True,
            "primary_doc": primary.get("doc_name"),
            "primary_doc_rank": int(primary.get("doc_rank", 0) or 0),
            "evidence": kept_docs
        }

        if DEBUG:
            out["debug"] = {
                "index_dir": resolved_dir,
                "rows": len(rows),
                "active_rows": len(active_rows),
                "faiss_dim": d_idx,
                "queries_used": queries,
                "llm_queries": llm_queries,
                "short_mode": short_mode,
                "schedule_mode": sched_mode,
                "yesno_mode": yesno_mode,
                "definition_mode": def_mode,
                "raw_hit_count": len(hits),
                "final_hit_count": sum(len(d.get("hits", [])) for d in kept_docs),
                "lex_added": len(lex_best),
                "adaptive_used": adaptive_used,
                "sim_threshold": SIM_THRESHOLD,
                "min_sim_floor": MIN_SIM_FLOOR,
                "rerank_enabled": RERANK_ENABLED,
                "rerank_alpha": RERANK_ALPHA,
                "rerank_beta": RERANK_BETA,
            }

        return out

    except Exception as e:
        if DEBUG:
            empty["debug"] = {"index_dir": resolved_dir, "error": str(e)}
        return empty
