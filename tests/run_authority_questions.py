#!/usr/bin/env python
"""
PERA AI — Authority-level question harness.

Reads tests/authority_questions.yaml, POSTs every question to a running
backend at http://127.0.0.1:8000/api/ask, captures the answer + metadata,
and writes a CSV report classifying each row.

Usage:
    # Smoke (first 30 questions)
    python tests/run_authority_questions.py --limit 30

    # Full run
    python tests/run_authority_questions.py

    # Single section
    python tests/run_authority_questions.py --section 5

    # Only high-risk numeric DB-backed questions
    python tests/run_authority_questions.py --tag kind:numeric --tag risk:high

    # Custom output
    python tests/run_authority_questions.py --out reports/run_2026-05-18.csv

The runner is parallel (default 4 workers) and uses a wide per-request
timeout because some live-API queries take >15s. Set BACKEND_URL to point
at a different host (e.g. staging).

Classification (status column):
    ok            — answered with non-empty content, no error markers
    no_data       — bot returned an "I couldn't find" / "no data" answer
    api_error     — HTTP error, network failure, or backend 5xx
    routing_gap   — answered from docs when intent suggested structured
    suspicious    — answer contains "n/a", "Unknown", or empty table rows
    timeout       — request exceeded --timeout window
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import yaml  # PyYAML — already in repo's environment
except ImportError:
    print("ERROR: PyYAML missing. Run: pip install pyyaml", file=sys.stderr)
    sys.exit(2)


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_YAML = REPO_ROOT / "tests" / "authority_questions.yaml"
DEFAULT_OUT_DIR = REPO_ROOT / "tests" / "reports"


# ── Classifiers ─────────────────────────────────────────────────
_NO_DATA_PATTERNS = [
    r"could ?n'?t find",
    r"don'?t have (?:any )?(?:information|data)",
    r"do not contain",
    r"no information (?:available|found)",
    r"no data (?:available|found)",
    r"unable to (?:fetch|find|locate)",
    r"i (?:could not|cannot) (?:find|locate)",
    r"is not available in",
    r"insufficient (?:data|information|context)",
]
_NO_DATA_RE = re.compile("|".join(_NO_DATA_PATTERNS), re.I)

_SUSPICIOUS_PATTERNS = [
    r"\b(?:n/a|not applicable|tbd|tba)\b",
    r"\bunknown\b",
    r"\|\s*0\s*\|\s*0\s*\|\s*0\s*\|",      # all-zero table rows
    r"\|\s*-\s*\|\s*-\s*\|",                # all-dash table rows
]
_SUSPICIOUS_RE = re.compile("|".join(_SUSPICIOUS_PATTERNS), re.I)


def classify_response(http_status: int,
                      ok: bool,
                      answer: str,
                      latency: float,
                      timeout_window: float) -> str:
    if not ok:
        return "api_error"
    if http_status >= 500:
        return "api_error"
    if latency >= timeout_window - 1:
        return "timeout"
    a = (answer or "").strip()
    if not a:
        return "no_data"
    if _NO_DATA_RE.search(a):
        return "no_data"
    if _SUSPICIOUS_RE.search(a):
        return "suspicious"
    return "ok"


# ── HTTP ────────────────────────────────────────────────────────
def post_ask(url: str, question: str,
             mode: str, timeout: float) -> Tuple[int, bool, Dict[str, Any], float]:
    """POST one question. Return (http_status, ok, body, latency_seconds)."""
    payload = json.dumps({
        "question": question,
        "answer_source_mode": mode,
    }).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            elapsed = time.perf_counter() - t0
            body = json.loads(raw) if raw else {}
            return resp.status, True, body, elapsed
    except urllib.error.HTTPError as e:
        elapsed = time.perf_counter() - t0
        try:
            raw = e.read().decode("utf-8", errors="replace")
            body = json.loads(raw) if raw else {}
        except Exception:
            body = {"error": str(e)}
        return e.code, False, body, elapsed
    except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
        elapsed = time.perf_counter() - t0
        return 0, False, {"error": str(e)}, elapsed
    except Exception as e:  # noqa: BLE001
        elapsed = time.perf_counter() - t0
        return 0, False, {"error": f"{type(e).__name__}: {e}"}, elapsed


# ── Tag helpers ────────────────────────────────────────────────
def parse_tag_filter(filters: List[str]) -> Dict[str, str]:
    """--tag kind:numeric --tag risk:high → {'kind':'numeric','risk':'high'}"""
    out: Dict[str, str] = {}
    for f in filters or []:
        if ":" not in f:
            continue
        k, v = f.split(":", 1)
        out[k.strip()] = v.strip()
    return out


def matches_tag_filter(item_tags: List[str], filt: Dict[str, str]) -> bool:
    if not filt:
        return True
    have: Dict[str, str] = {}
    for t in item_tags or []:
        if ":" in t:
            k, v = t.split(":", 1)
            have[k.strip()] = v.strip()
    for k, v in filt.items():
        if have.get(k) != v:
            return False
    return True


# ── Main ───────────────────────────────────────────────────────
def load_questions(yaml_path: Path) -> List[Dict[str, Any]]:
    with yaml_path.open("r", encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    if not doc or "sections" not in doc:
        raise SystemExit(f"Bad YAML at {yaml_path}: missing 'sections'.")
    flat: List[Dict[str, Any]] = []
    for sect in doc["sections"]:
        sid = sect.get("id")
        stitle = sect.get("title", "")
        for item in sect.get("items", []) or []:
            flat.append({
                "section_id": sid,
                "section_title": stitle,
                "question": item.get("q", "").strip(),
                "tags": item.get("tags") or [],
            })
    return flat


def run(args: argparse.Namespace) -> int:
    yaml_path = Path(args.questions).resolve()
    questions = load_questions(yaml_path)

    tag_filter = parse_tag_filter(args.tag or [])
    if tag_filter:
        questions = [q for q in questions if matches_tag_filter(q["tags"], tag_filter)]
    if args.section is not None:
        questions = [q for q in questions if q["section_id"] == args.section]
    if args.limit:
        questions = questions[: args.limit]

    if not questions:
        print("No questions match filters.", file=sys.stderr)
        return 1

    out_path = Path(args.out) if args.out else (
        DEFAULT_OUT_DIR / f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)

    url = args.url.rstrip("/") + "/api/ask"
    print(f"Backend         : {url}")
    print(f"Questions       : {len(questions)}")
    print(f"Mode            : {args.mode}")
    print(f"Workers         : {args.workers}")
    print(f"Per-req timeout : {args.timeout}s")
    print(f"Output          : {out_path}")
    print()

    rows: List[Dict[str, Any]] = [None] * len(questions)  # preserve order

    def _job(idx: int, q: Dict[str, Any]) -> Dict[str, Any]:
        if args.delay_ms > 0:
            time.sleep(args.delay_ms / 1000.0)
        http_s, ok, body, lat = post_ask(
            url, q["question"], args.mode, args.timeout,
        )
        answer = body.get("answer") if isinstance(body, dict) else ""
        decision = body.get("decision") if isinstance(body, dict) else ""
        src = body.get("source_mode_label") if isinstance(body, dict) else ""
        grounding = body.get("grounding") if isinstance(body, dict) else None
        gconf = (grounding or {}).get("confidence") if isinstance(grounding, dict) else ""
        gscore = (grounding or {}).get("score") if isinstance(grounding, dict) else ""
        gstate = (grounding or {}).get("support_state") if isinstance(grounding, dict) else ""
        status = classify_response(http_s, ok, answer or "", lat, args.timeout)
        return {
            "idx":              idx + 1,
            "section_id":       q["section_id"],
            "section_title":    q["section_title"],
            "question":         q["question"],
            "tags":             ";".join(q["tags"]),
            "http_status":      http_s,
            "latency_s":        round(lat, 2),
            "status":           status,
            "decision":         decision or "",
            "source_label":     src or "",
            "g_confidence":     gconf or "",
            "g_score":          gscore if gscore != "" else "",
            "g_state":          gstate or "",
            "answer_preview":   (answer or "").replace("\n", " ").strip()[:400],
        }

    t_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        fut_to_idx = {
            ex.submit(_job, i, q): i
            for i, q in enumerate(questions)
        }
        done = 0
        for fut in as_completed(fut_to_idx):
            i = fut_to_idx[fut]
            try:
                rows[i] = fut.result()
            except Exception as e:  # noqa: BLE001
                rows[i] = {
                    "idx": i + 1,
                    "section_id": questions[i]["section_id"],
                    "section_title": questions[i]["section_title"],
                    "question": questions[i]["question"],
                    "tags": ";".join(questions[i]["tags"]),
                    "http_status": 0,
                    "latency_s": 0.0,
                    "status": "api_error",
                    "decision": "",
                    "source_label": "",
                    "g_confidence": "",
                    "g_score": "",
                    "g_state": "",
                    "answer_preview": f"runner_exception: {type(e).__name__}: {e}",
                }
            done += 1
            if done % 5 == 0 or done == len(questions):
                print(f"  [{done:4d}/{len(questions)}]  "
                      f"elapsed={time.perf_counter() - t_start:5.1f}s")

    total = time.perf_counter() - t_start
    print()
    print(f"Wall time: {total:.1f}s "
          f"(avg {total / len(questions):.2f}s/q)")

    # Write CSV
    fieldnames = [
        "idx", "section_id", "section_title", "question", "tags",
        "http_status", "latency_s", "status", "decision",
        "source_label", "g_confidence", "g_score", "g_state",
        "answer_preview",
    ]
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    # Summary
    by_status: Dict[str, int] = {}
    by_section_status: Dict[Tuple[int, str], int] = {}
    for r in rows:
        s = r["status"]
        by_status[s] = by_status.get(s, 0) + 1
        key = (r["section_id"], s)
        by_section_status[key] = by_section_status.get(key, 0) + 1

    print()
    print("Status summary:")
    for k in ("ok", "no_data", "suspicious", "routing_gap",
              "timeout", "api_error"):
        if k in by_status:
            print(f"  {k:14s} {by_status[k]:4d}")

    print()
    print("Per-section pass rate (ok / total):")
    sections_seen: Dict[int, str] = {}
    section_totals: Dict[int, int] = {}
    section_ok: Dict[int, int] = {}
    for r in rows:
        sid = r["section_id"]
        sections_seen[sid] = r["section_title"]
        section_totals[sid] = section_totals.get(sid, 0) + 1
        if r["status"] == "ok":
            section_ok[sid] = section_ok.get(sid, 0) + 1
    for sid in sorted(sections_seen):
        ok_n = section_ok.get(sid, 0)
        tot = section_totals[sid]
        pct = (ok_n / tot * 100) if tot else 0
        print(f"  §{sid:2d} {sections_seen[sid][:38]:38s} "
              f"{ok_n:3d}/{tot:3d}  {pct:5.1f}%")

    print(f"\nReport: {out_path}")
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--questions", default=str(DEFAULT_YAML),
                   help="path to authority_questions.yaml")
    p.add_argument("--url", default=os.environ.get("BACKEND_URL",
                                                   "http://127.0.0.1:8000"),
                   help="backend base URL (default http://127.0.0.1:8000 "
                        "or $BACKEND_URL)")
    p.add_argument("--mode", default="both",
                   choices=("both", "stored_api", "documents"),
                   help="answer_source_mode (default both)")
    p.add_argument("--timeout", type=float, default=120.0,
                   help="per-request timeout seconds (default 120)")
    p.add_argument("--workers", type=int, default=4,
                   help="parallel workers (default 4)")
    p.add_argument("--limit", type=int, default=0,
                   help="stop after N questions (0 = all)")
    p.add_argument("--section", type=int,
                   help="run only section id N (1..24)")
    p.add_argument("--tag", action="append",
                   help="filter by tag key:value (repeatable). "
                        "Example: --tag kind:numeric --tag risk:high")
    p.add_argument("--out", help="explicit CSV output path "
                                  "(default tests/reports/run_<ts>.csv)")
    p.add_argument("--delay-ms", type=int, default=0,
                   help="extra sleep BEFORE each request, milliseconds. "
                        "Use to stay under OpenAI TPM caps. With "
                        "--workers 2 --delay-ms 1500 the harness emits "
                        "~80 q/min, well under the 30k-TPM ceiling.")
    args = p.parse_args(argv)
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
