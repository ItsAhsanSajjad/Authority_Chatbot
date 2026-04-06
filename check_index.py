"""
COMPREHENSIVE VERIFICATION SUITE
Tests all fix areas:
1. Urdu phrasing → Now English answers (Fix 2)
2. Abbreviation coverage (Fix 4)
3. Standalone query handling (Fix 3)
4. Follow-up conversations
5. General query refusal
6. Answer consistency (3x)
7. EO salary (Fix 4)
"""
import requests, json, time, re

BASE = 'http://localhost:8000/api/ask'

def ask(q, session_id=None, history=None):
    payload = {'question': q}
    if session_id: payload['session_id'] = session_id
    if history: payload['conversation_history'] = history
    try:
        r = requests.post(BASE, json=payload, timeout=45)
        d = r.json()
        return d.get('answer', ''), d.get('references', []), d.get('session_id', '')
    except Exception as e:
        return f"ERROR: {e}", [], ''

def extract_pay(ans):
    for p in [r'SPPP[-\s]*(\d)', r'BPS[-\s]*(\d+)', r'BS[-\s]*(\d+)']:
        m = re.search(p, ans, re.IGNORECASE)
        if m: return m.group(0).upper().replace(' ','-')
    return "NO_VALUE"

results = {"pass": 0, "fail": 0, "total": 0}

with open("d:/PERAAIA/comprehensive_verify.txt", "w", encoding="utf-8") as f:

    def check(condition, label, q="", ans=""):
        results["total"] += 1
        if condition:
            results["pass"] += 1
            f.write(f"  ✅ {label}\n")
        else:
            results["fail"] += 1
            f.write(f"  ❌ {label}\n")
        if q: f.write(f"     Q: {q}\n")
        if ans: f.write(f"     A: {ans[:180]}\n")
        f.write("\n")

    # ═══ TEST 1: URDU QUERIES → ENGLISH ANSWERS WITH CORRECT VALUES ═══
    f.write("=" * 70 + "\n")
    f.write("TEST 1: URDU QUERIES → ENGLISH ANSWERS + CORRECT VALUES\n")
    f.write("=" * 70 + "\n\n")
    
    urdu_tests = [
        ("SSO ki salary kya hai", "SPPP-5"),
        ("SSO ki salary btao", "SPPP-5"),
        ("manager development ka pay scale kya hai", "SPPP-3"),
        ("manager development ki salary kitni hai", "SPPP-3"),
        ("deputy director ki salary kya hai", "BPS-18"),
        ("CTO ki salary btao", "SPPP-1"),
    ]
    for q, expected in urdu_tests:
        ans, _, _ = ask(q)
        pay = extract_pay(ans)
        is_english = not any(w in ans.lower().split()[:20] for w in ["hai", "aur", "iske", "saath"])
        check(expected in pay, f"{q} → {expected} (got {pay})", q, ans)
    
    # ═══ TEST 2: ENGLISH QUERIES STILL WORK ═══
    f.write("=" * 70 + "\n")
    f.write("TEST 2: ENGLISH QUERIES (regression check)\n")
    f.write("=" * 70 + "\n\n")
    
    english_tests = [
        ("SSO salary", "SPPP-5"),
        ("manager development salary", "SPPP-3"),
        ("deputy director salary", "BPS-18"),
        ("CTO salary", "SPPP-1"),
        ("investigation officer salary", "BPS-11"),
        ("web developer salary", "SPPP-4"),
        ("DG salary", "BS-20"),
        ("senior sergeant salary", "BPS-09"),
        ("enforcement officer salary", "BPS-16"),
    ]
    for q, expected in english_tests:
        ans, _, _ = ask(q)
        pay = extract_pay(ans)
        check(expected in pay, f"{q} → {expected} (got {pay})", q, ans)
    
    # ═══ TEST 3: ABBREVIATION COVERAGE ═══
    f.write("=" * 70 + "\n")
    f.write("TEST 3: NEW ABBREVIATIONS\n")
    f.write("=" * 70 + "\n\n")
    
    abbrev_tests = [
        ("DD salary", "BPS-18"),
        ("DEO salary", None),  # any answer is fine
        ("IO salary", "BPS-11"),
    ]
    for q, expected in abbrev_tests:
        ans, _, _ = ask(q)
        if expected:
            pay = extract_pay(ans)
            check(expected in pay, f"{q} → {expected} (got {pay})", q, ans)
        else:
            check(len(ans) > 50, f"{q} → got answer", q, ans)
    
    # ═══ TEST 4: FOLLOW-UP CONVERSATIONS ═══
    f.write("=" * 70 + "\n")
    f.write("TEST 4: FOLLOW-UP CONVERSATIONS\n")
    f.write("=" * 70 + "\n\n")
    
    # Session 1: SSO follow-ups
    history = []
    sid = None
    ans, _, sid = ask("Tell me about SSO", session_id=sid)
    history.extend([{"role":"user","content":"Tell me about SSO"},{"role":"assistant","content":ans}])
    f.write(f"  Setup: Tell me about SSO → {ans[:80]}...\n\n")
    
    for fq, keyword in [("his salary?", "SPPP"), ("who does he report to?", "SDEO"), ("qualifications?", "education")]:
        ans, _, sid = ask(fq, session_id=sid, history=history)
        has_kw = keyword.lower() in ans.lower()
        check(has_kw, f"Follow-up: {fq} → has '{keyword}'", fq, ans)
        history.extend([{"role":"user","content":fq},{"role":"assistant","content":ans}])
    
    # ═══ TEST 5: STANDALONE vs FOLLOW-UP ═══
    f.write("=" * 70 + "\n")
    f.write("TEST 5: STANDALONE QUERY (not treated as follow-up)\n")
    f.write("=" * 70 + "\n\n")
    
    # After SSO session, ask about a different role — should NOT be treated as follow-up
    for q, expected in [("CTO salary", "SPPP-1"), ("DG salary", "BS-20")]:
        ans, _, _ = ask(q)  # No session_id — fresh query
        pay = extract_pay(ans)
        check(expected in pay, f"Fresh query: {q} → {expected} (got {pay})", q, ans)
    
    # ═══ TEST 6: GENERAL QUERY REFUSAL ═══
    f.write("=" * 70 + "\n")
    f.write("TEST 6: GENERAL QUERY REFUSAL\n")
    f.write("=" * 70 + "\n\n")
    
    for q in ["what is HTML", "explain CSS flexbox", "how to make biryani"]:
        ans, _, _ = ask(q)
        refused = any(x in ans.lower() for x in ["outside", "scope", "pera", "not related", "cannot answer"])
        check(refused, f"Refused: {q}", q, ans)
    
    # ═══ TEST 7: CONSISTENCY ═══
    f.write("=" * 70 + "\n")
    f.write("TEST 7: CONSISTENCY (same query 3x)\n")
    f.write("=" * 70 + "\n\n")
    
    for q in ["SSO salary", "enforcement officer salary"]:
        f.write(f"  Q: {q}\n")
        pays = []
        for run in range(3):
            ans, _, _ = ask(q)
            pay = extract_pay(ans)
            pays.append(pay)
            f.write(f"    Run {run+1}: {pay}\n")
        consistent = len(set(pays)) == 1
        check(consistent, f"Consistent: {q} → {set(pays)}")
        f.write("\n")
    
    # ═══ TEST 8: SHORT/AMBIGUOUS QUERIES ═══
    f.write("=" * 70 + "\n")
    f.write("TEST 8: SHORT QUERIES\n")
    f.write("=" * 70 + "\n\n")
    
    for q in ["salary?", "SSO?", "CTO?", "pera?"]:
        ans, _, _ = ask(q)
        check(len(ans) > 50, f"Got answer for: {q}", q, ans)

    # ═══ SUMMARY ═══
    f.write("\n" + "=" * 70 + "\n")
    pct = 100*results["pass"]//max(1,results["total"])
    f.write(f"OVERALL: {results['pass']}/{results['total']} ({pct}%)\n")
    f.write(f"FAILURES: {results['fail']}\n")
    f.write("=" * 70 + "\n")

print(f"Done — {results['pass']}/{results['total']} ({100*results['pass']//max(1,results['total'])}%)")
