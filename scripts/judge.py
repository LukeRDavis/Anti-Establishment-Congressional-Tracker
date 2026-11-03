#!/usr/bin/env python3
"""
Hourly anti-establishment judge.

The full pipeline (every 4 h) rebuilds everything and judges a handful of people
inside its time limit. This job only judges: it reads the people, votes, money and
petitions the pipeline already gathered (candidates.json + data/cache), spends the
free AI tiers on as many reviews as they allow in about 40 minutes, and writes every
cached verdict back into data.json so the site shows them without waiting for the
next full run.
"""
import json, os, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("AECT_AI_DEADLINE_MIN", "40")
os.environ.setdefault("AECT_TIER_BUDGET", "150")
os.environ.setdefault("AECT_BILLS_BUDGET", "60")
os.environ.setdefault("AECT_HEADLINE_BUDGET", "60")

from aect import establishment, discharge, ai
from aect.net import load_cache, now_iso, STREAMS

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load(name, default):
    try:
        with open(os.path.join(ROOT, name)) as f:
            return json.load(f)
    except Exception:
        return default


def _write(name, obj):
    path = os.path.join(ROOT, name)
    with open(path + ".tmp", "w") as f:
        json.dump(obj, f, separators=(",", ":"), default=str)
    os.replace(path + ".tmp", path)


def verdict_fields(p):
    counted = bool(p.get("antiArms")) and p.get("antiTier") in (None, "hard")
    return counted, {
        "antiTier": p.get("antiTier"), "tier_source": p.get("tier_source"), "tier_reason": p.get("tier_reason"),
        "tier_evidence": p.get("tier_evidence", []), "tier_risk": p.get("tier_risk"),
        "tier_screen": p.get("tier_screen"), "writeup": p.get("tier_writeup", ""),
        "bottom_line": p.get("tier_bottom_line", ""), "principle": p.get("tier_principle"),
        "judged_model": p.get("tier_model"), "judged_at": p.get("tier_judged_at"),
    }


def patch(data, people):
    """Write the people's current verdicts into data.json's members, field and races."""
    n = 0
    for m in data.get("members", []):
        p = people.get(f"BG:{m.get('bioguide_id')}") if m.get("bioguide_id") else None
        if not p:
            continue
        counted, f = verdict_fields(p)
        m.update(f)
        m["antiArms"] = bool(p.get("antiArms"))
        n += 1
    for c in data.get("field", []):
        p = people.get(c.get("id"))
        if not p:
            continue
        counted, f = verdict_fields(p)
        c.update({k: f[k] for k in ("writeup", "bottom_line", "judged_model", "judged_at", "tier_source")})
        c["tier_reason"], c["anti"] = f["tier_reason"], counted
    for r in data.get("races", []):
        p = people.get(r.get("id"))
        if p:
            r["antiTier"] = p.get("antiTier")
    meta = data.setdefault("meta", {})
    meta["anti_intervention_count"] = sum(1 for m in data.get("members", [])
                                          if m.get("antiArms") and m.get("antiTier") in (None, "hard"))
    meta["judge"] = {"updated_at": now_iso(), "ai_calls": ai.CALLS,
                     "note": (STREAMS.get("establishment_tiers") or {}).get("notes", [""])[-1:]}
    return n


def main():
    data = _load("data.json", {})
    people = _load("candidates.json", {}).get("people", {})
    if not data or not people:
        print("No data yet; the full pipeline has to run first.")
        return 0
    ie = (load_cache("fec_ie") or {}).get("ie", {})
    fec_cands = (load_cache("fec_candidates") or {}).get("candidates", {})
    fec_totals = (load_cache("fec_totals") or {}).get("totals", {})

    petitions = discharge.fetch()                      # cached for 24 h
    house = [p for p in people.values() if p.get("incumbent") and p.get("chamber") == "house"]
    parties = {}
    for p in house:
        parties[p.get("caucus") or p.get("party")] = parties.get(p.get("caucus") or p.get("party"), 0) + 1
    majority = max(parties, key=parties.get) if parties else None
    for p in house:
        p["_discharge"] = petitions.get(p.get("bioguide_id"), [])
        p["_party_holds_house"] = (p.get("caucus") or p.get("party")) == majority

    print(f"Judging (free tiers first; budget {establishment.BUDGET}, ~{os.environ['AECT_AI_DEADLINE_MIN']} min)")
    establishment.run(people, ie, fec_cands, fec_totals)
    n = patch(data, people)
    _write("data.json", data)
    _write("candidates.json", {**_load("candidates.json", {}), "people": people})
    print(f"Patched {n} members · AI calls {ai.CALLS} · {(STREAMS.get('establishment_tiers') or {}).get('notes')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
