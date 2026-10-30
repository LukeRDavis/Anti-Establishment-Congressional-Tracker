#!/usr/bin/env python3
"""
Anti-Establishment Congressional Tracker — data pipeline v10.

Nothing about any person, race, vote or bill is written into this code.
Every run rebuilds the data from live sources, in this order:

  1. Current members of Congress      House Clerk + Senate rosters, checked
                                      against the most recent roll calls
  2. Every federal candidate          FEC filings for the current cycle
  3. General information              Wikipedia · Ballotpedia · Grokipedia,
                                      state primary calendar
  4. Anti-interventionist evidence
       a. Roll-call votes              discovered from clerk/senate XML;
                                      AI (with web search) decides which
                                      votes matter and which side is anti
       b. Candidates' own statements   AI web research, cited sources only
       c. Trackers                     TrackAIPAC · RLC (corroboration and
                                      research priority, never sole proof)
       d. Money                        FEC totals + outside spending
  5. Prediction markets               Polymarket · Kalshi · PredictIt

Outputs: data.json (website), candidates.json (one record per person),
markets.json, pipeline_report.json (which data streams arrived this run).
"""
import json, os, sys, time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from aect import ai, roster, fec, trackers, votes, enrich, positions, markets, build
from aect.config import KEYS, CYCLE, CURRENT_CONGRESS, TODAY
from aect.net import STREAMS, stream, now_iso

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _write(name, obj, compact=False):
    path = os.path.join(ROOT, name)
    with open(path + ".tmp", "w") as f:
        if compact:
            json.dump(obj, f, separators=(",", ":"), default=str)
        else:
            json.dump(obj, f, indent=1, default=str)
    os.replace(path + ".tmp", path)


def _load(name, default):
    try:
        with open(os.path.join(ROOT, name)) as f:
            return json.load(f)
    except Exception:
        return default


def history_events(old_members, new_members):
    hist = _load("history.json", {"events": []})
    events = hist.get("events", [])
    old = {m["name"]: m for m in old_members}
    today = TODAY.isoformat()
    added = 0
    for m in new_members:
        o = old.get(m["name"])
        if o is None or bool(o.get("antiArms")) == bool(m.get("antiArms")):
            continue
        events.append({"date": today, "detected_at": now_iso(), "member": m["name"],
                       "party": m["party"], "state": m["state"], "chamber": m["chamber"],
                       "change": "pro_to_anti" if m["antiArms"] else "anti_to_pro",
                       "trigger": (m.get("evidence") or ["status changed"])[0][:200],
                       "source": m.get("antiArmsBasis") or "pipeline"})
        added += 1
    hist["events"] = events[-3000:]
    hist["updated_at"] = now_iso()
    return hist, added


def main():
    t0 = time.time()
    print(f"AECT pipeline v10 — {TODAY} · {CURRENT_CONGRESS}th Congress · {CYCLE} cycle\n")
    print("[Keys]")
    for k, v in KEYS.items():
        print(f"  {k:20} {'present' if v else 'MISSING'}")
        stream(f"key:{k}", ok=bool(v))

    print("\n[1] Current members of Congress")
    members, vacancies, delegates = roster.fetch_current_members()
    if len(members) < 400:
        print(f"ABORT: only {len(members)} members found — keeping the previous data.json")
        _write("pipeline_report.json", {"run_at": now_iso(), "aborted": True, "streams": STREAMS})
        return 1

    print("\n[2] FEC candidates and finance")
    fec_cands = fec.fetch_candidates()
    fec_totals = fec.fetch_totals()

    print("\n[4c] Trackers")
    tr = trackers.fetch_all()

    people = build.build_people(members, fec_cands, tr)
    print(f"\n  People universe: {len(people)} "
          f"({sum(p['incumbent'] for p in people.values())} members, "
          f"{sum(not p['incumbent'] for p in people.values())} candidates)")

    print("\n[4a] Roll-call votes")
    relevant = votes.sync_votes()
    votes.score_members([p for p in people.values() if p.get("incumbent")], relevant)

    print("\n[3] General information")
    calendar = enrich.primary_calendar()
    tracked = [k for k, p in people.items() if any(s.startswith(("trackaipac", "rlc")) for s in p["sources"])]
    voters = [k for k, p in people.items() if any(v["anti"] for v in p.get("vote_record", []))]
    by_money = sorted((k for k, p in people.items() if not p["incumbent"]),
                      key=lambda k: -max([(fec_totals.get(c) or {}).get("raised") or 0
                                          for c in people[k].get("fec_ids", [])] or [0]))
    priority = list(dict.fromkeys(tracked + voters + [k for k, p in people.items() if p["incumbent"]] + by_money))
    enrichment = enrich.enrich_people(people, priority)
    build.apply_status(people, enrichment, calendar, fec_cands)

    print("\n[4b] Candidate positions (AI research)")
    live = [k for k in priority if not people[k]["incumbent"] and people[k].get("ps") != "DEFEATED"]
    sites = fec.fetch_websites([cid for k in live[:600]
                                for fid in people[k].get("fec_ids", [])
                                for cid in fec_cands.get(fid, {}).get("committee_ids", [])[:1]])
    for k in live:
        if not people[k].get("website"):
            for fid in people[k].get("fec_ids", []):
                for cid in fec_cands.get(fid, {}).get("committee_ids", [])[:1]:
                    if sites.get(cid):
                        people[k]["website"] = sites[cid]
    no_vote_members = [k for k in tracked if people[k]["incumbent"] and not people[k].get("vote_record")]
    pos = positions.research(people, list(dict.fromkeys(
        [k for k in live if k in tracked] + no_vote_members + live)))

    build.classify(people, pos)

    print("\n[4d] Outside spending")
    anti_fec = [cid for p in people.values() if p["antiArms"] for cid in p.get("fec_ids", [])]
    ie = fec.fetch_outside_spending(anti_fec)

    print("\n[5] Prediction markets")
    mk = markets.match_people({k: p for k, p in people.items() if p["antiArms"] and p.get("on_ballot")})

    history = votes.congress_history(relevant)
    old = _load("data.json", {})
    data = build.frontend_data(people, mk, fec_totals, ie, history, relevant, {}, {
        "fetched_at": now_iso(), "vacancies": vacancies,
        "ai_calls": ai.CALLS, "runtime_s": None,
        "incumbent_retention": votes.incumbent_retention()})
    hist, n_events = history_events(old.get("members", []), data["members"])

    # final stream report
    report = {"run_at": now_iso(), "runtime_s": round(time.time() - t0),
              "congress": CURRENT_CONGRESS, "cycle": CYCLE, "ai_calls": ai.CALLS,
              "streams": STREAMS,
              "summary": {"members": len(data["members"]),
                          "anti_members": data["meta"]["anti_intervention_count"],
                          "anti_by_party": data["meta"]["anti_by_party"],
                          "anti_candidates": len(data["candidates"]),
                          "relevant_votes": len(relevant), "odds": len(data["poly"]),
                          "history_events_added": n_events}}
    data["meta"]["streams"] = {k: {"ok": v["ok"], "count": v["count"], "errors": v["errors"][:2]}
                               for k, v in STREAMS.items() if not k.startswith("key:")}
    data["meta"]["runtime_s"] = report["runtime_s"]

    _write("data.json", data, compact=True)
    _write("candidates.json", {"generated_at": now_iso(), "cycle": CYCLE, "people": people}, compact=True)
    _write("markets.json", {"generated_at": now_iso(), "markets": mk}, compact=True)
    _write("challengers.json", {"generated_at": now_iso(), "challengers": data["candidates"]})
    _write("history.json", hist, compact=True)
    _write("pipeline_report.json", report)

    print("\n" + "═" * 72)
    print(f"{'STREAM':28} {'STATUS':8} {'COUNT':>7}  NOTES / ERRORS")
    print("─" * 72)
    for name, s in sorted(STREAMS.items()):
        if name.startswith("key:"):
            continue
        status = "OK" if s["ok"] else ("FAIL" if s["ok"] is False else "—")
        detail = (s["errors"][:1] or s["notes"][-1:] or [""])[0]
        print(f"{name:28} {status:8} {s['count']:>7}  {detail[:110]}")
    print("═" * 72)
    print(json.dumps(report["summary"], indent=1))
    return 0


def run():
    import traceback
    try:
        return main()
    except Exception:
        tb = traceback.format_exc()
        print(tb)
        _write("pipeline_report.json", {"run_at": now_iso(), "crashed": True, "traceback": tb,
                                        "ai_calls": ai.CALLS, "streams": STREAMS})
        return 1


if __name__ == "__main__":
    sys.exit(run())
