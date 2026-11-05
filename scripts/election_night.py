#!/usr/bin/env python3
"""
Light election-night refresh (every 15 minutes, only around a general election).

Re-reads prediction markets and Wikipedia result boxes for the races already in
data.json, averages the odds, freezes each race's odds one minute before its
polls close, and marks races won or lost. Everything else in data.json is left
as the full pipeline wrote it.
"""
import json, os, sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from aect import night, markets, results
from aect.net import now_iso, stream, STREAMS

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


def main():
    data = _load("data.json", {})
    gd = (data.get("meta") or {}).get("general_date")
    if not night.in_window(gd):
        print(f"Not election night (general {gd}); nothing to do.")
        return 0
    people = _load("candidates.json", {}).get("people", {})
    race_ids = {r["id"] for r in data.get("races", [])}
    # the tracked races plus everyone else still on those ballots (for results and opponents)
    seats = {(r.get("state"), r.get("chamber")) for r in data.get("races", [])}
    # every race on the ballot, not only tracked ones: the "Next Congress" view needs all winners
    subset = {k: p for k, p in people.items() if k in race_ids or p.get("on_ballot")}

    results.TTL_H = 0.2                       # re-read result pages every run tonight
    cache = results.fetch(subset)
    closes = night.poll_closes(gd)
    now = datetime.now(timezone.utc)
    for k, p in subset.items():
        close = closes.get(p.get("state"))
        if not close or now < datetime.fromisoformat(close):
            continue                          # nothing is called before that state's polls close
        r = results.result_for(p, cache)
        if r["general"] and p.get("primary_result") != "LOST":
            p["wiki_result"], p["general_result_source"] = r["general"], r["source"]
        else:
            p.pop("wiki_result", None)        # no posted winner (or an earlier misread)


    markets.SETTLED.clear()
    mk = markets.match_people({k: people[k] for k in race_ids if k in people})   # also collects settled Polymarket markets
    # second source for results: settled who-wins markets, cross-checked against Wikipedia
    markets.load_kalshi_settled()
    night.cross_check(people, markets.settlements(subset), closes)
    night.apply(data, mk, people, old_data=_load("data.json", {}))   # clock read after markets load

    for c in data.get("field", []):           # candidates' results, for the Next Congress view
        p = people.get(c.get("id"))
        if p is not None:
            c["general_result"] = p.get("general_result")
            c["general_result_by"], c["result_conflict"] = p.get("general_result_by"), p.get("result_conflict")
    by_name = {p["name"]: p for p in people.values() if p.get("incumbent")}
    for m in data.get("members", []):
        p = by_name.get(m["name"])
        if p:
            m["general_result"] = p.get("general_result")
            m["general_result_by"], m["result_conflict"] = p.get("general_result_by"), p.get("result_conflict")
            if p.get("general_result") == "LOST":
                m["ps"] = "DEFEATED"
    hist = _load("history.json", {"events": []})
    n = night.history(hist, data)
    hist["updated_at"] = now_iso()

    from aect.net import load_cache
    if (load_cache("reform") or {}).get("items"):
        data["reform"] = load_cache("reform")   # keep the electoral-reform data on the page
    data["meta"]["streams_night"] = {k: {"ok": v["ok"], "count": v["count"]} for k, v in STREAMS.items()}
    _write("data.json", data)
    _write("candidates.json", {**_load("candidates.json", {}), "people": people})
    _write("history.json", hist)
    print(f"Election night: {data['meta']['election_night']} · {n} history events")
    print(f"NEXT_REFRESH_SECONDS={night.refresh_every(gd)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
