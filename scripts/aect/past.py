"""
Past Congresses: who sat in each House and Senate since 2001 and whether they count as
anti-establishment by the same standard used today.

Source: Voteview (voteview.com), which publishes every Congress's members (with Bioguide
IDs), roll calls and individual votes. For each Congress:
  1. roll calls that may concern foreign military aid, arms or war are picked by keyword,
     and the same AI vote judge used for the current Congress decides which are arms/aid
     votes on the focus recipients and which side is the anti side;
  2. each member's votes on those roll calls are read, and an anti vote counts only when
     the member broke from their own party's majority on it (party-line votes don't count);
  3. members are rated by the same strict rule as today, on their record so far: anti in
     more than one Congress, mostly anti, still anti in the latest Congress they voted in.
A finished Congress is saved once to data/history/congress_<n>.json and never recomputed.
Nothing about any person or vote is written here.
"""
import csv, io, json, os

from .config import CURRENT_CONGRESS, congress_years, FOCUS_RECIPIENTS, FOCUS_CATEGORY, ai_time_left
from .net import get_bytes, stream, load_cache, save_cache, now_iso
from . import votes as V

FIRST = int(os.environ.get("AECT_HISTORY_FROM", "107"))
OUT_DIR = os.path.join("data", "history")
VERSION = 2
URL = "https://voteview.com/static/data/out/{kind}/{ch}{congress}_{kind}.csv"
CAST = {1: "Yea", 2: "Yea", 3: "Yea", 4: "Nay", 5: "Nay", 6: "Nay"}


def _csv(kind, ch, congress):
    raw = get_bytes(URL.format(kind=kind, ch=ch, congress=congress), stream_name="history_voteview", timeout=120)
    return list(csv.DictReader(io.StringIO(raw.decode("utf-8", errors="replace")))) if raw else []


def _num(x, default=0):
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return default


def _party(code):
    return {100: "D", 200: "R"}.get(_num(code), "I")


def _name(bioname):
    # "LAST, First Middle" -> "First Middle Last"
    if "," in (bioname or ""):
        last, first = bioname.split(",", 1)
        return f"{first.strip()} {last.strip().title()}".strip()
    return (bioname or "").title()


def _focus_then(j, rec):
    """Arms/military aid to the focus recipients, as it was usually voted on before 2023.
    Today's test (V.is_focus) needs the focus recipients alone and a 'major' rating; in earlier
    Congresses the aid mostly rode in omnibus defense and foreign-operations bills covering
    dozens of countries, which say nothing about Israel in particular, while the standalone
    measures (security-cooperation acts, Iron Dome funding, resolutions backing military
    operations) were often rated 'minor'. So: any arms/aid vote whose recipients are only the
    focus countries, or the focus country with one other, named in the measure itself."""
    if V.is_focus(j):
        return True
    if j.get("category") != V.FOCUS_CATEGORY:
        return False
    rec_set = {r.lower() for r in (j.get("recipients") or [])}
    focus = {f.lower() for f in FOCUS_RECIPIENTS}
    if not rec_set & focus:
        return False
    others = rec_set - focus
    text = " ".join([rec.get("legis", ""), rec.get("desc", ""), j.get("summary", "")]).lower()
    named = any(f in text for f in focus)
    return not others or (len(others) <= 1 and named and "consideration of" not in text)


def _judged_focus(congress, ch_letter, judgments, budget):
    """Roll calls in this Congress that are focus votes, with the anti side, as {roll: rec}."""
    chamber = "house" if ch_letter == "H" else "senate"
    rolls = _csv("rollcalls", ch_letter, congress)
    out, pending = {}, 0
    for r in rolls:
        if (r.get("chamber") or "").lower() != chamber:
            continue
        text = " ".join(r.get(f, "") for f in ("bill_number", "vote_question", "vote_desc", "dtl_desc"))
        if not V.TOPIC.search(text):
            continue
        n = _num(r.get("rollnumber"))
        date = r.get("date", "")
        # same judgment keys as the current-Congress pipeline: House by the Clerk's yearly roll
        # number, Senate by Voteview's roll number
        rec = {"chamber": chamber, "congress": congress,
               "roll": (_num(r.get("clerk_rollnumber")) or n) if chamber == "house" else n,
               "year": _num(date[:4]), "date": date, "legis": r.get("bill_number", ""),
               "question": r.get("vote_question", ""),
               "desc": (r.get("vote_desc") or r.get("dtl_desc") or "")[:400], "result": r.get("vote_result", ""),
               "url": f"https://voteview.com/rollcall/R{ch_letter}{congress:03d}{n:04d}"}
        if budget["left"] <= 0 or ai_time_left() < 60:
            pending += 1
            continue
        j = V._judge(rec, judgments, budget)
        if j is None:
            pending += 1
            continue
        if j.get("relevant") and _focus_then(j, rec):
            # a measure that funds or backs the aid: voting no is the anti side; one that cuts it: yes
            anti = j.get("anti_position") or ("Yea" if j.get("measure_restricts") else "Nay")
            out[n] = {**rec, "anti_position": anti, "summary": j.get("summary", ""), "measure": rec["legis"]}
    return out, pending


def build_congress(congress, judgments, budget):
    """Return the finished roster for one Congress, or None if roll calls are still unjudged."""
    members, focus = [], {}
    pending = 0
    for ch in ("H", "S"):
        f, p = _judged_focus(congress, ch, judgments, budget)
        pending += p
        focus[ch] = f
    if pending:
        return None, pending
    first, last = congress_years(congress)
    for ch in ("H", "S"):
        chamber = "house" if ch == "H" else "senate"
        rows = [m for m in _csv("members", ch, congress)
                if (m.get("chamber") or "").lower() == chamber and m.get("bioguide_id")]
        votes = {}
        if focus[ch]:
            rows_v = _csv("votes", ch, congress)
            if not rows_v:                    # a failed download must not look like "nobody voted"
                print(f"  history: {congress}th {chamber} votes file unavailable; will retry")
                return None, 1
            for v in rows_v:
                n = _num(v.get("rollnumber"))
                if n in focus[ch]:
                    cast = CAST.get(_num(v.get("cast_code"), 9))
                    if cast:
                        votes.setdefault(v.get("icpsr"), {})[n] = cast
        # party tallies per roll call (for "did they break from their own party?")
        party_of = {m.get("icpsr"): _party(m.get("party_code")) for m in rows}
        side = lambda p: "R" if p == "R" else "D"
        tally = {}
        for icpsr, vs in votes.items():
            for n, cast in vs.items():
                t = tally.setdefault(n, {}).setdefault(side(party_of.get(icpsr, "I")), [0, 0])
                t[0] += cast == focus[ch][n]["anti_position"]
                t[1] += 1
        # one member per seat: whoever cast the most votes in this Congress
        best = {}
        for m in rows:
            seat = (m.get("state_abbrev"), _num(m.get("district_code")) if chamber == "house" else None)
            best.setdefault(seat, []).append(m)
        for seat, ms in best.items():
            ms.sort(key=lambda m: -_num(m.get("nominate_number_of_votes")))
            for m in ms[: (2 if chamber == "senate" else 1)]:
                party = _party(m.get("party_code"))
                rec = []
                for n, cast in sorted((votes.get(m.get("icpsr")) or {}).items()):
                    fv = focus[ch][n]
                    anti = cast == fv["anti_position"]
                    a, total = (tally.get(n, {}).get(side(party)) or [0, 0])
                    share = round(100 * a / total) if total else 0
                    rec.append({"congress": congress, "chamber": chamber, "roll": fv["roll"], "date": fv["date"],
                                "measure": fv["measure"], "position": cast, "anti": anti,
                                "summary": fv["summary"] or fv["desc"], "url": fv["url"],
                                "own_party_share": share, "party_line": anti and share > 50})
                members.append({"bioguide_id": m["bioguide_id"], "name": _name(m.get("bioname")), "party": party,
                                "chamber": chamber, "state": m.get("state_abbrev", ""),
                                "district": seat[1], "votes": rec})
    return {"congress": congress, "years": [first, last], "v": VERSION, "built_at": now_iso(),
            "focus_rolls": {ch: len(focus[ch]) for ch in focus}, "members": members}, 0


def rate(history):
    """Apply today's strict rule to each Congress, on each member's record up to that Congress."""
    record = {}
    for c in sorted(history):
        for m in history[c]["members"]:
            rec = record.setdefault(m["bioguide_id"], [])
            rec.extend(m["votes"])
            judged = [v for v in rec if not (v["anti"] and v.get("party_line"))]
            anti = [v for v in judged if v["anti"]]
            congs = sorted({v["congress"] for v in anti})
            last_c = max((v["congress"] for v in judged), default=None)
            last = [v for v in judged if v["congress"] == last_c]
            held = bool(last) and sum(v["anti"] for v in last) / len(last) >= 0.5
            ratio = len(anti) / len(judged) if judged else 0
            m["antiArms"] = len(anti) >= 2 and len(congs) >= 2 and ratio >= 0.6 and held
            m["antiTier"] = "hard" if m["antiArms"] else None
            m["tier_source"] = "rule:history"
            m["tier_facts"] = {"focus_votes": len(rec), "anti_focus_votes": len(anti), "anti_congresses": congs,
                               "latest_congress_focus": f"{sum(v['anti'] for v in last)}/{len(last)} anti in the {last_c}th"
                               if last else None,
                               "party_line_anti_votes_not_counted": sum(1 for v in rec if v["anti"] and v.get("party_line"))}
            m["bottom_line"] = (("Counts as anti-establishment: " if m["antiArms"] else "Does not count: ")
                                + (f"broke from their own party to vote anti on {len(anti)} arms-aid votes across "
                                   f"{len(congs)} Congress{'es' if len(congs) != 1 else ''} by this point"
                                   if anti else "no anti vote on arms aid that broke from their own party by this point")
                                + ". (Rule-based from Voteview roll calls.)")
    return history


def sync(max_congresses=3):
    """Build any Congress not yet saved (oldest first), then rate them all and write the files."""
    os.makedirs(OUT_DIR, exist_ok=True)
    judgments = load_cache("vote_judgments")
    budget = {"left": int(os.environ.get("AECT_HISTORY_AI_BUDGET", "150")), "pending": 0}
    history, built, waiting = {}, 0, 0
    for c in range(FIRST, CURRENT_CONGRESS):
        path = os.path.join(OUT_DIR, f"raw_{c}.json")
        if os.path.exists(path):
            with open(path) as f:
                d = json.load(f)
            if d.get("v") == VERSION:
                history[c] = d
                continue
        if built >= max_congresses or ai_time_left() < 120:
            waiting += 1
            continue
        d, pending = build_congress(c, judgments, budget)
        save_cache("vote_judgments", judgments)
        if d is None:
            waiting += 1
            print(f"  history: {c}th Congress waiting on {pending} roll-call judgments")
            continue
        with open(path, "w") as f:
            json.dump(d, f, separators=(",", ":"))
        history[c] = d
        built += 1
        print(f"  history: {c}th Congress built ({len(d['members'])} members, focus rolls {d['focus_rolls']})")
    rate(history)
    index = []
    for c, d in sorted(history.items()):
        with open(os.path.join(OUT_DIR, f"congress_{c}.json"), "w") as f:
            json.dump({k: d[k] for k in ("congress", "years", "focus_rolls", "members")}, f, separators=(",", ":"))
        index.append({"congress": c, "years": d["years"],
                      "anti": {ch: sum(1 for m in d["members"] if m["chamber"] == ch and m["antiArms"])
                               for ch in ("house", "senate")}})
    with open(os.path.join(OUT_DIR, "index.json"), "w") as f:
        json.dump({"updated_at": now_iso(), "first": FIRST, "current": CURRENT_CONGRESS,
                   "congresses": index, "remaining": waiting}, f, indent=1)
    stream("history", ok=bool(history), count=len(history),
           note=f"{len(history)} past Congresses ready · {waiting} still to build")
    return waiting
