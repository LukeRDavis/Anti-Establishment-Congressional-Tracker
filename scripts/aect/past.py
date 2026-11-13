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
import csv, io, json, os, re

from .config import (CURRENT_CONGRESS, congress_years, FOCUS_RECIPIENTS, FOCUS_CATEGORY, ai_time_left,
                     CONGRESS_API, CONGRESS_KEY)
from .net import get_bytes, get_json, stream, load_cache, save_cache, now_iso
from . import votes as V

FIRST = int(os.environ.get("AECT_HISTORY_FROM", "107"))
OUT_DIR = os.path.join("data", "history")
VERSION = 10
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
    # Voteview: "LAST, Given Middle (Nickname), Suffix" -> "Nickname-or-Given Last Suffix"
    if "," not in (bioname or ""):
        return (bioname or "").title()
    last, rest = bioname.split(",", 1)
    given, _, suffix = rest.partition(",")
    nick = re.search(r"\(([^)]+)\)", given)
    first = nick.group(1) if nick else (given.split() or [""])[0]
    return " ".join(x for x in (first.strip(), last.strip().title(), suffix.strip()) if x)


BILL_TYPES = {"HR": "hr", "S": "s", "HRES": "hres", "SRES": "sres", "HJRES": "hjres", "SJRES": "sjres",
              "HCONRES": "hconres", "SCONRES": "sconres"}
_BILLS = None


def _bill_context(congress, bill_number):
    """Official title and Congressional Research Service summary of a bill, from Congress.gov.
    Roll-call records often carry only a generic title ("Making emergency supplemental
    appropriations..."), which tells the judge nothing about what the money is for."""
    global _BILLS
    if _BILLS is None:
        _BILLS = load_cache("bill_context")
    m = re.match(r"^([A-Z]+?)(\d+)$", (bill_number or "").replace(" ", "").replace(".", "").upper())
    if not m or m.group(1) not in BILL_TYPES or not CONGRESS_KEY:
        return ""
    key = f"{congress}-{m.group(1)}{m.group(2)}"
    if key in _BILLS:
        return _BILLS[key]
    base = f"{CONGRESS_API}/bill/{congress}/{BILL_TYPES[m.group(1)]}/{m.group(2)}"
    d = get_json(f"{base}?format=json&api_key={CONGRESS_KEY}", stream_name="history_bills")
    if d is None:
        return ""                          # not cached: try again next run
    title = ((d or {}).get("bill") or {}).get("title", "")
    sm = get_json(f"{base}/summaries?format=json&api_key={CONGRESS_KEY}", stream_name="history_bills") or {}
    summ = ((sm.get("summaries") or [{}])[-1].get("text") or "")
    summ = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", summ)).strip()[:1500]
    _BILLS[key] = (f"Bill title: {title}. " if title else "") + (f"CRS summary: {summ}" if summ else "")
    return _BILLS[key]


def _focus_then(j, rec):
    """Arms/military aid to the focus recipients, as it was usually voted on before 2023.
    Today's test (V.is_focus) needs the focus recipients alone and a 'major' rating; in earlier
    Congresses the aid mostly rode in omnibus defense and foreign-operations bills covering
    dozens of countries, which say nothing about Israel in particular, while the standalone
    measures (security-cooperation acts, Iron Dome funding, resolutions backing military
    operations) were often rated 'minor'. So: any arms/aid vote whose recipients are only the
    focus countries, or the focus country with one other, named in the measure itself."""
    if j.get("area") in V.AREAS:          # judged under the five-area rules: the judge decided
        return V.is_focus(j)
    if V.area(j) != "israel_aid":         # older judgment, other areas: only votes about that area
        return V.is_focus(j)
    if V.is_focus(j):
        return True
    rec_set = {r.lower() for r in (j.get("recipients") or [])}
    focus = {f.lower() for f in FOCUS_RECIPIENTS}
    if not rec_set & focus:
        return False
    # the roll call's own description (Voteview), not the AI's summary, must name the focus
    # recipient for anything but a measure aimed only at it; a general defense or spending
    # bill that merely includes the aid among everything else says nothing about the member
    own = " ".join([rec.get("legis", ""), rec.get("question", ""), rec.get("vv_desc", rec.get("desc", ""))]).lower()
    named = any(f in own for f in focus)
    if OMNIBUS.search(own) and not named:
        return False
    others = rec_set - focus
    return (not others and (named or not OMNIBUS.search(j.get("summary", "")))) or \
        (len(others) <= 1 and named and "consideration of" not in own)


PASSAGE = re.compile(r"passage|pass the bill|suspend the rules and pass|conference report|concur|override", re.I)
SPEND = re.compile(r"appropriat|supplemental|authoriz", re.I)
OMNIBUS = re.compile(r"defense authorization|authorization act for fiscal|department of defense appropriations|"
                     r"defense appropriations|consolidated appropriations|omnibus|continuing appropriations|"
                     r"further appropriations|making appropriations for", re.I)


def _judged_focus(congress, ch_letter, judgments, budget):
    """Roll calls in this Congress that are focus votes, with the anti side, as {roll: rec}."""
    chamber = "house" if ch_letter == "H" else "senate"
    rolls = _csv("rollcalls", ch_letter, congress)
    out, pending = {}, 0
    for r in rolls:
        if (r.get("chamber") or "").lower() != chamber:
            continue
        text = " ".join(r.get(f, "") for f in ("bill_number", "vote_question", "vote_desc", "dtl_desc"))
        # standalone aid bills often carry only a generic title ("Making emergency supplemental
        # appropriations for ..."), so passage votes on any spending bill go to the judge too
        if not (V.TOPIC.search(text) or (PASSAGE.search(r.get("vote_question", "")) and SPEND.search(text))):
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
        # passage-type votes: give the judge the bill's own title and summary; a verdict made
        # without them is redone when they name a focus recipient it didn't pick up
        if PASSAGE.search(rec["question"]):
            ctx = _bill_context(congress, rec["legis"])
            if ctx:
                rec["vv_desc"] = rec["desc"]
                rec["desc"] = (rec["desc"] + " | " + ctx)[:1900]
                key = f"{chamber}-{rec['year']}-{rec['roll']}"
                old = judgments.get(key)
                mentions = any(f.lower() in ctx.lower() for f in FOCUS_RECIPIENTS) or "iron dome" in ctx.lower()
                if old and mentions and not old.get("bill_context") and not (old.get("relevant") and _focus_then(old, rec)):
                    judgments.pop(key)
        if budget["left"] <= 0 or ai_time_left() < 60:
            key = f"{chamber}-{rec['year']}-{rec['roll']}"
            if key not in judgments:
                pending += 1
                continue
        j = V._judge(rec, judgments, budget)
        if j is not None and PASSAGE.search(rec["question"]) and "|" in rec["desc"]:
            j["bill_context"] = True
        if j is None:
            pending += 1
            continue
        if j.get("relevant") and _focus_then(j, rec):
            # a measure that funds or backs the aid: voting no is the anti side; one that cuts it: yes
            anti = j.get("anti_position") or ("Yea" if j.get("measure_restricts") else "Nay")
            out[n] = {**rec, "anti_position": anti, "summary": j.get("summary", ""), "measure": rec["legis"],
                      "area": V.area(j)}
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
                    share = share if anti else 100 - share      # own party voting the same way
                    rec.append({"congress": congress, "chamber": chamber, "roll": fv["roll"], "date": fv["date"],
                                "measure": fv["measure"], "position": cast, "anti": anti, "area": fv["area"],
                                "summary": fv["summary"] or fv["desc"], "url": fv["url"],
                                "own_party_share": share, "party_line": share > 50})
                members.append({"bioguide_id": m["bioguide_id"], "name": _name(m.get("bioname")), "party": party,
                                "chamber": chamber, "state": m.get("state_abbrev", ""),
                                "district": seat[1], "votes": rec})
    return {"congress": congress, "years": [first, last], "v": VERSION, "built_at": now_iso(),
            "focus_rolls": {ch: len(focus[ch]) for ch in focus}, "members": members}, 0


def rate(history):
    """Apply today's rule to each Congress, on each member's record up to that Congress: at least one
    real breaking vote (anti-interventionist against most of their own party; discharge-petition data
    is not available for past Congresses), and consistent on the anchor area, aid to Israel; the other
    areas are supporting evidence (military aid and sanctions judged country by country)."""
    record, anchor_seen = {}, set()
    for c in sorted(history):
        # a chamber that has not yet held a roll call on the anchor can't be judged on it:
        # until it has, members there are judged on the old bar (consistent in every area engaged)
        anchor_seen |= {m.get("chamber") for m in history[c]["members"]
                        if any(v.get("area") == V.ANCHOR for v in m["votes"])}
        for m in history[c]["members"]:
            rec = record.setdefault(m["bioguide_id"], [])
            rec.extend(v for v in m["votes"] if v.get("area"))
            anti = [v for v in rec if v["anti"]]
            breaking = [v for v in anti if not v.get("party_line")]
            areas = V.area_record(rec)
            has_anchor = m.get("chamber") in anchor_seen
            if has_anchor:
                ok = V.consistent(areas)
                # same bar as today's rule fallback: at least one breaking vote on the anchor itself
                m["antiArms"] = any(v.get("area") == V.ANCHOR for v in breaking) and ok
            else:
                ok = bool(anti) and not V.slips(areas)
                m["antiArms"] = bool(breaking) and ok
            m["antiTier"] = "hard" if m["antiArms"] else None
            m["tier_source"] = "rule:history"
            m["tier_facts"] = {"focus_votes": len(rec), "anti_focus_votes": len(anti),
                               "breaking_anti_votes": len(breaking), "areas": areas,
                               "anti_congresses": sorted({v["congress"] for v in anti})}
            engaged = "; ".join(f"{x['label']}: {x['status']} ({x['anti']} of {x['votes']} anti)"
                                for x in areas.values() if x["anti"])
            anc = areas.get(V.ANCHOR)
            sl = V.slips(areas)
            if m["antiArms"]:
                why = "Counts as anti-establishment: " + engaged + (f"; lower confidence: went back on {', '.join(sl)}" if sl else "")
            elif not anti:
                why = "Does not count: no anti-interventionist vote"
            elif not has_anchor:
                why = ("Does not count: " + (f"went back to the interventionist side on {', '.join(sl)}" if sl
                                             else "no anti-interventionist vote against most of their own party"))
            elif anc and anc["anti"] and not ok:
                why = f"Does not count: {anc['status']} on {anc['label']}"
            elif not ok:
                why = f"Does not count: no anti-interventionist vote on {V.AREA_LABEL[V.ANCHOR]}"
            else:
                why = f"Does not count: no anti-interventionist vote on {V.AREA_LABEL[V.ANCHOR]} against most of their own party"
            m["bottom_line"] = (why + (f" ({engaged})" if engaged and not m["antiArms"] else "") + " by this point."
                                + ("" if has_anchor else f" No roll call on {V.AREA_LABEL[V.ANCHOR]} had been held in this "
                                   "chamber yet, so this is judged on consistency in every area engaged.")
                                + " (Rule-based from Voteview roll calls.)")
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
        if _BILLS is not None:
            save_cache("bill_context", _BILLS)
        if d is None:
            waiting += 1
            print(f"  history: {c}th Congress waiting on {pending} roll-call judgments")
            continue
        with open(path, "w") as f:
            json.dump(d, f, separators=(",", ":"))
        history[c] = d
        built += 1
        print(f"  history: {c}th Congress built ({len(d['members'])} members, focus rolls {d['focus_rolls']})")
    # members who still serve show under the name the current roster uses
    try:
        with open("data.json") as f:
            names = {m["bioguide_id"]: m["name"] for m in json.load(f).get("members", []) if m.get("bioguide_id")}
    except Exception:
        names = {}
    for d in history.values():
        for m in d["members"]:
            m["name"] = names.get(m["bioguide_id"], m["name"])
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
