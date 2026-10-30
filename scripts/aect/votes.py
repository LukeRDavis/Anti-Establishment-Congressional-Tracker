"""
Step 4a — the voting record, discovered rather than listed.

House:  every roll call XML from clerk.house.gov for each year of the tracked
        Congresses (no key; votes keyed by bioguide ID).
Senate: the senate.gov vote menu + per-vote XML (no key).

Each roll call is cached permanently once fetched (a recorded vote never
changes). A cheap topical pre-filter narrows thousands of roll calls to the
ones plausibly about foreign military aid, arms, or war powers, and then AI —
grounded in live web search, because many amendment roll calls carry no
description at all ("Massie of Kentucky Part A Amendment No. 8") — decides:
  • is this vote actually about foreign military aid / arms / war authority?
  • which side (Yea or Nay) is the anti-interventionist position?
  • is it major (the vote's main purpose) or minor (a broad bill that only
    partly touches it)?
Judgments are cached forever per roll call, so each costs one AI call ever.
"""
import re, time
import xml.etree.ElementTree as ET

from . import ai
from .config import (CURRENT_CONGRESS, HISTORY_CONGRESSES, TODAY, CLERK_ROLL_XML,
                     CLERK_ROLL_PAGE, SENATE_VOTE_MENU, SENATE_VOTE_XML, congress_years,
                     ROLL_FETCH_BUDGET, AI_BUDGET_PER_RUN)
from .net import parse_xml, get_bytes, stream, load_cache, save_cache, now_iso

TOPIC = re.compile(
    r"israel|gaza|palestin|ukrain|russia|iran|taiwan|yemen|houthi|hamas|hezbollah|lebanon|syria|iraq|"
    r"afghan|venezuel|military|armed forces|\barms\b|weapon|munition|missile|defen[cs]e|national security|"
    r"department of state|state, foreign operations|foreign (?:military|assistance|aid|operations)|"
    r"security assistance|war powers|use of military force|\baumf\b|troops|deploy|nato|"
    r"foreign military (?:sale|financing)|arms sale|proposed sale|disapproval", re.I)

_VOTE_NORM = {"aye": "Yea", "yea": "Yea", "yes": "Yea", "no": "Nay", "nay": "Nay",
              "present": "Present", "not voting": "Not Voting"}

JUDGE_SYSTEM = """You classify U.S. congressional roll-call votes for a tracker of anti-interventionist voting: opposition to foreign military aid, arms sales or transfers, war authorizations, foreign military deployments, and military financing for foreign governments.

For the roll call given, use web search to find out exactly what the measure or amendment did, then decide:
- relevant: true only if the vote is substantively about foreign military aid/financing, arms sales or transfers, war powers / use of force, or deploying U.S. forces abroad. Domestic defense matters (base names, pay, procurement for U.S. forces, cultural issues) are NOT relevant. Procedural votes are not relevant.
- anti_position: "Yea" or "Nay" — the vote that REDUCES, BLOCKS, CONDITIONS or ENDS the aid/sale/authorization/deployment. A vote to cut aid → Yea is anti. A vote to approve or increase aid → Nay is anti. A war-powers resolution to remove forces → Yea is anti.
- significance: "major" if restricting or approving foreign military aid/arms/war is the vote's main purpose (supplemental aid packages, amendments cutting or adding aid, arms-sale disapproval resolutions, war-powers resolutions). "minor" if it is a broad bill (e.g. final passage of an NDAA or full appropriations act) where foreign military aid is only one part.
- summary: one plain sentence saying what the vote did, including dollar amounts and countries when known.

Respond with JSON only:
{"relevant": true|false, "anti_position": "Yea"|"Nay"|null, "significance": "major"|"minor"|null, "topic": "short label e.g. Israel military aid", "summary": "..."}"""


def _iso_date(raw):
    from datetime import datetime
    raw = (raw or "").strip()
    for fmt in ("%d-%b-%Y", "%B %d, %Y, %I:%M %p", "%B %d, %Y"):
        try:
            return datetime.strptime(re.sub(r"\s+", " ", raw), fmt).date().isoformat()
        except ValueError:
            continue
    m = re.match(r"([A-Za-z]+ \d{1,2}, \d{4})", raw)
    if m:
        try:
            return datetime.strptime(m.group(1), "%B %d, %Y").date().isoformat()
        except ValueError:
            pass
    return raw


def _norm_vote(v):
    return _VOTE_NORM.get((v or "").strip().lower(), (v or "").strip())


# ── House ─────────────────────────────────────────────────────────────────────
def _parse_house_roll(raw, year, roll):
    root = parse_xml(raw, "votes_house_xml", f"{year} roll {roll}")
    if root is None:
        return None
    meta = root.find("vote-metadata")
    if meta is None:
        stream("votes_house_xml", error=f"{year} roll {roll}: no <vote-metadata> (root <{root.tag}>)")
        return None
    g = lambda t: (meta.findtext(t) or "").strip()
    rec = {
        "chamber": "house", "year": year, "roll": roll, "congress": int(g("congress") or 0),
        "session": g("session"), "legis": g("legis-num"), "question": g("vote-question"),
        "desc": g("vote-desc"), "amendment_num": g("amendment-num"),
        "amendment_author": g("amendment-author"), "type": g("vote-type"),
        "result": g("vote-result"), "date": _iso_date(g("action-date")),
        "url": CLERK_ROLL_PAGE.format(year=year, roll=roll),
        "party_totals": {},
    }
    for tp in meta.iter("totals-by-party"):
        p = (tp.findtext("party") or "").strip()
        rec["party_totals"][p] = {k: int(tp.findtext(f"{k}-total") or 0)
                                  for k in ("yea", "nay", "present", "not-voting")}
    positions = {}
    for rv in root.iter("recorded-vote"):
        leg = rv.find("legislator")
        if leg is None:
            continue
        bid = leg.get("name-id") or ""
        if bid:
            positions[bid] = [_norm_vote(rv.findtext("vote")), leg.get("party", ""), leg.get("state", "")]
    return rec, positions


def _sync_house_year(year, cache, budget):
    """Fetch every roll call for `year` not already cached. Returns rolls fetched."""
    yc = cache.setdefault(str(year), {"rolls": {}, "complete": False})
    if yc.get("complete"):
        return 0
    fetched, misses = 0, 0
    roll = max([int(r) for r in yc["rolls"]] or [0]) + 1
    while fetched < budget and misses < 3:
        raw = get_bytes(CLERK_ROLL_XML.format(year=year, roll=roll), stream_name="votes_house_xml",
                        timeout=20, retries=1)
        if not raw:
            misses += 1
            roll += 1
            continue
        parsed = _parse_house_roll(raw, year, roll)
        if not parsed:
            misses += 1
            roll += 1
            continue
        misses = 0
        if parsed:
            rec, positions = parsed
            rec["candidate"] = _is_candidate(rec, yc)
            if rec["candidate"]:
                rec["positions"] = positions
            yc["rolls"][str(roll)] = rec
        fetched += 1
        roll += 1
        time.sleep(0.05)
    if misses >= 3 and year < TODAY.year and len(yc["rolls"]) > 100:
        yc["complete"] = True
    return fetched


def _is_candidate(rec, yc):
    text = " ".join([rec.get("legis", ""), rec.get("question", ""), rec.get("desc", ""),
                     rec.get("amendment_author", "")])
    # remember each bill's title: amendment roll calls often carry no description
    titles = yc.setdefault("bill_titles", {})
    if rec.get("desc") and rec.get("legis") and not rec.get("amendment_author"):
        titles.setdefault(rec["legis"], rec["desc"])
    if TOPIC.search(text):
        return True
    title = titles.get(rec.get("legis", ""), "")
    return bool(rec.get("amendment_author") and TOPIC.search(title))


def _backfill_candidates(yc, year):
    """Later passage votes reveal a bill's title; re-check earlier amendment rolls."""
    titles = yc.get("bill_titles", {})
    newly = []
    for r, rec in yc["rolls"].items():
        if rec.get("candidate") or not rec.get("amendment_author"):
            continue
        if TOPIC.search(titles.get(rec.get("legis", ""), "")):
            newly.append(int(r))
    for r in newly:
        raw = get_bytes(CLERK_ROLL_XML.format(year=year, roll=r), stream_name="votes_house_xml")
        parsed = _parse_house_roll(raw, year, r) if raw else None
        if parsed:
            rec, positions = parsed
            rec["candidate"] = True
            rec["positions"] = positions
            yc["rolls"][str(r)] = rec


# ── Senate ────────────────────────────────────────────────────────────────────
def _sync_senate_session(congress, session, cache, budget):
    key = f"{congress}-{session}"
    sc = cache.setdefault(key, {"rolls": {}, "complete": False})
    if sc.get("complete"):
        return 0
    raw = get_bytes(SENATE_VOTE_MENU.format(congress=congress, session=session),
                    stream_name="votes_senate_xml")
    if not raw:
        return 0
    root = parse_xml(raw, "votes_senate_xml", f"vote menu {key}")
    if root is None:
        return 0
    fetched = 0
    year = congress_years(congress)[session - 1]
    for v in root.iter("vote"):
        n = int(v.findtext("vote_number") or 0)
        if not n or str(n) in sc["rolls"]:
            continue
        title = (v.findtext("title") or "").strip()
        issue = (v.findtext("issue") or "").strip()
        question = (v.findtext("question") or "").strip()
        rec = {"chamber": "senate", "year": year, "roll": n, "congress": congress,
               "session": session, "legis": issue, "question": question, "desc": title,
               "result": (v.findtext("result") or "").strip(),
               "date": (v.findtext("vote_date") or "").strip(),
               "url": f"https://www.senate.gov/legislative/LIS/roll_call_votes/vote{congress}{session}/vote_{congress}_{session}_{n:05d}.htm"}
        rec["candidate"] = bool(TOPIC.search(f"{issue} {question} {title}"))
        if rec["candidate"] and fetched < budget:
            vraw = get_bytes(SENATE_VOTE_XML.format(congress=congress, session=session, vote=n),
                             stream_name="votes_senate_xml")
            vroot = parse_xml(vraw, "votes_senate_xml", f"vote {n}") if vraw else None
            if vroot is not None:
                rec["date"] = _iso_date(vroot.findtext("vote_date") or "") or rec["date"]
                amend = vroot.find("amendment")
                if amend is not None:
                    rec["amendment_num"] = (amend.findtext("amendment_number") or "").strip()
                    rec["amendment_purpose"] = (amend.findtext("amendment_purpose") or "").strip()
                rec["positions"] = {}
                for m in vroot.iter("member"):
                    k = f"{(m.findtext('last_name') or '').strip()}|{(m.findtext('state') or '').strip()}"
                    rec["positions"][k] = [_norm_vote(m.findtext("vote_cast")),
                                           (m.findtext("party") or "").strip(),
                                           (m.findtext("state") or "").strip()]
                fetched += 1
                time.sleep(0.05)
            else:
                continue  # retry next run
        sc["rolls"][str(n)] = rec
    if year < TODAY.year:
        sc["complete"] = True
    return fetched


# ── Senate via Voteview (senate.gov blocks GitHub's runners with HTTP 403) ────
VOTEVIEW = "https://voteview.com/static/data/out/{kind}/S{congress}_{kind}.csv"
_VV_CAST = {1: "Yea", 2: "Yea", 3: "Yea", 4: "Nay", 5: "Nay", 6: "Nay", 7: "Present", 8: "Present"}
_VV_PARTY = {100: "D", 200: "R"}


def _csv(url, stream_name):
    import csv, io
    raw = get_bytes(url, stream_name=stream_name, timeout=60)
    if not raw:
        return []
    return list(csv.DictReader(io.StringIO(raw.decode("utf-8", errors="replace"))))


def _sync_senate_voteview(congress, cache):
    key = f"vv-{congress}"
    sc = cache.setdefault(key, {"rolls": {}, "complete": False})
    if sc.get("complete"):
        return 0
    rollcalls = _csv(VOTEVIEW.format(kind="rollcalls", congress=congress), "votes_senate_voteview")
    if not rollcalls:
        return 0
    members = _csv(VOTEVIEW.format(kind="members", congress=congress), "votes_senate_voteview")
    icpsr = {}
    for m in members:
        if m.get("chamber") != "Senate":
            continue
        try:
            pc = int(float(m.get("party_code") or 0))
        except ValueError:
            pc = 0
        icpsr[m.get("icpsr")] = (m.get("bioguide_id") or "", _VV_PARTY.get(pc, "I"), m.get("state_abbrev", ""))
    candidates = {}
    for r in rollcalls:
        if r.get("chamber") != "Senate":
            continue
        n = int(float(r.get("rollnumber") or 0))
        if not n or str(n) in sc["rolls"]:
            continue
        text = " ".join(r.get(f, "") for f in ("bill_number", "vote_question", "vote_desc", "dtl_desc"))
        rec = {"chamber": "senate", "congress": congress, "roll": n,
               "year": int((r.get("date") or "0")[:4] or 0), "date": r.get("date", ""),
               "session": r.get("session"), "legis": r.get("bill_number", ""),
               "question": r.get("vote_question", ""),
               "desc": (r.get("vote_desc") or r.get("dtl_desc") or "")[:400],
               "result": r.get("vote_result", ""), "candidate": bool(TOPIC.search(text)),
               "url": (f"https://www.senate.gov/legislative/LIS/roll_call_votes/vote{congress}{r.get('session')}/"
                       f"vote_{congress}_{r.get('session')}_{int(float(r.get('clerk_rollnumber') or n)):05d}.htm")}
        sc["rolls"][str(n)] = rec
        if rec["candidate"]:
            candidates[n] = rec
    if candidates:
        for v in _csv(VOTEVIEW.format(kind="votes", congress=congress), "votes_senate_voteview"):
            if v.get("chamber") != "Senate":
                continue
            n = int(float(v.get("rollnumber") or 0))
            rec = candidates.get(n)
            if not rec:
                continue
            who = icpsr.get(v.get("icpsr"))
            if not who or not who[0]:
                continue
            cast = _VV_CAST.get(int(float(v.get("cast_code") or 9)), "Not Voting")
            rec.setdefault("positions", {})[who[0]] = [cast, who[1], who[2]]
    first, last = congress_years(congress)
    if last < TODAY.year:
        sc["complete"] = True
    return len(candidates)


# ── AI judgment ───────────────────────────────────────────────────────────────
def _judge(rec, judgments, budget_state):
    key = f"{rec['chamber']}-{rec['year']}-{rec['roll']}"
    if key in judgments:
        return judgments[key]
    if budget_state["left"] <= 0 or not ai.available(search=True):
        budget_state["pending"] += 1
        return None
    pt = rec.get("party_totals") or {}
    lines = [
        f"Chamber: {'House' if rec['chamber']=='house' else 'Senate'} roll call {rec['roll']}, "
        f"{rec.get('date','')} ({rec.get('congress')}th Congress)",
        f"Measure: {rec.get('legis','')}",
        f"Question: {rec.get('question','')}",
        f"Description: {rec.get('desc','') or '(none recorded)'}",
    ]
    if rec.get("amendment_author") or rec.get("amendment_num"):
        lines.append(f"Amendment: {rec.get('amendment_author','')} {rec.get('amendment_num','')}".strip())
    if rec.get("amendment_purpose"):
        lines.append(f"Amendment purpose: {rec['amendment_purpose']}")
    if pt:
        lines.append("Party totals: " + "; ".join(
            f"{p}: {v['yea']} yea / {v['nay']} nay" for p, v in pt.items()))
    lines.append(f"Result: {rec.get('result','')}  Source: {rec.get('url','')}")
    result, sources, provider = ai.ask_json(JUDGE_SYSTEM, "\n".join(lines), search=True)
    budget_state["left"] -= 1
    if not isinstance(result, dict) or "relevant" not in result:
        budget_state["pending"] += 1
        return None
    j = {"relevant": bool(result.get("relevant")),
         "anti_position": result.get("anti_position") if result.get("anti_position") in ("Yea", "Nay") else None,
         "significance": result.get("significance") if result.get("significance") in ("major", "minor") else "minor",
         "topic": (result.get("topic") or "")[:80], "summary": (result.get("summary") or "")[:400],
         "sources": (sources or [])[:4], "provider": provider, "judged_at": now_iso()}
    judgments[key] = j
    if j["relevant"]:
        print(f"    ✓ {key} {rec.get('legis','')}: {j['significance']} — anti={j['anti_position']} — {j['summary'][:90]}")
    return j


# ── Entry point ───────────────────────────────────────────────────────────────
def sync_votes():
    house_cache = load_cache("votes_house")
    senate_cache = load_cache("votes_senate")
    judgments = load_cache("vote_judgments")

    congresses = list(range(CURRENT_CONGRESS - HISTORY_CONGRESSES + 1, CURRENT_CONGRESS + 1))
    budget = ROLL_FETCH_BUDGET
    fetched_h = fetched_s = 0
    for c in reversed(congresses):          # newest first, so budget goes to what matters now
        for i, year in reversed(list(enumerate(congress_years(c)))):
            if year > TODAY.year:
                continue
            n = _sync_house_year(year, house_cache, budget - fetched_h)
            fetched_h += n
            _backfill_candidates(house_cache[str(year)], year)
        fetched_s += _sync_senate_voteview(c, senate_cache)
    save_cache("votes_house", house_cache)
    save_cache("votes_senate", senate_cache)

    all_rolls = []
    for yc in house_cache.values():
        all_rolls += [r for r in yc["rolls"].values() if r.get("candidate")]
    for sc in senate_cache.values():
        all_rolls += [r for r in sc["rolls"].values() if r.get("candidate") and r.get("positions")]
    total_house = sum(len(yc["rolls"]) for yc in house_cache.values())
    stream("votes_house_xml", ok=total_house > 0, count=total_house,
           note=f"{total_house} House roll calls cached (+{fetched_h} this run)")
    total_sen = sum(len(sc["rolls"]) for sc in senate_cache.values())
    stream("votes_senate_voteview", ok=total_sen > 0, count=total_sen,
           note=f"{total_sen} Senate roll calls indexed from Voteview (+{fetched_s} topical this run)")

    # newest first so the AI budget is spent on current votes before history
    all_rolls.sort(key=lambda r: (r.get("year", 0), r.get("roll", 0)), reverse=True)
    state = {"left": AI_BUDGET_PER_RUN // 2, "pending": 0}
    relevant = []
    for rec in all_rolls:
        j = _judge(rec, judgments, state)
        if j and j["relevant"] and j["anti_position"]:
            relevant.append({**rec, **j})
    save_cache("vote_judgments", judgments)
    stream("votes_ai_relevance", ok=bool(judgments) and state["pending"] == 0 or bool(relevant),
           count=len(relevant),
           note=f"{len(all_rolls)} topical roll calls · {len(relevant)} judged relevant · "
                f"{state['pending']} awaiting AI judgment"
                + ("" if ai.available(search=True) else " — NO search-capable AI key (GEMINI_KEY or XAI_API_KEY)"))
    return relevant


def score_members(members, relevant_votes):
    """
    Attach each member's record on the relevant votes. House votes match by
    bioguide ID; Senate votes by (last name, state).
    """
    by_bio = {m["bioguide_id"]: m for m in members if m.get("bioguide_id")}
    by_sen = {f"{m['last_name']}|{m['state']}": m for m in members if m["chamber"] == "senate"}
    for m in members:
        m["vote_record"] = []
    for v in relevant_votes:
        for key, (pos, party, state) in (v.get("positions") or {}).items():
            m = by_bio.get(key) or by_sen.get(key)
            if not m or pos not in ("Yea", "Nay"):
                continue
            m["vote_record"].append({
                "congress": v["congress"], "chamber": v["chamber"], "roll": v["roll"],
                "date": v["date"], "measure": v.get("legis", ""), "summary": v.get("summary", ""),
                "significance": v["significance"], "position": pos,
                "anti": pos == v["anti_position"], "url": v.get("url", ""),
            })
    return members


def incumbent_retention():
    """
    Share of House members voting in the previous Congress who are still
    voting in the current one — the empirical re-election/retention rate,
    used for incumbents that have no prediction market.
    """
    cache = load_cache("votes_house")
    seen = {}
    for yc in cache.values():
        for rec in yc.get("rolls", {}).values():
            if rec.get("positions"):
                seen.setdefault(rec.get("congress"), set()).update(rec["positions"].keys())
    prev, cur = seen.get(CURRENT_CONGRESS - 1), seen.get(CURRENT_CONGRESS)
    if not prev or not cur or len(prev) < 300:
        return None
    return round(len(prev & cur) / len(prev), 3)


def congress_history(relevant_votes):
    """Per-Congress counts of members who took the anti position on a major vote."""
    hist = {}
    by_c = {}
    for v in relevant_votes:
        if v["significance"] != "major":
            continue
        by_c.setdefault(v["congress"], []).append(v)
    for c, votes in by_c.items():
        tally = {}
        for v in votes:
            for key, (pos, party, state) in (v.get("positions") or {}).items():
                if pos not in ("Yea", "Nay"):
                    continue
                t = tally.setdefault(key, {"party": party[:1], "anti": 0, "total": 0})
                t["total"] += 1
                t["anti"] += pos == v["anti_position"]
        rec = {"congress": c, "label": f"{c}th", "years": "–".join(map(str, congress_years(c))),
               "major_votes": len(votes), "source": "clerk.house.gov + senate.gov roll calls (AI-judged)"}
        for p in ("R", "D", "I"):
            anti = [t for t in tally.values() if t["party"] == p and t["anti"] > 0]
            rec[f"hard{p}"] = sum(1 for t in anti if t["anti"] / t["total"] >= 0.5)
            rec[f"soft{p}"] = sum(1 for t in anti if t["anti"] / t["total"] < 0.5)
        rec["key_events"] = [f"{v['date']}: {v.get('legis','')} — {v.get('summary','')}"
                             for v in sorted(votes, key=lambda v: v["date"])][-8:]
        if c == CURRENT_CONGRESS:
            rec["is_live"] = True
        hist[str(c)] = rec
    return hist
