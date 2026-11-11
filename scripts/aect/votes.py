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
from .config import (CONGRESS_API, CONGRESS_KEY,
                     CURRENT_CONGRESS, HISTORY_CONGRESSES, TODAY, CLERK_ROLL_XML,
                     CLERK_ROLL_PAGE, SENATE_VOTE_MENU, SENATE_VOTE_XML, congress_years,
                     ROLL_FETCH_BUDGET, AI_BUDGET_PER_RUN, FOCUS_CATEGORY, FOCUS_RECIPIENTS)
from .net import parse_xml, get_bytes, get_json, stream, load_cache, save_cache, now_iso, qs, circuit_ok

TOPIC = re.compile(
    r"israel|gaza|palestin|ukrain|russia|iran|taiwan|yemen|houthi|hamas|hezbollah|lebanon|syria|iraq|"
    r"afghan|venezuel|military|armed forces|\barms\b|weapon|munition|missile|defen[cs]e|national security|"
    r"department of state|state, foreign operations|foreign (?:military|assistance|aid|operations)|"
    r"security assistance|war powers|use of military force|\baumf\b|troops|deploy|nato|"
    r"foreign military (?:sale|financing)|arms sale|proposed sale|disapproval|"
    r"iron dome|david'?s sling|arrow (?:missile|program)|qualitative military edge|"
    r"sanction|embargo|export control|declaration of war|authorization for (?:the )?use", re.I)
_TOPIC_ID = __import__("hashlib").sha1(TOPIC.pattern.encode()).hexdigest()[:10]
# votes the v3 judge set aside that may be sanctions votes, which it was not asked about
_RECHECK = re.compile(r"sanction|embargo", re.I)

_VOTE_NORM = {"aye": "Yea", "yea": "Yea", "yes": "Yea", "no": "Nay", "nay": "Nay",
              "present": "Present", "not voting": "Not Voting"}

JUDGE_SYSTEM = """You classify U.S. congressional roll-call votes for a non-partisan tracker of anti-interventionist voting. The tracker measures five areas:
1. israel_aid: any aid to the State of Israel (its government or military): military aid, financing, arms, missile defense, or any other aid given to the Israeli state.
2. military_aid: military aid, military financing, or arms sales/transfers to any other foreign government or armed force.
3. sanctions: sanctions, embargoes or similar economic punishment of a foreign country (or blocking their lifting). Sanctions aimed at human-rights abuses, genocide or mass killing are NEUTRAL; sanctions for geopolitical, strategic or trade-rivalry reasons are interventionist. A measure that bars a country from trade, financing, loans, investment or support through international institutions IS a sanction: imposing, extending or tightening it is the interventionist side, and lifting, easing or blocking it is the anti-interventionist side. A measure that cuts or bars military aid or arms to a country is military_aid (or israel_aid), not sanctions.
Sovereignty-defense exception (military_aid only, never israel_aid): legitimate lend-lease deals and MOSTLY defensive arms (air and missile defense, defensive equipment) for a country resisting a direct violation of its territorial sovereignty — an invasion, annexation or occupation of its territory recognized as unlawful under international law standards (Hague conventions, UN General Assembly or Security Council, ICC, ICJ, or major human rights organizations) — are NEUTRAL. Deterrence against a threat that has not happened, offensive or long-range strike weapons as a main part, open-ended financing, and arms to a government not itself defending its own territory are not covered. A package that bundles covered aid with aid that is not covered is judged by the uncovered part.
4. use_of_force: deploying or keeping U.S. forces abroad, strikes and hostilities. Restraining use of force that does not answer an imminent threat to U.S. soil or citizens and has no formal declaration of war is the anti-interventionist side.
5. war_authorization: declarations of war and authorizations for use of military force (AUMFs), including repealing them. A broad or open-ended authorization is interventionist; a narrow, time-limited one with constrained goals aimed at preventing genocide or mass killing is NEUTRAL; voting against broad authorizations or to repeal them is anti-interventionist.
Non-military aid (humanitarian, development, food, refugee) to countries other than Israel is NEUTRAL.

For the roll call given, use the description, bill title and summary given (and web search if available) to find out exactly what the measure or amendment did, then decide:
- relevant: true if the vote is substantively about any of the five areas. Domestic defense matters (base names, pay, procurement for U.S. forces, cultural issues) and purely procedural votes are NOT relevant.
- area: one of "israel_aid", "military_aid", "sanctions", "use_of_force", "war_authorization", or "other".
- neutral: true if, under the rules above, neither side of the vote is interventionist (human-rights or genocide sanctions, narrow atrocity-prevention authorizations, non-military aid, sovereignty-defense lend-lease or mostly defensive arms).
- anti_position: "Yea" or "Nay": the vote on the anti-interventionist side, the one that REDUCES, BLOCKS, CONDITIONS, NARROWS or ENDS the aid, arms, sanctions, hostilities or authorization (for sanctions: the side against imposing or keeping them). Null if neutral.
- significance: "major" if one of the five areas is the vote's main purpose (aid packages, amendments cutting or adding aid, arms-sale disapprovals, sanctions bills, war-powers resolutions, AUMFs or their repeal). "minor" if it is a broad bill (e.g. final passage of an NDAA or full appropriations act) where these are only one part.
- severe_interventionist: for a "minor" broad bill only: true if it nonetheless contains particularly severe interventionist provisions (e.g. a large new war authorization, tens of billions in new military aid, or a new sanctions regime as a headline part), so that its final passage is a fair test; false otherwise.
- category: "arms_aid" for aid, financing or arms; "war_powers" for forces, hostilities or authorizations; "sanctions" for sanctions; "other" otherwise.
- sanctions_effect (sanctions votes only, else null): "imposes" if the measure or amendment voted on imposes, extends, tightens or makes sanctions harder to lift (including barring waivers, rescinding licenses or relief, or reviewing/limiting the President's power to ease them); "eases" if it lifts, eases, suspends or terminates them.
- sovereignty_defense (military_aid votes only, else false): true if the recipient is resisting a direct violation of its own territorial sovereignty (an invasion, annexation or occupation of its territory) recognized as unlawful by the UN, ICJ, ICC, the Hague conventions or major human rights organizations.
- defensive_or_lend_lease (military_aid votes only, else false): true if the aid in question is a lend-lease arrangement or MOSTLY defensive arms or equipment.
- measure_restricts: true if the measure or amendment being voted on itself cuts, blocks, conditions or ends the aid/sale/sanctions/hostilities/authorization; false if it provides, approves, imposes or expands them (a bill barring a country from financing or trade imposes a sanction, so it is false).
- recipients: the foreign countries the vote is about, as English country names (e.g. ["Israel"], ["Ukraine", "Israel"], ["Iran"]). Empty if none.
- summary: one plain sentence saying what the vote did, including dollar amounts and countries when known.

Respond with JSON only:
{"relevant": true|false, "area": "israel_aid"|"military_aid"|"sanctions"|"use_of_force"|"war_authorization"|"other", "neutral": true|false, "anti_position": "Yea"|"Nay"|null, "significance": "major"|"minor"|null, "severe_interventionist": true|false, "category": "arms_aid"|"war_powers"|"sanctions"|"other", "measure_restricts": true|false, "sanctions_effect": "imposes"|"eases"|null, "sovereignty_defense": true|false, "defensive_or_lend_lease": true|false, "recipients": ["..."], "topic": "short label", "summary": "..."}"""

AREAS = ("israel_aid", "military_aid", "sanctions", "use_of_force", "war_authorization")
AREA_LABEL = {"israel_aid": "aid to Israel", "military_aid": "military aid to other countries",
              "sanctions": "sanctions", "use_of_force": "use of force abroad",
              "war_authorization": "war authorizations"}
JUDGE_VERSION = 7   # bump when the judgment schema changes; older relevant judgments are re-asked
# versions 5 and 6 changed one area each (sanctions; the sovereignty-defense exception for
# military aid): older judgments outside the areas changed since stay current
_CHANGED_SINCE = {4: ("sanctions", "military_aid"), 5: ("sanctions", "military_aid"),
                  6: ("sanctions", "military_aid")}


def _current(j):
    v = j.get("version", 1)
    return v >= JUDGE_VERSION or (v in _CHANGED_SINCE and j.get("area") not in _CHANGED_SINCE[v])


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
        if rec.get("candidate"):
            continue
        own = " ".join([rec.get("legis", ""), rec.get("question", ""), rec.get("desc", "")])
        if TOPIC.search(own) or (rec.get("amendment_author") and TOPIC.search(titles.get(rec.get("legis", ""), ""))):
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
    # a finished Congress is re-read when the topic filter has changed since it was indexed
    if sc.get("complete") and sc.get("topic") == _TOPIC_ID:
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
        text = " ".join(r.get(f, "") for f in ("bill_number", "vote_question", "vote_desc", "dtl_desc"))
        old = sc["rolls"].get(str(n))
        if not n or (old and (old.get("candidate") or not TOPIC.search(text))):
            continue
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
        sc["topic"] = _TOPIC_ID
    return len(candidates)


# ── Congress.gov context for amendment roll calls ────────────────────────────
_SHOWN_FIELDS = set()


def _cg(path, **params):
    if not CONGRESS_KEY or not circuit_ok("congress_gov"):
        return None
    return get_json(f"{CONGRESS_API}{path}?" + qs({"api_key": CONGRESS_KEY, "format": "json", **params}),
                    stream_name="congress_gov")


def _amendment_context(rec):
    """Fill in what an amendment did, from Congress.gov, when the roll call XML doesn't say."""
    if rec.get("amendment_purpose") or rec["chamber"] != "house" or not rec.get("amendment_author"):
        return
    sess = rec.get("session") or (1 if rec["year"] % 2 else 2)
    sess = re.sub(r"\D", "", str(sess)) or "1"
    d = _cg(f"/house-vote/{rec['congress']}/{sess}/{rec['roll']}")
    v = (d or {}).get("houseRollCallVote") or {}
    if v and "house-vote" not in _SHOWN_FIELDS:
        _SHOWN_FIELDS.add("house-vote")
        stream("congress_gov", note=f"house-vote fields: {sorted(v.keys())[:30]}")
    atype = (v.get("amendmentType") or "").lower()
    anum = v.get("amendmentNumber")
    if not atype or not anum:
        return
    a = (_cg(f"/amendment/{rec['congress']}/{atype}/{anum}") or {}).get("amendment") or {}
    if a and "amendment" not in _SHOWN_FIELDS:
        _SHOWN_FIELDS.add("amendment")
        stream("congress_gov", note=f"amendment fields: {sorted(a.keys())[:30]}")
    purpose = a.get("purpose") or a.get("description") or ""
    if purpose:
        rec["amendment_purpose"] = purpose[:600]
        rec["congress_gov_amendment"] = f"{atype.upper()} {anum}"


_BILL_TYPES = {"HR": "hr", "S": "s", "HRES": "hres", "SRES": "sres", "HJRES": "hjres",
               "SJRES": "sjres", "HCONRES": "hconres", "SCONRES": "sconres"}


def _sponsor(rec):
    """Bioguide ID of whoever offered the measure (amendment author, else bill sponsor), from Congress.gov."""
    if "sponsor" in rec:
        return rec["sponsor"]
    sid = None
    if rec.get("congress_gov_amendment"):
        atype, anum = rec["congress_gov_amendment"].lower().split()
        a = (_cg(f"/amendment/{rec['congress']}/{atype}/{anum}") or {}).get("amendment") or {}
        sid = ((a.get("sponsors") or [{}])[0]).get("bioguideId")
    if not sid and rec.get("chamber") == "senate" and rec.get("amendment_num"):
        n = re.search(r"(\d+)", rec["amendment_num"])
        if n:
            a = (_cg(f"/amendment/{rec['congress']}/samdt/{n.group(1)}") or {}).get("amendment") or {}
            sid = ((a.get("sponsors") or [{}])[0]).get("bioguideId")
    # a vote on an amendment is never checked against the underlying bill's sponsor:
    # the bill's author often votes against an amendment that cuts its own bill
    on_amendment = bool(rec.get("amendment_num") or rec.get("amendment_author") or rec.get("congress_gov_amendment")
                        or re.search(r"amendment", f"{rec.get('question','')} {rec.get('desc','')}", re.I))
    if not sid and not on_amendment:
        m = re.match(r"([A-Z\. ]+?)\s*(\d+)$", (rec.get("legis") or "").upper().strip())
        if m:
            btype = _BILL_TYPES.get(re.sub(r"[\. ]", "", m.group(1)))
            if btype:
                b = (_cg(f"/bill/{rec['congress']}/{btype}/{m.group(2)}") or {}).get("bill") or {}
                sid = ((b.get("sponsors") or [{}])[0]).get("bioguideId")
    rec["sponsor"] = sid
    return sid


def _apply_rules(j):
    """Turn the AI's narrow yes/no answers into the tracker's rules, so the direction and the
    exceptions don't hang on the model getting a compound judgment right in one step."""
    if j.get("area") == "sanctions" and j.get("sanctions_effect"):
        # the anti-interventionist side opposes imposing sanctions and supports easing them
        j["measure_restricts"] = j["sanctions_effect"] == "eases"
    if j.get("area") == "military_aid" and j.get("sovereignty_defense") and j.get("defensive_or_lend_lease"):
        j["neutral"] = True                    # the sovereignty-defense exception
        j["anti_position"] = None
    return j


# a vote whose Yea advances the measure itself (not a motion to table or recommit it)
_YEA_ADVANCES = re.compile(r"^\s*on (?:passage|the passage|agreeing to the (?:amendment|resolution|conference report)|"
                           r"motion to suspend the rules and (?:pass|agree)|the (?:amendment|joint resolution|bill|"
                           r"resolution|concurrent resolution)|the motion to proceed|cloture|the cloture motion)", re.I)


def direction_check(rec, j):
    """
    Cross-check the AI's anti side against the record: whoever offered a
    restricting measure votes for it, so their vote is the anti side (and the
    opposite for a measure that provides aid). Corrects a flipped judgment.
    """
    if j.get("measure_restricts") is None or not j.get("anti_position"):
        return j
    sid = j.get("sponsor") or _sponsor(rec)
    pos = ((rec.get("positions") or {}).get(sid) or [None])[0] if sid else None
    if pos not in ("Yea", "Nay"):
        # no sponsor vote to check against: on a straight vote on the measure, Yea is for it
        if _YEA_ADVANCES.search(rec.get("question") or ""):
            expected = "Yea" if j["measure_restricts"] else "Nay"
            if expected != j["anti_position"]:
                print(f"    ↺ {rec['chamber']}-{rec['year']}-{rec['roll']}: anti side corrected "
                      f"{j['anti_position']}→{expected} (Yea advances a measure that "
                      f"{'restricts' if j['measure_restricts'] else 'provides or imposes'})")
                j["anti_position"] = expected
                j["direction"] = "corrected_by_question"
        return j
    expected = pos if j["measure_restricts"] else ("Nay" if pos == "Yea" else "Yea")
    j["sponsor"] = sid
    if expected != j["anti_position"]:
        print(f"    ↺ {rec['chamber']}-{rec['year']}-{rec['roll']}: anti side corrected "
              f"{j['anti_position']}→{expected} (sponsor {sid} voted {pos})")
        j["anti_position"] = expected
        j["direction"] = "corrected_by_sponsor_vote"
    else:
        j["direction"] = "confirmed_by_sponsor_vote"
    return j


def _recheck_direction(rec, j):
    """Redo the sponsor cross-check with the amendment-aware sponsor lookup, undoing a
    correction that was made against the underlying bill's sponsor."""
    if not CONGRESS_KEY or not circuit_ok("congress_gov"):
        return                  # can't look the sponsor up now; try again next run
    _amendment_context(rec)
    rec.pop("sponsor", None)
    sid = _sponsor(rec)
    if sid != j.get("sponsor"):
        if j.get("direction") == "corrected_by_sponsor_vote" and j.get("anti_position") in ("Yea", "Nay"):
            j["anti_position"] = "Nay" if j["anti_position"] == "Yea" else "Yea"
            print(f"    ↺ {rec['chamber']}-{rec['year']}-{rec['roll']}: undid a correction made against the bill sponsor")
        j.pop("sponsor", None)
        j["direction"] = None
        if sid:
            direction_check(rec, j)
    j["sponsor_check"] = 2


# ── AI judgment ───────────────────────────────────────────────────────────────
def _judge(rec, judgments, budget_state):
    key = f"{rec['chamber']}-{rec['year']}-{rec['roll']}"
    old = judgments.get(key)
    if old and old.get("relevant") and old.get("direction") and old.get("sponsor_check") != 2:
        _recheck_direction(rec, old)
    if old and (_current(old) or (not old.get("relevant") and not _RECHECK.search(
            " ".join(str(rec.get(x, "")) for x in ("legis", "question", "desc")) + " " + old.get("summary", "")))):
        return old
    if budget_state["left"] <= 0 or not ai.available_any():
        budget_state["pending"] += 1
        return old              # keep using the older judgment until it can be refreshed
    _amendment_context(rec)
    # Without web search the model must be given the measure's own text;
    # an amendment with neither a description nor a purpose is left for later.
    has_text = bool(rec.get("desc") or rec.get("amendment_purpose"))
    if not ai.available(search=True) and not has_text:
        budget_state["pending"] += 1
        return old
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
    result, sources, provider = ai.ask_json(JUDGE_SYSTEM, "\n".join(lines), search=True,
                                            allow_ungrounded=has_text)
    budget_state["left"] -= 1
    if not isinstance(result, dict) or "relevant" not in result:
        budget_state["pending"] += 1
        return old
    j = {"relevant": bool(result.get("relevant")),
         "anti_position": result.get("anti_position") if result.get("anti_position") in ("Yea", "Nay") else None,
         "significance": result.get("significance") if result.get("significance") in ("major", "minor") else "minor",
         "category": result.get("category") if result.get("category") in ("arms_aid", "war_powers", "sanctions") else "other",
         "recipients": [str(c).strip()[:40] for c in (result.get("recipients") or []) if str(c).strip()][:6],
         "measure_restricts": result.get("measure_restricts") if isinstance(result.get("measure_restricts"), bool) else None,
         "area": result.get("area") if result.get("area") in AREAS else "other",
         "neutral": bool(result.get("neutral")),
         "severe_interventionist": bool(result.get("severe_interventionist")),
         "sanctions_effect": result.get("sanctions_effect") if result.get("sanctions_effect") in ("imposes", "eases") else None,
         "sovereignty_defense": bool(result.get("sovereignty_defense")),
         "defensive_or_lend_lease": bool(result.get("defensive_or_lend_lease")),
         "version": JUDGE_VERSION,
         "topic": (result.get("topic") or "")[:80], "summary": (result.get("summary") or "")[:400],
         "sources": (sources or [])[:4], "provider": provider, "grounded": bool(sources),
         "context": rec.get("amendment_purpose", "")[:300], "judged_at": now_iso()}
    _apply_rules(j)
    if j["relevant"]:
        direction_check(rec, j)
        j["sponsor_check"] = 2
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
    def _stale(r):
        j = judgments.get(f"{r['chamber']}-{r['year']}-{r['roll']}")
        return bool(j and j.get("relevant") and not _current(j))
    all_rolls.sort(key=lambda r: not _stale(r))      # stable: stale relevant ones first, still newest first
    state = {"left": int(__import__("os").environ.get("AECT_VOTE_AI_BUDGET", "200")), "pending": 0}
    relevant = []
    for i, rec in enumerate(all_rolls):
        j = _judge(rec, judgments, state)
        if j and j["relevant"] and j["anti_position"]:
            relevant.append({**rec, **j})
        if i % 20 == 19:
            save_cache("vote_judgments", judgments)   # keep progress if the run is cut short
    save_cache("vote_judgments", judgments)
    stream("votes_ai_relevance", ok=bool(judgments) and state["pending"] == 0 or bool(relevant),
           count=len(relevant),
           note=f"{len(all_rolls)} topical roll calls · {len(relevant)} judged relevant · "
                f"{state['pending']} awaiting AI judgment"
                + ("" if ai.available(search=True) else " — web-search AI unavailable; judging only votes with a recorded description"))
    return relevant


def area(v):
    """Which of the five areas a judged vote is in. Judgments made before areas existed
    (version 3) are mapped from their category and recipients until they are re-judged."""
    a = v.get("area")
    if a in AREAS:
        return a
    rec = {r.lower() for r in (v.get("recipients") or [])}
    if v.get("category") == "arms_aid":
        return "israel_aid" if rec & FOCUS_RECIPIENTS else "military_aid"
    if v.get("category") == "war_powers":
        text = (v.get("summary", "") + " " + v.get("topic", "")).lower()
        return "war_authorization" if re.search(r"authoriz|aumf|declar", text) else "use_of_force"
    if v.get("category") == "sanctions":
        return "sanctions"
    return None


def is_focus(v):
    """Does this vote count toward the anti-interventionist record? A vote in one of the five
    areas, with an anti side (not neutral), that is either about that area or (for a broad bill)
    carries particularly severe interventionist provisions."""
    if not v.get("anti_position") or v.get("neutral") or not area(v):
        return False
    pointed = v.get("measure_restricts") is True or v.get("significance") == "major"
    if v.get("area") in AREAS:            # judged under the five-area rules
        return pointed or bool(v.get("severe_interventionist"))
    # older judgment: aid that mixes the focus recipient with others counts only when major
    rec = {r.lower() for r in (v.get("recipients") or [])}
    if area(v) == "israel_aid":
        return pointed and rec <= FOCUS_RECIPIENTS
    return v.get("significance") == "major"


# Israel aid is the anchor: a member must be consistent there; the other four areas are
# supporting evidence, where a slip lowers confidence instead of disqualifying.
ANCHOR = "israel_aid"
# military aid and sanctions are judged country by country: anti on arms to one country doesn't
# oblige anti on arms to another, but once anti on a given recipient, stay consistent on it
BY_RECIPIENT = ("military_aid", "sanctions")
_RANK = {"consistent": 0, "became consistent": 1, "turning": 2, "inconsistent": 3}
OK = ("consistent", "became consistent")


def _status(vs):
    """Status of one run of votes (sorted by date) on one area, or one recipient within it.

    A member who turned (voted the interventionist side, then began voting anti) counts once the
    anti votes since their last interventionist vote form a run of two or more that reaches their
    latest Congress voting there; until then they are "turning"."""
    first = next((i for i, v in enumerate(vs) if v["anti"]), None)
    pro_after = sum(1 for v in vs[first:] if not v["anti"]) if first is not None else 0
    last_pro = max((i for i, v in enumerate(vs) if not v["anti"]), default=-1)
    streak = vs[last_pro + 1:]
    if first is None:
        return "interventionist", first, pro_after, streak
    if first == 0 and pro_after == 0:
        return "consistent", first, pro_after, streak
    if len(streak) >= 2 and streak[-1]["congress"] == vs[-1]["congress"]:
        return "became consistent", first, pro_after, streak
    if pro_after == 0:
        return "turning", first, pro_after, streak
    return "inconsistent", first, pro_after, streak


def area_record(votes):
    """Per-area consistency, from a member's counted votes (each with area, anti, date, congress,
    recipients). Military aid and sanctions are checked recipient by recipient, and the area takes
    the worst status among the recipients the member has voted anti on."""
    out = {}
    for a in AREAS:
        vs = sorted([v for v in votes if v.get("area") == a], key=lambda v: v["date"])
        if not vs:
            continue
        anti = [v for v in vs if v["anti"]]
        status, first, pro_after, streak = _status(vs)
        by_r = None
        if a in BY_RECIPIENT and anti:
            groups = {}
            for v in vs:
                for r in (v.get("recipients") or ["unspecified"]):
                    groups.setdefault(r.strip().lower(), []).append(v)
            by_r = {r: _status(g)[0] for r, g in groups.items()}
            engaged = [st for st in by_r.values() if st != "interventionist"]
            status = max(engaged, key=_RANK.get)
            pro_after = sum(_status(g)[2] for g in groups.values())
        out[a] = {"label": AREA_LABEL[a], "votes": len(vs), "anti": len(anti), "status": status,
                  "first_anti": vs[first]["date"] if first is not None else None,
                  "interventionist_after_first_anti": pro_after,
                  "consistent_since": streak[0]["date"] if status == "became consistent" and streak and not by_r else None,
                  "breaking_anti_votes": sum(1 for v in anti if not v.get("party_line"))}
        if by_r:
            out[a]["by_recipient"] = dict(sorted(by_r.items(), key=lambda kv: _RANK.get(kv[1], 4))[:12])
    return out


def consistent(areas):
    """The anchor test: has voted anti-interventionist on aid to Israel and stayed consistent there."""
    x = areas.get(ANCHOR)
    return bool(x and x["anti"] and x["status"] in OK)


def slips(areas):
    """Supporting areas where the member started anti and later went back: lower confidence only."""
    return [x["label"] for k, x in areas.items() if k != ANCHOR and x["anti"] and x["status"] not in OK]


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
                "category": v.get("category", "other"), "recipients": v.get("recipients", []),
                "focus": is_focus(v),
                "area": area(v),
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
        if not is_focus(v):
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
               "focus_votes": len(votes), "source": "clerk.house.gov + senate.gov roll calls (AI-judged)"}
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
