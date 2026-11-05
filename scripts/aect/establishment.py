"""
Step 4e — reliable ("hard") vs soft anti-establishment.

A recorded anti vote or a campaign statement shows what someone did once, not
whether they will hold the line when it costs them. Many members who voted to
cut Israel's military aid in 2026 did so for the first time, months before a
primary and amid outrage over civilian deaths in Gaza. That can be sincere, or
it can be positioning; either way it can't be counted on for a blocking bloc.

So everyone the votes or statements mark as anti is sorted into:

  hard  — a sustained record that held when it cost something: anti positions
          across more than one Congress / year, consistent public statements,
          not funded by the pro-Israel lobby (often targeted by it instead).
          Only these count toward the coalition thresholds.
  soft  — one-off, recent or pre-primary anti votes/statements, or ones
          contradicted by lobby money or pro-aid votes. Shown, not counted.
  none  — on reflection not anti-interventionist on the focus issue.

Candidates who have never served in Congress are judged on their state-level
record when they have one; otherwise on whether their statements hold up
against their donors, business relationships and the trackers.

The judgment is made by an AI model reading a dossier this module scrapes
from free public sources — Wikipedia, Grokipedia, Ballotpedia, news and
opinion headlines (Google News / Bing News search feeds) — together with the
facts the pipeline already holds: every focus roll call, TrackAIPAC lobby
money, FEC outside spending (who spent for or against them), top donor
employers, RLC scores and tracker endorsements. When no AI is available a
conservative rule decides, and the output says which one did.
"""
import hashlib, json, os, re, urllib.parse
from datetime import date

from . import ai
from .config import (CYCLE, CURRENT_CONGRESS, FEC_API, FEC_KEY, FOCUS_RECIPIENTS, STATE_NAMES,
                     WIKIPEDIA_SUMMARY, GROKIPEDIA, BALLOTPEDIA, ai_time_left)
from .net import (get_text, get_json, stream, load_cache, save_cache, now_iso, age_hours,
                  strip_html, qs, circuit_ok)

JUDGE_VERSION = 6
TTL_H = float(os.environ.get("AECT_TIER_TTL_H", str(24 * 7)))
BUDGET = int(os.environ.get("AECT_TIER_BUDGET", "40"))
# once everyone due has been reviewed, verdicts at least this old are re-reviewed with spare capacity
REREVIEW_H = float(os.environ.get("AECT_REREVIEW_H", "6"))
# breaking from one's own party toward the anti side this many times screens a member in
DEFECTION_MIN = int(os.environ.get("AECT_DEFECTION_MIN", "3"))
# an independent / third-party candidate who has raised this much is screened in as an outsider
OUTSIDER_MIN_RAISED = float(os.environ.get("AECT_OUTSIDER_MIN_RAISED", "250000"))
# money from the focus-issue lobby this large marks a member as funded by it
LOBBY_LIMIT = float(os.environ.get("AECT_LOBBY_LIMIT", "10000"))
# outside spenders that are the focus-issue lobby (regex over FEC committee names)
LOBBY_SPENDERS = re.compile(os.environ.get(
    "AECT_LOBBY_SPENDERS",
    r"UNITED DEMOCRACY PROJECT|DEMOCRATIC MAJORITY FOR ISRAEL|\bDMFI\b|AMERICAN ISRAEL|\bAIPAC\b|NORPAC|"
    r"REPUBLICAN JEWISH COALITION|\bRJC\b|PRO-ISRAEL|PRESERVE AMERICA"), re.I)
_FOCUS = ", ".join(sorted(c.title() for c in FOCUS_RECIPIENTS)) or "the focus recipient"
_KEYWORDS = re.compile(r"israel|gaza|palestin|aipac|zion|military aid|arms|weapons|war|intervention|"
                       r"genocide|ceasefire|foreign policy|state senate|state house|assembly|legislat|"
                       r"city council|mayor|donor|super pac|business|ceo|founder|lobby", re.I)


# ── Facts ────────────────────────────────────────────────────────────────────
def _lobby_ie(p, ie):
    sup = opp = 0.0
    names = set()
    for fid in p.get("fec_ids", []):
        for s in (ie.get(fid) or {}).get("spenders", []):
            if LOBBY_SPENDERS.search(s.get("committee") or ""):
                names.add(s["committee"])
                if s["side"] == "support":
                    sup += s["amount"]
                else:
                    opp += s["amount"]
    return sup, opp, sorted(names)


def facts(p, ie):
    rec = p.get("vote_record", [])
    focus = sorted([v for v in rec if v.get("focus")], key=lambda v: v["date"])
    # an anti vote counts toward the record only when the member broke from their own party's
    # majority; voting with one's party against the other party's bill is partisanship
    anti_all = [v for v in focus if v["anti"]]
    anti = [v for v in anti_all if not v.get("party_line")]
    congs = sorted({v["congress"] for v in anti})
    # has the record HELD? the latest Congress in which they cast focus votes must be mostly anti
    last_c = max((v["congress"] for v in focus), default=None)
    # party-line anti votes count neither way: drop them from the record being measured
    judged = [v for v in focus if not (v["anti"] and v.get("party_line"))]
    last = [v for v in judged if v["congress"] == last_c]
    held = bool(last) and sum(v["anti"] for v in last) / len(last) >= 0.5
    pd = p.get("primary_date")
    pre_primary = bool(anti and pd and all(
        0 <= (date.fromisoformat(pd) - date.fromisoformat(v["date"])).days <= 365 for v in anti))
    sup, opp, spenders = _lobby_ie(p, ie)
    other = p.get("other_foreign_policy") or {}
    tr = p.get("tracker", {})
    dfx = p.get("_defections", [])
    return {
        "party_defections_to_anti": len(dfx),
        "bills_on_arms_aid_war": p.get("_bills", []),
        "defection_examples": [f"{v['date']} {v['measure']}: {v['summary'][:110]}" for v in dfx[-6:]],
        "incumbent": bool(p.get("incumbent")), "chamber": p.get("chamber"),
        "focus_votes": len(focus), "anti_focus_votes": len(anti),
        "party_line_anti_votes_not_counted": len(anti_all) - len(anti),
        "anti_congresses": congs, "first_anti_vote": anti[0]["date"] if anti else None,
        "anti_ratio": round(len(anti) / len(judged), 2) if judged else None,
        "latest_congress_focus": f"{sum(v['anti'] for v in last)}/{len(last)} anti in the {last_c}th" if last else None,
        "record_held_in_latest_congress": held,
        "all_anti_within_year_before_primary": pre_primary,
        "lobby_money_trackaipac": p.get("lobby_total") or 0,
        "lobby_outside_support": round(sup), "lobby_outside_oppose": round(opp),
        "lobby_spenders": spenders,
        "trackaipac_endorsed": "trackaipac_endorsed" in p.get("sources", []),
        "trackaipac_approved": bool((tr.get("trackaipac_congress") or {}).get("approved")),
        "rlc_score": p.get("rlc_score"), "rlc_endorsed": "rlc_endorsed" in p.get("sources", []),
        "other_foreign_policy_anti": f"{other.get('anti', 0)}/{other.get('votes', 0)}",
        "statement": (p.get("position_research") or {}).get("summary") or None,
        "discharge_petitions_signed": [f"No. {d['no']} {d['measure']}: {d['about'][:120]} "
                                       f"({d['signers']} signers, {d['own_party_signers']} from their own party)"
                                       for d in p.get("_discharge", [])][:8],
        # signed while their own party holds the House majority: going around their own Speaker
        "discharge_petitions_against_own_leadership": sum(1 for _ in p.get("_discharge", []))
            if p.get("_party_holds_house") else 0,
    }


def mark_party_line(people, relevant, chamber_party):
    """Tag each focus vote with how much of the member's own party voted the same way.
    relevant: roll calls with party_anti counts; chamber_party: {chamber: {R, D, I}} seat counts."""
    by_key = {(v.get("chamber"), v.get("congress"), v.get("roll")): v for v in relevant or []}
    # members are grouped with the side they caucus with (independents who caucus with
    # Democrats count with Democrats)
    side = lambda x: "R" if (x.get("caucus") or x.get("party")) == "R" else "D"
    for p in people.values():
        mine = side(p)
        for v in p.get("vote_record", []):
            if not v.get("focus") or not v.get("anti"):
                continue
            rv = by_key.get((v.get("chamber"), v.get("congress"), v.get("roll")))
            seats = chamber_party.get(v.get("chamber")) or {}
            size = sum(n for k, n in seats.items() if side({"party": k}) == mine)
            if not rv or not size:
                continue
            anti = sum(n for k, n in (rv.get("party_anti") or {}).items() if side({"party": k}) == mine)
            share = min(100, round(100 * anti / size))
            v["own_party_share"] = share
            v["party_line"] = share > 50


def _conf_index(res):
    """0-100 confidence in a verdict: the judge's number, or its high/medium/low word."""
    try:
        v = int(float(res.get("confidence_index")))
        if 0 <= v <= 100:
            return v
    except (TypeError, ValueError):
        pass
    return {"high": 85, "medium": 60, "low": 35}.get(str(res.get("confidence") or "").lower())


def _sustained(f):
    """The bar an incumbent must clear to count: anti on the focus votes in 2+ Congresses,
    mostly anti overall, and still anti in the latest Congress they voted in."""
    return (f["anti_focus_votes"] >= 2 and len(f["anti_congresses"]) >= 2
            and (f["anti_ratio"] or 0) >= 0.6 and f["record_held_in_latest_congress"])


def meets_floor(f):
    """The coalition's stated minimum for a sitting member to be considered at all: at least one
    anti-arms vote that broke from their own party, plus a recorded act of going around their own
    leadership (a discharge petition; in the Senate, which has none, a second such vote).
    Meeting it makes a member eligible; whether they count is the judge's call."""
    defied = f["discharge_petitions_against_own_leadership"] >= 1 or (
        f.get("chamber") == "senate" and f["anti_focus_votes"] >= 2)
    return f["anti_focus_votes"] >= 1 and defied


def rule_tier(f, p):
    """Conservative fallback until the AI has reviewed someone: counts only a sustained record.
    The reason always says, in plain words, what the record shows and why it does or doesn't count."""
    funded = f["lobby_money_trackaipac"] >= LOBBY_LIMIT or f["lobby_outside_support"] > 0
    if f["incumbent"] and f["focus_votes"]:
        a, n = f["anti_focus_votes"], f["focus_votes"]
        record = (f"anti on {a} of {n} arms-aid votes that broke from their own party"
                  + (f" ({f['latest_congress_focus']})" if f.get("latest_congress_focus") else ""))
        if _sustained(f) and not funded and not f["all_anti_within_year_before_primary"]:
            return "hard", record + ", held across Congresses, with no focus-lobby money"
        why = ("they take focus-lobby money" if funded
               else "the anti record did not hold in the latest Congress" if a >= 1 and not f["record_held_in_latest_congress"]
               else "the anti votes came only in the year before a primary" if a and f["all_anti_within_year_before_primary"]
               else "the anti record is one-off or limited to one Congress" if a
               else "no anti vote on arms aid that broke from their own party")
        floor = " Meets the coalition's minimum (an anti vote plus a discharge petition), so it awaits the AI judge's call." \
            if meets_floor(f) else ""
        return ("soft" if a or p.get("antiArms") else "none"), f"{record}; not counted because {why}.{floor}"
    if not p.get("antiArms"):
        if f["party_defections_to_anti"] >= DEFECTION_MIN:
            return "soft", (f"broke from their party on {f['party_defections_to_anti']} foreign-policy votes, but has no "
                            "arms-aid record yet; awaiting the AI judge")
        if f["rlc_endorsed"] or f["trackaipac_endorsed"]:
            return "soft", "endorsed by an anti-establishment tracker, but not yet corroborated by a record, donors or statements"
        if (p.get("position_research") or {}).get("anti_interventionist"):
            return "soft", "has cited anti-interventionist statements, but none on arms aid to the focus recipients"
        return "none", "no anti-establishment signal found yet"
    # never served in Congress (or no focus votes cast): need corroboration beyond their own words
    targeted = f["lobby_outside_oppose"] > 0 or f["trackaipac_endorsed"]
    if targeted and not funded and (p.get("position_research") or {}).get("strength") == "strong":
        return "hard", "strong cited statements, endorsed by or targeted over the issue, no lobby money"
    return "soft", "statements not yet corroborated by a record, donors or trackers"


# ── Dossier (free sources) ───────────────────────────────────────────────────
def _keyword_paras(text, cap):
    paras = [x.strip() for x in re.split(r"\n+|(?<=\.)\s{2,}", text or "") if len(x.strip()) > 60]
    hits = [x for x in paras if _KEYWORDS.search(x)]
    out, n = [], 0
    for x in (hits or paras[:2]):
        if n + len(x) > cap:
            break
        out.append(x)
        n += len(x)
    return "\n".join(out)


def _wikipedia(p):
    url = p.get("wikipedia_url")
    if not url:
        return None
    title = urllib.parse.unquote(url.rsplit("/", 1)[-1])
    d = get_json("https://en.wikipedia.org/w/api.php?" + urllib.parse.urlencode({
        "action": "query", "prop": "extracts", "explaintext": 1, "redirects": 1, "format": "json",
        "formatversion": 2, "titles": title}), stream_name="dossier_wikipedia", retries=2)
    pages = ((d or {}).get("query") or {}).get("pages") or []
    text = pages[0].get("extract", "") if pages else ""
    return (url, _keyword_paras(text, 2600)) if text else None


def _grokipedia(p):
    url = GROKIPEDIA.format(slug=urllib.parse.quote(re.sub(r"\s+", "_", p["name"])))
    html = get_text(url, stream_name="dossier_grokipedia", retries=1, headers={"Accept": "text/html"})
    if not html:
        return None
    paras = [strip_html(x) for x in re.findall(r"<p[^>]*>(.*?)</p>", html, re.S | re.I)]
    text = "\n".join(x for x in paras if len(x) > 60 and "cookie" not in x.lower())
    st = STATE_NAMES.get(p.get("state", ""), "")
    if not text or (st and st not in text and "Congress" not in text):
        return None
    return url, _keyword_paras(text, 1600)


def _ballotpedia(p):
    url = p.get("ballotpedia_url")
    if not url or not circuit_ok("ballotpedia"):
        return None
    html = get_text(url, stream_name="dossier_ballotpedia", retries=1)
    text = strip_html(html or "")
    return (url, _keyword_paras(text, 1600)) if len(text) > 500 else None


def _news(p):
    """News and opinion headlines on the person — the focus issue, and anti-establishment / anti-war
    stances generally — from free search feeds."""
    focus = " OR ".join(sorted(c.title() for c in FOCUS_RECIPIENTS) or ["Israel"])
    rows = []
    for q in (f'"{p["name"]}" ({focus} OR Gaza OR AIPAC)',
              f'"{p["name"]}" (war OR "foreign aid" OR Ukraine OR "America First" OR establishment OR "military aid" OR donors)'):
        for link, line in _news_query(q):
            if link not in {r[0] for r in rows}:
                rows.append((link, line))
    return rows[:14]


def _news_query(q):
    feeds = [("https://news.google.com/rss/search?" + urllib.parse.urlencode(
                 {"q": q, "hl": "en-US", "gl": "US", "ceid": "US:en"}), "dossier_news_google"),
             ("https://www.bing.com/news/search?" + urllib.parse.urlencode(
                 {"q": q, "format": "rss"}), "dossier_news_bing")]
    for url, sname in feeds:
        xml = get_text(url, stream_name=sname, retries=1, headers={"Accept": "application/rss+xml"})
        items = re.findall(r"<item>(.*?)</item>", xml or "", re.S)
        rows = []
        for it in items[:8]:
            g = lambda tag: (re.search(rf"<{tag}[^>]*>(.*?)</{tag}>", it, re.S) or [None, ""])[1]
            title = strip_html(re.sub(r"<!\[CDATA\[|\]\]>", "", g("title")))
            link = re.sub(r"<!\[CDATA\[|\]\]>", "", g("link")).strip()
            when = g("pubDate")[:16]
            src = strip_html(g("source"))
            if title and link:
                rows.append((link, f"{when} · {src} · {title}"))
        if rows:
            return rows
    return []


def dossier(p):
    parts, urls = [], []
    for fn in (_wikipedia, _grokipedia, _ballotpedia):
        try:
            r = fn(p)
        except Exception as e:                       # one bad source must not stop the rest
            stream("dossier", error=f"{fn.__name__} {p['name']}: {e}")
            r = None
        if r and r[1]:
            parts.append(f"[{r[0]}]\n{r[1]}")
            urls.append(r[0])
    news = _news(p)
    if news:
        parts.append("[news and opinion headlines]\n" + "\n".join(f"- {t} <{u}>" for u, t in news))
        urls += [u for u, _ in news]
    if p.get("website"):
        urls.append(p["website"])
    for e in (p.get("position_research") or {}).get("evidence", []):
        parts.append(f"[{e.get('url')}]\n{e.get('claim', '')}")
        urls.append(e.get("url"))
    return "\n\n".join(parts)[:7000], [u for u in urls if u]


def employers(p, fec_cands, cache):
    """Top donor employers for a candidate's principal committee — the business relationships."""
    if not FEC_KEY:
        return []
    out = []
    for fid in p.get("fec_ids", [])[:1]:
        for cid in (fec_cands.get(fid) or {}).get("committee_ids", [])[:1]:
            hit = cache.get(cid)
            if hit and age_hours(hit.get("at")) < 24 * 7:
                return hit["rows"]
            d = get_json(f"{FEC_API}/schedules/schedule_a/by_employer/?" + qs({
                "api_key": FEC_KEY, "committee_id": cid, "cycle": CYCLE, "sort": "-total",
                "per_page": 12}), stream_name="fec_employers")
            rows = [f"{r.get('employer')}: ${round(r.get('total') or 0):,}"
                    for r in (d or {}).get("results", [])
                    if r.get("employer") and not re.search(r"NOT EMPLOYED|RETIRED|SELF|NONE|N/A|INFORMATION REQUESTED",
                                                           r["employer"], re.I)][:10]
            cache[cid] = {"at": now_iso(), "rows": rows}
            out = rows
    return out


# ── Judge ────────────────────────────────────────────────────────────────────
SYSTEM = f"""You are a realistic political operative working for a non-partisan anti-establishment coalition of progressives, constitutionalists and libertarians. Your job is to tell the coalition who it can actually COUNT ON when a vote gets hard. For now the coalition's line is anti-intervention: opposing U.S. military aid and arms to {_FOCUS} above all, and more broadly foreign wars, war-powers grabs, large foreign-aid packages and the foreign-policy lobby's money.

The one question you answer about every person: is this a RELIABLE anti-establishment ally, or are they playing political games?

How to think:
- Be pessimistic about where things stand: most of Congress is establishment, including many who campaign as outsiders, America First, progressive or anti-war. Be realistic about what counts, and stay open to real allies wherever they come from.
- Separate principle from partisanship. Opposing the other party's leadership is cheap; it is what minority members do anyway. Breaking with one's OWN party and leadership is costly and is the strongest evidence of principle: votes against one's own party on arms, aid or war; signing discharge petitions that go around one's own party's Speaker and Rules Committee; sponsoring or cosponsoring measures one's leadership opposes. A member who only defects on spending or procedure is not anti-intervention.
- Judge what the record shows, not what the person says about themselves. Recency and consistency matter: an anti vote followed by later votes for arms to {_FOCUS} is a record that did not hold. A first anti vote cast in the months before a primary, or in a single wave of reaction, is positioning until shown otherwise.
- Follow the money. Money from the {_FOCUS} lobby (TrackAIPAC-tracked contributions, United Democracy Project, DMFI, AIPAC-aligned super PACs) and dependence on donors and businesses that profit from the status quo are reasons to doubt. Being targeted by that lobby's spending is a reason to believe.

Evidence, in order of weight:
1. Sitting or former members of Congress: their roll-call votes first, then their own-party defections, discharge petitions and bills.
2. Candidates who never served in Congress but held state or local office: that record, judged the same way.
3. Candidates with no record: their donors and business relationships. If those are clean or lean neither way, judge their words: boilerplate or specific? consistent across statements, interviews and social media? do they stand and work alongside people who hold the same positions? Explicit, specific, consistent positions with clean relationships can count; vague or borrowed language cannot.

Verdicts:
- HARD = a reliable anti-establishment ally. Counted on the tracker and seated in the center of the chart.
- SOFT = real signals that do not yet add up to reliability. Treated as establishment.
- NONE = establishment, no meaningful anti-establishment signal.
Most people are SOFT or NONE. When in doubt, do not count them.

Use ONLY the facts and sources given; do not use outside knowledge. Never state a number, date, vote or event that is not in the facts or sources below; if something is unknown, say it is unknown. Cite dossier or roll-call URLs for claims. Make the bottom line agree with the tier you give. Write plain text: no Markdown, asterisks or other formatting marks.
If a previous verdict is given, re-examine the record from scratch: keep it if the record still supports it, change it if not, and say in the write-up what changed.
Respond with JSON only:
{{"tier": "hard"|"soft"|"none", "confidence": "high"|"medium"|"low", "confidence_index": 0-100 (how sure you are of THIS verdict given how complete and consistent the record is: 90+ only for a long, unambiguous record), "scope": "focus"|"broad", "prior_office": "congress"|"state_or_local"|"none"|"unknown", "principle_vs_partisanship": "principled"|"mixed"|"partisan"|"unclear", "political_risk": "low"|"medium"|"high", "capital_risk": "low"|"medium"|"high", "bottom_line": "one sentence beginning 'Counts as anti-establishment because' or 'Does not count because'", "writeup": "a full write-up of 5-9 sentences for the public: who they are, what their record and words show on intervention, own-party defections and leadership defiance, money and donors, whether it is principle or partisanship, and how likely they are to hold under pressure", "reason": "one or two sentences", "risk_notes": "one sentence", "evidence": [{{"claim": "...", "url": "..."}}]}}"""


def _prompt(p, f, text, emp, prev=None):
    seat = f"{p.get('state')}-{p.get('district')}" if p.get("chamber") == "house" else f"{p.get('state')} Senate"
    votes = [f"{v['date']} {v['measure']}: {v['position']} ({'anti' if v['anti'] else 'pro'}"
             f"{'; PARTY-LINE: ' + str(v.get('own_party_share')) + '% of their own party voted the same way, not counted as principle' if v.get('party_line') else ''}"
             f") — {v['summary'][:140]}"
             for v in sorted([v for v in p.get("vote_record", []) if v.get("focus")], key=lambda v: v["date"])]
    lines = [f"Person: {p['name']} ({p.get('party')}), {seat}, {'sitting member' if f['incumbent'] else 'candidate'}",
             f"Why screened: {p.get('_screen') or 'marked anti on the focus issue'}",
             f"Primary date: {p.get('primary_date') or 'unknown'} · status: {p.get('ps') or ''}",
             "Facts: " + json.dumps(f, default=str),
             "Focus roll calls (oldest first):\n" + ("\n".join(votes) or "none cast"),
             "Top donor employers this cycle: " + ("; ".join(emp) or "not available"),
             "Dossier:\n" + (text or "(no sources found)")]
    if prev and prev.get("source", "").startswith("ai"):
        lines.append(f"Previous verdict ({(prev.get('judged_at') or prev.get('at') or '')[:10]}, "
                     f"{prev.get('model') or prev.get('source')}): {prev.get('tier')} — {prev.get('bottom_line') or prev.get('reason')}")
    return "\n\n".join(lines)


def _fingerprint(f, text):
    # anything here changing means the record moved: the write-up is redone on the next pass
    keep = {k: f[k] for k in ("anti_focus_votes", "focus_votes", "lobby_money_trackaipac",
                              "lobby_outside_support", "lobby_outside_oppose", "trackaipac_endorsed",
                              "discharge_petitions_against_own_leadership")}
    keep["petitions"] = len(f.get("discharge_petitions_signed") or [])
    keep["bills"] = len(f.get("bills_on_arms_aid_war") or [])
    return hashlib.sha1((json.dumps(keep, sort_keys=True) + str(len(text) // 500)).encode()).hexdigest()[:12]


def _mark_defections(people):
    """For every sitting member: the votes where they took the anti side while most of their own party didn't."""
    tally = {}
    for p in people.values():
        if not p.get("incumbent"):
            continue
        pty = p.get("caucus") or p.get("party")
        for v in p.get("vote_record", []):
            if not (v.get("focus") or v.get("significance") == "major"):
                continue
            t = tally.setdefault((v.get("chamber"), v["congress"], v["roll"]), {}).setdefault(pty, [0, 0])
            t[0] += bool(v["anti"])
            t[1] += 1
    for p in people.values():
        pty = p.get("caucus") or p.get("party")
        out = []
        for v in p.get("vote_record", []) if p.get("incumbent") else []:
            t = (tally.get((v.get("chamber"), v["congress"], v["roll"])) or {}).get(pty)
            if v.get("anti") and t and t[1] >= 5 and t[0] / t[1] < 0.5:
                out.append(v)
        p["_defections"] = sorted(out, key=lambda v: v["date"])


def _screen(p, fec_totals):
    """Why this person deserves a look beyond the focus votes (None = not screened in)."""
    if p.get("antiArms"):
        return "marked anti on the focus issue by votes or statements"
    if p.get("incumbent") and len(p.get("_defections", [])) >= DEFECTION_MIN:
        return f"breaks from own party toward the anti side on {len(p['_defections'])} foreign-policy votes"
    if not p.get("incumbent") and not p.get("on_ballot"):
        return None
    if "rlc_endorsed" in p.get("sources", []) or "trackaipac_endorsed" in p.get("sources", []):
        return "endorsed by " + " and ".join(s.split("_")[0].upper().replace("TRACKAIPAC", "TrackAIPAC")
                                            for s in p.get("sources", []) if s.endswith("_endorsed"))
    if (p.get("position_research") or {}).get("anti_interventionist"):
        return "cited anti-interventionist statements (not on the focus issue)"
    if not p.get("incumbent") and (p.get("party") or "") not in ("D", "R", "DEM", "REP"):
        raised = max([((fec_totals or {}).get(c) or {}).get("raised") or 0 for c in p.get("fec_ids", [])] or [0])
        if raised >= OUTSIDER_MIN_RAISED:
            return f"independent / third-party outsider campaign (raised ${raised:,.0f})"
    return None


_BILL_TOPIC = re.compile(r"israel|gaza|palestin|arms|weapon|munition|military (?:aid|assistance|financing)|"
                         r"security assistance|foreign military|embargo|war powers|authorization for use of military|"
                         r"\baumf\b|hostilities|ukraine|taiwan|iran|yemen|foreign aid|block the bombs", re.I)
BILLS_BUDGET = int(os.environ.get("AECT_BILLS_BUDGET", "200"))


def _bills(p, cache, budget):
    """A member's sponsored + cosponsored bills on arms, aid and war (current and previous Congress),
    from Congress.gov. Returns (rows, fetched_now)."""
    from .votes import _cg
    bg = p.get("bioguide_id")
    if not bg:
        return [], False
    hit = cache.get(bg) or {}
    if age_hours(hit.get("at")) < 72 or budget <= 0:
        return hit.get("rows", []), False
    rows = []
    for kind, label in (("sponsored-legislation", "sponsored"), ("cosponsored-legislation", "cosponsored")):
        d = _cg(f"/member/{bg}/{kind}", limit=250)
        if d is None:
            return hit.get("rows", []), True
        items = d.get("sponsoredLegislation") or d.get("cosponsoredLegislation") or []
        for it in items:
            title = (it.get("title") or (it.get("latestTitle") or {}).get("title") or "").strip()
            if not title or not _BILL_TOPIC.search(title) or (it.get("congress") or 0) < CURRENT_CONGRESS - 1:
                continue
            num = f"{(it.get('type') or '').upper()} {it.get('number', '')}".strip()
            rows.append(f"{label} {num} ({it.get('congress')}th, {it.get('introducedDate', '')}): {title[:150]}")
    cache[bg] = {"at": now_iso(), "rows": rows[:25]}
    return rows[:25], True


_MARKERS = re.compile(r"america first|anti-?war|libertarian|aipac|foreign aid|israel|gaza|anti-?establishment|"
                      r"populist|freedom caucus|justice democrats|\bdsa\b|democratic socialist|insurgent|"
                      r"outsider|ceasefire|forever wars?|interventionis|military aid|rand paul|massie", re.I)
HEADLINE_BUDGET = int(os.environ.get("AECT_HEADLINE_BUDGET", "120"))


def _headline_screen(p, cache):
    """No-AI first look for November nominees: do news headlines about them carry anti-establishment markers?"""
    hit = cache.get(p["key"]) or {}
    if age_hours(hit.get("at")) < 72:
        return hit.get("why"), False
    seat = STATE_NAMES.get(p.get("state", ""), "")
    heads = [t for _, t in _news_query(f'"{p["name"]}" {seat} congress')]
    marks = sorted({m.group(0).lower() for t in heads for m in _MARKERS.finditer(t)})
    why = f"news headlines mention: {', '.join(marks[:5])}" if marks else None
    cache[p["key"]] = {"at": now_iso(), "why": why, "n": len(heads)}
    return why, True


def run(people, ie, fec_cands, fec_totals=None):
    """Investigate everyone with an anti-establishment signal and set antiTier / tier_source /
    tier_reason / tier_evidence / tier_risk. Soft verdicts are shown, never counted."""
    cache = load_cache("establishment")
    emp_cache = load_cache("fec_employers")
    _mark_defections(people)
    # Everyone is reviewed: every sitting member and every candidate still in the running.
    # Signals (focus votes, party defections, endorsements, statements, outsider campaigns,
    # news markers, arms/aid/war bills) only decide who is reviewed FIRST.
    bill_cache = load_cache("member_bills")
    head_cache = load_cache("headline_screen")
    bills_left, heads_left = BILLS_BUDGET, HEADLINE_BUDGET
    keys = []
    for k, p in people.items():
        running = (not p.get("incumbent") and p.get("on_ballot") and p.get("ps") not in ("DEFEATED", "WITHDREW")
                   and p.get("primary_result") not in ("LOST", "WITHDREW"))
        if not (p.get("incumbent") or running):
            continue
        if p.get("incumbent"):
            p["_bills"], fetched = _bills(p, bill_cache, bills_left)
            bills_left -= fetched
        why = _screen(p, fec_totals)
        if not why and p.get("_bills"):
            why = f"sponsors/cosponsors {len(p['_bills'])} arms, aid or war bills"
        if not why and running and p.get("primary_result") == "WON":
            why, fetched = _headline_screen({**p, "key": k}, head_cache) if heads_left > 0 or \
                age_hours((head_cache.get(k) or {}).get("at")) < 72 else (None, False)
            heads_left -= fetched
        p["_signal"] = bool(why)
        p["_screen"] = why or "routine review (no signal found yet)"
        keys.append(k)
    save_cache("member_bills", bill_cache)
    save_cache("headline_screen", head_cache)
    # never-AI-judged first; then focus-marked members, candidates on the ballot, party defectors
    def prio(k):
        p, old = people[k], cache.get(k) or {}
        judged_ai = old.get("v") == JUDGE_VERSION and old.get("source", "").startswith("ai")
        was_hard = old.get("tier") == "hard"
        if judged_ai:                        # already reviewed: the oldest verdict is next in line
            return (True, old.get("judged_at") or old.get("at") or "")
        floor = p.get("incumbent") and meets_floor(facts(p, ie))
        return (False, "", not floor, not was_hard, not p.get("antiArms"), not p.get("_signal"), not p.get("incumbent"))
    keys.sort(key=prio)
    judged = 0
    counts = {"hard": 0, "soft": 0, "none": 0}
    sources = {"ai": 0, "rule": 0}
    can_ai = ai.available_any()
    for k in keys:
        p = people[k]
        f = facts(p, ie)
        old = cache.get(k) or {}
        ttl = TTL_H * (2 if old.get("tier") == "none" else 1)
        fresh = old.get("v") == JUDGE_VERSION and age_hours(old.get("at")) < ttl and old.get("source", "").startswith("ai")
        if fresh and can_ai and judged < BUDGET and ai_time_left() > 120 and age_hours(old.get("at")) >= REREVIEW_H:
            fresh = False                    # spare free capacity: re-examine the oldest verdicts again
        verdict = old if fresh and old.get("facts_fp") == _fingerprint(f, "x" * old.get("text_len", 0)) else None
        if verdict is None and can_ai and judged < BUDGET and ai_time_left() > 120:
            text, urls = dossier(p)
            emp = [] if f["incumbent"] else employers(p, fec_cands, emp_cache)
            res, _, provider = ai.ask_json(SYSTEM, _prompt(p, f, text, emp, old))
            judged += 1
            if isinstance(res, dict) and res.get("tier") in ("hard", "soft", "none"):
                ok_urls = set(urls) | {v.get("url") for v in p.get("vote_record", [])}
                ev = [e for e in (res.get("evidence") or []) if isinstance(e, dict)
                      and str(e.get("url", "")).startswith("http") and e["url"] in ok_urls][:4]
                tier = res["tier"]
                if tier == "hard" and not ev and not f["focus_votes"]:
                    tier = "soft"                    # a new candidate's HARD needs a citation
                risk = {"political": res.get("political_risk"), "capital": res.get("capital_risk"),
                        "notes": (res.get("risk_notes") or "")[:240]}
                # The judge already weighs folding risk into its verdict. The tracker's own limits
                # below can only lower it; when one does, the bottom line says so, so the page
                # never shows "counts" next to a member who is not counted.
                overruled = None
                money = f["lobby_money_trackaipac"] >= LOBBY_LIMIT or f["lobby_outside_support"] > 0
                if tier == "hard" and risk["capital"] == "high" and money:
                    tier, overruled = "soft", "the judge rated their dependence on lobby or establishment money high"
                # floor for a sitting member: a held record across Congresses, or an anti vote plus
                # a recorded act of defying their own leadership (House: discharge petitions;
                # Senate, which has none: breaking with their own party on two or more arms votes)
                if tier == "hard" and f["incumbent"] and f["focus_votes"] and not _sustained(f) and not meets_floor(f):
                    tier, overruled = "soft", ("their record has not held across Congresses, and they have no "
                                               "anti-arms vote paired with going around their own leadership")
                if tier == "hard" and not f["incumbent"] and res.get("confidence") != "high":
                    tier, overruled = "soft", "a new candidate needs a high-confidence, cited verdict"
                bl = (res.get("bottom_line") or "")[:240]
                if overruled:
                    bl = f"Does not count: {overruled}. (The judge leaned toward counting them; see the write-up.)"
                verdict = {"tier": tier, "reason": (res.get("reason") or "")[:300], "evidence": ev,
                           "writeup": (res.get("writeup") or "")[:2400],
                           "bottom_line": bl, "overruled": overruled,
                           "principle": res.get("principle_vs_partisanship"),
                           "model": ai.LAST.get("model"), "judged_at": now_iso(),
                           "risk": risk, "scope": res.get("scope"),
                           "confidence": res.get("confidence"), "prior_office": res.get("prior_office"),
                           "confidence_index": _conf_index(res),
                           "source": f"ai:{provider}", "at": now_iso(), "v": JUDGE_VERSION,
                           "facts_fp": _fingerprint(f, text), "text_len": len(text)}
                cache[k] = verdict
                if judged % 10 == 0:
                    save_cache("establishment", cache)
                    save_cache("fec_employers", emp_cache)
        if verdict is None and old.get("source", "").startswith("ai") and old.get("v") == JUDGE_VERSION:
            verdict = old                            # stale AI verdict beats the rule until refreshed
        if verdict is None:
            tier, why = rule_tier(f, p)
            verdict = {"tier": tier, "reason": why, "evidence": [], "source": "rule"}
        p["antiTier"] = verdict["tier"] if (p.get("antiArms") or verdict["tier"] == "hard") else None
        if verdict["tier"] == "hard" and not p.get("antiArms"):
            # screened in on broader signals (endorsements, outsider campaign, news, bills) and
            # judged reliable: counts like any anti-establishment member. Soft signals are not shown.
            p["antiArms"], p["antiArmsLevel"], p["antiArmsBasis"] = True, verdict["tier"], "anti_establishment"
            p["antiArmsEvidence"] = ([f"{e['claim']} ({e['url']})" for e in verdict.get("evidence", [])[:2]]
                                     or [verdict["reason"]])
            if not p.get("note"):
                p["note"] = verdict["reason"][:180]
        if p.get("antiArms") and verdict["tier"] == "none":
            p["antiTier"] = "soft"                   # marked by a vote/statement; judged not reliable
        p["tier_source"] = verdict["source"]
        p["tier_reason"] = verdict["reason"]
        p["tier_evidence"] = verdict.get("evidence", [])
        p["tier_facts"] = f
        p["tier_risk"] = verdict.get("risk")
        p["tier_screen"] = p.get("_screen")
        p["tier_writeup"] = verdict.get("writeup") or ""
        p["tier_bottom_line"] = verdict.get("bottom_line") or ""
        p["tier_principle"] = verdict.get("principle")
        p["tier_model"] = verdict.get("model")
        p["tier_judged_at"] = verdict.get("judged_at") or verdict.get("at")
        p["tier_confidence"] = verdict.get("confidence_index")
        p["discharge_petitions"] = [{k: d[k] for k in ("no", "measure", "about", "url", "signed")}
                                    for d in p.get("_discharge", [])][:12]
        if p.get("antiTier"):
            counts[p["antiTier"]] += 1
            sources["ai" if verdict["source"].startswith("ai") else "rule"] += 1
    save_cache("establishment", cache)
    save_cache("fec_employers", emp_cache)
    stream("establishment_tiers", ok=bool(keys), count=counts["hard"],
           note=f"{counts['hard']} hard · {counts['soft']} soft · judged {judged} with AI this run · "
                f"verdicts from AI {sources['ai']}, rule {sources['rule']} · AI has reviewed "
                f"{sum(1 for k in keys if (cache.get(k) or {}).get('source', '').startswith('ai'))} of {len(keys)} "
                f"members and running candidates ({sum(1 for k in keys if people[k].get('_signal'))} with signals)")
    return counts
