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

JUDGE_VERSION = 1
TTL_H = float(os.environ.get("AECT_TIER_TTL_H", str(24 * 7)))
BUDGET = int(os.environ.get("AECT_TIER_BUDGET", "40"))
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
    anti = [v for v in focus if v["anti"]]
    congs = sorted({v["congress"] for v in anti})
    pd = p.get("primary_date")
    pre_primary = bool(anti and pd and all(
        0 <= (date.fromisoformat(pd) - date.fromisoformat(v["date"])).days <= 365 for v in anti))
    sup, opp, spenders = _lobby_ie(p, ie)
    other = p.get("other_foreign_policy") or {}
    tr = p.get("tracker", {})
    return {
        "incumbent": bool(p.get("incumbent")),
        "focus_votes": len(focus), "anti_focus_votes": len(anti),
        "anti_congresses": congs, "first_anti_vote": anti[0]["date"] if anti else None,
        "anti_ratio": round(len(anti) / len(focus), 2) if focus else None,
        "all_anti_within_year_before_primary": pre_primary,
        "lobby_money_trackaipac": p.get("lobby_total") or 0,
        "lobby_outside_support": round(sup), "lobby_outside_oppose": round(opp),
        "lobby_spenders": spenders,
        "trackaipac_endorsed": "trackaipac_endorsed" in p.get("sources", []),
        "trackaipac_approved": bool((tr.get("trackaipac_congress") or {}).get("approved")),
        "rlc_score": p.get("rlc_score"), "rlc_endorsed": "rlc_endorsed" in p.get("sources", []),
        "other_foreign_policy_anti": f"{other.get('anti', 0)}/{other.get('votes', 0)}",
        "statement": (p.get("position_research") or {}).get("summary") or None,
    }


def rule_tier(f, p):
    """Conservative fallback when no AI is available: hard needs a record that has cost something."""
    if not p.get("antiArms"):
        return "none", "not marked anti by votes or statements"
    funded = f["lobby_money_trackaipac"] >= LOBBY_LIMIT or f["lobby_outside_support"] > 0
    if f["incumbent"] and f["focus_votes"]:
        sustained = f["anti_focus_votes"] >= 2 and len(f["anti_congresses"]) >= 2 and (f["anti_ratio"] or 0) >= 0.6
        if sustained and not funded and not f["all_anti_within_year_before_primary"]:
            return "hard", "anti on the focus votes across more than one Congress, without focus-lobby money"
        why = ("lobby-funded" if funded else "anti record is recent or one-off" if not sustained
               else "anti votes only in the year before a primary")
        return "soft", why
    # never served in Congress (or no focus votes cast): need corroboration beyond their own words
    targeted = f["lobby_outside_oppose"] > 0 or f["trackaipac_endorsed"]
    if targeted and not funded and (p.get("position_research") or {}).get("strength") == "strong":
        return "hard", "strong cited statements, endorsed by or targeted over the issue, no lobby money"
    return "soft", "statements not yet corroborated by record, donors or trackers"


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
    """News and opinion headlines on the person and the focus issue, from free search feeds."""
    q = f'"{p["name"]}" ({" OR ".join(sorted(c.title() for c in FOCUS_RECIPIENTS) or ["Israel"])} OR Gaza OR AIPAC)'
    feeds = [("https://news.google.com/rss/search?" + urllib.parse.urlencode(
                 {"q": q, "hl": "en-US", "gl": "US", "ceid": "US:en"}), "dossier_news_google"),
             ("https://www.bing.com/news/search?" + urllib.parse.urlencode(
                 {"q": q, "format": "rss"}), "dossier_news_bing")]
    for url, sname in feeds:
        xml = get_text(url, stream_name=sname, retries=1, headers={"Accept": "application/rss+xml"})
        items = re.findall(r"<item>(.*?)</item>", xml or "", re.S)
        rows = []
        for it in items[:10]:
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
SYSTEM = f"""You decide whether a U.S. member of Congress or congressional candidate is a RELIABLE anti-interventionist / anti-establishment vote on U.S. military aid and arms to {_FOCUS} — someone who can be COUNTED ON in a blocking bloc against leadership — or only a SOFT one.

HARD (reliable, counted): a sustained record that held when it cost something — anti positions across more than one Congress or several years, not just in 2026; consistent public statements; NOT funded by the {_FOCUS} lobby (TrackAIPAC lobby money, United Democracy Project / DMFI / AIPAC support). Being targeted by those groups, or endorsed by TrackAIPAC, supports HARD.
SOFT (shown, NOT counted): anti votes or statements that look first-time, one-off or recent (e.g. a 2026 vote reacting to civilian deaths in Gaza, or positioning in the months before a primary), or that are contradicted by lobby money, pro-aid votes or establishment alignment. Many 2026 first-time anti voters are SOFT and likely still establishment.
NONE: not actually anti-interventionist on this issue.

Candidates who have NEVER served in Congress: if the sources show a state-level or local record (state legislature, council, mayor), judge that record the same way. If there is no such record, judge the INTEGRITY of their statements against their donors, business relationships and the trackers: HARD only if the statements are explicit and solid AND their relationships are clean or irrelevant (no {_FOCUS}-lobby money or support, no donor/business ties that contradict them). Otherwise SOFT.

Use ONLY the facts and sources given. Do not use outside knowledge. Cite source URLs from the dossier for any claim from it.
Respond with JSON only:
{{"tier": "hard"|"soft"|"none", "confidence": "high"|"medium"|"low", "prior_office": "congress"|"state_or_local"|"none"|"unknown", "reason": "one or two sentences", "evidence": [{{"claim": "...", "url": "..."}}]}}"""


def _prompt(p, f, text, emp):
    seat = f"{p.get('state')}-{p.get('district')}" if p.get("chamber") == "house" else f"{p.get('state')} Senate"
    votes = [f"{v['date']} {v['measure']}: {v['position']} ({'anti' if v['anti'] else 'pro'}) — {v['summary'][:140]}"
             for v in sorted([v for v in p.get("vote_record", []) if v.get("focus")], key=lambda v: v["date"])]
    lines = [f"Person: {p['name']} ({p.get('party')}), {seat}, {'sitting member' if f['incumbent'] else 'candidate'}",
             f"Primary date: {p.get('primary_date') or 'unknown'} · status: {p.get('ps') or ''}",
             "Facts: " + json.dumps(f, default=str),
             "Focus roll calls (oldest first):\n" + ("\n".join(votes) or "none cast"),
             "Top donor employers this cycle: " + ("; ".join(emp) or "not available"),
             "Dossier:\n" + (text or "(no sources found)")]
    return "\n\n".join(lines)


def _fingerprint(f, text):
    keep = {k: f[k] for k in ("anti_focus_votes", "focus_votes", "lobby_money_trackaipac",
                              "lobby_outside_support", "lobby_outside_oppose", "trackaipac_endorsed")}
    return hashlib.sha1((json.dumps(keep, sort_keys=True) + str(len(text) // 500)).encode()).hexdigest()[:12]


def run(people, ie, fec_cands):
    """Set antiTier / tier_source / tier_reason / tier_evidence on everyone marked anti (and on
    tracker-endorsed candidates, whose dossier may show a stance the first pass missed)."""
    cache = load_cache("establishment")
    emp_cache = load_cache("fec_employers")
    keys = [k for k, p in people.items() if p.get("antiArms")]
    keys += [k for k, p in people.items() if not p.get("antiArms") and not p.get("incumbent")
             and p.get("on_ballot") and "trackaipac_endorsed" in p.get("sources", [])]
    # sitting members first, then candidates with the strongest signals
    keys.sort(key=lambda k: (not people[k].get("incumbent"), not people[k].get("antiArms")))
    judged = 0
    counts = {"hard": 0, "soft": 0, "none": 0}
    sources = {"ai": 0, "rule": 0}
    can_ai = ai.available_any()
    for k in keys:
        p = people[k]
        f = facts(p, ie)
        old = cache.get(k) or {}
        fresh = old.get("v") == JUDGE_VERSION and age_hours(old.get("at")) < TTL_H and old.get("source", "").startswith("ai")
        verdict = old if fresh and old.get("facts_fp") == _fingerprint(f, "x" * old.get("text_len", 0)) else None
        if verdict is None and can_ai and judged < BUDGET and ai_time_left() > 120:
            text, urls = dossier(p)
            emp = [] if f["incumbent"] else employers(p, fec_cands, emp_cache)
            res, _, provider = ai.ask_json(SYSTEM, _prompt(p, f, text, emp))
            judged += 1
            if isinstance(res, dict) and res.get("tier") in ("hard", "soft", "none"):
                ok_urls = set(urls) | {v.get("url") for v in p.get("vote_record", [])}
                ev = [e for e in (res.get("evidence") or []) if isinstance(e, dict)
                      and str(e.get("url", "")).startswith("http") and e["url"] in ok_urls][:4]
                tier = res["tier"]
                if tier == "hard" and not ev and not f["focus_votes"]:
                    tier = "soft"                    # a new candidate's HARD needs a citation
                verdict = {"tier": tier, "reason": (res.get("reason") or "")[:300], "evidence": ev,
                           "confidence": res.get("confidence"), "prior_office": res.get("prior_office"),
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
            # the dossier found a stance the first pass missed (tracker-endorsed candidate)
            p["antiArms"], p["antiArmsLevel"], p["antiArmsBasis"] = True, "yes", "ai_dossier"
            p["antiArmsEvidence"] = [f"{e['claim']} ({e['url']})" for e in verdict["evidence"][:2]]
        if p.get("antiArms") and verdict["tier"] == "none":
            p["antiTier"] = "soft"                   # marked by a vote/statement; judged not reliable
        p["tier_source"] = verdict["source"]
        p["tier_reason"] = verdict["reason"]
        p["tier_evidence"] = verdict.get("evidence", [])
        p["tier_facts"] = f
        if p.get("antiTier"):
            counts[p["antiTier"]] += 1
            sources["ai" if verdict["source"].startswith("ai") else "rule"] += 1
    save_cache("establishment", cache)
    save_cache("fec_employers", emp_cache)
    stream("establishment_tiers", ok=bool(keys), count=counts["hard"],
           note=f"{counts['hard']} hard · {counts['soft']} soft · judged {judged} with AI this run · "
                f"verdicts from AI {sources['ai']}, rule {sources['rule']}")
    return counts
