"""
Step 3 — general information about each person, independent of any tracker.

  • Wikipedia REST summary        — short bio
  • Ballotpedia person page        — campaign site, retirement, and this
                                     cycle's primary result (only accepted
                                     when the sentence names a date in the
                                     current cycle — this is what stops the
                                     tracker from marking a primary "won"
                                     before it has happened)
  • Grokipedia person page         — fallback bio
  • Ballotpedia primary calendar   — every state's primary / runoff date
"""
import re, time, urllib.parse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from . import ai
from .config import (BALLOTPEDIA, WIKIPEDIA_SUMMARY, GROKIPEDIA, CYCLE, STATE_NAMES, STATE_ABBR,
                     TTL_ENRICH_H, ENRICH_BUDGET_PER_RUN, TODAY, VOTING_STATES)
from .net import get_text, get_json, stream, load_cache, save_cache, now_iso, age_hours, strip_html

MONTHS = "January|February|March|April|May|June|July|August|September|October|November|December"
MONTHS_ANY = MONTHS + "|Jan\\.|Feb\\.|Aug\\.|Sept\\.|Sep\\.|Oct\\.|Nov\\.|Dec\\."
DATE_RE = rf"((?:{MONTHS}) \d{{1,2}}, {CYCLE})"
DATE_ANY = rf"((?:{MONTHS_ANY}) \d{{1,2}}, {CYCLE})"


def _iso(d):
    d = (d or "").replace("Sept.", "Sep.").replace(".", "")
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(d, fmt).date().isoformat()
        except Exception:
            continue
    return None


def _bp_url(title):
    return f"{BALLOTPEDIA}/{urllib.parse.quote(_slug(title))}"


# ── Primary calendar ──────────────────────────────────────────────────────────
def primary_calendar():
    cache = load_cache("primary_calendar")
    if cache.get("cycle") == CYCLE and age_hours(cache.get("fetched_at")) < 24 * 14 and cache.get("dates"):
        stream("primary_calendar", ok=True, count=len(cache["dates"]), note="cache fresh")
        return cache["dates"]
    dates = {}
    for path in (f"State_and_federal_candidate_filing_deadlines_for_{CYCLE}",
                 f"Statewide_primary_elections_calendar", f"{CYCLE}_election_and_voting_dates"):
        html = get_text(f"{BALLOTPEDIA}/{path}", stream_name="primary_calendar")
        if not html:
            continue
        # table rows: <td>Alabama</td><td>May 19, 2026</td> ...
        for row in re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S | re.I):
            cells = [strip_html(c) for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.S | re.I)]
            if len(cells) >= 2 and cells[0] in STATE_ABBR.keys() | {v for v in STATE_NAMES.values()}:
                ab = STATE_ABBR.get(cells[0].lower())
                m = re.match(DATE_ANY, cells[1])
                if ab and ab in VOTING_STATES and m and ab not in dates:
                    dates[ab] = {"primary": _iso(m.group(1)), "source": f"ballotpedia:{path}"}
        text = strip_html(html)
        for ab, full in STATE_NAMES.items():
            if ab not in VOTING_STATES or ab in dates:
                continue
            m = re.search(rf"\b{re.escape(full)}\b[^.]{{0,80}}?{DATE_ANY}", text)
            if m:
                rec = {"primary": _iso(m.group(1))}
                after = text[m.end():m.end() + 160]
                r = re.search(rf"runoff[^.]{{0,40}}?{DATE_RE}", after, re.I)
                if r:
                    rec["runoff"] = _iso(r.group(1))
                dates[ab] = rec
        if len(dates) >= 45:
            break
    missing = sorted(VOTING_STATES - set(dates))
    if missing and ai.available(search=True):
        result, sources, _ = ai.ask_json(
            "You return official U.S. election calendar data as JSON.",
            f"For the {CYCLE} U.S. congressional primaries, give the statewide primary date and any "
            f"primary runoff date for these states: {', '.join(missing)}. "
            'JSON: {"XX": {"primary": "YYYY-MM-DD", "runoff": "YYYY-MM-DD" or null}}', search=True)
        if isinstance(result, dict):
            for ab, rec in result.items():
                if ab in missing and isinstance(rec, dict) and rec.get("primary"):
                    dates[ab] = {"primary": rec.get("primary"), "runoff": rec.get("runoff"),
                                 "source": "ai_search"}
    if dates:
        save_cache("primary_calendar", {"cycle": CYCLE, "fetched_at": now_iso(), "dates": dates})
    stream("primary_calendar", ok=len(dates) >= 45, count=len(dates),
           note=f"{len(dates)} state primary dates for {CYCLE}"
                + (f"; missing {sorted(VOTING_STATES - set(dates))}" if len(dates) < 50 else ""))
    return dates or cache.get("dates", {})


# ── Per-person sources ───────────────────────────────────────────────────────
def _slug(name):
    return re.sub(r"\s+", "_", re.sub(r"[^\w\s\.\-']", "", name).strip())


def wikipedia(name, state, chamber):
    for title in (name, f"{name} (politician)", f"{name} ({STATE_NAMES.get(state, state)} politician)"):
        d = get_json(WIKIPEDIA_SUMMARY.format(slug=urllib.parse.quote(_slug(title))),
                     stream_name="wikipedia", retries=1)
        if not d or d.get("type") == "disambiguation":
            continue
        extract = d.get("extract", "")
        blob = (extract + " " + d.get("description", "")).lower()
        looks_political = any(w in blob for w in ("politician", "congress", "representative",
                                                  "senator", "candidate", "activist", "attorney"))
        if extract and looks_political and (STATE_NAMES.get(state, "").lower() in blob or chamber in blob
                                            or "politician" in blob):
            return {"description": extract[:600],
                    "wikipedia_url": d.get("content_urls", {}).get("desktop", {}).get("page", "")}
    return {}


def ballotpedia(name, state, chamber):
    full = STATE_NAMES.get(state, state)
    for title in (name, f"{name} ({full})", f"{name} ({full} congressional candidate)"):
        html = get_text(_bp_url(title), stream_name="ballotpedia", retries=1)
        if not html or "may refer to" in html[:20000]:
            continue
        text = strip_html(html)
        if full not in text:
            continue
        out = {"ballotpedia_url": _bp_url(title)}
        site = re.search(r'href="(https?://[^"]+)"[^>]*>\s*(?:Campaign website|Official website|Campaign Website)',
                         html, re.I)
        if site:
            out["website"] = site.group(1)
        # Election facts — every one of these is anchored to a date in this cycle
        m = re.search(rf"(advanced from|won|lost in|lost|was defeated in|withdrew (?:from|before))"
                      rf" the (\w+ )?primary(?: runoff)?[^.]{{0,60}}?on {DATE_RE}", text)
        if m:
            verb = m.group(1)
            out["primary_result"] = ("WON" if verb in ("advanced from", "won")
                                     else "WITHDREW" if verb.startswith("withdrew") else "LOST")
            out["primary_result_date"] = _iso(m.group(3))
            out["primary_result_text"] = text[max(0, m.start() - 80):m.end()][:260]
        pd = re.search(rf"primary (?:will occur|is scheduled|occurred|was held|took place) on {DATE_RE}", text)
        if pd:
            out["primary_date"] = _iso(pd.group(1))
        gd = re.search(rf"general election (?:will occur|is scheduled|occurred|was held) on {DATE_RE}", text)
        if gd:
            out["general_date"] = _iso(gd.group(1))
        if re.search(rf"(not running for re-?election|is not seeking re-?election|announced (?:he|she|they) "
                     rf"would not (?:run|seek))[^.]{{0,80}}{CYCLE}", text, re.I):
            out["retiring"] = True
        other = re.search(rf"(?:is|was) running for (?:election to )?(?:the )?(Governor|U\.S\. Senate|Attorney General|"
                          rf"Lieutenant Governor|Secretary of State|Mayor)[^.]{{0,60}}{CYCLE}", text)
        if other and not (chamber == "senate" and "senate" in other.group(1).lower()):
            out["running_for_other_office"] = other.group(1)
        return out
    return {}


def grokipedia(name):
    html = get_text(GROKIPEDIA.format(slug=urllib.parse.quote(_slug(name))), stream_name="grokipedia",
                    retries=1, headers={"Accept": "text/html"})
    if not html:
        return {}
    paras = [strip_html(p) for p in re.findall(r"<p[^>]*>(.*?)</p>", html, re.S | re.I)]
    paras = [p for p in paras if len(p) > 80 and "cookie" not in p.lower()]
    return {"grokipedia_description": " ".join(paras[:2])[:600]} if paras else {}


def enrich_person(p):
    out = {}
    out.update(ballotpedia(p["name"], p.get("state", ""), p.get("chamber", "house")))
    out.update(wikipedia(p["name"], p.get("state", ""), p.get("chamber", "house")))
    if not out.get("description"):
        g = grokipedia(p["name"])
        if g:
            out["description"] = g["grokipedia_description"]
            out["description_source"] = "grokipedia"
    elif out.get("description"):
        out["description_source"] = "wikipedia"
    out["enriched_at"] = now_iso()
    return out


def enrich_people(people, priority_keys):
    """
    people: {key: person}. priority_keys get refreshed first. Results cached
    per key for TTL_ENRICH_H; around election dates the TTL is short so results land quickly.
    """
    cache = load_cache("enrichment")
    todo = []
    for key in list(priority_keys) + [k for k in people if k not in priority_keys]:
        rec = cache.get(key, {})
        p = people[key]
        ttl = TTL_ENRICH_H
        pdate = rec.get("primary_date") or p.get("primary_date")
        if pdate:
            try:
                days = abs((datetime.fromisoformat(pdate).date() - TODAY).days)
                if days <= 10:
                    ttl = 8
            except Exception:
                pass
        if age_hours(rec.get("enriched_at")) > ttl:
            todo.append(key)
    todo = todo[:ENRICH_BUDGET_PER_RUN]
    done = 0
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(enrich_person, people[k]): k for k in todo}
        for f in as_completed(futs):
            k = futs[f]
            try:
                cache[k] = {**cache.get(k, {}), **f.result()}
                done += 1
            except Exception as e:
                stream("ballotpedia", error=f"{people[k]['name']}: {e}")
    save_cache("enrichment", cache)
    have_desc = sum(1 for k in people if cache.get(k, {}).get("description"))
    stream("enrichment", ok=done > 0 or have_desc > 0, count=have_desc,
           note=f"refreshed {done}/{len(todo)} this run · {have_desc}/{len(people)} people have a bio")
    return cache
