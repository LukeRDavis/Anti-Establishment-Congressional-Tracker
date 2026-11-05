"""
Electoral reform: where alternative voting systems are used, adopted, repealed, on the
ballot, or being petitioned for (ranked-choice, approval, STAR and similar), for
government elections and for parties' own internal elections.

Sources, read fresh each week:
  • Wikipedia articles on ranked-choice voting in the US, approval voting, STAR voting
    and electoral reform in the US (plain-text extracts via the MediaWiki API)
  • recent news headlines on alternative-voting petitions and ballot measures
The free AI models turn those texts into structured entries; every entry must cite one
of the source URLs it was given. Nothing about any state or measure is written here.
"""
import hashlib, json, re, urllib.parse

from . import ai
from .establishment import _news_query
from .net import get_json, stream, load_cache, save_cache, now_iso, age_hours

TTL_H = 24 * 7
PAGES = ["Ranked-choice voting in the United States", "Approval voting", "STAR voting",
         "Electoral reform in the United States", "Instant-runoff voting"]
NEWS = ["ranked choice voting petition signatures", "ranked choice voting ballot measure",
        "approval voting ballot initiative", "STAR voting initiative",
        "party convention ranked choice voting Republican", "party primary ranked choice voting Democrats",
        "ban ranked choice voting law"]
_KEEP = re.compile(r"ranked|instant[- ]runoff|approval voting|STAR voting|score voting|alternative vot|"
                   r"single transferable|top[- ]four|top[- ]five|final[- ]five|ballot measure|initiative|"
                   r"petition|repeal|ban|convention|caucus|party", re.I)

SYSTEM = """You compile a factual reference for U.S. voters on alternative voting systems: ranked-choice / instant-runoff voting (RCV), approval voting, STAR voting, and similar reforms such as top-four or final-five primaries paired with RCV.

From ONLY the texts given, list every distinct U.S. state-level item you can support:
- states that use an alternative system in government elections (statewide, for Congress, for primaries, or in some localities);
- states where one was adopted, repealed, banned by law, or rejected by voters;
- ballot measures and petition drives (signature gathering) to adopt or repeal one, with the year;
- political parties (Republican, Democratic, Libertarian, Green, Independent Party, etc.) that use one in their own conventions, caucuses or primaries in a state.

Rules: use only facts in the texts; do not guess dates or outcomes; if the texts disagree, prefer the most recent; say "unknown" rather than invent. Every item must cite one URL from the list given with that text. Write plain text, no Markdown.

Respond with JSON only:
{"items": [{"state": "two-letter code or 'US' for national", "system": "RCV"|"Approval"|"STAR"|"Top-four/Final-five + RCV"|"Other", "scope": "statewide"|"congressional"|"primaries"|"local"|"party"|"presidential primary", "party": "party name if scope is party, else empty", "status": "in use"|"adopted"|"ballot measure"|"petition drive"|"repealed"|"banned"|"rejected"|"proposed", "year": "year or empty", "detail": "one sentence", "url": "source URL"}]}"""


def _extract(title):
    d = get_json("https://en.wikipedia.org/w/api.php?" + urllib.parse.urlencode({
        "action": "query", "prop": "extracts", "explaintext": 1, "redirects": 1, "format": "json",
        "titles": title}), stream_name="reform_wikipedia")
    for page in ((d or {}).get("query") or {}).get("pages", {}).values():
        text = page.get("extract") or ""
        if text:
            url = "https://en.wikipedia.org/wiki/" + urllib.parse.quote(page.get("title", title).replace(" ", "_"))
            paras = [x.strip() for x in re.split(r"\n+", text) if len(x.strip()) > 40 and _KEEP.search(x)]
            return url, "\n".join(paras)
    return None, ""


def fetch():
    cache = load_cache("reform")
    # a complete set is kept for a week; a partial one (the free AI quota ran out partway)
    # is completed on the next pass, keeping what was already extracted
    if age_hours(cache.get("at")) < (0.5 if cache.get("partial") else TTL_H) and cache.get("items"):
        stream("reform", ok=True, count=len(cache["items"]), note="cached")
        return cache
    chunks = []
    for title in PAGES:
        url, text = _extract(title)
        if text:
            # long articles are sent in pieces so each fits a free model's context
            for i in range(0, min(len(text), 24000), 6000):
                chunks.append((title, [url], text[i:i + 6000]))
    news = []
    for q in NEWS:
        news += _news_query(q)
    if news:
        seen, rows = set(), []
        for link, line in news:
            if link not in seen:
                seen.add(link)
                rows.append((link, line))
        chunks.append(("news", [l for l, _ in rows[:40]],
                       "\n".join(f"{line} — {link}" for link, line in rows[:40])))
    done = set(cache.get("done_chunks", [])) if cache.get("partial") else set()
    items = list(cache.get("items", [])) if cache.get("partial") else []
    failed = 0
    for title, urls, text in chunks:
        cid = f"{title}|{hashlib.sha1(text.encode()).hexdigest()[:12]}"
        if cid in done:
            continue
        prompt = f"Source: {title}\nURLs you may cite: {', '.join(urls)}\n\nText:\n{text}"
        res, _, provider = ai.ask_json(SYSTEM, prompt)
        if not isinstance(res, dict):
            failed += 1
            continue
        done.add(cid)
        for it in (res or {}).get("items", []) if isinstance(res, dict) else []:
            if not isinstance(it, dict) or not it.get("state") or not it.get("system"):
                continue
            # a news item may cite its own article; anything else must cite the page it came from
            if not (it.get("url") in urls or (title == "news" and str(it.get("url", "")).startswith("http"))):
                it["url"] = urls[0]
            it["state"] = str(it["state"]).upper()[:2] if len(str(it["state"])) <= 3 else "US"
            it["source"] = "news" if title == "news" else "wikipedia"
            it["model"] = ai.LAST.get("model")
            items.append({k: str(it.get(k) or "")[:300] for k in
                          ("state", "system", "scope", "party", "status", "year", "detail", "url", "source", "model")})
    # one entry per (state, system, scope, party, status): the most detailed wins
    best = {}
    for it in items:
        key = (it["state"], it["system"], it["scope"], it["party"].lower(), it["status"])
        if key not in best or len(it["detail"]) > len(best[key]["detail"]):
            best[key] = it
    items = sorted(best.values(), key=lambda x: (x["state"], x["status"], x["system"]))
    if items:
        cache = {"at": now_iso(), "items": items, "sources": [u for _, us, _ in chunks for u in us][:60],
                 "partial": failed > 0, "done_chunks": sorted(done)}
        save_cache("reform", cache)
    stream("reform", ok=bool(items), count=len(items),
           note=f"{len(items)} items from {len(chunks)} source texts" + (f" · {failed} still to read" if failed else ""))
    return cache if cache.get("items") else {"items": []}
