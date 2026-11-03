"""
Election results — primaries now, the general once it is counted.

Source: each state's Wikipedia election page for this cycle ("{cycle} United
States House of Representatives elections in {State}", "{cycle} United States
Senate election in {State}"), read as wikitext through the MediaWiki API.
Those pages carry the standard {{Election box ...}} templates, so results are
parsed from structure, not prose:

  • a person who appears in a primary box and in the general-election box
    advanced; one who appears only in the primary box did not
    (this works for party primaries, top-two and top-four alike);
  • with no general box yet, the primary box's "winning candidate" template
    (or the most votes, once votes are posted) decides;
  • a general box with votes posted gives the general result.

Nothing is accepted before the election date it belongs to; build.apply_status
enforces that with the official calendar.
"""
import re, unicodedata, urllib.parse

from .config import CYCLE, STATE_NAMES, VOTING_STATES
from .net import get_json, stream, load_cache, save_cache, now_iso, age_hours

API = "https://en.wikipedia.org/w/api.php"
TTL_H = 6
PARSE_VERSION = 4          # bump when the parser changes so cached pages are re-read


def _norm(s):
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z ]", " ", s)


def _clean_name(raw):
    raw = re.sub(r"<ref[^>]*/>|<ref[^>]*>.*?</ref>", "", raw or "", flags=re.S)
    raw = re.sub(r"\[\[(?:[^\]|]*\|)?([^\]]*)\]\]", r"\1", raw)      # [[Target|Shown]] → Shown
    raw = re.sub(r"\{\{[^{}]*\}\}", "", raw)
    raw = re.sub(r"\((?:incumbent|write-in|withdrawn)[^)]*\)", "", raw, flags=re.I)
    return re.sub(r"\s+", " ", raw.replace("'''", "").replace("''", "")).strip(" *")


def _num(s):
    s = re.sub(r"[^\d.]", "", s or "")
    try:
        return float(s) if s else None
    except ValueError:
        return None


def _param(tpl, name):
    m = re.search(r"\|\s*" + name + r"\s*=\s*((?:\[\[[^\]]*\]\]|\{\{[^{}]*\}\}|[^|\n}])*)", tpl, re.I)
    return m.group(1).strip() if m else ""


def _boxes(text):
    """Every {{Election box begin}} … {{Election box end}} block, with title and candidates."""
    out = []
    starts = [m.start() for m in re.finditer(r"\{\{\s*Election[ _]box[ _](?:begin|top)", text, re.I)]
    for i, s in enumerate(starts):
        m_end = re.compile(r"\{\{\s*Election[ _]box[ _]end", re.I).search(text, s)
        e = m_end.start() if m_end else -1
        nxt = starts[i + 1] if i + 1 < len(starts) else len(text)
        block = text[s:(e if e != -1 and e < nxt else nxt)]
        title = _clean_name(_param(block, "title"))
        cands = []
        for m in re.finditer(r"\{\{\s*Election[ _]box[ _](winning[ _]candidate|candidate)[^|]*((?:\{\{[^{}]*\}\}|[^{}])*)\}\}",
                             block, re.I):
            name = _clean_name(_param(m.group(2), "candidate"))
            if not name or name.lower() in ("write-in", "write-ins", "other", "none of these candidates"):
                continue
            cands.append({"name": name, "votes": _num(_param(m.group(2), "votes")),
                          "pct": _num(_param(m.group(2), "percentage")),
                          "winner": m.group(1).lower().startswith("winning")})
        t = title.lower()
        kind = "runoff" if "runoff" in t else "primary" if re.search(r"primar|nonpartisan|top[- ]two|jungle", t) else "general"
        out.append({"title": title, "kind": kind, "candidates": cands, "special": "special" in t,
                    "counted": any((c["votes"] or 0) > 0 for c in cands)})
    return out


def _sections(text, chamber):
    """{district: wikitext} for a House page; {None: page} for a Senate page or an at-large state."""
    if chamber == "senate":
        return {None: text}
    # "== District 7 ==", "=== [[California's 7th congressional district|District 7]] ===",
    # "== District 7 (Sacramento) ==", or "== 2026 California's 7th congressional district election =="
    parts = re.split(r"\n(={2,4})[^=\n]*?(?:District\s+(\d+)\b|\b(\d+)(?:st|nd|rd|th)\s+congressional\s+district)"
                     r"[^=\n]*\1[ \t]*(?=\n)", "\n" + text, flags=re.I)
    if len(parts) == 1:
        return {0: text}
    out = {}
    for i in range(1, len(parts) - 3, 4):
        level, num, body = len(parts[i]), parts[i + 1] or parts[i + 2], parts[i + 3]
        nxt = re.search(r"\n={2,%d}[^=]" % level, body)  # stop at the next heading of the same or higher level
        d = int(num)
        out[d] = out.get(d, "") + (body[:nxt.start()] if nxt else body)
    return out


def _titles(state, chamber):
    sn = STATE_NAMES[state]
    if chamber == "senate":
        return [f"{CYCLE} United States Senate election in {sn}",
                f"{CYCLE} United States Senate special election in {sn}"]
    return [f"{CYCLE} United States House of Representatives elections in {sn}",
            f"{CYCLE} United States House of Representatives election in {sn}"]


def _fetch_page(title):
    url = f"{API}?" + urllib.parse.urlencode({"action": "parse", "page": title, "prop": "wikitext",
                                              "format": "json", "formatversion": 2, "redirects": 1})
    d = get_json(url, stream_name="wikipedia_results", trip_on=(401,))
    if not d or "parse" not in d:
        return None, None
    return d["parse"].get("wikitext", ""), "https://en.wikipedia.org/wiki/" + urllib.parse.quote(
        d["parse"].get("title", title).replace(" ", "_"))


def _ordinal(n):
    return f"{n}{'th' if 10 <= n % 100 <= 20 else {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')}"


def _district_title(state, n):
    return f"{CYCLE} {STATE_NAMES[state]}'s {_ordinal(n)} congressional district election"


def fetch(people):
    """Parse result boxes for every state/chamber with someone tracked on the ballot.

    Some statewide pages (California's, for one) carry no result templates and
    link out to one page per district instead; those districts are read from
    their own "{cycle} {State}'s Nth congressional district election" page."""
    cache = load_cache("election_results")
    want, districts = set(), {}
    for p in people.values():
        if not (p.get("on_ballot") and p.get("state") in VOTING_STATES):
            continue
        ch = p.get("seeking", {}).get("chamber", p["chamber"])
        want.add((p["state"], ch))
        d = p.get("seeking", {}).get("district", p.get("district"))
        if ch == "house" and d:
            districts.setdefault(p["state"], set()).add(int(d))
    # a state's real districts are the ones its sitting delegation holds (FEC filers sometimes
    # list a state-legislative district, e.g. "FL-59"); don't go looking for pages that can't exist
    seats = {}
    for p in people.values():
        if p.get("incumbent") and p["chamber"] == "house" and p.get("district") is not None:
            seats[p["state"]] = max(seats.get(p["state"], 0), int(p["district"] or 0))
    fetched = 0
    for state, chamber in sorted(want):
        key = f"{state}|{chamber}"
        old = cache.get(key) or {}
        if old.get("v") == PARSE_VERSION and age_hours(old.get("fetched_at")) < TTL_H:
            continue
        entry = {"fetched_at": now_iso(), "v": PARSE_VERSION, "url": None, "races": {}}
        for title in _titles(state, chamber):
            text, url = _fetch_page(title)
            if text:
                if chamber == "house" and not _boxes(text):
                    # big delegations split results into sub-articles ("{{Main|… (Districts 1–26)}}");
                    # read every linked sub-article of this same election and parse them together
                    subs = []
                    for m in re.finditer(r"\{\{\s*(?:main|see also|further)\s*\|([^}]*)\}\}", text, re.I):
                        for t in m.group(1).split("|"):
                            t = t.strip()
                            if "=" not in t and str(CYCLE) in t and STATE_NAMES[state] in t and t not in subs:
                                subs.append(t)
                    for t in subs[:6]:
                        sub, _ = _fetch_page(t)
                        if sub:
                            text += "\n" + sub
                            fetched += 1
                entry.update(url=url, races={str(d): _boxes(sec) for d, sec in _sections(text, chamber).items()})
                if not any(entry["races"].values()):
                    heads = re.findall(r"\n(={2,4}[^\n]{0,80})", text)[:12]
                    tpls = sorted({t.strip().lower() for t in re.findall(r"\{\{\s*([A-Za-z][^|}\n]{2,40})", text)})[:40]
                    print(f"    results: {title} has no result boxes · {len(text)} chars · headings {heads} · templates {tpls}")
                fetched += 1
                break
        if chamber == "house":
            pages = {}
            for d in sorted(districts.get(state, ())):
                if entry["races"].get(str(d)) or (state in seats and d > seats[state]):
                    continue
                text, url = _fetch_page(_district_title(state, d))
                if text:
                    entry["races"][str(d)] = _boxes(text)
                    pages[str(d)] = url
                    fetched += 1
                else:
                    print(f"    results: no page \"{_district_title(state, d)}\"")
            if pages:
                entry["district_urls"] = pages
        cache[key] = entry
    save_cache("election_results", cache)
    pages = sum(1 for v in cache.values() if v.get("url")) + \
        sum(len(v.get("district_urls", {})) for v in cache.values())
    boxes = sum(len(b) for v in cache.values() for b in v.get("races", {}).values())
    stream("wikipedia_results", ok=pages > 0, count=pages,
           note=f"{pages} state election pages ({fetched} refreshed) · {boxes} result boxes")
    return cache


def _same_person(p, cand_name):
    c = _norm(cand_name).split()
    n = _norm(p.get("full_name") or p["name"]).split()
    if not c or not n:
        return False
    last = _norm(p.get("last_name") or p["name"]).split()
    if not last or last[-1] not in c:
        return False
    firsts = {n[0], _norm(p["name"]).split()[0]}
    if c[0] in firsts or any(f[:1] == c[0][:1] for f in firsts if f):
        return True
    # goes by a middle name or its short form ("Warren Kenneth Paxton" on the ballot as "Ken Paxton")
    middles = [m for m in n[1:-1] if len(m) >= 3]
    return len(c[0]) >= 3 and any(m.startswith(c[0]) or c[0].startswith(m) for m in middles)


def result_for(p, cache):
    """{'primary': 'WON'|'LOST'|None, 'general': ..., 'source': url} for one person."""
    chamber = p.get("seeking", {}).get("chamber", p["chamber"])
    dist = p.get("seeking", {}).get("district", p.get("district"))
    page = cache.get(f"{p.get('state')}|{chamber}") or {}
    races = page.get("races", {})
    boxes = races.get("None" if chamber == "senate" else str(dist or 0)) or \
        (races.get("0") if chamber == "house" and len(races) == 1 else None) or []
    mine = lambda b: next((c for c in b["candidates"] if _same_person(p, c["name"])), None)
    general = [b for b in boxes if b["kind"] == "general" and b["candidates"]]
    primaries = [b for b in boxes if b["kind"] in ("primary", "runoff") and mine(b)]
    # a district or seat holding a special election and a regular one on the same page:
    # pair each primary only with the general of the same election
    if len({b["special"] for b in boxes if b["kind"] != "general"}) > 1 and primaries:
        flag = primaries[-1]["special"]
        general = [b for b in general if b.get("special") == flag]
    src = (page.get("district_urls") or {}).get(str(dist or 0)) or page.get("url")
    out = {"primary": None, "general": None, "source": src}
    if not primaries and any(mine(b) for b in general):
        out["primary"] = "WON"          # on the general ballot with no contested primary (nominee / unopposed)
    if primaries:
        if general:
            out["primary"] = "WON" if any(mine(b) for b in general) else "LOST"
        else:
            b = primaries[-1]
            if any(c["winner"] for c in b["candidates"]):
                out["primary"] = "WON" if mine(b)["winner"] else "LOST"
            elif b["counted"]:
                top = max(b["candidates"], key=lambda c: c["votes"] or 0)
                out["primary"] = "WON" if top is mine(b) else "LOST"
    for b in general:
        me = mine(b)
        if me and b["counted"]:
            if any(c["winner"] for c in b["candidates"]):
                out["general"] = "WON" if me["winner"] else "LOST"
    return out
