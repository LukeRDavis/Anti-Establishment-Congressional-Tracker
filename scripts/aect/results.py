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
    starts = [m.start() for m in re.finditer(r"\{\{\s*Election box begin", text, re.I)]
    for i, s in enumerate(starts):
        e = text.find("{{Election box end", s)
        nxt = starts[i + 1] if i + 1 < len(starts) else len(text)
        block = text[s:(e if e != -1 and e < nxt else nxt)]
        title = _clean_name(_param(block, "title"))
        cands = []
        for m in re.finditer(r"\{\{\s*Election box (winning candidate|candidate)[^|]*((?:\{\{[^{}]*\}\}|[^{}])*)\}\}",
                             block, re.I):
            name = _clean_name(_param(m.group(2), "candidate"))
            if not name or name.lower() in ("write-in", "write-ins", "other", "none of these candidates"):
                continue
            cands.append({"name": name, "votes": _num(_param(m.group(2), "votes")),
                          "pct": _num(_param(m.group(2), "percentage")),
                          "winner": m.group(1).lower() == "winning candidate"})
        t = title.lower()
        kind = "runoff" if "runoff" in t else "primary" if re.search(r"primar|nonpartisan|top[- ]two|jungle", t) else "general"
        out.append({"title": title, "kind": kind, "candidates": cands,
                    "counted": any((c["votes"] or 0) > 0 for c in cands)})
    return out


def _sections(text, chamber):
    """{district: wikitext} for a House page; {None: page} for a Senate page or an at-large state."""
    if chamber == "senate":
        return {None: text}
    parts = re.split(r"\n(={2,4})\s*District\s+(\d+)\s*\1\s*\n", "\n" + text)
    if len(parts) == 1:
        return {0: text}
    out = {}
    for i in range(1, len(parts) - 2, 3):
        body = parts[i + 2]
        nxt = re.search(r"\n={2}[^=]", body)            # stop at the next top-level section
        out[int(parts[i + 1])] = body[:nxt.start()] if nxt else body
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


def fetch(people):
    """Parse result boxes for every state/chamber with someone tracked on the ballot."""
    cache = load_cache("election_results")
    want = sorted({(p["state"], p.get("seeking", {}).get("chamber", p["chamber"]))
                   for p in people.values() if p.get("on_ballot") and p.get("state") in VOTING_STATES})
    fetched = 0
    for state, chamber in want:
        key = f"{state}|{chamber}"
        if age_hours((cache.get(key) or {}).get("fetched_at")) < TTL_H:
            continue
        for title in _titles(state, chamber):
            text, url = _fetch_page(title)
            if text:
                cache[key] = {"fetched_at": now_iso(), "url": url,
                              "races": {str(d): _boxes(sec) for d, sec in _sections(text, chamber).items()}}
                fetched += 1
                break
        else:
            cache[key] = {"fetched_at": now_iso(), "url": None, "races": {}}
    save_cache("election_results", cache)
    pages = sum(1 for v in cache.values() if v.get("url"))
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
    return c[0] in firsts or any(f[:1] == c[0][:1] for f in firsts if f)


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
    out = {"primary": None, "general": None, "source": page.get("url")}
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
