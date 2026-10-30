"""
Step 4c — outside trackers, used as corroborating signals and as a source of
candidate names. None of them is required; a candidate a tracker misses is
still found through FEC and judged on their own words and record.

  • TrackAIPAC /congress     — lobby money per member, "approved" flag
  • TrackAIPAC /endorsements — endorsed challengers
  • RLC Liberty Index        — PDF discovered from rlc.org (newest edition)
  • RLC endorsements         — national news feed
"""
import io, re
from datetime import datetime

from .config import (TRACKAIPAC_CONGRESS, TRACKAIPAC_ENDORSEMENTS, RLC_NEWS, RLC_INDEX_PAGE,
                     STATE_ABBR, TTL_TRACKERS_H, TODAY)
from .net import get_text, get_bytes, stream, load_cache, save_cache, now_iso, age_hours, strip_html

_DIST = re.compile(r'([A-Z]{2})-(\d+|AL|SEN|SENATE)\s*\[([RDIL])\]')


def _parse_dist(raw):
    m = _DIST.search(raw or "")
    if not m:
        return None
    st, d, p = m.groups()
    if d in ("SEN", "SENATE"):
        return st, None, p, "senate"
    return st, (0 if d == "AL" else int(d)), p, "house"


def _h2_blocks(html):
    for block in re.split(r'<h2[^>]*>', html)[1:]:
        m = re.match(r'([^<]+)</h2>', block)
        if not m:
            continue
        name = re.sub(r'\s+', ' ', m.group(1)).strip()
        if 3 <= len(name) <= 60 and name.lower() != "vacant":
            yield name, block, strip_html(block)


def trackaipac_congress():
    html = get_text(TRACKAIPAC_CONGRESS, stream_name="trackaipac")
    out = {}
    for name, block, text in _h2_blocks(html or ""):
        d = _parse_dist(text)
        lobby = re.search(r'Israel Lobby Total:\s*\$([\d,]+)', text)
        pacs = re.findall(r'\b(AIPAC|UDP|DMFI|RJC|JAC|NORPAC|AGG|AMP|USI|PIA|FIPAC|JStreetPAC)\b', text)
        out[name.lower()] = {
            "name": name, "state": d[0] if d else "", "district": d[1] if d else None,
            "party": d[2] if d else "", "chamber": d[3] if d else "",
            "approved": "Track AIPAC Approved" in text or "rejects aipac" in text.lower(),
            "lobby_total": int(lobby.group(1).replace(",", "")) if lobby else 0,
            "pacs": sorted(set(pacs))[:8],
        }
    stream("trackaipac", ok=bool(out), count=len(out),
           note=f"/congress: {len(out)} members, {sum(v['approved'] for v in out.values())} approved")
    return out


def trackaipac_endorsements():
    html = get_text(TRACKAIPAC_ENDORSEMENTS, stream_name="trackaipac")
    out = []
    for name, block, text in _h2_blocks(html or ""):
        d = _parse_dist(text)
        if not d:
            continue
        site = re.search(r'href="(https?://(?!www\.trackaipac)[^"]+)"', block)
        out.append({"name": name, "state": d[0], "district": d[1], "party": d[2],
                    "chamber": d[3], "website": site.group(1) if site else "",
                    "source": "trackaipac_endorsed"})
    stream("trackaipac", note=f"/endorsements: {len(out)} endorsed candidates")
    return out


def _rlc_pdf_urls():
    """Find the newest Liberty Index PDFs linked from rlc.org — no fixed file names."""
    html = get_text(RLC_INDEX_PAGE, stream_name="rlc") or ""
    links = re.findall(r'href="(https?://rlc\.org/wp-content/uploads/[^"]+?\.pdf)"', html, re.I)
    house = [u for u in links if re.search(r'house', u, re.I)]
    senate = [u for u in links if re.search(r'senate', u, re.I)]
    yr = lambda u: max([int(y) for y in re.findall(r'20\d\d', u)] or [0])
    return (max(house, key=yr) if house else None, max(senate, key=yr) if senate else None)


def rlc_liberty_index():
    cache = load_cache("rlc_index")
    if age_hours(cache.get("fetched_at")) < 24 * 7 and cache.get("scores"):
        stream("rlc", ok=True, count=len(cache["scores"]), note="Liberty Index cache fresh")
        return cache["scores"]
    try:
        import pdfplumber
    except ImportError:
        stream("rlc", ok=False, note="pdfplumber not installed — Liberty Index skipped")
        return cache.get("scores", {})
    house_url, senate_url = _rlc_pdf_urls()
    scores = {}
    for chamber, url in (("house", house_url), ("senate", senate_url)):
        if not url:
            stream("rlc", note=f"no {chamber} Liberty Index PDF link found on {RLC_INDEX_PAGE}")
            continue
        raw = get_bytes(url, stream_name="rlc")
        if not raw:
            continue
        try:
            with pdfplumber.open(io.BytesIO(raw)) as pdf:
                text = "\n".join(p.extract_text() or "" for p in pdf.pages)
        except Exception as e:
            stream("rlc", error=f"PDF parse error {url}: {e}")
            continue
        n = 0
        for line in text.splitlines():
            line = line.strip()
            m = (re.match(r"^([A-Z][A-Za-z'\-]+),\s+([A-Z][A-Za-z'\-\. ]+?)\s+.*?(\d{1,3})\s*$", line)
                 or re.match(r"^([A-Z][A-Za-z'\-\.]+(?: [A-Z][A-Za-z'\-\.]+){1,3})\s+.*?(\d{1,3})\s*$", line))
            if not m:
                continue
            if len(m.groups()) == 3:
                name, score = f"{m.group(2).strip()} {m.group(1)}", int(m.group(3))
            else:
                name, score = m.group(1), int(m.group(2))
            if 0 <= score <= 110 and len(name) > 5:
                scores[name.lower()] = {"name": name, "score": score, "chamber": chamber,
                                        "edition": url.rsplit("/", 1)[-1]}
                n += 1
        stream("rlc", note=f"{chamber}: {n} scores parsed from {url.rsplit('/',1)[-1]}")
    if scores:
        save_cache("rlc_index", {"fetched_at": now_iso(), "scores": scores})
    stream("rlc", ok=bool(scores), count=len(scores))
    return scores or cache.get("scores", {})


def rlc_endorsements():
    out, seen = [], set()
    for page in range(1, 5):
        url = RLC_NEWS if page == 1 else f"{RLC_NEWS}page/{page}/"
        html = get_text(url, stream_name="rlc")
        if not html:
            break
        for name, where in re.findall(
                r'(?:Endorses|Endorsed)\s+([A-Z][A-Za-z\.\'\- ]{3,40}?)\s+(?:for|in)\s+([^<\n"]{5,90})', html):
            if name.lower() in seen:
                continue
            seen.add(name.lower())
            w = where.lower()
            state = next((ab for full, ab in STATE_ABBR.items() if full in w), "")
            dm = re.search(r'(\d+)(?:st|nd|rd|th)', where)
            senate = "senate" in w
            out.append({"name": name.strip(), "state": state, "party": "R",
                        "chamber": "senate" if senate else "house",
                        "district": None if senate else (int(dm.group(1)) if dm else None),
                        "source": "rlc_endorsed", "endorsed_for": where.strip()[:90]})
    stream("rlc", note=f"endorsements: {len(out)} candidates")
    return out


def fetch_all():
    cache = load_cache("trackers")
    if age_hours(cache.get("fetched_at")) < TTL_TRACKERS_H and cache.get("ta_congress"):
        for s in ("trackaipac", "rlc"):
            stream(s, ok=True, note="cache fresh")
        return cache
    out = {
        "ta_congress": trackaipac_congress(),
        "ta_endorsed": trackaipac_endorsements(),
        "rlc_index": rlc_liberty_index(),
        "rlc_endorsed": rlc_endorsements(),
        "fetched_at": now_iso(),
    }
    # keep previous values for any source that failed this run
    for k in ("ta_congress", "ta_endorsed", "rlc_index", "rlc_endorsed"):
        if not out[k] and cache.get(k):
            out[k] = cache[k]
    save_cache("trackers", out)
    return out
