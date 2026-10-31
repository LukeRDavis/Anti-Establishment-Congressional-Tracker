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

from .config import (TRACKAIPAC_CONGRESS, TRACKAIPAC_ENDORSEMENTS, RLC_NEWS, RLC_INDEX_PAGES,
                     RLC_ENDORSE_PAGE, STATE_ABBR, TTL_TRACKERS_H, TODAY, CYCLE)
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
    """Find the newest Liberty Index PDFs linked from rlc.org's scorecard pages."""
    links = []
    for page in RLC_INDEX_PAGES:
        html = get_text(page, stream_name="rlc") or ""
        links += re.findall(r'(https?://rlc\.org/wp-content/uploads/[^"\'\s>]+?\.pdf)', html, re.I)
    house = [u for u in links if re.search(r'house', u, re.I)]
    senate = [u for u in links if re.search(r'senate', u, re.I)]
    # edition year is the one in the file name, not the upload folder
    yr = lambda u: max([int(y) for y in re.findall(r'20\d\d', u.rsplit("/", 1)[-1])] or [0])
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
            stream("rlc", note=f"no {chamber} Liberty Index PDF link found on the RLC scorecard pages")
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
    """
    RLC posts a yearly '{cycle} National Endorsements' page plus individual news
    posts. Both use sentences like 'The Republican Liberty Caucus proudly
    endorses Brandon Herrera for Congress in TX-23.'
    """
    pat = re.compile(r'endorse[sd]?\s+(?:Congressman |Congresswoman |Senator |Rep\. |Sen\. )?'
                     r'([A-Z][A-Za-z\.\'\- ]{3,40}?)\s+(?:for [A-Za-z\. ]{3,30}?\s+)?in\s+'
                     r'([A-Z]{2})-(\d{1,2}|AL|Senate)\b')
    pat_long = re.compile(r'(?:Endorses|Endorsed)\s+([A-Z][A-Za-z\.\'\- ]{3,40}?)\s+(?:for|in)\s+([^<\n"]{5,90})')
    out, seen = [], set()

    def add(name, state, district, senate, where):
        name = name.strip()
        if name.lower() in seen or len(name) < 4:
            return
        seen.add(name.lower())
        out.append({"name": name, "state": state, "party": "R",
                    "chamber": "senate" if senate else "house",
                    "district": None if senate else district,
                    "source": "rlc_endorsed", "endorsed_for": where[:90]})

    pages = [RLC_ENDORSE_PAGE.format(cycle=CYCLE)] + [RLC_NEWS if p == 1 else f"{RLC_NEWS}page/{p}/"
                                                       for p in range(1, 4)]
    for url in pages:
        html = get_text(url, stream_name="rlc")
        if not html:
            continue
        text = strip_html(html)
        for name, st, d in pat.findall(text):
            senate = d.lower() == "senate"
            add(name, st, None if senate else (0 if d == "AL" else int(d)), senate, f"{st}-{d}")
        for name, where in pat_long.findall(html):
            w = where.lower()
            state = next((ab for full, ab in STATE_ABBR.items() if full in w), "")
            if not state:
                continue
            dm = re.search(r'(\d+)(?:st|nd|rd|th)', where)
            add(name, state, int(dm.group(1)) if dm else None, "senate" in w, where.strip())
    stream("rlc", note=f"endorsements: {len(out)} candidates")
    return out


def fetch_all():
    """Refresh each tracker on its own schedule; an empty result is retried next run."""
    cache = load_cache("trackers")
    fresh = age_hours(cache.get("fetched_at")) < TTL_TRACKERS_H
    sources = {"ta_congress": trackaipac_congress, "ta_endorsed": trackaipac_endorsements,
               "rlc_index": rlc_liberty_index, "rlc_endorsed": rlc_endorsements}
    out = {"fetched_at": cache.get("fetched_at") if fresh else now_iso()}
    for k, fn in sources.items():
        if fresh and cache.get(k):
            out[k] = cache[k]
            continue
        out[k] = fn() or cache.get(k) or ({} if k in ("ta_congress", "rlc_index") else [])
    stream("trackaipac", ok=bool(out["ta_congress"]))
    stream("rlc", ok=bool(out["rlc_index"] or out["rlc_endorsed"]),
           count=len(out["rlc_index"]) + len(out["rlc_endorsed"]))
    save_cache("trackers", out)
    return out
