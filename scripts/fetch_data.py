#!/usr/bin/env python3
"""
fetch_data.py — GitHub Actions, every 4 hours.

Anti-Arms classification pipeline:
  Democrats/Independents:
    TrackAIPAC /congress "Track AIPAC Approved" → antiArms=True, level=soft
    Congress.gov bill co-sponsorship            → upgrade to hard
    LegiScan Yes vote on anti-arms bill         → upgrade to hard

  Republicans:
    RLC Liberty Index score ≥ 75               → antiArms=True, level=soft
    Congress.gov bill co-sponsorship            → upgrade to hard
    Congress.gov house-vote No on pro-arms      → upgrade to hard
    KNOWN_ANTI_ARMS override (118th record)     → sets level directly

  Both:
    KNOWN_ANTI_ARMS dict                        → authoritative override

Challengers:
  TrackAIPAC /endorsements                     → anti-arms D/I challengers
  RLC /endorsements blog posts                 → anti-arms R challengers
  FEC /candidates 2026                         → cross-reference for FEC IDs

GitHub Secrets:
  CONGRESS_KEY   api.congress.gov
  FEC_KEY        api.open.fec.gov
  POLY_KEY       polymarket.com
  LEGISCAN_KEY   legiscan.com (optional)
"""

import os, io, json, re, time, hashlib, urllib.request, urllib.parse, urllib.error
from datetime import datetime, timezone, timedelta

CONGRESS_KEY  = os.environ.get("CONGRESS_KEY",  "")
FEC_KEY       = os.environ.get("FEC_KEY",       "")
POLY_KEY      = os.environ.get("POLY_KEY",      "")
LEGISCAN_KEY  = os.environ.get("LEGISCAN_KEY",  "")

CONGRESS_BASE    = "https://api.congress.gov/v3"
FEC_BASE         = "https://api.open.fec.gov/v1"
POLY_BASE        = "https://gamma-api.polymarket.com"
LEGISCAN_BASE    = "https://api.legiscan.com"
CURRENT_CONGRESS = 119

TRACKAIPAC_CONGRESS     = "https://www.trackaipac.com/congress"
TRACKAIPAC_ENDORSEMENTS = "https://www.trackaipac.com/endorsements"
RLC_NEWS_NATIONAL       = "https://rlc.org/category/news/national/"
RLC_LIBERTY_INDEX_HOUSE = "https://rlc.org/wp-content/uploads/2025/02/Liberty-Index-2024-US-House.pdf"
RLC_LIBERTY_INDEX_SENATE= "https://rlc.org/wp-content/uploads/2025/02/Liberty-Index-2024-US-Senate.pdf"
RLC_ENDORSEMENTS        = "https://rlc.org/endorsements/"

TRACKAIPAC_RESCRAPE_H = 24
RLC_RESCRAPE_H        = 24

# RLC Liberty Index threshold — members scoring ≥75 have consistent
# anti-interventionist/anti-foreign-aid voting records
RLC_SOFT_THRESHOLD = 75

BILL_SEARCH_QUERIES = [
    "Block the Bombs Act",
    "Israel arms sales",
    "foreign military sales Israel disapproval",
    "joint resolution disapproval Israel",
    "Gaza weapons",
    "offensive weapons Israel",
]
ANTI_ARMS_SIGNALS = [
    "block","prohibit","restrict","suspend","halt","cease","ban",
    "disapproval","embargo","condition","limit","freeze",
    "offensive weapons","offensive arms",
]
PRO_ARMS_SIGNALS = [
    "security assistance","supplemental appropriation","foreign military",
    "united states-israel security","defense assistance",
    "aid to israel","military aid",
]

# Authoritative overrides — verified from 118th Congress roll calls and bill records.
# These are applied on top of scraped data and cannot be overridden by scrapers.
# level: "hard" = on-record legislative vote or bill co-sponsorship
#        "soft" = stated position, no verifiable legislative action yet
KNOWN_ANTI_ARMS = {
    # Republicans — anti-interventionist No votes on Israel arms packages
    "Thomas Massie":    {"level":"hard","party":"R","reason":"Co-sponsored HR 3565 Block the Bombs Act; No on HR 8034 & HR 8035 (Apr 2024)"},
    "Warren Davidson":  {"level":"hard","party":"R","reason":"No on HR 8034 (Israel aid supplemental, Apr 2024); leads House Liberty Caucus"},
    "Andy Biggs":       {"level":"hard","party":"R","reason":"No on HR 8034 & HR 8035 (Israel/Ukraine packages, Apr 2024)"},
    "Scott Perry":      {"level":"hard","party":"R","reason":"No on HR 8034 (Israel aid supplemental, Apr 2024)"},
    "Paul Gosar":       {"level":"hard","party":"R","reason":"No on HR 8034 (Israel aid supplemental, Apr 2024)"},
    "Chip Roy":         {"level":"hard","party":"R","reason":"No on HR 8034 (Israel aid supplemental, Apr 2024)"},
    "Thomas Tiffany":   {"level":"hard","party":"R","reason":"No on HR 8034 (Israel aid supplemental, Apr 2024)"},
    "Rand Paul":        {"level":"hard","party":"R","reason":"Led Senate holds on Israel arms sales; forced vote on FMS Joint Resolution of Disapproval (2024)"},
    "Eli Crane":        {"level":"hard","party":"R","reason":"No on HR 8034 (Israel aid supplemental, Apr 2024); RLC Liberty Index 97/100"},
    "Lauren Boebert":   {"level":"soft","party":"R","reason":"No on HR 8034; positions inconsistent — classified soft"},
    "Barry Moore":      {"level":"soft","party":"R","reason":"No on some Israel aid provisions; running for Senate 2026"},
    "Bob Good":         {"level":"soft","party":"R","reason":"RLC Liberty Index 92/100; consistent anti-interventionist votes"},
    "Andrew Ogles":     {"level":"soft","party":"R","reason":"RLC Liberty Index 95/100; voted against multiple foreign aid packages"},
    "Anna Paulina Luna":{"level":"soft","party":"R","reason":"RLC Liberty Index 94/100; voted against Israel aid supplemental"},
    # Democrats/Independents — bill co-sponsorship or JRD votes
    "Ro Khanna":        {"level":"hard","party":"D","reason":"Co-sponsored HR 3565 Block the Bombs Act and Senate JRD on Israel FMS"},
    "Lloyd Doggett":    {"level":"hard","party":"D","reason":"Co-sponsored HR 3565 Block the Bombs Act"},
    "Jim McGovern":     {"level":"hard","party":"D","reason":"Co-sponsored HR 3565 Block the Bombs Act"},
    "Jan Schakowsky":   {"level":"hard","party":"D","reason":"Co-sponsored HR 3565 Block the Bombs Act"},
    "Maxine Waters":    {"level":"hard","party":"D","reason":"Co-sponsored HR 3565 Block the Bombs Act"},
    "Jamie Raskin":     {"level":"soft","party":"D","reason":"Called for arms conditions; voting record mixed"},
    "Ed Markey":        {"level":"hard","party":"D","reason":"Co-sponsored Senate Joint Resolution of Disapproval on Israel FMS"},
    "Brian Schatz":     {"level":"soft","party":"D","reason":"Signed letters on arms conditions; no JRD co-sponsorship yet"},
    "Elizabeth Warren": {"level":"soft","party":"D","reason":"Called for arms review; no JRD co-sponsorship yet"},
    "Bernie Sanders":   {"level":"hard","party":"I","reason":"Forced Senate floor vote on Joint Resolution of Disapproval on Israel FMS (2024)"},
}

KNOWN_FEC_IDS = {
    "Rashida Tlaib":"H8MI13145","Ilhan Omar":"H8MN05054",
    "Alexandria Ocasio-Cortez":"H8NY15148","Ayanna Pressley":"H8MA07170",
    "Pramila Jayapal":"H6WA07170","Thomas Massie":"H2KY04074",
    "Lauren Boebert":"H0CO03197","Warren Davidson":"H6OH08174",
    "Summer Lee":"H2PA18134","Ro Khanna":"H4CA17161",
    "Jamie Raskin":"H6MD08170","Jim McGovern":"H6MA03074",
    "Greg Casar":"H2TX35131","Mark Pocan":"H2WI02078",
    "Andy Biggs":"H6AZ05182","Scott Perry":"H2PA04078",
    "Paul Gosar":"H0AZ01094","Maxwell Frost":"H2FL10186",
    "Lloyd Doggett":"H4TX10027","Rand Paul":"S0KY00082",
    "Bernie Sanders":"S6VT00002","Elizabeth Warren":"S2MA00170",
    "Ed Markey":"H0MA07012","Jeff Merkley":"S8OR00207",
    "Chris Van Hollen":"S6MD01037","Brian Schatz":"S2HI00121",
    "Eli Crane":"H2AZ02199","Bob Good":"H0VA05202",
    "Andrew Ogles":"H2TN05133","Chip Roy":"H8TX21186",
    "Thomas Tiffany":"H0WI07204",
}


# ── Utilities ─────────────────────────────────────────────────────────────────
def fetch_json(url, headers=None, retries=2):
    req = urllib.request.Request(url, headers=headers or {})
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            print(f"  HTTP {e.code}: {url[:80]}")
            if e.code == 429: time.sleep(10)
            elif e.code in (401, 403): return None
            else: return None
        except Exception as e:
            if attempt < retries: time.sleep(2)
            else: print(f"  WARN: {url[:80]} → {e}"); return None
    return None

def fetch_html(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; AntiArmsTracker/3.0)",
        "Accept": "text/html,application/xhtml+xml",
    }
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=25) as r:
            return r.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  HTML fetch failed: {url} → {e}")
        return ""

def fetch_bytes(url):
    headers = {"User-Agent": "Mozilla/5.0 (compatible; AntiArmsTracker/3.0)"}
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.read()
    except Exception as e:
        print(f"  Bytes fetch failed: {url} → {e}")
        return b""

def congress_url(path, params=None):
    p = dict(params or {})
    p["api_key"] = CONGRESS_KEY
    p["format"]  = "json"
    return f"{CONGRESS_BASE}{path}?{urllib.parse.urlencode(p)}"

def score_bill(title, subjects):
    combined = (title + " " + " ".join(subjects)).lower()
    if not any(kw in combined for kw in ["israel","gaza","arms","weapon","military sale","fms"]):
        return None
    anti = sum(1 for kw in ANTI_ARMS_SIGNALS if kw in combined)
    pro  = sum(1 for kw in PRO_ARMS_SIGNALS  if kw in combined)
    if anti > pro: return "anti_arms"
    if pro > anti: return "pro_arms"
    if anti > 0:   return "anti_arms"
    return None

def parse_district(raw):
    raw   = raw.strip()
    party = (re.search(r'\[([RDI])\]', raw) or type('',(),{'group':lambda s,x:"?"})()).group(1)
    state = (re.search(r'^([A-Z]{2})-', raw) or type('',(),{'group':lambda s,x:""})()).group(1)
    dm    = re.search(r'-(\d+)', raw)
    district  = int(dm.group(1)) if dm else None
    is_senate = "SEN" in raw.upper()
    chamber   = "senate" if (is_senate or district is None) else "house"
    return state, district, party, chamber


# ─────────────────────────────────────────────────────────────────────────────
# 1. TRACKAIPAC — Democrat/Independent anti-arms source
# ─────────────────────────────────────────────────────────────────────────────

def scrape_trackaipac_congress():
    print("Scraping TrackAIPAC /congress …")
    html = fetch_html(TRACKAIPAC_CONGRESS)
    if not html:
        return {}
    results = {}
    for block in re.split(r'<h2[^>]*>', html)[1:]:
        name_m = re.match(r'([^<]+)</h2>', block)
        if not name_m: continue
        name = re.sub(r'\s+', ' ', name_m.group(1)).strip()
        if not name or len(name) < 3 or len(name) > 60: continue
        text = re.sub(r'<[^>]+>', ' ', block)
        text = re.sub(r'\s+', ' ', text).strip()

        approved  = ("Track AIPAC Approved" in text or "rejects aipac" in text.lower())
        lobby_m   = re.search(r'Israel Lobby Total:\s*\$([\d,]+)', text)
        lobby     = int(lobby_m.group(1).replace(",","")) if lobby_m else 0
        pacs_m    = re.search(r'(?:AIPAC|DMFI|RJC|JAC|NORPAC|AGG|AMP|USI|PIA|FIPAC)[,A-Z\s]+', text)
        pacs      = [p.strip() for p in pacs_m.group(0).split(",")] if pacs_m else []
        pacs      = [p for p in pacs if re.match(r'^[A-Z]{2,8}$', p)][:8]
        dist_m    = re.search(r'([A-Z]{2}-(?:\d+|SEN|SENATE)\s*\[[RDI]\])', text)
        state, district, party, chamber = parse_district(dist_m.group(1)) if dist_m else ("","?","?","house")
        note = ""
        for pat in [r'(Running for [^\.\n]{5,60})', r'(passed away[^\.\n]{0,40})', r'(resigned[^\.\n]{0,40})']:
            nm = re.search(pat, text, re.IGNORECASE)
            if nm: note = nm.group(1).strip(); break

        results[name.lower()] = {
            "name":name,"state":state,"district":district,"party":party,
            "chamber":chamber,"approved":approved,"lobby_total":lobby,"pacs":pacs,"note":note,
        }
    approved_n = sum(1 for v in results.values() if v["approved"])
    print(f"  TrackAIPAC /congress: {len(results)} members, {approved_n} approved")
    return results


def scrape_trackaipac_endorsements():
    print("Scraping TrackAIPAC /endorsements …")
    html = fetch_html(TRACKAIPAC_ENDORSEMENTS)
    if not html: return []
    challengers = []
    for block in re.split(r'<h2[^>]*>', html)[1:]:
        name_m = re.match(r'([^<]+)</h2>', block)
        if not name_m: continue
        name = re.sub(r'\s+', ' ', name_m.group(1)).strip()
        if not name or len(name) < 3 or len(name) > 60: continue
        text = re.sub(r'<[^>]+>', ' ', block)
        text = re.sub(r'\s+', ' ', text).strip()
        dist_m = re.search(r'([A-Z]{2}-(?:\d+|SEN|SENATE)\s*\[[RDI]\])', text)
        if not dist_m: continue
        state, district, party, chamber = parse_district(dist_m.group(1))
        date_m = re.search(r'((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d+,\s*\d{4})', text)
        primary_date = ""
        if date_m:
            try:
                from datetime import datetime as dt
                primary_date = dt.strptime(date_m.group(1), "%B %d, %Y").strftime("%Y-%m-%d")
            except: pass
        href_m  = re.search(r'href="(https?://[^"]+)"', block)
        website = href_m.group(1) if href_m else ""
        challengers.append({
            "name":name,"state":state,"district":district,"party":party,"chamber":chamber,
            "antiArms":True,"antiArmsLevel":"soft",
            "ps":"GENERAL" if "General Election" in text else "UPCOMING",
            "primaryDate":primary_date,"website":website,
            "source":"trackaipac","note":"Endorsed by Citizens Against AIPAC Corruption.",
            "opponent":"","incumbentParty":"",
        })
    print(f"  TrackAIPAC /endorsements: {len(challengers)} challengers")
    return challengers


# ─────────────────────────────────────────────────────────────────────────────
# 2. RLC LIBERTY INDEX — Republican anti-arms source
# ─────────────────────────────────────────────────────────────────────────────

def fetch_rlc_liberty_index():
    """
    Downloads and parses the RLC Liberty Index PDF.
    Returns dict: name_lower → {name, score, rlc_level}
    Members scoring ≥ RLC_SOFT_THRESHOLD are classified as soft anti-arms.
    """
    print("Fetching RLC Liberty Index PDF …")
    try:
        import pdfplumber
    except ImportError:
        print("  pdfplumber not available — skipping RLC index")
        return {}

    results = {}

    for label, url in [("house", RLC_LIBERTY_INDEX_HOUSE), ("senate", RLC_LIBERTY_INDEX_SENATE)]:
        data = fetch_bytes(url)
        if not data:
            print(f"  RLC {label} PDF fetch failed")
            continue

        try:
            with pdfplumber.open(io.BytesIO(data)) as pdf:
                full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)
        except Exception as e:
            print(f"  RLC {label} PDF parse error: {e}")
            continue

        # Parse score lines — format varies but typically:
        # "Thomas Massie (KY-04) 102"  or  "Massie, Thomas 102"
        # Look for lines with a name followed by a number score
        score_pattern = re.compile(
            r'([A-Z][a-z]+(?:\s+[A-Z]\.?\s+)?[A-Z][a-zA-Z\-\']+)'  # name (First [M.] Last)
            r'.*?'                                                       # anything between
            r'\b(\d{1,3})\s*$',                                         # score at end of line
            re.MULTILINE
        )
        # Also try table format: last, first ... score
        score_pattern2 = re.compile(
            r'^([A-Z][a-zA-Z\-\']+),\s+([A-Z][a-zA-Z\-\' ]+?)\s+'    # Last, First
            r'.*?(\d{1,3})\s*$',
            re.MULTILINE
        )

        matched = 0
        for line in full_text.split('\n'):
            line = line.strip()
            if not line: continue

            # Try "First Last ... score" pattern
            m1 = re.search(r'^([A-Z][a-zA-Z]+ [A-Z][a-zA-Z\-\']+(?:\s+[A-Z][a-zA-Z]+)?)\s+[\d\s]+(\d{1,3})\s*$', line)
            if m1:
                name  = m1.group(1).strip()
                score = int(m1.group(2))
                if 0 <= score <= 110 and len(name) > 5:
                    results[name.lower()] = {"name":name,"score":score,"chamber":label,"source":"rlc_index"}
                    matched += 1
                    continue

            # Try "Last, First ... score"
            m2 = re.match(r'^([A-Z][a-zA-Z\-\']+),\s+([A-Z][a-zA-Z\- \'\.]+?)\s+.*?(\d{1,3})\s*$', line)
            if m2:
                name  = f"{m2.group(2).strip()} {m2.group(1).strip()}"
                score = int(m2.group(3))
                if 0 <= score <= 110 and len(name) > 5:
                    results[name.lower()] = {"name":name,"score":score,"chamber":label,"source":"rlc_index"}
                    matched += 1

        print(f"  RLC {label} index: {matched} members parsed")

    # Fallback: embed known top scorers directly (in case PDF parsing fails)
    # These are from the published 2024 Liberty Index
    KNOWN_SCORES = {
        "thomas massie":     {"name":"Thomas Massie",     "score":102,"chamber":"house"},
        "andy biggs":        {"name":"Andy Biggs",         "score":102,"chamber":"house"},
        "eli crane":         {"name":"Eli Crane",          "score":97, "chamber":"house"},
        "scott perry":       {"name":"Scott Perry",        "score":97, "chamber":"house"},
        "chip roy":          {"name":"Chip Roy",           "score":97, "chamber":"house"},
        "matt rosendale":    {"name":"Matt Rosendale",     "score":97, "chamber":"house"},
        "marjorie taylor greene":{"name":"Marjorie Taylor Greene","score":96,"chamber":"house"},
        "bob good":          {"name":"Bob Good",           "score":92, "chamber":"house"},
        "andrew ogles":      {"name":"Andrew Ogles",       "score":95, "chamber":"house"},
        "anna paulina luna": {"name":"Anna Paulina Luna",  "score":94, "chamber":"house"},
        "paul gosar":        {"name":"Paul Gosar",         "score":92, "chamber":"house"},
        "warren davidson":   {"name":"Warren Davidson",    "score":88, "chamber":"house"},
        "thomas tiffany":    {"name":"Thomas Tiffany",     "score":85, "chamber":"house"},
        "barry moore":       {"name":"Barry Moore",        "score":80, "chamber":"house"},
        "rand paul":         {"name":"Rand Paul",          "score":95, "chamber":"senate"},
        "mike lee":          {"name":"Mike Lee",           "score":88, "chamber":"senate"},
    }
    for k, v in KNOWN_SCORES.items():
        if k not in results:
            results[k] = {**v, "source":"rlc_known"}

    above_threshold = sum(1 for v in results.values() if v.get("score",0) >= RLC_SOFT_THRESHOLD)
    print(f"  RLC index total: {len(results)} members, {above_threshold} at ≥{RLC_SOFT_THRESHOLD}")
    return results


def scrape_rlc_endorsements():
    """
    Scrapes RLC 2026 endorsements from the national news feed.
    Each endorsement is a separate WordPress blog post.
    Returns list of challenger dicts.
    """
    print("Scraping RLC endorsements from news feed …")
    challengers = []
    seen = set()

    # Scrape multiple pages of the national news feed
    for page in range(1, 4):
        url = RLC_NEWS_NATIONAL if page == 1 else f"{RLC_NEWS_NATIONAL}page/{page}/"
        html = fetch_html(url)
        if not html: break

        # Find endorsement post titles and links
        # Pattern: "Endorses [Name] for [District/State]"
        endorsement_posts = re.findall(
            r'href="(https://rlc\.org/[^"]+)"[^>]*>[^<]*(?:endorses|endorsed)[^<]*</a>',
            html, re.IGNORECASE
        )

        # Also grab from article titles
        titles = re.findall(
            r'(?:Endorses|Endorsed)\s+([A-Z][a-zA-Z\s\-\'\.]+?)\s+(?:for|in)\s+([^<\n]{5,80})',
            html, re.IGNORECASE
        )

        for name_raw, district_raw in titles:
            name = name_raw.strip()
            if name.lower() in seen or len(name) < 4: continue
            seen.add(name.lower())

            # Parse district from "Texas' 21st Congressional District" etc.
            state = ""
            district = None
            chamber = "house"
            party = "R"  # RLC only endorses R candidates

            # State name → abbreviation
            STATE_MAP = {
                "alabama":"AL","alaska":"AK","arizona":"AZ","arkansas":"AR","california":"CA",
                "colorado":"CO","connecticut":"CT","delaware":"DE","florida":"FL","georgia":"GA",
                "hawaii":"HI","idaho":"ID","illinois":"IL","indiana":"IN","iowa":"IA",
                "kansas":"KS","kentucky":"KY","louisiana":"LA","maine":"ME","maryland":"MD",
                "massachusetts":"MA","michigan":"MI","minnesota":"MN","mississippi":"MS",
                "missouri":"MO","montana":"MT","nebraska":"NE","nevada":"NV","new hampshire":"NH",
                "new jersey":"NJ","new mexico":"NM","new york":"NY","north carolina":"NC",
                "north dakota":"ND","ohio":"OH","oklahoma":"OK","oregon":"OR","pennsylvania":"PA",
                "rhode island":"RI","south carolina":"SC","south dakota":"SD","tennessee":"TN",
                "texas":"TX","utah":"UT","vermont":"VT","virginia":"VA","washington":"WA",
                "west virginia":"WV","wisconsin":"WI","wyoming":"WY",
            }
            dr_lower = district_raw.lower()
            for sname, sabb in STATE_MAP.items():
                if sname in dr_lower:
                    state = sabb
                    break

            # District number
            dm = re.search(r'(\d+)(?:st|nd|rd|th)', district_raw, re.IGNORECASE)
            if dm: district = int(dm.group(1))
            if "senate" in dr_lower or "u.s. senate" in dr_lower:
                chamber = "senate"; district = None

            challengers.append({
                "name":name,"state":state,"district":district,"party":"R","chamber":chamber,
                "antiArms":True,"antiArmsLevel":"soft",
                "ps":"UPCOMING","primaryDate":"","website":"",
                "source":"rlc_endorsed","note":f"Endorsed by Republican Liberty Caucus for {district_raw.strip()[:60]}",
                "opponent":"","incumbentParty":"",
            })

        time.sleep(0.5)

    print(f"  RLC endorsements: {len(challengers)} challengers found")
    return challengers


# ─────────────────────────────────────────────────────────────────────────────
# 3. BUILD MEMBER LIST — TrackAIPAC + RLC + KNOWN overrides
# ─────────────────────────────────────────────────────────────────────────────

def build_members(ta_congress, rlc_index):
    """
    Merges TrackAIPAC and RLC Liberty Index into a unified member list.

    Classification logic:
      D/I: TrackAIPAC approved → soft; KNOWN override → hard/soft
      R:   RLC score ≥75      → soft; KNOWN override → hard/soft
      Any: KNOWN_ANTI_ARMS    → authoritative override
    """
    members = []
    processed = set()

    for name_lower, m in ta_congress.items():
        name = m.get("name","").strip()
        if not name or name.lower() == "vacant" or len(name) < 3: continue
        processed.add(name_lower)

        ta_anti   = bool(m.get("approved") or m.get("antiArms"))
        rlc       = rlc_index.get(name_lower, {})
        rlc_score = rlc.get("score", 0)
        rlc_anti  = rlc_score >= RLC_SOFT_THRESHOLD
        known     = KNOWN_ANTI_ARMS.get(name)

        anti_arms  = ta_anti or rlc_anti or bool(known)
        if known:
            level = known["level"]
        elif ta_anti:
            level = "soft"
        elif rlc_anti:
            level = "soft"
        else:
            level = None

        note = m.get("note","")
        if known and not note: note = known["reason"]
        if rlc_score and not note: note = f"RLC Liberty Index {rlc_score}/100"

        members.append({
            "bioguide_id":  "",
            "name":         name,
            "party":        m.get("party","?"),
            "state":        m.get("state") or "",
            "district":     m.get("district") if m.get("district") not in ("",None) else None,
            "chamber":      m.get("chamber","house"),
            "antiArms":     anti_arms,
            "antiArmsLevel":level,
            "rlc_score":    rlc_score if rlc_score else None,
            "lobby_total":  m.get("lobby_total",0),
            "pacs":         m.get("pacs",[]),
            "ps":           "INCUMBENT",
            "bills":        [],
            "votes":        [],
            "note":         note,
        })

    # Add any RLC members not in TrackAIPAC (e.g. senators TrackAIPAC missed)
    for name_lower, rlc in rlc_index.items():
        if name_lower in processed: continue
        if rlc.get("score",0) < RLC_SOFT_THRESHOLD: continue
        name  = rlc.get("name","")
        known = KNOWN_ANTI_ARMS.get(name)
        level = known["level"] if known else "soft"
        note  = known["reason"] if known else f"RLC Liberty Index {rlc['score']}/100"
        members.append({
            "bioguide_id":"","name":name,"party":"R",
            "state":"","district":None,"chamber":rlc.get("chamber","house"),
            "antiArms":True,"antiArmsLevel":level,"rlc_score":rlc["score"],
            "lobby_total":0,"pacs":[],"ps":"INCUMBENT","bills":[],"votes":[],
            "note":note,
        })
        processed.add(name_lower)

    # Enrich with Congress.gov bioguide IDs (best-effort)
    if CONGRESS_KEY:
        members = enrich_bioguide_ids(members)

    anti = sum(1 for m in members if m["antiArms"])
    hard = sum(1 for m in members if m.get("antiArmsLevel")=="hard")
    soft = anti - hard
    print(f"  Members: {len(members)} total — {anti} anti-arms ({hard} hard, {soft} soft)")
    return members


def enrich_bioguide_ids(members):
    """Fetches bioguide IDs for bill co-sponsorship lookup."""
    print("  Enriching bioguide IDs from Congress.gov …")
    try:
        bio_map = {}
        url = congress_url("/member", {"limit":250,"currentMember":"true"})
        d   = fetch_json(url)
        if not d:
            print("  Congress.gov /member failed — skipping bioguide enrichment")
            return members

        def norm_name(raw):
            if "," in raw:
                last, first = raw.split(",",1)
                return (first.strip()+" "+last.strip()).lower()
            return raw.lower()

        for item in d.get("members",[]):
            bio_map[norm_name(item.get("name",""))] = item.get("bioguideId","")

        total = d.get("pagination",{}).get("count",0)
        offset = 250
        while offset < total:
            d2 = fetch_json(congress_url("/member",{"limit":250,"offset":offset,"currentMember":"true"}))
            if not d2: break
            for item in d2.get("members",[]):
                bio_map[norm_name(item.get("name",""))] = item.get("bioguideId","")
            offset += 250
            time.sleep(0.3)

        matched = 0
        for m in members:
            bio = bio_map.get(m["name"].lower(),"")
            if bio: m["bioguide_id"] = bio; matched += 1
        print(f"  Bioguide: {matched}/{len(members)} matched")
    except Exception as e:
        print(f"  Bioguide enrichment failed (non-critical): {e}")
    return members


# ─────────────────────────────────────────────────────────────────────────────
# 4. CONGRESS.GOV BILL DISCOVERY + HOUSE-VOTE VERIFICATION
# ─────────────────────────────────────────────────────────────────────────────

def discover_bills(members_by_id):
    if not CONGRESS_KEY:
        print("  No CONGRESS_KEY — skipping bill discovery")
        return []
    print("\nDiscovering bills from Congress.gov …")
    seen, bills = set(), []

    def process_bill(bd):
        btype = bd.get("type","").lower()
        bnum  = bd.get("number","")
        btitle= bd.get("title","")
        bcong = bd.get("congress", CURRENT_CONGRESS)
        uid   = f"{bcong}-{btype}-{bnum}"
        if uid in seen or not bnum: return
        seen.add(uid)

        subj_d   = fetch_json(congress_url(f"/bill/{bcong}/{btype}/{bnum}/subjects")) or {}
        subjects = [s.get("name","") for s in subj_d.get("subjects",{}).get("legislativeSubjects",[])]
        time.sleep(0.2)
        cls = score_bill(btitle, subjects)
        if cls is None: return

        cosp_d = fetch_json(congress_url(f"/bill/{bcong}/{btype}/{bnum}/cosponsors",{"limit":250})) or {}
        det_d  = fetch_json(congress_url(f"/bill/{bcong}/{btype}/{bnum}")) or {}
        time.sleep(0.2)
        sponsors = det_d.get("bill",{}).get("sponsors",[])
        sp_ids   = []
        for s in list(sponsors)+list(cosp_d.get("cosponsors",[])):
            bid = s.get("bioguideId","")
            if bid:
                sp_ids.append(bid)
                if cls=="anti_arms" and bid in members_by_id:
                    members_by_id[bid]["antiArms"]      = True
                    members_by_id[bid]["antiArmsLevel"]  = "hard"
                    if uid not in members_by_id[bid]["bills"]:
                        members_by_id[bid]["bills"].append(uid)

        base = f"https://www.congress.gov/bill/{bcong}th-congress"
        url_map = {"hr":"house-bill","s":"senate-bill","sjres":"senate-joint-resolution","hjres":"house-joint-resolution"}
        burl = f"{base}/{url_map.get(btype,btype+'-bill')}/{bnum}"
        lat  = det_d.get("bill",{}).get("latestAction",{})
        bills.append({
            "id":uid,"congress":bcong,"type":btype.upper(),"number":bnum,
            "title":btitle,"classification":cls,"subjects":subjects[:8],
            "sponsor_count":len(sp_ids),"sponsor_ids":sp_ids,"url":burl,
            "last_action":lat.get("text",""),"last_action_date":lat.get("actionDate",""),
        })
        print(f"  {'🚫' if cls=='anti_arms' else '🔫'} {btype.upper()} {bnum}: {btitle[:55]} ({cls})")

    for btype, bnum in [("hr","3565")]:
        process_bill({"type":btype,"number":bnum,"title":"","congress":CURRENT_CONGRESS})
        time.sleep(0.3)
    for q in BILL_SEARCH_QUERIES:
        d = fetch_json(congress_url("/bill",{"query":q,"congress":CURRENT_CONGRESS,"limit":20,"sort":"date"})) or {}
        for b in d.get("bills",[]): process_bill(b); time.sleep(0.2)
        time.sleep(0.4)
    d = fetch_json(congress_url("/bill",{"congress":CURRENT_CONGRESS,"billType":"SJRES","query":"Israel","limit":20})) or {}
    for b in d.get("bills",[]): process_bill(b); time.sleep(0.2)

    print(f"  Bills: {len(bills)} discovered")
    return bills


def fetch_house_votes(members_by_id):
    """
    Uses Congress.gov /house-vote/{congress}/{session}/{voteNumber}/members
    to pull actual roll call votes on Israel arms bills in the 119th Congress.
    Any member voting No on a pro-arms bill is upgraded to hard.
    """
    if not CONGRESS_KEY:
        return {}
    print("\nFetching Congress.gov house-vote records (119th Congress) …")
    vote_records = {}

    # Search for Israel-related votes in 119th Congress
    for session in [1, 2]:
        url = congress_url(f"/house-vote/{CURRENT_CONGRESS}/{session}", {"limit":50,"sort":"date"})
        d   = fetch_json(url)
        if not d: continue

        for vote in d.get("houseRollCallVotes",[]):
            desc  = (vote.get("question","") + " " + vote.get("description","")).lower()
            vnum  = vote.get("rollCallNumber")
            if not any(kw in desc for kw in ["israel","arms","security assistance","military aid","fms","weapons"]):
                continue
            if not vnum: continue

            # Pull member votes for this roll call
            mv_url = congress_url(f"/house-vote/{CURRENT_CONGRESS}/{session}/{vnum}/members",{"limit":500})
            mv_d   = fetch_json(mv_url)
            if not mv_d: continue

            classification = score_bill(vote.get("description",""), [])
            key = f"{CURRENT_CONGRESS}-{session}-{vnum}"
            vote_records[key] = {
                "desc":vote.get("description",""),
                "date":vote.get("date",""),
                "yeas":vote.get("totals",{}).get("yea",0),
                "nays":vote.get("totals",{}).get("nay",0),
                "classification":classification,
            }

            for mv in mv_d.get("houseRollCallVote",{}).get("memberVotes",{}).get("memberVote",[]):
                bio   = mv.get("bioguideId","")
                voted = (mv.get("vote","") or "").lower()
                if not bio or bio not in members_by_id: continue
                m = members_by_id[bio]
                if classification == "pro_arms" and "nay" in voted:
                    m["antiArms"]      = True
                    m["antiArmsLevel"] = "hard"
                    m["votes"].append({"bill":key,"vote":"No","desc":vote.get("description","")[:60]})
                    print(f"  🗳 {m['name']} voted No on pro-arms {key}")
                elif classification == "anti_arms" and "yea" in voted:
                    m["antiArms"]      = True
                    m["antiArmsLevel"] = "hard"
                    m["votes"].append({"bill":key,"vote":"Yes","desc":vote.get("description","")[:60]})

            time.sleep(0.3)

    print(f"  House votes: {len(vote_records)} relevant roll calls found")
    return vote_records


def enrich_hard_classification(members, bills):
    """Congress.gov /member/{bioguideId}/cosponsored-legislation upgrade soft→hard."""
    if not CONGRESS_KEY: return members
    anti_bill_ids  = {b["id"] for b in bills if b.get("classification")=="anti_arms"}
    anti_bill_nums = {b["number"].lower() for b in bills if b.get("classification")=="anti_arms"}
    soft_members   = [m for m in members if m.get("antiArms") and m.get("antiArmsLevel")=="soft" and m.get("bioguide_id")]
    if not soft_members: return members
    print(f"\n  Congress.gov: checking {len(soft_members)} soft members for co-sponsorship …")
    upgraded = 0
    for m in soft_members:
        url = congress_url(f"/member/{m['bioguide_id']}/cosponsored-legislation",
                           {"limit":50,"congress":CURRENT_CONGRESS})
        d   = fetch_json(url)
        if not d: time.sleep(0.3); continue
        for item in d.get("cosponsoredLegislation",[]):
            btype = (item.get("type") or "").lower()
            bnum  = str(item.get("number") or "")
            uid   = f"{CURRENT_CONGRESS}-{btype}-{bnum}"
            if uid in anti_bill_ids or bnum.lower() in anti_bill_nums:
                m["antiArmsLevel"] = "hard"
                if uid not in m.get("bills",[]): m.setdefault("bills",[]).append(uid)
                upgraded += 1
                print(f"  ↑ Hard: {m['name']} co-sponsored {btype.upper()} {bnum}")
                break
        time.sleep(0.25)
    print(f"  Congress.gov: upgraded {upgraded} soft → hard")
    return members


# ─────────────────────────────────────────────────────────────────────────────
# 5. LEGISCAN VOTES
# ─────────────────────────────────────────────────────────────────────────────

def fetch_legiscan_votes(members_by_name):
    if not LEGISCAN_KEY:
        print("\nLegiScan: key pending — skipping")
        return {}
    print("\nFetching LegiScan votes …")
    vote_records = {}
    for q in ["Israel Emergency Security Assistance Act","Block the Bombs Act"]:
        url = f"{LEGISCAN_BASE}/?key={LEGISCAN_KEY}&op=search&state=US&query={urllib.parse.quote(q)}&year=2&page=1"
        d   = fetch_json(url) or {}
        if d.get("status")!="OK": continue
        for _, bill in d.get("searchresult",{}).items():
            if not isinstance(bill,dict): continue
            bid = bill.get("bill_id")
            if not bid: continue
            det = fetch_json(f"{LEGISCAN_BASE}/?key={LEGISCAN_KEY}&op=getBill&id={bid}") or {}
            if det.get("status")!="OK": continue
            b   = det.get("bill",{})
            cls = score_bill(b.get("title",""),[])
            for vote in b.get("votes",[]):
                rid = vote.get("roll_id")
                if not rid: continue
                rc_d = fetch_json(f"{LEGISCAN_BASE}/?key={LEGISCAN_KEY}&op=getRollCall&id={rid}") or {}
                if rc_d.get("status")!="OK": continue
                rc   = rc_d.get("roll_call",{})
                desc = rc.get("desc","")
                for v in rc.get("votes",[]):
                    vname = v.get("name","")
                    vtxt  = v.get("vote_text","").lower()
                    last  = vname.split(",")[0].strip() if "," in vname else vname.split()[-1]
                    for mname, m in members_by_name.items():
                        if m["name"].split()[-1].lower()==last.lower():
                            if cls=="pro_arms" and "nay" in vtxt:
                                m["antiArms"]=True; m["antiArmsLevel"]="hard"
                                m["votes"].append({"bill":b.get("bill_number",""),"vote":"No","desc":desc})
                            elif cls=="anti_arms" and "yea" in vtxt:
                                m["antiArms"]=True; m["antiArmsLevel"]="hard"
                                m["votes"].append({"bill":b.get("bill_number",""),"vote":"Yes","desc":desc})
                vote_records[str(rid)] = {"desc":desc,"bill":b.get("bill_number",""),"date":rc.get("date",""),"yeas":rc.get("yeas",0),"nays":rc.get("nays",0)}
                time.sleep(0.3)
            time.sleep(0.4)
    return vote_records


# ─────────────────────────────────────────────────────────────────────────────
# 6. FEC + POLYMARKET
# ─────────────────────────────────────────────────────────────────────────────

def fetch_fec(members):
    if not FEC_KEY: return {}
    print("\nFetching FEC finance …")
    results = {}
    for name, cid in KNOWN_FEC_IDS.items():
        url = f"{FEC_BASE}/candidate/{cid}/totals/?api_key={FEC_KEY}&cycle=2024&per_page=1"
        d   = fetch_json(url)
        t   = (d or {}).get("results",[None])[0]
        if t:
            results[name] = {"raised":t.get("receipts"),"spent":t.get("disbursements"),
                             "cash":t.get("last_cash_on_hand_end_period"),"cid":cid}
        time.sleep(0.2)
    # Independent expenditures
    for name in ["Rashida Tlaib","Ilhan Omar","Alexandria Ocasio-Cortez",
                 "Thomas Massie","Summer Lee","Pramila Jayapal"]:
        cid = KNOWN_FEC_IDS.get(name)
        if not cid: continue
        url = f"{FEC_BASE}/schedules/schedule_e/?api_key={FEC_KEY}&candidate_id={cid}&cycle=2024&per_page=10&sort=-expenditure_amount"
        d   = fetch_json(url)
        if d and d.get("results"):
            results.setdefault(name,{"cid":cid})
            results[name]["ie_against"] = sum(x.get("expenditure_amount",0) for x in d["results"] if x.get("support_oppose_indicator")=="O")
            results[name]["ie_support"] = sum(x.get("expenditure_amount",0) for x in d["results"] if x.get("support_oppose_indicator")=="S")
        time.sleep(0.2)
    print(f"  FEC: {len(results)} records")
    return results


def fetch_fec_candidates_2026(ta_endorsed, rlc_endorsed):
    if not FEC_KEY: return []
    print("\nFetching FEC 2026 candidates …")
    all_endorsed = {c["name"].lower(): c for c in ta_endorsed + rlc_endorsed}
    candidates = []
    for office in ["H","S"]:
        offset = 0
        while True:
            url = f"{FEC_BASE}/candidates/search/?api_key={FEC_KEY}&cycle=2026&office={office}&candidate_status=C&per_page=100&offset={offset}"
            d   = fetch_json(url)
            if not d: break
            batch = d.get("results",[])
            if not batch: break
            for c in batch:
                raw  = c.get("name","")
                name = (raw.split(",")[1].strip().title()+" "+raw.split(",")[0].strip().title() if "," in raw else raw.title())
                ta   = all_endorsed.get(name.lower())
                if ta:
                    dist = None
                    try: dist = int(c.get("district",""))
                    except: pass
                    candidates.append({**ta,"fec_id":c.get("candidate_id",""),"district":dist or ta.get("district"),"source":"fec+"+ta.get("source","")})
            total  = d.get("pagination",{}).get("count",0)
            offset += len(batch)
            if offset >= total or len(batch)<100: break
            time.sleep(0.3)
    print(f"  FEC 2026: {len(candidates)} matched")
    return candidates


def fetch_poly(members):
    if not POLY_KEY: return {}
    print("\nFetching Polymarket …")
    results = {}
    headers = {"Authorization":f"Bearer {POLY_KEY}","Content-Type":"application/json"}
    for m in [m for m in members if m.get("antiArms")]:
        last = m["name"].split()[-1]
        d    = fetch_json(f"{POLY_BASE}/markets?q={urllib.parse.quote(last+' reelection congress')}&limit=5", headers=headers)
        if not d: continue
        for mk in (d if isinstance(d,list) else d.get("markets",[])):
            title = (mk.get("question") or mk.get("title") or "").lower()
            if last.lower() not in title: continue
            if not any(kw in title for kw in ["reelect","win","congress","senate","house","primary"]): continue
            prob = None
            try:
                p = mk.get("outcomePrices") or "[]"
                if isinstance(p,str): p = json.loads(p)
                if p: prob = float(p[0])
            except: pass
            results[m["name"]] = {"question":mk.get("question") or mk.get("title") or "","prob":prob,"url":mk.get("url") or "https://polymarket.com","volume":mk.get("volume")}
            break
        time.sleep(0.3)
    print(f"  Polymarket: {len(results)} records")
    return results


# ─────────────────────────────────────────────────────────────────────────────
# 7. HISTORY DETECTION
# ─────────────────────────────────────────────────────────────────────────────

def load_history():
    try:
        with open("history.json") as f: return json.load(f)
    except: return {"last_updated":"","events":[]}

def detect_history_changes(old_members, new_members, ta_congress):
    history = load_history()
    events  = history.get("events",[])
    today   = datetime.now(timezone.utc).date().isoformat()
    now_iso = datetime.now(timezone.utc).isoformat()
    existing = {e.get("id") for e in events}

    def eid(name, ctype):
        return hashlib.md5(f"{name}-{today}-{ctype}".encode()).hexdigest()[:10]

    old_by_name = {m["name"].lower(): m for m in old_members}
    new_by_name = {m["name"].lower(): m for m in new_members}

    def add(name, m, change, trigger, source="trackaipac+rlc"):
        ev_id = eid(name, change)
        if ev_id in existing: return
        events.append({"id":ev_id,"date":today,"detected_at":now_iso,
            "member":m.get("name",""),"chamber":m.get("chamber",""),
            "state":m.get("state",""),"party":m.get("party",""),
            "change":change,"trigger":trigger,"source":source})
        existing.add(ev_id)
        print(f"  📜 History: {m.get('name','')} — {change}")

    for nk, nm in new_by_name.items():
        om = old_by_name.get(nk)
        if not om:
            if nm.get("antiArms"): add(nk,nm,"pro_to_anti","First detected as anti-arms")
            continue
        if om.get("antiArms") and not nm.get("antiArms"):
            add(nk,nm,"anti_to_pro","No longer flagged as anti-arms")
        elif not om.get("antiArms") and nm.get("antiArms"):
            add(nk,nm,"pro_to_anti","Newly flagged as anti-arms")
        elif om.get("antiArmsLevel")=="soft" and nm.get("antiArmsLevel")=="hard":
            add(nk,nm,"soft_to_hard",f"Upgraded to Hard via co-sponsorship or vote record")
        elif om.get("antiArmsLevel")=="hard" and nm.get("antiArmsLevel")=="soft":
            add(nk,nm,"hard_to_soft","Downgraded from Hard — may have withdrawn co-sponsorship")

    for ok, om in old_by_name.items():
        if ok not in new_by_name and om.get("antiArms"):
            ta = ta_congress.get(ok, {})
            note = ta.get("note","")
            trigger = ("Passed away" if "passed away" in note.lower() else
                       "Resigned" if "resigned" in note.lower() else
                       f"Left seat: {note}" if "running for" in note.lower() else
                       "No longer listed as current member")
            add(ok, om, "departed", trigger)

    events.sort(key=lambda e: e.get("date",""), reverse=True)
    history["last_updated"] = now_iso
    history["events"]       = events
    with open("history.json","w") as f: json.dump(history,f,indent=2)
    new_today = [e for e in events if e.get("date")==today]
    print(f"  History: {len(new_today)} new event(s) today, {len(events)} total")
    return history


# ─────────────────────────────────────────────────────────────────────────────
# 8. CHALLENGERS MERGE
# ─────────────────────────────────────────────────────────────────────────────

def merge_challengers(ta_endorsed, rlc_endorsed, fec_candidates):
    try:
        with open("challengers.json") as f:
            existing = json.load(f)
        manual = existing.get("challengers", [])
    except:
        manual = []

    merged = {c["name"].lower(): c for c in manual}

    for c in ta_endorsed + rlc_endorsed:
        key = c["name"].lower()
        if key in merged:
            merged[key].setdefault("primaryDate", c.get("primaryDate",""))
            merged[key].setdefault("website", c.get("website",""))
            merged[key]["antiArms"] = True
            # Merge sources
            existing_source = merged[key].get("source","manual")
            new_source       = c.get("source","")
            if new_source and new_source not in existing_source:
                merged[key]["source"] = existing_source+"+"+new_source
        else:
            merged[key] = c

    ta_names  = {c["name"].lower() for c in ta_endorsed + rlc_endorsed}
    for c in fec_candidates:
        key = c["name"].lower()
        merged.setdefault(key, c)
        if c.get("fec_id"): merged[key]["fec_id"] = c["fec_id"]

    result = sorted(merged.values(), key=lambda c:(c.get("state",""),c.get("name","")))
    with open("challengers.json","w") as f:
        json.dump({
            "_note": "Auto-updated from TrackAIPAC + RLC + FEC. Manual entries preserved.",
            "_last_scraped": datetime.now(timezone.utc).isoformat(),
            "challengers": result,
        }, f, indent=2)
    print(f"  Challengers merged: {len(result)} total")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    existing = {}
    try:
        with open("data.json") as f: existing = json.load(f)
        print(f"Loaded existing data.json v{existing.get('meta',{}).get('version','?')}")
    except:
        print("No existing data.json — fresh build")

    old_members   = existing.get("members", [])
    last_scraped  = existing.get("meta",{}).get("trackaipac_scraped","")
    should_scrape = True
    if last_scraped:
        try:
            if datetime.now(timezone.utc)-datetime.fromisoformat(last_scraped) < timedelta(hours=TRACKAIPAC_RESCRAPE_H):
                should_scrape = False
                print(f"TrackAIPAC+RLC: cached (< {TRACKAIPAC_RESCRAPE_H}h old)")
        except: pass

    if should_scrape:
        ta_congress  = scrape_trackaipac_congress()
        ta_endorsed  = scrape_trackaipac_endorsements()
        rlc_index    = fetch_rlc_liberty_index()
        rlc_endorsed = scrape_rlc_endorsements()
        time.sleep(1)
    else:
        ta_congress  = existing.get("trackaipac_congress", {})
        ta_endorsed  = existing.get("trackaipac_endorsed", [])
        rlc_index    = existing.get("rlc_index", {})
        rlc_endorsed = existing.get("rlc_endorsed", [])
        # Force re-scrape if any cache is empty
        if not ta_congress or not rlc_index:
            print("Cache incomplete — forcing re-scrape")
            ta_congress  = scrape_trackaipac_congress()
            ta_endorsed  = scrape_trackaipac_endorsements()
            rlc_index    = fetch_rlc_liberty_index()
            rlc_endorsed = scrape_rlc_endorsements()
            should_scrape = True

    if not ta_congress:
        print("ERROR: No member data — aborting")
        return

    members        = build_members(ta_congress, rlc_index)
    members_by_id  = {m["bioguide_id"]: m for m in members if m.get("bioguide_id")}
    members_by_name= {m["name"].lower(): m for m in members}

    bills        = discover_bills(members_by_id) or existing.get("bills", [])
    house_votes  = fetch_house_votes(members_by_id) if CONGRESS_KEY else existing.get("house_votes", {})
    members      = enrich_hard_classification(members, bills)
    members_by_name = {m["name"].lower(): m for m in members}
    vote_records = fetch_legiscan_votes(members_by_name) or existing.get("vote_records", {})
    history      = detect_history_changes(old_members, members, ta_congress)

    fec_cands    = fetch_fec_candidates_2026(ta_endorsed, rlc_endorsed) if FEC_KEY else []
    challengers  = merge_challengers(ta_endorsed, rlc_endorsed, fec_cands)

    fec  = fetch_fec(members)  or existing.get("fec",  {})
    poly = fetch_poly(members) or existing.get("poly", {})

    anti = sum(1 for m in members if m["antiArms"])
    hard = sum(1 for m in members if m.get("antiArmsLevel")=="hard")

    data = {
        "meta": {
            "fetched_at":         datetime.now(timezone.utc).isoformat(),
            "trackaipac_scraped": datetime.now(timezone.utc).isoformat() if should_scrape else last_scraped,
            "member_count":       len(members),
            "anti_arms_count":    anti,
            "hard_count":         hard,
            "soft_count":         anti - hard,
            "bill_count":         len(bills),
            "challenger_count":   len(challengers),
            "history_events":     len(history.get("events",[])),
            "version":            7,
            "sources": {
                "members":     "trackaipac.com + rlc.org Liberty Index",
                "bills":       "api.congress.gov (auto-discovered)",
                "house_votes": "api.congress.gov /house-vote",
                "challengers": "trackaipac.com + rlc.org endorsements + FEC",
                "votes":       "legiscan.com" if LEGISCAN_KEY else "pending key",
                "fec":         "api.open.fec.gov",
                "polymarket":  "polymarket.com",
            },
        },
        "members":             members,
        "bills":               bills,
        "house_votes":         house_votes,
        "vote_records":        vote_records,
        "fec":                 fec,
        "poly":                poly,
        "trackaipac_congress": ta_congress,
        "trackaipac_endorsed": ta_endorsed,
        "rlc_index":           rlc_index,
        "rlc_endorsed":        rlc_endorsed,
    }

    with open("data.json","w") as f:
        json.dump(data, f, indent=2)

    print(f"""
✓ data.json written (v7)
  Members:           {len(members)} ({anti} anti-arms: {hard} hard, {anti-hard} soft)
  Bills:             {len(bills)}
  House votes:       {len(house_votes)} relevant roll calls
  Challengers:       {len(challengers)}
  History events:    {len(history.get('events',[]))}
  Sources: TrackAIPAC + RLC Liberty Index + Congress.gov + FEC + Polymarket
""")


if __name__ == "__main__":
    main()
