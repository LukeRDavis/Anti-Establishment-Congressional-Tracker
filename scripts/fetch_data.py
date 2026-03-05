#!/usr/bin/env python3
"""
fetch_data.py — GitHub Actions, every 4 hours.

Member source:  TrackAIPAC (scrapes all 533 members with antiArms classification)
Bill source:    Congress.gov API (auto-discovers relevant legislation)
Finance:        OpenFEC API
Odds:           Polymarket API
Votes:          LegiScan API (activates when LEGISCAN_KEY set)
Challengers:    TrackAIPAC /endorsements + FEC 2026 candidates
History:        Detects antiArms status changes between runs

GitHub Secrets:
  CONGRESS_KEY   api.congress.gov
  FEC_KEY        api.open.fec.gov
  POLY_KEY       polymarket.com
  LEGISCAN_KEY   legiscan.com (optional, add when received)
"""

import os, json, re, time, hashlib, urllib.request, urllib.parse, urllib.error
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
TRACKAIPAC_RESCRAPE_H   = 24

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
            if e.code == 429:
                time.sleep(10)
            elif e.code in (401, 403):
                print(f"  Auth error — check API key")
                return None
            else:
                return None
        except Exception as e:
            if attempt < retries:
                time.sleep(2)
            else:
                print(f"  WARN: {url[:80]} → {e}")
                return None
    return None

def fetch_html(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; AntiArmsTracker/2.0)",
        "Accept": "text/html,application/xhtml+xml",
    }
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=25) as r:
            return r.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  HTML fetch failed: {url} → {e}")
        return ""

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
    if anti > pro:  return "anti_arms"
    if pro > anti:  return "pro_arms"
    if anti > 0:    return "anti_arms"
    return None

def parse_district(raw):
    raw   = raw.strip()
    party = (re.search(r'\[([RDI])\]', raw) or type('',(),{'group':lambda s,x:"?"})()).group(1)
    state = (re.search(r'^([A-Z]{2})-', raw) or type('',(),{'group':lambda s,x:""})()).group(1)
    dm    = re.search(r'-(\d+)', raw)
    district = int(dm.group(1)) if dm else None
    is_senate = "SEN" in raw.upper()
    chamber   = "senate" if (is_senate or district is None) else "house"
    return state, district, party, chamber


# ─────────────────────────────────────────────────────────────────────────────
# 1. TRACKAIPAC — Primary member + challenger source
# ─────────────────────────────────────────────────────────────────────────────

def scrape_trackaipac_congress():
    """
    Scrapes all 533 current members from trackaipac.com/congress.
    This is our PRIMARY member source — it has name, state, district,
    party, chamber, antiArms classification, and AIPAC money totals.
    """
    print("Scraping TrackAIPAC /congress (primary member source)…")
    html = fetch_html(TRACKAIPAC_CONGRESS)
    if not html:
        print("  ERROR: TrackAIPAC /congress fetch failed")
        return {}

    results = {}
    blocks  = re.split(r'<h2[^>]*>', html)[1:]

    for block in blocks:
        name_match = re.match(r'([^<]+)</h2>', block)
        if not name_match:
            continue
        name = re.sub(r'\s+', ' ', name_match.group(1)).strip()
        if not name or len(name) < 3 or len(name) > 60:
            continue

        text = re.sub(r'<[^>]+>', ' ', block)
        text = re.sub(r'\s+', ' ', text).strip()

        approved = ("Track AIPAC Approved" in text or
                    "rejects aipac" in text.lower() or
                    "rejected aipac" in text.lower())

        lobby_m  = re.search(r'Israel Lobby Total:\s*\$([\d,]+)', text)
        lobby    = int(lobby_m.group(1).replace(",","")) if lobby_m else 0

        pacs_m   = re.search(r'(?:AIPAC|DMFI|RJC|JAC|NORPAC|AGG|AMP|USI|PIA|FIPAC)[,A-Z\s]+', text)
        pacs     = [p.strip() for p in pacs_m.group(0).split(",")] if pacs_m else []
        pacs     = [p for p in pacs if re.match(r'^[A-Z]{2,8}$', p)][:8]

        dist_m   = re.search(r'([A-Z]{2}-(?:\d+|SEN|SENATE)\s*\[[RDI]\])', text)
        state, district, party, chamber = parse_district(dist_m.group(1)) if dist_m else ("","?",party if 'party' in dir() else "?","house")

        note_patterns = [
            r'(Running for [^\.\n]{5,60})',
            r'(passed away[^\.\n]{0,40})',
            r'(resigned[^\.\n]{0,40})',
            r'(not seeking re-election[^\.\n]{0,40})',
        ]
        note = ""
        for pat in note_patterns:
            nm = re.search(pat, text, re.IGNORECASE)
            if nm:
                note = nm.group(1).strip()
                break

        results[name.lower()] = {
            "name":        name,
            "state":       state,
            "district":    district,
            "party":       party,
            "chamber":     chamber,
            "antiArms":    approved,
            "lobby_total": lobby,
            "pacs":        pacs,
            "note":        note,
        }

    approved_n = sum(1 for v in results.values() if v["antiArms"])
    print(f"  TrackAIPAC /congress: {len(results)} members, {approved_n} anti-arms")
    return results


def scrape_trackaipac_endorsements():
    """Scrapes endorsed challengers from trackaipac.com/endorsements."""
    print("Scraping TrackAIPAC /endorsements…")
    html = fetch_html(TRACKAIPAC_ENDORSEMENTS)
    if not html:
        return []

    challengers = []
    blocks = re.split(r'<h2[^>]*>', html)[1:]

    for block in blocks:
        name_m = re.match(r'([^<]+)</h2>', block)
        if not name_m:
            continue
        name = re.sub(r'\s+', ' ', name_m.group(1)).strip()
        if not name or len(name) < 3 or len(name) > 60:
            continue

        text = re.sub(r'<[^>]+>', ' ', block)
        text = re.sub(r'\s+', ' ', text).strip()

        dist_m = re.search(r'([A-Z]{2}-(?:\d+|SEN|SENATE)\s*\[[RDI]\])', text)
        if not dist_m:
            continue
        state, district, party, chamber = parse_district(dist_m.group(1))

        date_m = re.search(
            r'((?:January|February|March|April|May|June|July|August'
            r'|September|October|November|December)\s+\d+,\s*\d{4})', text)
        primary_date = ""
        if date_m:
            try:
                from datetime import datetime as dt
                primary_date = dt.strptime(date_m.group(1), "%B %d, %Y").strftime("%Y-%m-%d")
            except:
                pass

        href_m   = re.search(r'href="(https?://[^"]+)"', block)
        website  = href_m.group(1) if href_m else ""
        is_gen   = "General Election" in text

        challengers.append({
            "name":          name,
            "state":         state,
            "district":      district,
            "party":         party,
            "chamber":       chamber,
            "antiArms":      True,
            "ps":            "GENERAL" if is_gen else "UPCOMING",
            "primaryDate":   primary_date,
            "website":       website,
            "source":        "trackaipac_endorsed",
            "note":          "Endorsed by Citizens Against AIPAC Corruption. Rejects AIPAC contributions.",
            "opponent":      "",
            "incumbentParty":"",
        })

    print(f"  TrackAIPAC /endorsements: {len(challengers)} endorsed challengers")
    return challengers


# ─────────────────────────────────────────────────────────────────────────────
# 2. BUILD MEMBER LIST from TrackAIPAC data
#    (Congress.gov used only for bill co-sponsorship bioguide lookups)
# ─────────────────────────────────────────────────────────────────────────────

def build_members_from_trackaipac(ta_congress):
    """
    Converts TrackAIPAC member dict → standardised member list.
    This is the authoritative source — all 533 members with classifications.
    """
    members = []
    for name_lower, m in ta_congress.items():
        if not m.get("name") or not m.get("state"):
            continue
        members.append({
            "bioguide_id": "",          # filled in later if Congress.gov available
            "name":        m["name"],
            "party":       m.get("party", "?"),
            "state":       m.get("state", ""),
            "district":    m.get("district"),
            "chamber":     m.get("chamber", "house"),
            "antiArms":    m.get("antiArms", False),
            "ps":          "INCUMBENT",
            "bills":       [],
            "votes":       [],
            "lobby_total": m.get("lobby_total", 0),
            "pacs":        m.get("pacs", []),
            "note":        m.get("note", ""),
        })

    # Enrich with bioguide IDs from Congress.gov (best-effort)
    if CONGRESS_KEY:
        members = enrich_bioguide_ids(members)

    anti = sum(1 for m in members if m["antiArms"])
    print(f"  Member list: {len(members)} members, {anti} anti-arms")
    return members


def enrich_bioguide_ids(members):
    """
    Fetches bioguide IDs from Congress.gov to enable bill co-sponsorship lookup.
    Non-critical — if it fails, members still show correctly.
    """
    print("  Enriching bioguide IDs from Congress.gov…")
    try:
        url = congress_url("/member", {"limit": 250, "currentMember": "true"})
        d   = fetch_json(url)
        if not d:
            print("  Congress.gov /member: no response — skipping bioguide enrichment")
            return members

        # Build name→bioguide map
        bio_map = {}
        for item in d.get("members", []):
            raw_name = item.get("name", "")
            if "," in raw_name:
                last, first = raw_name.split(",", 1)
                norm = (first.strip() + " " + last.strip()).lower()
            else:
                norm = raw_name.lower()
            bio_map[norm] = item.get("bioguideId", "")

        # Paginate if needed
        total = d.get("pagination", {}).get("count", 0)
        offset = 250
        while offset < total:
            url2 = congress_url("/member", {"limit": 250, "offset": offset, "currentMember": "true"})
            d2 = fetch_json(url2)
            if not d2: break
            for item in d2.get("members", []):
                raw_name = item.get("name", "")
                if "," in raw_name:
                    last, first = raw_name.split(",", 1)
                    norm = (first.strip() + " " + last.strip()).lower()
                else:
                    norm = raw_name.lower()
                bio_map[norm] = item.get("bioguideId", "")
            offset += 250
            time.sleep(0.3)

        # Match to our members list
        matched = 0
        for m in members:
            bio = bio_map.get(m["name"].lower(), "")
            if bio:
                m["bioguide_id"] = bio
                matched += 1

        print(f"  Bioguide: matched {matched}/{len(members)} members")
    except Exception as e:
        print(f"  Bioguide enrichment failed (non-critical): {e}")

    return members


# ─────────────────────────────────────────────────────────────────────────────
# 3. CONGRESS.GOV BILL DISCOVERY
# ─────────────────────────────────────────────────────────────────────────────

def discover_bills(members_by_id):
    if not CONGRESS_KEY:
        print("  No CONGRESS_KEY — skipping bill discovery")
        return []
    print("\nDiscovering bills from Congress.gov…")
    seen, bills = set(), []

    def process_bill(bd):
        btype  = bd.get("type", "").lower()
        bnum   = bd.get("number", "")
        btitle = bd.get("title", "")
        bcong  = bd.get("congress", CURRENT_CONGRESS)
        uid    = f"{bcong}-{btype}-{bnum}"
        if uid in seen or not bnum:
            return
        seen.add(uid)

        subj_d   = fetch_json(congress_url(f"/bill/{bcong}/{btype}/{bnum}/subjects")) or {}
        subjects = [s.get("name","") for s in subj_d.get("subjects",{}).get("legislativeSubjects",[])]
        time.sleep(0.2)

        cls = score_bill(btitle, subjects)
        if cls is None:
            return

        cosp_d     = fetch_json(congress_url(f"/bill/{bcong}/{btype}/{bnum}/cosponsors", {"limit":250})) or {}
        cosponsors = cosp_d.get("cosponsors", [])
        time.sleep(0.2)

        detail_d = fetch_json(congress_url(f"/bill/{bcong}/{btype}/{bnum}")) or {}
        sponsors = detail_d.get("bill", {}).get("sponsors", [])
        time.sleep(0.2)

        sp_ids = []
        for s in list(sponsors) + list(cosponsors):
            bid = s.get("bioguideId", "")
            if bid:
                sp_ids.append(bid)
                if cls == "anti_arms" and bid in members_by_id:
                    members_by_id[bid]["antiArms"] = True
                    if uid not in members_by_id[bid]["bills"]:
                        members_by_id[bid]["bills"].append(uid)

        base = f"https://www.congress.gov/bill/{bcong}th-congress"
        url_map = {"hr":"house-bill","s":"senate-bill","sjres":"senate-joint-resolution","hjres":"house-joint-resolution"}
        burl = f"{base}/{url_map.get(btype, btype+'-bill')}/{bnum}"

        lat = detail_d.get("bill",{}).get("latestAction",{})
        bills.append({
            "id":    uid,
            "congress": bcong,
            "type":  btype.upper(),
            "number": bnum,
            "title": btitle,
            "classification": cls,
            "subjects": subjects[:8],
            "sponsor_count": len(sp_ids),
            "sponsor_ids": sp_ids,
            "url":   burl,
            "last_action": lat.get("text",""),
            "last_action_date": lat.get("actionDate",""),
        })
        print(f"  {'🚫' if cls=='anti_arms' else '🔫'} {btype.upper()} {bnum}: {btitle[:55]} ({cls})")

    # Known bills
    for btype, bnum in [("hr","3565")]:
        process_bill({"type":btype,"number":bnum,"title":"","congress":CURRENT_CONGRESS})
        time.sleep(0.3)

    # Search queries
    for q in BILL_SEARCH_QUERIES:
        url = congress_url("/bill", {"query":q, "congress":CURRENT_CONGRESS, "limit":20, "sort":"date"})
        d   = fetch_json(url) or {}
        for b in d.get("bills", []):
            process_bill(b)
            time.sleep(0.2)
        time.sleep(0.4)

    # Senate JRDs
    url = congress_url("/bill", {"congress":CURRENT_CONGRESS, "billType":"SJRES", "query":"Israel", "limit":20})
    d   = fetch_json(url) or {}
    for b in d.get("bills", []):
        process_bill(b)
        time.sleep(0.2)

    print(f"  Bills discovered: {len(bills)}")
    return bills


# ─────────────────────────────────────────────────────────────────────────────
# 4. FEC — Finance + 2026 candidates
# ─────────────────────────────────────────────────────────────────────────────

def fetch_fec(members):
    if not FEC_KEY:
        return {}
    print("\nFetching FEC finance…")
    results = {}
    for name, cid in KNOWN_FEC_IDS.items():
        url = f"{FEC_BASE}/candidate/{cid}/totals/?api_key={FEC_KEY}&cycle=2024&per_page=1"
        d   = fetch_json(url)
        t   = (d or {}).get("results", [None])[0]
        if t:
            results[name] = {
                "raised": t.get("receipts"),
                "spent":  t.get("disbursements"),
                "cash":   t.get("last_cash_on_hand_end_period"),
                "cid":    cid,
            }
        time.sleep(0.2)

    for name in ["Rashida Tlaib","Ilhan Omar","Alexandria Ocasio-Cortez",
                 "Thomas Massie","Summer Lee","Nida Allam","Pramila Jayapal"]:
        cid = KNOWN_FEC_IDS.get(name)
        if not cid: continue
        url = (f"{FEC_BASE}/schedules/schedule_e/"
               f"?api_key={FEC_KEY}&candidate_id={cid}&cycle=2024&per_page=10"
               f"&sort=-expenditure_amount")
        d = fetch_json(url)
        if d and d.get("results"):
            against = sum(x.get("expenditure_amount",0) for x in d["results"] if x.get("support_oppose_indicator")=="O")
            support = sum(x.get("expenditure_amount",0) for x in d["results"] if x.get("support_oppose_indicator")=="S")
            results.setdefault(name, {"cid":cid})
            results[name]["ie_against"] = against
            results[name]["ie_support"] = support
        time.sleep(0.2)

    print(f"  FEC: {len(results)} records")
    return results


def fetch_fec_candidates_2026(ta_endorsed):
    """Cross-reference FEC 2026 filings with TrackAIPAC endorsements."""
    if not FEC_KEY:
        return []
    print("\nFetching FEC 2026 candidates…")
    endorsed_names = {c["name"].lower(): c for c in ta_endorsed}
    candidates = []

    for office in ["H", "S"]:
        offset = 0
        while True:
            url = (f"{FEC_BASE}/candidates/search/"
                   f"?api_key={FEC_KEY}&cycle=2026&office={office}"
                   f"&candidate_status=C&per_page=100&offset={offset}")
            d = fetch_json(url)
            if not d: break
            batch = d.get("results", [])
            if not batch: break

            for c in batch:
                raw = c.get("name","")
                name = (raw.split(",")[1].strip().title() + " " + raw.split(",")[0].strip().title()
                        if "," in raw else raw.title())
                ta = endorsed_names.get(name.lower())
                if ta:
                    cid = c.get("candidate_id","")
                    dist = None
                    try: dist = int(c.get("district",""))
                    except: pass
                    candidates.append({**ta, "fec_id": cid, "district": dist or ta.get("district"),
                                       "source": "fec+trackaipac"})
                    print(f"  FEC+TA: {name}")

            total = d.get("pagination",{}).get("count", 0)
            offset += len(batch)
            if offset >= total or len(batch) < 100: break
            time.sleep(0.3)

    print(f"  FEC 2026: {len(candidates)} matched candidates")
    return candidates


# ─────────────────────────────────────────────────────────────────────────────
# 5. POLYMARKET
# ─────────────────────────────────────────────────────────────────────────────

def fetch_poly(members):
    if not POLY_KEY:
        return {}
    print("\nFetching Polymarket…")
    results = {}
    headers = {"Authorization": f"Bearer {POLY_KEY}", "Content-Type": "application/json"}
    for m in [m for m in members if m.get("antiArms")]:
        last = m["name"].split()[-1]
        d    = fetch_json(f"{POLY_BASE}/markets?q={urllib.parse.quote(last+' reelection congress')}&limit=5", headers=headers)
        if not d: continue
        for mk in (d if isinstance(d, list) else d.get("markets",[])):
            title = (mk.get("question") or mk.get("title") or "").lower()
            if last.lower() not in title: continue
            if not any(kw in title for kw in ["reelect","win","congress","senate","house","primary"]): continue
            prob = None
            try:
                p = mk.get("outcomePrices") or "[]"
                if isinstance(p, str): p = json.loads(p)
                if p: prob = float(p[0])
            except: pass
            results[m["name"]] = {
                "question": mk.get("question") or mk.get("title") or "",
                "prob": prob, "url": mk.get("url") or "https://polymarket.com",
                "volume": mk.get("volume"),
            }
            break
        time.sleep(0.3)
    print(f"  Polymarket: {len(results)} records")
    return results


# ─────────────────────────────────────────────────────────────────────────────
# 6. LEGISCAN VOTES
# ─────────────────────────────────────────────────────────────────────────────

def fetch_legiscan_votes(members_by_name):
    if not LEGISCAN_KEY:
        print("\nLegiScan: key pending — skipping")
        return {}
    print("\nFetching LegiScan votes…")
    vote_records = {}
    for q in ["Israel Emergency Security Assistance Act", "Block the Bombs Act"]:
        url = (f"{LEGISCAN_BASE}/?key={LEGISCAN_KEY}"
               f"&op=search&state=US&query={urllib.parse.quote(q)}&year=2&page=1")
        d = fetch_json(url) or {}
        if d.get("status") != "OK": continue
        for key, bill in d.get("searchresult",{}).items():
            if not isinstance(bill, dict): continue
            bid = bill.get("bill_id")
            if not bid: continue
            det = fetch_json(f"{LEGISCAN_BASE}/?key={LEGISCAN_KEY}&op=getBill&id={bid}") or {}
            if det.get("status") != "OK": continue
            b = det.get("bill",{})
            for vote in b.get("votes",[]):
                rid = vote.get("roll_id")
                if not rid: continue
                rc_d = fetch_json(f"{LEGISCAN_BASE}/?key={LEGISCAN_KEY}&op=getRollCall&id={rid}") or {}
                if rc_d.get("status") != "OK": continue
                rc   = rc_d.get("roll_call",{})
                desc = rc.get("desc","")
                cls  = score_bill(b.get("title",""),[])
                for v in rc.get("votes",[]):
                    vname = v.get("name","")
                    vtxt  = v.get("vote_text","").lower()
                    last  = vname.split(",")[0].strip() if "," in vname else vname.split()[-1]
                    for mname, m in members_by_name.items():
                        if m["name"].split()[-1].lower() == last.lower():
                            if cls == "pro_arms" and "nay" in vtxt:
                                m["antiArms"] = True
                                m["votes"].append({"bill":b.get("bill_number",""),"vote":"No","desc":desc})
                            elif cls == "anti_arms" and "yea" in vtxt:
                                m["antiArms"] = True
                                m["votes"].append({"bill":b.get("bill_number",""),"vote":"Yes","desc":desc})
                vote_records[str(rid)] = {
                    "desc":desc,"bill":b.get("bill_number",""),
                    "date":rc.get("date",""),"yeas":rc.get("yeas",0),"nays":rc.get("nays",0),
                }
                time.sleep(0.3)
            time.sleep(0.4)
    return vote_records


# ─────────────────────────────────────────────────────────────────────────────
# 7. HISTORY DETECTION
# ─────────────────────────────────────────────────────────────────────────────

def load_history():
    try:
        with open("history.json") as f:
            return json.load(f)
    except:
        return {"last_updated":"","events":[]}

def detect_history_changes(old_members, new_members, ta_congress):
    history  = load_history()
    events   = history.get("events", [])
    today    = datetime.now(timezone.utc).date().isoformat()
    now_iso  = datetime.now(timezone.utc).isoformat()
    existing = {e.get("id") for e in events}

    def eid(name, ctype):
        return hashlib.md5(f"{name}-{today}-{ctype}".encode()).hexdigest()[:10]

    old_by_name = {m["name"].lower(): m for m in old_members}
    new_by_name = {m["name"].lower(): m for m in new_members}

    def add(name, m, change, trigger, source="trackaipac"):
        ev_id = eid(name, change)
        if ev_id in existing: return
        events.append({
            "id":name, "date":today, "detected_at":now_iso,
            "member":m.get("name",""), "chamber":m.get("chamber",""),
            "state":m.get("state",""), "party":m.get("party",""),
            "change":change, "trigger":trigger, "source":source,
        })
        existing.add(ev_id)
        print(f"  📜 History: {m.get('name','')} — {change}")

    for nk, nm in new_by_name.items():
        om = old_by_name.get(nk)
        if not om:
            if nm.get("antiArms"):
                add(nk, nm, "pro_to_anti", "First detected as anti-arms by TrackAIPAC")
            continue
        if om.get("antiArms") and not nm.get("antiArms"):
            add(nk, nm, "anti_to_pro", "No longer flagged as anti-arms by TrackAIPAC", "trackaipac")
        elif not om.get("antiArms") and nm.get("antiArms"):
            add(nk, nm, "pro_to_anti", "Newly flagged as anti-arms by TrackAIPAC", "trackaipac")

    for ok, om in old_by_name.items():
        if ok not in new_by_name and om.get("antiArms"):
            ta = ta_congress.get(ok, {})
            note = ta.get("note","")
            if "passed away" in note.lower():   trigger = "Passed away — seat now vacant"
            elif "resigned" in note.lower():    trigger = "Resigned from office"
            elif "running for" in note.lower(): trigger = f"Left seat: {note}"
            else:                               trigger = "No longer listed as current member"
            add(ok, om, "departed", trigger)

    events.sort(key=lambda e: e.get("date",""), reverse=True)
    history["last_updated"] = now_iso
    history["events"]       = events

    with open("history.json","w") as f:
        json.dump(history, f, indent=2)

    new_today = [e for e in events if e.get("date")==today]
    print(f"  History: {len(new_today)} new event(s) today, {len(events)} total")
    return history


# ─────────────────────────────────────────────────────────────────────────────
# 8. CHALLENGERS MERGE
# ─────────────────────────────────────────────────────────────────────────────

def merge_challengers(ta_endorsed, fec_candidates):
    try:
        with open("challengers.json") as f:
            existing = json.load(f)
        manual = existing.get("challengers", [])
    except:
        manual = []

    merged = {c["name"].lower(): c for c in manual}

    for c in ta_endorsed:
        key = c["name"].lower()
        if key in merged:
            merged[key].setdefault("primaryDate", c.get("primaryDate",""))
            merged[key].setdefault("website", c.get("website",""))
            merged[key]["antiArms"] = True
            merged[key]["source"]   = "manual+trackaipac"
        else:
            merged[key] = c

    ta_names = {c["name"].lower() for c in ta_endorsed}
    for c in fec_candidates:
        key = c["name"].lower()
        if key in ta_names:
            merged.setdefault(key, c)
            if c.get("fec_id"):
                merged[key]["fec_id"] = c["fec_id"]

    result = sorted(merged.values(), key=lambda c:(c.get("state",""),c.get("name","")))

    with open("challengers.json","w") as f:
        json.dump({
            "_note": "Auto-updated from TrackAIPAC + FEC. Manual entries preserved.",
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
        with open("data.json") as f:
            existing = json.load(f)
        print(f"Loaded existing data.json v{existing.get('meta',{}).get('version','?')}")
    except:
        print("No existing data.json — fresh build")

    old_members = existing.get("members", [])

    # ── TrackAIPAC scrape (every 24h) ─────────────────────────────────────────
    last_scraped  = existing.get("meta",{}).get("trackaipac_scraped","")
    should_scrape = True
    if last_scraped:
        try:
            if datetime.now(timezone.utc) - datetime.fromisoformat(last_scraped) \
               < timedelta(hours=TRACKAIPAC_RESCRAPE_H):
                should_scrape = False
                print(f"TrackAIPAC: cached (< {TRACKAIPAC_RESCRAPE_H}h old)")
        except: pass

    if should_scrape:
        ta_congress = scrape_trackaipac_congress()
        ta_endorsed = scrape_trackaipac_endorsements()
        time.sleep(1)
    else:
        ta_congress = existing.get("trackaipac_congress", {})
        ta_endorsed = existing.get("trackaipac_endorsed", [])
        # If cache is empty for some reason, force re-scrape
        if not ta_congress:
            print("Cache empty — forcing re-scrape")
            ta_congress = scrape_trackaipac_congress()
            ta_endorsed = scrape_trackaipac_endorsements()
            should_scrape = True

    if not ta_congress:
        print("ERROR: No member data available — aborting")
        return

    # ── Build member list from TrackAIPAC ─────────────────────────────────────
    members        = build_members_from_trackaipac(ta_congress)
    members_by_id  = {m["bioguide_id"]: m for m in members if m.get("bioguide_id")}
    members_by_name= {m["name"].lower(): m for m in members}

    # ── Bills from Congress.gov ───────────────────────────────────────────────
    bills = discover_bills(members_by_id) or existing.get("bills", [])

    # ── LegiScan votes ────────────────────────────────────────────────────────
    vote_records = fetch_legiscan_votes(members_by_name) or existing.get("vote_records", {})

    # ── History ───────────────────────────────────────────────────────────────
    history = detect_history_changes(old_members, members, ta_congress)

    # ── Challengers ───────────────────────────────────────────────────────────
    fec_cands   = fetch_fec_candidates_2026(ta_endorsed) if FEC_KEY else []
    challengers = merge_challengers(ta_endorsed, fec_cands)

    # ── FEC finance + Polymarket ──────────────────────────────────────────────
    fec  = fetch_fec(members)  or existing.get("fec",  {})
    poly = fetch_poly(members) or existing.get("poly", {})

    anti_count = sum(1 for m in members if m["antiArms"])

    # ── Write data.json ───────────────────────────────────────────────────────
    data = {
        "meta": {
            "fetched_at":         datetime.now(timezone.utc).isoformat(),
            "trackaipac_scraped": datetime.now(timezone.utc).isoformat() if should_scrape else last_scraped,
            "member_count":       len(members),
            "anti_arms_count":    anti_count,
            "bill_count":         len(bills),
            "challenger_count":   len(challengers),
            "history_events":     len(history.get("events",[])),
            "version":            6,
            "sources": {
                "members":    "trackaipac.com (primary)",
                "bills":      "api.congress.gov",
                "challengers":"trackaipac.com + FEC",
                "votes":      "legiscan.com" if LEGISCAN_KEY else "pending key",
                "fec":        "api.open.fec.gov",
                "polymarket": "polymarket.com",
            },
        },
        "members":             members,
        "bills":               bills,
        "vote_records":        vote_records,
        "fec":                 fec,
        "poly":                poly,
        "trackaipac_congress": ta_congress,
        "trackaipac_endorsed": ta_endorsed,
    }

    with open("data.json","w") as f:
        json.dump(data, f, indent=2)

    print(f"""
✓ data.json written (v6)
  Members:       {len(members)} ({anti_count} anti-arms)
  Bills:         {len(bills)}
  Challengers:   {len(challengers)}
  History events:{len(history.get('events',[]))}
  FEC records:   {len(fec)}
  Polymarket:    {len(poly)}
""")


if __name__ == "__main__":
    main()
