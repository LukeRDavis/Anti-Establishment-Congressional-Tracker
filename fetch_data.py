#!/usr/bin/env python3
"""
fetch_data.py — GitHub Actions, runs every 4 hours.

Data sources:
  Congress.gov API   → all 535 current members + bill co-sponsorship
  TrackAIPAC.com     → anti-arms classification + endorsed challengers (scraped every 24h)
  OpenFEC API        → campaign finance + independent expenditures
  FEC /candidates    → all 2026 filed candidates (cross-ref with TrackAIPAC for challengers)
  Polymarket API     → reelection odds
  LegiScan API       → roll call vote records (activates when LEGISCAN_KEY set)

history.json is updated whenever a member's antiArms status changes between runs.

GitHub Secrets required:
  CONGRESS_KEY   api.congress.gov
  FEC_KEY        api.open.fec.gov
  POLY_KEY       polymarket.com
  LEGISCAN_KEY   legiscan.com (add when received)
"""

import os, json, re, time, hashlib, urllib.request, urllib.parse, urllib.error
from datetime import datetime, timezone, timedelta

CONGRESS_KEY  = os.environ.get("CONGRESS_KEY",  "")
FEC_KEY       = os.environ.get("FEC_KEY",       "")
POLY_KEY      = os.environ.get("POLY_KEY",      "")
LEGISCAN_KEY  = os.environ.get("LEGISCAN_KEY",  "")

CONGRESS_BASE  = "https://api.congress.gov/v3"
FEC_BASE       = "https://api.open.fec.gov/v1"
POLY_BASE      = "https://gamma-api.polymarket.com"
LEGISCAN_BASE  = "https://api.legiscan.com"
CURRENT_CONGRESS = 119

TRACKAIPAC_CONGRESS    = "https://www.trackaipac.com/congress"
TRACKAIPAC_ENDORSEMENTS= "https://www.trackaipac.com/endorsements"
TRACKAIPAC_CANDIDATES  = "https://www.trackaipac.com/candidates"
TRACKAIPAC_RESCRAPE_H  = 24   # hours between TrackAIPAC scrapes

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

KNOWN_BILLS = [("hr","3565")]

KNOWN_FEC_IDS = {
    "Rashida Tlaib":"H8MI13145","Ilhan Omar":"H8MN05054",
    "Alexandria Ocasio-Cortez":"H8NY15148","Ayanna Pressley":"H8MA07170",
    "Pramila Jayapal":"H6WA07170","Thomas Massie":"H2KY04074",
    "Marjorie Taylor Greene":"H0GA14168","Lauren Boebert":"H0CO03197",
    "Warren Davidson":"H6OH08174","Summer Lee":"H2PA18134",
    "Ro Khanna":"H4CA17161","Jamie Raskin":"H6MD08170",
    "Jim McGovern":"H6MA03074","Greg Casar":"H2TX35131",
    "Mark Pocan":"H2WI02078","Andy Biggs":"H6AZ05182",
    "Scott Perry":"H2PA04078","Paul Gosar":"H0AZ01094",
    "Maxwell Frost":"H2FL10186","Lloyd Doggett":"H4TX10027",
    "Jan Schakowsky":"H8IL09052","Maxine Waters":"H0CA35020",
    "Rand Paul":"S0KY00082","Bernie Sanders":"S6VT00002",
    "Elizabeth Warren":"S2MA00170","Ed Markey":"H0MA07012",
    "Jeff Merkley":"S8OR00207","Chris Van Hollen":"S6MD01037",
    "Brian Schatz":"S2HI00121","Nida Allam":"H6NC04229",
}

# ── Utilities ─────────────────────────────────────────────────────────────────
def fetch_json(url, headers=None, retries=2):
    req = urllib.request.Request(url, headers=headers or {})
    for attempt in range(retries+1):
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code==429: time.sleep(10)
            else: print(f"  HTTP {e.code}: {url[:70]}"); return None
        except Exception as e:
            if attempt<retries: time.sleep(2)
            else: print(f"  WARN: {url[:70]} → {e}"); return None
    return None

def fetch_html(url):
    """Fetch a page as raw text with browser-like headers."""
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; AntiArmsTracker/2.0)",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    }
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            return r.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  HTML fetch failed: {url} → {e}")
        return ""

def congress_url(path, params=None):
    p = params or {}
    p["api_key"] = CONGRESS_KEY
    p["format"]  = "json"
    return f"{CONGRESS_BASE}{path}?{urllib.parse.urlencode(p)}"

def score_bill(title, subjects):
    combined = (title + " " + " ".join(subjects)).lower()
    if not any(kw in combined for kw in ["israel","gaza","arms","weapon","military sale","fms"]):
        return None
    anti = sum(1 for kw in ANTI_ARMS_SIGNALS if kw in combined)
    pro  = sum(1 for kw in PRO_ARMS_SIGNALS  if kw in combined)
    if anti > pro:   return "anti_arms"
    if pro  > anti:  return "pro_arms"
    if anti > 0:     return "anti_arms"
    return None

def parse_district(raw):
    """
    Parse 'TX-21 [R]' → (state='TX', district=21, party='R')
    Parse 'TX-SEN [D]' → (state='TX', district=None, party='D', chamber='senate')
    """
    raw = raw.strip()
    party_match = re.search(r'\[([RDI])\]', raw)
    party = party_match.group(1) if party_match else "?"
    state_match = re.search(r'^([A-Z]{2})-', raw)
    state = state_match.group(1) if state_match else ""
    dist_match  = re.search(r'-(\d+)', raw)
    district    = int(dist_match.group(1)) if dist_match else None
    is_senate   = "SEN" in raw.upper() or "SENATE" in raw.upper()
    chamber     = "senate" if (is_senate or district is None) else "house"
    return state, district, party, chamber


# ─────────────────────────────────────────────────────────────────────────────
# 1. TRACKAIPAC SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

def scrape_trackaipac_congress():
    """
    Scrapes trackaipac.com/congress for:
      - All listed members with Israel lobby totals
      - "Track AIPAC Approved" flag (anti-arms signal)
    Returns dict keyed by (name_lower) → {approved, lobby_total, pacs, note}
    """
    print("Scraping TrackAIPAC /congress …")
    html = fetch_html(TRACKAIPAC_CONGRESS)
    if not html:
        return {}

    results = {}

    # Each member block: <h2>Name</h2> followed by text nodes
    # Pattern: name in h2, then lines with district/party, lobby total, PACs, optional note
    blocks = re.split(r'<h2[^>]*>', html)[1:]   # split on each <h2>

    for block in blocks:
        # Name: everything before </h2>
        name_match = re.match(r'([^<]+)</h2>', block)
        if not name_match:
            continue
        name = re.sub(r'\s+', ' ', name_match.group(1)).strip()
        if not name or len(name) < 3 or len(name) > 60:
            continue

        # Strip all HTML tags from the block for text parsing
        text = re.sub(r'<[^>]+>', ' ', block)
        text = re.sub(r'\s+', ' ', text).strip()

        # Approved status
        approved = "Track AIPAC Approved" in text or "rejects AIPAC" in text.lower()

        # Israel Lobby Total
        lobby_match = re.search(r'Israel Lobby Total:\s*\$([\d,]+)', text)
        lobby_total = int(lobby_match.group(1).replace(",","")) if lobby_match else 0

        # PAC list (uppercase acronyms after a comma-separated pattern)
        pacs_match = re.search(r'(?:AIPAC|DMFI|RJC|JAC|NORPAC)[A-Z,\s]+', text)
        pacs = [p.strip() for p in pacs_match.group(0).split(",")] if pacs_match else []

        # District/party line e.g. "TX-21 [R]" or "AL-SEN [R]"
        dist_match = re.search(r'([A-Z]{2}-(?:\d+|SEN|SENATE|AL)\s*\[[RDI]\])', text)
        state, district, party, chamber = parse_district(dist_match.group(1)) if dist_match else ("","",party,"house")

        # Any note on the member (Running for X, passed away, resigned)
        note_patterns = [
            r'(Running for [^\n\.]+)',
            r'(passed away[^\n\.]+)',
            r'(resigned[^\n\.]+)',
            r'(retired[^\n\.]+)',
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
            "approved":    approved,
            "lobby_total": lobby_total,
            "pacs":        pacs[:8],
            "note":        note,
        }

    approved_count = sum(1 for v in results.values() if v["approved"])
    print(f"  TrackAIPAC /congress: {len(results)} members parsed, {approved_count} approved")
    return results


def scrape_trackaipac_endorsements():
    """
    Scrapes trackaipac.com/endorsements for anti-arms challenger candidates.
    Returns list of challenger dicts ready to merge into challengers.json.
    """
    print("Scraping TrackAIPAC /endorsements …")
    html = fetch_html(TRACKAIPAC_ENDORSEMENTS)
    if not html:
        return []

    challengers = []
    blocks = re.split(r'<h2[^>]*>', html)[1:]

    for block in blocks:
        name_match = re.match(r'([^<]+)</h2>', block)
        if not name_match:
            continue
        name = re.sub(r'\s+', ' ', name_match.group(1)).strip()
        if not name or len(name) < 3 or len(name) > 60:
            continue

        text = re.sub(r'<[^>]+>', ' ', block)
        text = re.sub(r'\s+', ' ', text).strip()

        # District/party
        dist_match = re.search(r'([A-Z]{2}-(?:\d+|SEN|SENATE|AL)\s*\[[RDI]\])', text)
        if not dist_match:
            continue
        state, district, party, chamber = parse_district(dist_match.group(1))

        # Election type + date
        date_match = re.search(r'((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d+,\s*\d{4})', text)
        election_date = date_match.group(1) if date_match else ""

        election_type = "UPCOMING"
        if "General Election" in text:
            election_type = "GENERAL"

        # Convert date to ISO format
        primary_date = ""
        if date_match:
            try:
                from datetime import datetime as dt
                primary_date = dt.strptime(date_match.group(1), "%B %d, %Y").strftime("%Y-%m-%d")
            except:
                primary_date = election_date

        # Website URL
        href_match = re.search(r'href="(https?://[^"]+)"', block)
        website = href_match.group(1) if href_match else ""

        challengers.append({
            "name":         name,
            "state":        state,
            "district":     district,
            "party":        party,
            "chamber":      chamber,
            "antiArms":     True,
            "ps":           election_type if election_type != "UPCOMING" else "UPCOMING",
            "primaryDate":  primary_date,
            "website":      website,
            "source":       "trackaipac_endorsed",
            "note":         f"Endorsed by TrackAIPAC / Citizens Against AIPAC Corruption. Rejects AIPAC contributions.",
            "opponent":     "",
            "incumbentParty":"",
        })

    print(f"  TrackAIPAC /endorsements: {len(challengers)} endorsed challengers")
    return challengers


# ─────────────────────────────────────────────────────────────────────────────
# 2. FEC 2026 CANDIDATE DISCOVERY
# ─────────────────────────────────────────────────────────────────────────────

def fetch_fec_candidates_2026(trackaipac_endorsed):
    """
    Pulls all 2026 filed candidates from FEC and cross-references with
    TrackAIPAC endorsements to flag anti-arms challengers.
    Returns list of candidate dicts (to merge with challengers.json).
    """
    if not FEC_KEY:
        print("No FEC_KEY — skipping 2026 candidate discovery")
        return []

    print("\nFetching FEC 2026 candidates …")

    # Build lookup of TrackAIPAC endorsed names (lowercase) for fast match
    endorsed_names = {c["name"].lower(): c for c in trackaipac_endorsed}

    candidates = []
    for office in ["H", "S"]:
        offset = 0
        while True:
            url = (f"{FEC_BASE}/candidates/search/"
                   f"?api_key={FEC_KEY}&cycle=2026&office={office}"
                   f"&candidate_status=C&per_page=100&sort=name&sort_null_only=false"
                   f"&offset={offset}")
            d = fetch_json(url)
            if not d:
                break
            batch = d.get("results", [])
            if not batch:
                break

            for c in batch:
                name      = c.get("name","")
                # FEC stores as "LAST, FIRST" — normalise
                if "," in name:
                    last, first = name.split(",",1)
                    name = first.strip().title() + " " + last.strip().title()
                else:
                    name = name.title()

                name_lower = name.lower()
                state      = c.get("state","")
                district   = c.get("district","")
                party      = {"DEM":"D","REP":"R","IND":"I"}.get(c.get("party",""),"?")
                cid        = c.get("candidate_id","")
                incumbent  = c.get("incumbent_challenge","") == "I"

                # Cross-reference with TrackAIPAC
                ta_match   = endorsed_names.get(name_lower)
                anti_arms  = bool(ta_match)

                district_int = None
                try:    district_int = int(district)
                except: pass

                chamber = "senate" if office=="S" else "house"

                if anti_arms:
                    cand = {
                        "name":         name,
                        "state":        state,
                        "district":     district_int,
                        "party":        party,
                        "chamber":      chamber,
                        "antiArms":     True,
                        "fec_id":       cid,
                        "ps":           "INCUMBENT" if incumbent else (ta_match.get("ps","UPCOMING") if ta_match else "UPCOMING"),
                        "primaryDate":  ta_match.get("primaryDate","") if ta_match else "",
                        "website":      ta_match.get("website","") if ta_match else "",
                        "source":       "fec+trackaipac",
                        "note":         ta_match.get("note","") if ta_match else "Endorsed by TrackAIPAC.",
                        "opponent":     ta_match.get("opponent","") if ta_match else "",
                        "incumbentParty": ta_match.get("incumbentParty","") if ta_match else "",
                    }
                    candidates.append(cand)
                    print(f"  FEC+TrackAIPAC match: {name} ({state}{'-'+str(district_int) if district_int else ''})")

            total = d.get("pagination",{}).get("count",0)
            offset += len(batch)
            if offset >= total or len(batch) < 100:
                break
            time.sleep(0.3)

    print(f"  FEC 2026: {len(candidates)} anti-arms candidates identified")
    return candidates


# ─────────────────────────────────────────────────────────────────────────────
# 3. CONGRESS.GOV MEMBERS + BILLS
# ─────────────────────────────────────────────────────────────────────────────

def fetch_all_members():
    if not CONGRESS_KEY:
        return []
    print("\nFetching all current members from Congress.gov …")
    members, offset = [], 0
    while True:
        url = congress_url("/member", {"limit":250,"offset":offset,"currentMember":"true"})
        d   = fetch_json(url)
        if not d: break
        batch = d.get("members",[])
        if not batch: break
        members.extend(batch)
        total = d.get("pagination",{}).get("count",0)
        print(f"  {len(members)}/{total} members …")
        if len(members)>=total or len(batch)<250: break
        offset += 250
        time.sleep(0.3)

    out = []
    for m in members:
        name = m.get("name","")
        if "," in name:
            last, first = name.split(",",1)
            name = first.strip()+" "+last.strip()
        terms   = m.get("terms",{}).get("item",[]) or []
        party   = "?"; chamber="?"; district=None
        state   = m.get("state","")
        if isinstance(terms,list) and terms:
            latest  = terms[-1]
            party   = latest.get("party",party)
            chamber = latest.get("chamber",chamber)
            dr      = latest.get("district")
            try: district=int(dr)
            except: district=dr
        pa = {"Republican":"R","Democrat":"D","Independent":"I"}.get(party, party[:1] if party else "?")
        out.append({
            "bioguide_id": m.get("bioguideId",""),
            "name":        name,
            "party":       pa,
            "state":       state,
            "district":    district,
            "chamber":     "house" if "House" in chamber else "senate",
            "antiArms":    False,
            "ps":          "INCUMBENT",
            "bills":       [],
            "votes":       [],
            "note":        "",
        })
    print(f"  Total members: {len(out)}")
    return out


def discover_bills(members_by_id):
    if not CONGRESS_KEY:
        return []
    print("\nDiscovering bills …")
    seen, bills = set(), []

    def process_bill(bill_data):
        btype = bill_data.get("type","").lower()
        bnum  = bill_data.get("number","")
        btitle= bill_data.get("title","")
        bcong = bill_data.get("congress", CURRENT_CONGRESS)
        uid   = f"{bcong}-{btype}-{bnum}"
        if uid in seen: return
        seen.add(uid)

        subj_d   = fetch_json(congress_url(f"/bill/{bcong}/{btype}/{bnum}/subjects")) or {}
        subjects = [s.get("name","") for s in subj_d.get("subjects",{}).get("legislativeSubjects",[])]
        time.sleep(0.2)

        cls = score_bill(btitle, subjects)
        if cls is None: return

        cosp_d     = fetch_json(congress_url(f"/bill/{bcong}/{btype}/{bnum}/cosponsors",{"limit":250})) or {}
        cosponsors = cosp_d.get("cosponsors",[])
        time.sleep(0.2)

        detail_d = fetch_json(congress_url(f"/bill/{bcong}/{btype}/{bnum}")) or {}
        sponsors = detail_d.get("bill",{}).get("sponsors",[])
        time.sleep(0.2)

        all_sp    = list(sponsors)+list(cosponsors)
        sp_ids    = []
        for s in all_sp:
            bid = s.get("bioguideId","")
            if bid:
                sp_ids.append(bid)
                if cls=="anti_arms" and bid in members_by_id:
                    members_by_id[bid]["antiArms"] = True
                    if uid not in members_by_id[bid]["bills"]:
                        members_by_id[bid]["bills"].append(uid)
                elif bid in members_by_id:
                    if uid not in members_by_id[bid]["bills"]:
                        members_by_id[bid]["bills"].append(uid)

        lat = detail_d.get("bill",{}).get("latestAction",{})
        base_url = f"https://www.congress.gov/bill/{bcong}th-congress"
        if btype=="hr":     burl = f"{base_url}/house-bill/{bnum}"
        elif btype=="s":    burl = f"{base_url}/senate-bill/{bnum}"
        elif btype=="sjres":burl = f"{base_url}/senate-joint-resolution/{bnum}"
        elif btype=="hjres":burl = f"{base_url}/house-joint-resolution/{bnum}"
        else:               burl = f"{base_url}/{btype}-bill/{bnum}"

        bills.append({
            "id":cls_icon+uid if (cls_icon:=("🚫" if cls=="anti_arms" else "🔫")) else uid,
            "id":uid, "congress":bcong, "type":btype.upper(), "number":bnum,
            "title":btitle, "classification":cls, "subjects":subjects[:8],
            "sponsor_count":len(all_sp), "sponsor_ids":sp_ids,
            "url":burl, "last_action":lat.get("text",""),
            "last_action_date":lat.get("actionDate",""),
        })
        print(f"  {'🚫' if cls=='anti_arms' else '🔫'} {btype.upper()} {bnum}: {btitle[:55]} ({cls})")

    for btype,bnum in KNOWN_BILLS:
        if bnum:
            process_bill({"type":btype,"number":bnum,"title":"","congress":CURRENT_CONGRESS})
            time.sleep(0.3)

    for q in BILL_SEARCH_QUERIES:
        url = congress_url("/bill",{"query":q,"congress":CURRENT_CONGRESS,"limit":20,"sort":"date"})
        d   = fetch_json(url) or {}
        for b in d.get("bills",[]): process_bill(b); time.sleep(0.2)
        time.sleep(0.5)

    jrd_url = congress_url("/bill",{"congress":CURRENT_CONGRESS,"billType":"SJRES","query":"Israel","limit":20})
    d = fetch_json(jrd_url) or {}
    for b in d.get("bills",[]): process_bill(b); time.sleep(0.2)

    print(f"  Bills: {len(bills)} discovered")
    return bills


# ─────────────────────────────────────────────────────────────────────────────
# 4. LEGISCAN VOTES
# ─────────────────────────────────────────────────────────────────────────────

def fetch_legiscan_votes(members_by_id):
    if not LEGISCAN_KEY:
        print("\nLegiScan: key pending — skipping")
        return {}
    print("\nFetching LegiScan votes …")
    vote_records = {}
    for q in ["Israel Emergency Security Assistance Act","Block the Bombs Act","joint resolution Israel arms"]:
        url = (f"{LEGISCAN_BASE}/?key={LEGISCAN_KEY}"
               f"&op=search&state=US&query={urllib.parse.quote(q)}&year=2&page=1")
        d = fetch_json(url) or {}
        if d.get("status")!="OK": continue
        for key, bill in d.get("searchresult",{}).items():
            if not isinstance(bill,dict): continue
            bid = bill.get("bill_id")
            if not bid: continue
            det = fetch_json(f"{LEGISCAN_BASE}/?key={LEGISCAN_KEY}&op=getBill&id={bid}") or {}
            if det.get("status")!="OK": continue
            b = det.get("bill",{})
            for vote in b.get("votes",[]):
                rid = vote.get("roll_id")
                if not rid: continue
                rc_d = fetch_json(f"{LEGISCAN_BASE}/?key={LEGISCAN_KEY}&op=getRollCall&id={rid}") or {}
                if rc_d.get("status")!="OK": continue
                rc = rc_d.get("roll_call",{})
                desc = rc.get("desc","")
                cls  = score_bill(b.get("title",""),[])
                for v in rc.get("votes",[]):
                    vname = v.get("name","")
                    vtxt  = v.get("vote_text","").lower()
                    last  = vname.split(",")[0].strip() if "," in vname else vname.split()[-1]
                    for bid2, m in members_by_id.items():
                        if m["name"].split()[-1].lower()==last.lower():
                            if cls=="pro_arms" and "nay" in vtxt:
                                m["antiArms"]=True
                                m["votes"].append({"bill":b.get("bill_number",""),"vote":"No","desc":desc})
                            elif cls=="anti_arms" and "yea" in vtxt:
                                m["antiArms"]=True
                                m["votes"].append({"bill":b.get("bill_number",""),"vote":"Yes","desc":desc})
                vote_records[str(rid)] = {"desc":desc,"bill":b.get("bill_number",""),"date":rc.get("date",""),"yeas":rc.get("yeas",0),"nays":rc.get("nays",0)}
                time.sleep(0.3)
            time.sleep(0.4)
    return vote_records


# ─────────────────────────────────────────────────────────────────────────────
# 5. FEC FINANCE + POLYMARKET
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
            print(f"  FEC OK: {name}")
        time.sleep(0.2)

    ie_targets = ["Rashida Tlaib","Ilhan Omar","Alexandria Ocasio-Cortez",
                  "Thomas Massie","Summer Lee","Nida Allam","Pramila Jayapal"]
    for name in ie_targets:
        cid = KNOWN_FEC_IDS.get(name)
        if not cid: continue
        url = (f"{FEC_BASE}/schedules/schedule_e/"
               f"?api_key={FEC_KEY}&candidate_id={cid}&cycle=2024&per_page=10&sort=-expenditure_amount")
        d = fetch_json(url)
        if d and d.get("results"):
            against = sum(x.get("expenditure_amount",0) for x in d["results"] if x.get("support_oppose_indicator")=="O")
            support = sum(x.get("expenditure_amount",0) for x in d["results"] if x.get("support_oppose_indicator")=="S")
            if name not in results: results[name]={"cid":cid}
            results[name]["ie_against"]=against
            results[name]["ie_support"]=support
            print(f"  IE OK: {name}  vs=${against:,.0f}")
        time.sleep(0.2)
    return results


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
            prob=None
            try:
                p=mk.get("outcomePrices") or "[]"
                if isinstance(p,str): p=json.loads(p)
                if p: prob=float(p[0])
            except: pass
            results[m["name"]]={
                "question":mk.get("question") or mk.get("title") or "",
                "prob":prob,"url":mk.get("url") or "https://polymarket.com",
                "volume":mk.get("volume"),
            }
            print(f"  POLY OK: {m['name']}  prob={prob}")
            break
        time.sleep(0.3)
    return results


# ─────────────────────────────────────────────────────────────────────────────
# 6. HISTORY CHANGE DETECTION
# ─────────────────────────────────────────────────────────────────────────────

def load_history():
    try:
        with open("history.json") as f:
            return json.load(f)
    except:
        return {"last_updated":"","events":[]}

def save_history(history):
    with open("history.json","w") as f:
        json.dump(history, f, indent=2)

def detect_history_changes(old_members, new_members, ta_congress):
    """
    Compares old vs new member lists and logs any antiArms status changes.
    Also catches:
      - Members who departed (resigned/passed away) since last run
      - New members who immediately appear as anti-arms
      - TrackAIPAC note changes (e.g. "passed away", "resigned")
    """
    history = load_history()
    events  = history.get("events", [])
    today   = datetime.now(timezone.utc).date().isoformat()
    now_iso = datetime.now(timezone.utc).isoformat()

    old_by_id  = {m["bioguide_id"]: m for m in old_members if m.get("bioguide_id")}
    new_by_id  = {m["bioguide_id"]: m for m in new_members if m.get("bioguide_id")}

    # Generate a stable event ID
    def eid(bid, change_type):
        return hashlib.md5(f"{bid}-{today}-{change_type}".encode()).hexdigest()[:10]

    existing_ids = {e.get("id") for e in events}

    def add_event(bid, m, change, trigger, source="congress.gov"):
        event_id = eid(bid, change)
        if event_id in existing_ids:
            return   # already logged this event
        events.append({
            "id":         event_id,
            "date":       today,
            "detected_at":now_iso,
            "member":     m.get("name",""),
            "bioguide_id":bid,
            "chamber":    m.get("chamber",""),
            "state":      m.get("state",""),
            "party":      m.get("party",""),
            "change":     change,
            "trigger":    trigger,
            "source":     source,
        })
        print(f"  📜 History event: {m.get('name','')} — {change} — {trigger}")

    # Check each new member against old data
    for bid, new_m in new_by_id.items():
        old_m     = old_by_id.get(bid)
        new_anti  = new_m.get("antiArms", False)

        if not old_m:
            # New seat (special election, etc.)
            if new_anti:
                add_event(bid, new_m, "pro_to_anti",
                    "New member detected as anti-arms via bill co-sponsorship or TrackAIPAC")
            continue

        old_anti = old_m.get("antiArms", False)

        if old_anti and not new_anti:
            add_event(bid, new_m, "anti_to_pro",
                "No longer detected as anti-arms — may have reversed position, left anti-arms bills, or voted pro-arms",
                "congress.gov+trackaipac")
        elif not old_anti and new_anti:
            trigger = "Co-sponsored anti-arms legislation or endorsed by TrackAIPAC"
            if new_m.get("bills"):
                trigger = f"Co-sponsored {new_m['bills'][0]}"
            add_event(bid, new_m, "pro_to_anti", trigger, "congress.gov+trackaipac")

    # Check for departed members (in old but not new = resigned/passed away/term ended)
    for bid, old_m in old_by_id.items():
        if bid not in new_by_id and old_m.get("antiArms"):
            # Check TrackAIPAC for departure note
            ta = ta_congress.get(old_m.get("name","").lower(), {})
            note = ta.get("note","")
            if "passed away" in note.lower():
                trigger = f"Passed away — seat now vacant"
            elif "resigned" in note.lower():
                trigger = f"Resigned from office"
            elif "running for" in note.lower():
                trigger = f"Left House to run for other office: {note}"
            else:
                trigger = "No longer listed as current member of Congress"
            add_event(bid, old_m, "departed", trigger, "congress.gov")

    # Sort events newest first
    events.sort(key=lambda e: e.get("date",""), reverse=True)

    history["last_updated"] = now_iso
    history["events"]       = events
    save_history(history)

    changed = [e for e in events if e["date"]==today]
    print(f"\n  History: {len(changed)} new event(s) today, {len(events)} total")
    return history


# ─────────────────────────────────────────────────────────────────────────────
# 7. CHALLENGERS MERGE (TrackAIPAC + FEC → challengers.json)
# ─────────────────────────────────────────────────────────────────────────────

def merge_challengers(ta_endorsed, fec_candidates):
    """
    Merges TrackAIPAC endorsements and FEC candidates into challengers.json.
    Preserves manually curated entries that aren't in either source.
    """
    try:
        with open("challengers.json") as f:
            existing = json.load(f)
        manual = existing.get("challengers", [])
    except:
        manual = []

    # Index manual entries by name (lowercase)
    manual_by_name = {c["name"].lower(): c for c in manual}

    merged = dict(manual_by_name)  # start with all manual entries

    # Merge TrackAIPAC endorsed
    for c in ta_endorsed:
        key = c["name"].lower()
        if key in merged:
            # Update with TrackAIPAC data but preserve manually set ps/primaryDate if richer
            existing_c = merged[key]
            if not existing_c.get("primaryDate") and c.get("primaryDate"):
                existing_c["primaryDate"] = c["primaryDate"]
            if not existing_c.get("website") and c.get("website"):
                existing_c["website"] = c["website"]
            existing_c["source"] = "manual+trackaipac"
            existing_c["antiArms"] = True
        else:
            merged[key] = c

    # Merge FEC candidates (only if also TrackAIPAC endorsed — FEC alone isn't enough)
    ta_names = {c["name"].lower() for c in ta_endorsed}
    for c in fec_candidates:
        key = c["name"].lower()
        if key in ta_names and key not in merged:
            merged[key] = c
        elif key in merged and c.get("fec_id"):
            merged[key]["fec_id"] = c["fec_id"]

    result = sorted(merged.values(), key=lambda c: (c.get("state",""), c.get("name","")))

    with open("challengers.json","w") as f:
        json.dump({
            "_note": "Auto-updated every 24h from TrackAIPAC + FEC. Manual entries preserved.",
            "_last_scraped": datetime.now(timezone.utc).isoformat(),
            "challengers": result,
        }, f, indent=2)

    print(f"\n  Challengers: {len(result)} total ({len(ta_endorsed)} from TrackAIPAC, {len(fec_candidates)} from FEC)")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    # Load existing data for comparison + fallback
    existing = {}
    try:
        with open("data.json") as f:
            existing = json.load(f)
        print(f"Loaded existing data.json v{existing.get('meta',{}).get('version','?')}")
    except:
        print("No existing data.json — fresh build")

    old_members = existing.get("members", [])

    # ── Should we re-scrape TrackAIPAC? (every 24h) ──────────────────────────
    last_scraped  = existing.get("meta",{}).get("trackaipac_scraped","")
    should_scrape = True
    if last_scraped:
        try:
            last_dt = datetime.fromisoformat(last_scraped)
            if datetime.now(timezone.utc) - last_dt < timedelta(hours=TRACKAIPAC_RESCRAPE_H):
                should_scrape = False
                print(f"TrackAIPAC: last scraped {last_scraped} — skipping (< {TRACKAIPAC_RESCRAPE_H}h)")
        except: pass

    # Load cached TrackAIPAC data if not re-scraping
    ta_congress  = existing.get("trackaipac_congress",  {})
    ta_endorsed  = existing.get("trackaipac_endorsed",  [])

    if should_scrape:
        ta_congress  = scrape_trackaipac_congress()
        ta_endorsed  = scrape_trackaipac_endorsements()
        time.sleep(1)

    # ── Congress.gov: all 535 members ─────────────────────────────────────────
    members = fetch_all_members() or old_members
    members_by_id = {m["bioguide_id"]: m for m in members if m["bioguide_id"]}

    # Apply TrackAIPAC "Approved" flag to members
    for name_lower, ta in ta_congress.items():
        if ta.get("approved"):
            matches = [m for m in members if m["name"].lower() == name_lower]
            for m in matches:
                m["antiArms"] = True
                if ta.get("note") and not m.get("note"):
                    m["note"] = ta["note"]

    # ── Bills + co-sponsor flagging ───────────────────────────────────────────
    bills = discover_bills(members_by_id) or existing.get("bills", [])

    # ── LegiScan votes ────────────────────────────────────────────────────────
    vote_records = fetch_legiscan_votes(members_by_id) or existing.get("vote_records", {})

    # Re-assemble from indexed dict
    members    = list(members_by_id.values())
    anti_count = sum(1 for m in members if m["antiArms"])

    # ── History: detect changes between old and new ───────────────────────────
    history = detect_history_changes(old_members, members, ta_congress)

    # ── FEC 2026 candidates + challenger merge ────────────────────────────────
    fec_cands = fetch_fec_candidates_2026(ta_endorsed) if FEC_KEY else []
    challengers = merge_challengers(ta_endorsed, fec_cands)

    # ── FEC finance ──────────────────────────────────────────────────────────
    fec  = fetch_fec(members) or existing.get("fec", {})
    poly = fetch_poly(members) or existing.get("poly", {})

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
            "fec_count":          len(fec),
            "poly_count":         len(poly),
            "version":            5,
            "sources": {
                "members":    "api.congress.gov",
                "bills":      "api.congress.gov (auto-discovered)",
                "trackaipac": "trackaipac.com (scraped)" if should_scrape else "trackaipac.com (cached)",
                "challengers":"trackaipac.com + FEC",
                "votes":      "legiscan.com" if LEGISCAN_KEY else "pending key",
                "fec":        "api.open.fec.gov",
                "polymarket": "polymarket.com",
            },
        },
        "members":           members,
        "bills":             bills,
        "vote_records":      vote_records,
        "fec":               fec,
        "poly":              poly,
        "trackaipac_congress": ta_congress,
        "trackaipac_endorsed": ta_endorsed,
    }

    with open("data.json","w") as f:
        json.dump(data, f, indent=2)

    print(f"""
✓ Complete
  Members:       {len(members)} ({anti_count} anti-arms)
  Bills:         {len(bills)}
  Challengers:   {len(challengers)}
  History events:{len(history.get('events',[]))}
  FEC records:   {len(fec)}
  Polymarket:    {len(poly)}
""")


if __name__ == "__main__":
    main()
