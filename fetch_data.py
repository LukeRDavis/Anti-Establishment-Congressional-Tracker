#!/usr/bin/env python3
"""
fetch_data.py — runs in GitHub Actions every 4 hours.
Reads API keys from environment variables (GitHub Secrets).
Writes data.json to repo root. Keys are never exposed to the client.

APIs used:
  - OpenFEC:  campaign finance + independent expenditures
  - Polymarket: incumbent reelection odds
  - LegiScan: bill co-sponsorship and vote records (when key is available)
"""

import os, json, time, urllib.request, urllib.parse, urllib.error
from datetime import datetime, timezone

FEC_KEY       = os.environ.get("FEC_KEY",       "")
POLY_KEY      = os.environ.get("POLY_KEY",      "")
LEGISCAN_KEY  = os.environ.get("LEGISCAN_KEY",  "")

FEC_BASE      = "https://api.open.fec.gov/v1"
POLY_BASE     = "https://gamma-api.polymarket.com"
LEGISCAN_BASE = "https://api.legiscan.com"

# ── FEC candidate IDs ─────────────────────────────────────────────────────────
FEC_IDS = {
    "Rashida Tlaib":              "H8MI13145",
    "Ilhan Omar":                 "H8MN05054",
    "Alexandria Ocasio-Cortez":   "H8NY15148",
    "Ayanna Pressley":            "H8MA07170",
    "Pramila Jayapal":            "H6WA07170",
    "Cori Bush":                  "H0MO01197",
    "Jamaal Bowman":              "H0NY16160",
    "Thomas Massie":              "H2KY04074",
    "Marjorie Taylor Greene":     "H0GA14168",
    "Lauren Boebert":             "H0CO03197",
    "Warren Davidson":            "H6OH08174",
    "Summer Lee":                 "H2PA18134",
    "Ro Khanna":                  "H4CA17161",
    "Jamie Raskin":               "H6MD08170",
    "Jim McGovern":               "H6MA03074",
    "Greg Casar":                 "H2TX35131",
    "Mark Pocan":                 "H2WI02078",
    "Andy Biggs":                 "H6AZ05182",
    "Scott Perry":                "H2PA04078",
    "Paul Gosar":                 "H0AZ01094",
    "Maxwell Frost":              "H2FL10186",
    "Lloyd Doggett":              "H4TX10027",
    "Jan Schakowsky":             "H8IL09052",
    "Maxine Waters":              "H0CA35020",
    "Rand Paul":                  "S0KY00082",
    "Bernie Sanders":             "S6VT00002",
    "Elizabeth Warren":           "S2MA00170",
    "Ed Markey":                  "H0MA07012",
    "Jeff Merkley":               "S8OR00207",
    "Chris Van Hollen":           "S6MD01037",
    "Brian Schatz":               "S2HI00121",
    # Challengers with known IDs
    "Nida Allam":                 "H6NC04229",
    "Kat Abughazaleh":            "H6IL09246",
    "Abdul el-Sayed":             "H6MI11197",
    "Junaid Ahmed":               "H6IL08237",
}

IE_TARGETS = [
    "Jamaal Bowman", "Cori Bush", "Rashida Tlaib",
    "Ilhan Omar", "Thomas Massie", "Alexandria Ocasio-Cortez",
    "Summer Lee", "Nida Allam", "Pramila Jayapal",
]

POLY_NAMES = [
    "Thomas Massie", "Rand Paul", "Alexandria Ocasio-Cortez",
    "Rashida Tlaib", "Ilhan Omar", "Ayanna Pressley",
    "Marjorie Taylor Greene", "Lauren Boebert",
    "Summer Lee", "Jamie Raskin", "Ro Khanna", "Scott Perry",
    "James Talarico", "Nida Allam", "Kat Abughazaleh",
    "Junaid Ahmed", "Abdul el-Sayed",
]

# LegiScan bill IDs to track (populate as known)
# Format: { "display_name": legiscan_bill_id }
# HR 3565 = Block the Bombs Act (119th Congress)
# Senate JRDs = Joint Resolutions of Disapproval on FMS to Israel
LEGISCAN_BILLS = {
    "HR 3565 — Block the Bombs Act":          None,  # update with real ID once key arrives
    "S.J.Res. — JRD FMS Arms Nov 2024":       None,
}


def fetch_json(url, headers=None):
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print(f"  WARN: {url[:80]} → {e}")
        return None


# ── FEC ───────────────────────────────────────────────────────────────────────
def fetch_fec():
    print("Fetching FEC data...")
    results = {}

    for name, cid in FEC_IDS.items():
        if not FEC_KEY:
            break
        url = (f"{FEC_BASE}/candidate/{cid}/totals/"
               f"?api_key={FEC_KEY}&cycle=2024&per_page=1")
        d = fetch_json(url)
        t = (d or {}).get("results", [None])[0]
        if t:
            results[name] = {
                "raised": t.get("receipts"),
                "spent":  t.get("disbursements"),
                "cash":   t.get("last_cash_on_hand_end_period"),
                "cid":    cid,
            }
            print(f"  FEC OK: {name}")
        time.sleep(0.25)

    # Schedule E — independent expenditures
    for name in IE_TARGETS:
        cid = FEC_IDS.get(name)
        if not cid or not FEC_KEY:
            continue
        url = (f"{FEC_BASE}/schedules/schedule_e/"
               f"?api_key={FEC_KEY}&candidate_id={cid}"
               f"&cycle=2024&per_page=10&sort=-expenditure_amount")
        d = fetch_json(url)
        if d and d.get("results"):
            against = sum(x.get("expenditure_amount", 0)
                          for x in d["results"]
                          if x.get("support_oppose_indicator") == "O")
            support = sum(x.get("expenditure_amount", 0)
                          for x in d["results"]
                          if x.get("support_oppose_indicator") == "S")
            top = [
                {"committee": x.get("committee", {}).get("name", "Unknown PAC"),
                 "amount":    x.get("expenditure_amount"),
                 "so":        x.get("support_oppose_indicator")}
                for x in d["results"][:3]
            ]
            if name not in results:
                results[name] = {"cid": cid}
            results[name]["ie_against"] = against
            results[name]["ie_support"] = support
            results[name]["ie_top"]     = top
            print(f"  IE OK: {name}  against=${against:,.0f}")
        time.sleep(0.25)

    return results


# ── POLYMARKET ────────────────────────────────────────────────────────────────
def fetch_poly():
    print("Fetching Polymarket data...")
    results = {}
    headers = {
        "Authorization": f"Bearer {POLY_KEY}",
        "Content-Type":  "application/json",
    }

    for name in POLY_NAMES:
        if not POLY_KEY:
            break
        last = name.split()[-1]
        q    = urllib.parse.quote(f"{last} reelection congress")
        url  = f"{POLY_BASE}/markets?q={q}&limit=5"
        d    = fetch_json(url, headers=headers)
        if not d:
            continue

        markets = d if isinstance(d, list) else d.get("markets", [])
        for mk in markets:
            title = (mk.get("question") or mk.get("title") or "").lower()
            if last.lower() not in title:
                continue
            if not any(kw in title for kw in
                       ["reelect", "win", "congress", "senate", "house", "primary"]):
                continue
            prob = None
            try:
                prices = mk.get("outcomePrices") or "[]"
                if isinstance(prices, str):
                    prices = json.loads(prices)
                if prices:
                    prob = float(prices[0])
            except Exception:
                pass
            results[name] = {
                "question": mk.get("question") or mk.get("title") or "",
                "prob":     prob,
                "url":      mk.get("url") or "https://polymarket.com",
                "volume":   mk.get("volume"),
                "liquidity":mk.get("liquidity"),
            }
            print(f"  POLY OK: {name}  prob={prob}")
            break
        time.sleep(0.3)

    return results


# ── LEGISCAN ──────────────────────────────────────────────────────────────────
def fetch_legiscan():
    """
    Fetches bill details and sponsor lists from LegiScan for tracked legislation.
    Returns a dict of bill data keyed by display name.

    LegiScan free tier: 30,000 requests/month.
    Endpoints used:
      - getBill: full bill details including sponsor list
      - getVotes: roll call vote data (when available)
      - searchBill: find bill by number if ID unknown

    When your key arrives, add it as GitHub Secret LEGISCAN_KEY and it will
    activate automatically on the next Actions run.
    """
    if not LEGISCAN_KEY:
        print("LegiScan key not yet set — skipping (add LEGISCAN_KEY secret when ready)")
        return {}

    print("Fetching LegiScan data...")
    results = {}

    # First: search for HR 3565 Block the Bombs Act in 119th Congress (2025-26)
    search_url = (f"{LEGISCAN_BASE}/?key={LEGISCAN_KEY}"
                  f"&op=search&state=US&query=Block+the+Bombs+Act&year=2&page=1")
    d = fetch_json(search_url)

    if d and d.get("status") == "OK":
        bills = d.get("searchresult", {})
        for key, bill in bills.items():
            if not isinstance(bill, dict):
                continue
            title = (bill.get("title") or "").lower()
            number = (bill.get("bill_number") or "").lower()
            # Match HR 3565 or Block the Bombs
            if ("block the bombs" in title or "3565" in number):
                bill_id = bill.get("bill_id")
                if bill_id:
                    print(f"  LegiScan found: {bill.get('bill_number')} id={bill_id}")
                    # Fetch full bill details
                    detail_url = (f"{LEGISCAN_BASE}/?key={LEGISCAN_KEY}"
                                  f"&op=getBill&id={bill_id}")
                    detail = fetch_json(detail_url)
                    if detail and detail.get("status") == "OK":
                        b = detail.get("bill", {})
                        sponsors = [
                            {"name": s.get("name"), "role": s.get("sponsor_type_id")}
                            for s in b.get("sponsors", [])
                        ]
                        results["HR 3565 — Block the Bombs Act"] = {
                            "bill_id":     bill_id,
                            "bill_number": b.get("bill_number"),
                            "title":       b.get("title"),
                            "status":      b.get("status"),
                            "sponsors":    sponsors,
                            "sponsor_count": len(sponsors),
                            "last_action": b.get("last_action"),
                            "last_action_date": b.get("last_action_date"),
                            "url":         b.get("url"),
                        }
                        print(f"  LegiScan bill detail OK — {len(sponsors)} sponsors")
                break
        time.sleep(0.5)

    # Search for Senate JRDs on Israel arms
    jrd_url = (f"{LEGISCAN_BASE}/?key={LEGISCAN_KEY}"
               f"&op=search&state=US&query=joint+resolution+Israel+arms&year=2&page=1")
    d2 = fetch_json(jrd_url)
    if d2 and d2.get("status") == "OK":
        bills2 = d2.get("searchresult", {})
        for key, bill in bills2.items():
            if not isinstance(bill, dict):
                continue
            title = (bill.get("title") or "").lower()
            if "israel" in title and ("joint resolution" in title or "foreign military" in title):
                bill_id = bill.get("bill_id")
                print(f"  LegiScan JRD found: {bill.get('bill_number')} id={bill_id}")
                results.setdefault("jrds", []).append({
                    "bill_id":     bill_id,
                    "bill_number": bill.get("bill_number"),
                    "title":       bill.get("title"),
                    "url":         bill.get("url"),
                })
        time.sleep(0.5)

    return results


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    # Load existing data so we can merge/preserve on partial failures
    existing = {}
    try:
        with open("data.json") as f:
            existing = json.load(f)
    except Exception:
        pass

    fec       = fetch_fec()       if FEC_KEY      else existing.get("fec",       {})
    poly      = fetch_poly()      if POLY_KEY      else existing.get("poly",      {})
    legiscan  = fetch_legiscan()                   # handles missing key internally

    # Preserve existing legiscan data if new fetch returned nothing
    if not legiscan and existing.get("legiscan"):
        legiscan = existing["legiscan"]
        print("  LegiScan: using cached data from previous run")

    data = {
        "meta": {
            "fetched_at":     datetime.now(timezone.utc).isoformat(),
            "fec_count":      len(fec),
            "poly_count":     len(poly),
            "legiscan_bills": len(legiscan),
            "version":        3,
        },
        "fec":      fec,
        "poly":     poly,
        "legiscan": legiscan,
    }

    with open("data.json", "w") as f:
        json.dump(data, f, indent=2)

    print(f"\n✓ data.json written")
    print(f"  FEC records:      {len(fec)}")
    print(f"  Polymarket:       {len(poly)}")
    print(f"  LegiScan bills:   {len(legiscan)}")


if __name__ == "__main__":
    main()
