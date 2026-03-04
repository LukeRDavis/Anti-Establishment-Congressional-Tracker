#!/usr/bin/env python3
"""
fetch_data.py
Runs in GitHub Actions every 4 hours.
Reads API keys from environment variables (GitHub Secrets).
Writes data.json to repo root — never exposes keys to client.
"""

import os, json, time, urllib.request, urllib.parse, urllib.error
from datetime import datetime, timezone

FEC_KEY  = os.environ.get("FEC_KEY", "")
POLY_KEY = os.environ.get("POLY_KEY", "")
FEC_BASE = "https://api.open.fec.gov/v1"
POLY_BASE= "https://gamma-api.polymarket.com"

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
    "Chip Roy":                   "H8TX21099",
    "Lauren Boebert":             "H0CO03197",
    "Warren Davidson":            "H6OH08170",
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
    "Nida Allam":                 "H6NC04229",
    "Summer Lee":                 "H2PA18134",
    "Kat Abughazaleh":            "H6IL05246",
    "Abdul el-Sayed":             "H6MI11197",
    "Maxwell Frost":              "H2FL10186",
}

IE_TARGETS = [
    "Jamaal Bowman", "Cori Bush", "Rashida Tlaib",
    "Ilhan Omar", "Thomas Massie", "Alexandria Ocasio-Cortez",
    "Summer Lee", "Nida Allam",
]

POLY_NAMES = [
    "Thomas Massie", "Rand Paul", "Alexandria Ocasio-Cortez",
    "Rashida Tlaib", "Ilhan Omar", "Ayanna Pressley",
    "Marjorie Taylor Greene", "Lauren Boebert", "Chip Roy",
    "Summer Lee", "Jamie Raskin", "Ro Khanna", "Scott Perry",
    "James Talarico", "Nida Allam", "Kat Abughazaleh",
]


def fetch_json(url, headers=None):
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print(f"  WARN: {url[:80]} → {e}")
        return None


def fetch_fec():
    print("Fetching FEC data...")
    results = {}
    for name, cid in FEC_IDS.items():
        if not FEC_KEY:
            break
        url = f"{FEC_BASE}/candidate/{cid}/totals/?api_key={FEC_KEY}&cycle=2024&per_page=1"
        d = fetch_json(url)
        t = (d or {}).get("results", [None])[0]
        if t:
            results[name] = {
                "raised":    t.get("receipts"),
                "spent":     t.get("disbursements"),
                "cash":      t.get("last_cash_on_hand_end_period"),
                "cid":       cid,
            }
            print(f"  FEC OK: {name}")
        time.sleep(0.25)  # be polite to FEC rate limits

    # Schedule E — outside independent expenditures
    for name in IE_TARGETS:
        cid = FEC_IDS.get(name)
        if not cid or not FEC_KEY:
            continue
        url = (f"{FEC_BASE}/schedules/schedule_e/"
               f"?api_key={FEC_KEY}&candidate_id={cid}&cycle=2024&per_page=10&sort=-expenditure_amount")
        d = fetch_json(url)
        if d and d.get("results"):
            against = sum(x.get("expenditure_amount", 0) for x in d["results"] if x.get("support_oppose_indicator") == "O")
            support = sum(x.get("expenditure_amount", 0) for x in d["results"] if x.get("support_oppose_indicator") == "S")
            top = [
                {"committee": x.get("committee", {}).get("name", "Unknown PAC"),
                 "amount": x.get("expenditure_amount"),
                 "so": x.get("support_oppose_indicator")}
                for x in d["results"][:3]
            ]
            if name not in results:
                results[name] = {"cid": cid}
            results[name]["ie_against"] = against
            results[name]["ie_support"] = support
            results[name]["ie_top"]     = top
            print(f"  IE OK: {name}  against=${against:,.0f}  for=${support:,.0f}")
        time.sleep(0.25)

    return results


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
            if not any(kw in title for kw in ["reelect", "win", "congress", "senate", "house"]):
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
                "url":      mk.get("url") or f"https://polymarket.com",
                "volume":   mk.get("volume"),
                "liquidity":mk.get("liquidity"),
            }
            print(f"  POLY OK: {name}  prob={prob}")
            break
        time.sleep(0.3)

    return results


def main():
    # Load existing data.json so we can merge rather than overwrite
    existing = {}
    try:
        with open("data.json") as f:
            existing = json.load(f)
    except Exception:
        pass

    fec  = fetch_fec()  if FEC_KEY  else existing.get("fec",  {})
    poly = fetch_poly() if POLY_KEY else existing.get("poly", {})

    data = {
        "meta": {
            "fetched_at":   datetime.now(timezone.utc).isoformat(),
            "fec_count":    len(fec),
            "poly_count":   len(poly),
            "version":      2,
        },
        "fec":  fec,
        "poly": poly,
    }

    with open("data.json", "w") as f:
        json.dump(data, f, indent=2)

    print(f"\n✓ data.json written — {len(fec)} FEC records, {len(poly)} Polymarket records")


if __name__ == "__main__":
    main()
