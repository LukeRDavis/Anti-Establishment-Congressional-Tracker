#!/usr/bin/env python3
"""
fetch_data.py — GitHub Actions, every 4 hours.

Anti-Intervention classification pipeline:
  Democrats/Independents:
    TrackAIPAC /congress "Track AIPAC Approved" → antiArms=True, level=soft
    Congress.gov bill co-sponsorship            → upgrade to hard
    LegiScan Yes vote on anti-intervention bill         → upgrade to hard

  Republicans:
    RLC Liberty Index score ≥ 75               → antiArms=True, level=soft
    Congress.gov bill co-sponsorship            → upgrade to hard
    Congress.gov house-vote No on pro-arms      → upgrade to hard
    KNOWN_ANTI_INTERVENTION override (118th record)     → sets level directly

  Both:
    KNOWN_ANTI_INTERVENTION dict                        → authoritative override

Challengers:
  TrackAIPAC /endorsements                     → anti-intervention D/I challengers
  RLC /endorsements blog posts                 → anti-intervention R challengers
  FEC /candidates 2026                         → cross-reference for FEC IDs

Win Probability Sources (priority order, no extra keys needed):
  1. Polymarket CLOB live    polymarket.com (no key needed for read)
  2. PredictIt               predictit.org/api/marketdata/all/ (public, no auth)
  3. Metaculus               metaculus.com/api2/questions/ (public, no auth)
  4. Manual estimate         RACES_2026 hardcoded win_prob fields

GitHub Secrets (optional — pipeline degrades gracefully without them):
  CONGRESS_KEY   api.congress.gov
  FEC_KEY        api.open.fec.gov
  POLY_KEY       polymarket.com (optional — Level 0 read is public, no key needed)
  LEGISCAN_KEY   legiscan.com (optional)
  GEMINI_KEY     AI classification (optional)
  OPENROUTER_API_KEY  AI classification fallback (optional)
  GROQ_API_KEY        AI classification fallback (optional)
"""

import os, io, json, re, time, hashlib, urllib.request, urllib.parse, urllib.error
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

CONGRESS_KEY  = os.environ.get("CONGRESS_KEY",  "")
XAI_KEY       = os.environ.get("XAI_API_KEY",   "")  # Grok web-search enrichment
FEC_KEY       = os.environ.get("FEC_KEY",       "")
POLY_KEY      = os.environ.get("POLY_KEY",      "")
LEGISCAN_KEY  = os.environ.get("LEGISCAN_KEY",  "")

# ── AI Classification keys ────────────────────────────────────────────────────
GEMINI_KEY      = os.environ.get("GEMINI_KEY",       "")
OPENROUTER_KEY  = os.environ.get("OPENROUTER_API_KEY","")
GROQ_KEY        = os.environ.get("GROQ_API_KEY",     "")

# ─────────────────────────────────────────────────────────────────────────────
# AI CLASSIFICATION ENGINE
# Tier 1: Google Gemini 2.0 Flash (free, 1500 req/day via AI Studio key)
# Tier 2: OpenRouter free models  (no daily hard limit on free tier)
# Tier 3: Groq free tier          (llama-3.1-8b-instant, very fast)
# Tier 4: Hardcoded KNOWN_ANTI_INTERVENTION dict (always available)
#
# Anti-intervention standard (strict):
#   QUALIFY:     Voted No on Israel/Ukraine arms authorization (HR 8034/8035 etc.)
#                Co-sponsored HR 3565 Block the Bombs Act
#                Forced or co-sponsored Senate JRD disapproving Israel FMS
#                Voted No on any Senate arm sale resolution
#   DO NOT QUALIFY: Signed letters, called for review, called for conditions,
#                   statements, press releases, "concerned" quotes
# ─────────────────────────────────────────────────────────────────────────────

_AI_SYSTEM_PROMPT = """You are a strict congressional vote analyst classifying US Congress members as anti-intervention (True/False).

CLASSIFICATION STANDARD — True ONLY if the member has at least ONE of:
1. On-record NO vote on Israel or Ukraine arms authorization (e.g. HR 8034, HR 8035, NDAA Israel provisions)
2. Co-sponsored HR 3565 (Block the Bombs Act) or equivalent anti-arms legislation
3. Forced or co-sponsored a Senate Joint Resolution of Disapproval on Israel FMS sales
4. Voted NO on any Senate privileged resolution approving Israel arms sales

DO NOT classify True for:
- Signed letters or wrote op-eds about arms conditions
- Called for arms review or pause (without a vote or bill co-sponsorship)
- Said they are "concerned" about civilian casualties
- Endorsed by TrackAIPAC but has no documented vote or bill action
- General anti-war or anti-interventionist statements

Return ONLY valid JSON — an array of objects, one per member:
[{"name": "Full Name", "antiArms": true/false, "confidence": "high/medium/low", "reason": "one sentence citing specific vote or bill, or explaining why not qualified"}]

Be conservative. When in doubt, return false."""

_AI_USER_TEMPLATE = """Classify these Congress members using the strict anti-intervention standard. 
Use their documented vote records and bill co-sponsorships only. 
If a member's only evidence is a statement, letter, or general position, classify false.

Members to classify:
{members_json}

Return only the JSON array, no other text."""


def _post_json(url, payload, headers):
    """POST JSON payload, return parsed response or None."""
    try:
        data = json.dumps(payload).encode()
        req  = urllib.request.Request(url, data=data, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        body = ""
        try: body = e.read().decode()[:200]
        except: pass
        print(f"    HTTP {e.code}: {body}")
        return None
    except Exception as e:
        print(f"    Request error: {e}")
        return None


def _call_gemini(members_batch):
    """Call Gemini 2.0 Flash. Returns list of classification dicts or None."""
    if not GEMINI_KEY:
        return None
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_KEY}"
    payload = {
        "system_instruction": {"parts": [{"text": _AI_SYSTEM_PROMPT}]},
        "contents": [{"parts": [{"text": _AI_USER_TEMPLATE.format(
            members_json=json.dumps(members_batch, indent=2)
        )}]}],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": 4096,
            "responseMimeType": "application/json",
        },
    }
    resp = _post_json(url, payload, {"Content-Type": "application/json"})
    if not resp:
        return None
    try:
        text = resp["candidates"][0]["content"]["parts"][0]["text"]
        return json.loads(text)
    except Exception as e:
        print(f"    Gemini parse error: {e}")
        return None


def _call_openrouter(members_batch):
    """Call OpenRouter free model. Returns list or None."""
    if not OPENROUTER_KEY:
        return None
    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization":  f"Bearer {OPENROUTER_KEY}",
        "Content-Type":   "application/json",
        "HTTP-Referer":   "https://lukerdavis.github.io/Anti-Establishment-Congressional-Tracker/",
        "X-Title":        "Anti-Establishment Congressional Tracker",
    }
    payload = {
        "model": "meta-llama/llama-3.3-70b-instruct:free",
        "messages": [
            {"role": "system",  "content": _AI_SYSTEM_PROMPT},
            {"role": "user",    "content": _AI_USER_TEMPLATE.format(
                members_json=json.dumps(members_batch, indent=2)
            )},
        ],
        "temperature": 0.1,
        "max_tokens":  4096,
        "response_format": {"type": "json_object"},
    }
    resp = _post_json(url, payload, headers)
    if not resp:
        return None
    try:
        text = resp["choices"][0]["message"]["content"]
        parsed = json.loads(text)
        # OpenRouter json_object wraps in a key sometimes
        if isinstance(parsed, dict):
            for v in parsed.values():
                if isinstance(v, list): return v
            return None
        return parsed
    except Exception as e:
        print(f"    OpenRouter parse error: {e}")
        return None


def _call_groq(members_batch):
    """Call Groq llama-3.1-8b-instant. Returns list or None."""
    if not GROQ_KEY:
        return None
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_KEY}",
        "Content-Type":  "application/json",
    }
    payload = {
        "model": "llama-3.1-8b-instant",
        "messages": [
            {"role": "system",  "content": _AI_SYSTEM_PROMPT},
            {"role": "user",    "content": _AI_USER_TEMPLATE.format(
                members_json=json.dumps(members_batch, indent=2)
            )},
        ],
        "temperature":   0.1,
        "max_tokens":    4096,
        "response_format": {"type": "json_object"},
    }
    resp = _post_json(url, payload, headers)
    if not resp:
        return None
    try:
        text = resp["choices"][0]["message"]["content"]
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            for v in parsed.values():
                if isinstance(v, list): return v
            return None
        return parsed
    except Exception as e:
        print(f"    Groq parse error: {e}")
        return None


def ai_classify_members(members, existing_ai_cache=None):
    """
    Run AI classification on members whose status needs verification.
    
    Targets:
      - Any member with antiArms=True but no vote record or bill co-sponsorship
        (i.e. only TrackAIPAC endorsement or RLC score — these need AI verification)
      - Members in KNOWN_ANTI_INTERVENTION with weak reasons (letters/statements)
    
    Falls back gracefully through Gemini → OpenRouter → Groq → hardcoded logic.
    Caches results keyed by member name to avoid re-classifying on every run.
    Returns updated members list and the ai_cache dict.
    """
    if not any([GEMINI_KEY, OPENROUTER_KEY, GROQ_KEY]):
        print("  AI classification: no API keys available — using hardcoded logic only")
        return members, existing_ai_cache or {}

    cache = dict(existing_ai_cache or {})
    cache_ttl_hours = 48  # re-classify every 48h

    # Identify members needing AI review
    WEAK_REASONS = [
        "signed letters", "called for", "review", "conditions",
        "concerned", "statements", "press release", "called on",
        "urged", "asked for",
    ]
    # Explicitly skip — we already have ground-truth votes for these
    SKIP_NAMES = set(KNOWN_ANTI_INTERVENTION.keys())

    to_classify = []
    now = datetime.now(timezone.utc)

    for m in members:
        name = m["name"]
        if name in SKIP_NAMES:
            continue  # Already handled by KNOWN_ANTI_INTERVENTION

        # Check cache freshness
        cached = cache.get(name)
        if cached:
            try:
                age_h = (now - datetime.fromisoformat(cached["ts"])).total_seconds() / 3600
                if age_h < cache_ttl_hours:
                    continue  # Fresh cache hit — skip
            except: pass

        # Only review members currently flagged as anti-intervention
        # (we don't want AI to discover new ones — that's a future feature)
        if not m.get("antiArms"):
            continue

        # Check if they have hard evidence already
        has_vote = bool(m.get("votes"))
        has_bill = bool(m.get("bills"))
        if has_vote or has_bill:
            continue  # Verified by bill/vote data — skip AI

        note = (m.get("note") or "").lower()
        needs_review = any(w in note for w in WEAK_REASONS)
        # Also review members with no note at all (only TrackAIPAC flag)
        if not needs_review and not note:
            needs_review = True

        if needs_review:
            to_classify.append({
                "name":    name,
                "party":   m.get("party","?"),
                "chamber": m.get("chamber","?"),
                "state":   m.get("state","?"),
                "note":    m.get("note",""),
                "source":  "TrackAIPAC endorsed" if not m.get("rlc_score") else f"RLC Liberty Index {m.get('rlc_score')}",
            })

    if not to_classify:
        print(f"  AI classification: all {len(members)} members verified — nothing to review")
        return members, cache

    print(f"\n  AI classification: reviewing {len(to_classify)} members needing verification …")

    # Batch into chunks of 20 to stay within token limits
    BATCH_SIZE = 20
    results_by_name = {}

    for batch_start in range(0, len(to_classify), BATCH_SIZE):
        batch = to_classify[batch_start:batch_start + BATCH_SIZE]
        batch_names = [b["name"] for b in batch]
        print(f"    Batch {batch_start//BATCH_SIZE + 1}: {', '.join(batch_names[:4])}{'...' if len(batch_names)>4 else ''}")

        result = None

        # Tier 1: Gemini
        if GEMINI_KEY and result is None:
            print("    → Trying Gemini 2.0 Flash …")
            result = _call_gemini(batch)
            if result: print(f"    ✓ Gemini: {len(result)} classifications")
            else:      print("    ✗ Gemini failed")

        # Tier 2: OpenRouter
        if OPENROUTER_KEY and result is None:
            print("    → Trying OpenRouter (llama-3.3-70b) …")
            result = _call_openrouter(batch)
            if result: print(f"    ✓ OpenRouter: {len(result)} classifications")
            else:      print("    ✗ OpenRouter failed")

        # Tier 3: Groq
        if GROQ_KEY and result is None:
            print("    → Trying Groq (llama-3.1-8b) …")
            result = _call_groq(batch)
            if result: print(f"    ✓ Groq: {len(result)} classifications")
            else:      print("    ✗ Groq failed")

        if not result:
            print("    ✗ All AI providers failed for this batch — keeping existing classification")
            continue

        for item in result:
            name = item.get("name","")
            if name:
                results_by_name[name] = item
        time.sleep(1)  # rate limit courtesy pause

    # Apply AI results to member list
    changed = 0
    for m in members:
        name = m["name"]
        if name in results_by_name:
            ai = results_by_name[name]
            old_anti = m["antiArms"]
            new_anti = bool(ai.get("antiArms", old_anti))
            confidence = ai.get("confidence","medium")
            reason     = ai.get("reason","")

            # Only demote if high confidence — low/medium keeps existing classification
            if old_anti and not new_anti and confidence != "high":
                new_anti = old_anti  # Keep — AI not confident enough to demote

            if new_anti != old_anti:
                print(f"    {'✓ Confirmed' if new_anti else '✗ Removed'} [{confidence}]: {name} — {reason[:70]}")
                m["antiArms"] = new_anti
                if not new_anti:
                    m["antiArmsLevel"] = None
                changed += 1

            if reason and not m.get("note"):
                m["note"] = reason

            # Cache the result
            cache[name] = {
                "antiArms":   new_anti,
                "confidence": confidence,
                "reason":     reason,
                "ts":         now.isoformat(),
                "provider":   ai.get("_provider","unknown"),
            }

    print(f"  AI classification complete: {changed} members updated, {len(results_by_name)} reviewed")
    return members, cache


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
# level: "yes" = on-record legislative vote or bill co-sponsorship
#        "yes" = stated position, no verifiable legislative action yet
KNOWN_ANTI_INTERVENTION = {
    # Republicans — anti-interventionist No votes on Israel arms packages
    "Thomas Massie":    {"level":"yes","party":"R","reason":"Co-sponsored HR 3565 Block the Bombs Act; No on HR 8034 & HR 8035 (Apr 2024)"},
    "Warren Davidson":  {"level":"yes","party":"R","reason":"No on HR 8034 (Israel aid supplemental, Apr 2024); leads House Liberty Caucus"},
    "Andy Biggs":       {"level":"yes","party":"R","reason":"No on HR 8034 & HR 8035 (Israel/Ukraine packages, Apr 2024)"},
    "Scott Perry":      {"level":"yes","party":"R","reason":"No on HR 8034 (Israel aid supplemental, Apr 2024)"},
    "Paul Gosar":       {"level":"yes","party":"R","reason":"No on HR 8034 (Israel aid supplemental, Apr 2024)"},
    "Chip Roy":         {"level":"yes","party":"R","reason":"No on HR 8034 (Israel aid supplemental, Apr 2024)"},
    "Thomas Tiffany":   {"level":"yes","party":"R","reason":"No on HR 8034 (Israel aid supplemental, Apr 2024)"},
    "Rand Paul":        {"level":"yes","party":"R","reason":"Led Senate holds on Israel arms sales; forced vote on FMS Joint Resolution of Disapproval (2024)"},
    "Eli Crane":        {"level":"yes","party":"R","reason":"No on HR 8034 (Israel aid supplemental, Apr 2024); RLC Liberty Index 97/100"},
    "Lauren Boebert":   {"level":"yes","party":"R","reason":"No on HR 8034 (Israel aid supplemental, Apr 2024); positions have shifted"},
    "Barry Moore":      {"level":"yes","party":"R","reason":"No on some Israel aid provisions; running for Senate 2026"},
    "Bob Good":         {"level":"yes","party":"R","reason":"RLC Liberty Index 92/100; consistent anti-interventionist votes"},
    "Andrew Ogles":     {"level":"yes","party":"R","reason":"RLC Liberty Index 95/100; voted against multiple foreign aid packages"},
    "Anna Paulina Luna":{"level":"yes","party":"R","reason":"RLC Liberty Index 94/100; voted against Israel aid supplemental"},
    # Democrats/Independents — bill co-sponsorship or JRD votes
    "Ro Khanna":        {"level":"yes","party":"D","reason":"Co-sponsored HR 3565 Block the Bombs Act and Senate JRD on Israel FMS"},
    "Lloyd Doggett":    {"level":"yes","party":"D","reason":"Co-sponsored HR 3565 Block the Bombs Act"},
    "Jim McGovern":     {"level":"yes","party":"D","reason":"Co-sponsored HR 3565 Block the Bombs Act"},
    "Jan Schakowsky":   {"level":"yes","party":"D","reason":"Co-sponsored HR 3565 Block the Bombs Act"},
    "Maxine Waters":    {"level":"yes","party":"D","reason":"Co-sponsored HR 3565 Block the Bombs Act"},
    "Ed Markey":        {"level":"yes","party":"D","reason":"Co-sponsored Senate Joint Resolution of Disapproval on Israel FMS"},
    # Warren/Schatz/Raskin removed — signed letters only, no qualifying vote or bill action
    "Bernie Sanders":   {"level":"yes","party":"I","reason":"Forced Senate floor vote on Joint Resolution of Disapproval on Israel FMS (2024)"},
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


# ── Historical anti-intervention counts by Congress ────────────────────────────────────
# Sources: Congress.gov co-sponsorship, clerk.house.gov roll calls, TrackAIPAC archives
# 117th: Block the Bombs Act (HR 3103) era, pre-Gaza
# 118th: Post-Oct 7 peak — HR 8034 Nay 58 total (21R+37D)
CONGRESS_HISTORY = {
    117: {
        "congress": 117, "years": "2021–22", "label": "117th",
        "hardR": 3, "softR": 1, "hardD": 13, "softD": 6, "hardI": 1, "softI": 0,
        "note": "Block the Bombs Act (HR 3103) introduced. Squad + libertarian R opposition. Pre-Gaza conflict.",
        "key_events": ["HR 3103 Block the Bombs Act", "Rand Paul FMS Senate holds"],
        "source": "Congress.gov co-sponsorship + known positions",
    },
    118: {
        "congress": 118, "years": "2023–24", "label": "118th",
        "hardR": 9, "softR": 4, "hardD": 26, "softD": 14, "hardI": 2, "softI": 0,
        "note": "Post-Oct 7 peak. HR 8034 Nay: 21R+37D (58 total). Block the Bombs Act (HR 3565) 22 co-sponsors. Senate JRDs forced.",
        "key_events": [
            "Oct 7, 2023: Hamas attack → Gaza war → surge in anti-intervention sentiment",
            "Apr 20, 2024: HR 8034 Israel Security Supplemental — 21R+37D voted Nay",
            "Jun 2024: Bernie Sanders forced Senate JRD floor vote on Israel FMS",
            "Aug 2024: Bowman (D-NY) and Bush (D-MO) lost primaries to AIPAC-backed challengers",
        ],
        "source": "clerk.house.gov/evs/2024/roll152.xml + Congress.gov",
    },
}

# ── 2026 Key Races ─────────────────────────────────────────────────────────────
# Tracks incumbents at risk and challengers who could change the 120th Congress count
# status: "won_primary" | "pending_primary" | "pending_general" | "confirmed" | "safe"
# impact: effect on 120th Congress anti-intervention count if candidate wins
RACES_2026 = [
    # ── RESULTS CONFIRMED (March 3, 2026 primaries) ──────────────────────────
    {
        "id": "talarico-tx-senate",
        "name": "James Talarico", "party": "D", "state": "TX", "chamber": "senate",
        "anti_intervention": True, "anti_intervention_level": "yes",
        "type": "challenger",  # not currently in Congress
        "status": "pending_general",
        "primary_date": "2026-03-03", "primary_result": "WON",
        "primary_pct": 53.1, "primary_opponent_pct": 46.0,
        "primary_opponent": "Jasmine Crockett",
        "general_date": "2026-11-03",
        "general_opponent": "John Cornyn or Ken Paxton (R runoff May 26)",
        "cook_rating": None,  # too early
        "win_prob": 0.22,  # Texas hasn't gone D statewide since 1994; Trump +13 in 2024
        "impact_if_wins": "+1 soft-D Senate (major historic upset)",
        "impact_if_loses": "No change (seat stays R)",
        "note": "Won D primary 53% vs Crockett 46% (Mar 3). Supports banning offensive weapons to Israel. Will face Cornyn or Paxton after May 26 runoff.",
        "polymarket_url": None,
    },
    {
        "id": "foushee-nc04",
        "name": "Valerie Foushee", "party": "D", "state": "NC", "chamber": "house", "district": 4,
        "anti_intervention": True, "anti_intervention_level": "yes",
        "type": "incumbent",
        "status": "pending_general",  # narrowly won primary (recount pending → Allam conceded)
        "primary_date": "2026-03-03", "primary_result": "WON",
        "primary_pct": 49.18, "primary_opponent_pct": 48.22,
        "primary_opponent": "Nida Allam",
        "general_date": "2026-11-03",
        "general_opponent": "TBD (R)",
        "cook_rating": "Likely Democratic",
        "win_prob": 0.88,
        "impact_if_wins": "+1 soft-D retained (Foushee pledged to block arms to Israel this cycle)",
        "impact_if_loses": "-1 D anti-intervention seat",
        "note": "Won primary by 1,202 votes (49.18% vs Allam 48.22%). Allam conceded Mar 4. Foushee swore off AIPAC money this cycle, pledged arms restrictions legislation.",
        "polymarket_url": None,
    },
    # ── PENDING PRIMARIES ─────────────────────────────────────────────────────
    {
        "id": "massie-ky04-primary",
        "name": "Thomas Massie", "party": "R", "state": "KY", "chamber": "house", "district": 4,
        "anti_intervention": True, "anti_intervention_level": "yes",
        "type": "incumbent",
        "status": "pending_primary",
        "primary_date": "2026-05-19",
        "primary_opponent": "Ed Gallrein (Trump-endorsed, Navy SEAL)",
        "general_date": "2026-11-03",
        "general_opponent": "TBD (D)",
        "cook_rating": "Solid Republican (general)",
        "win_prob": 0.62,  # Massie claims +17 own polling; Trump machine + $10M against him
        "impact_if_wins": "Retains 1 hard-R anti-intervention vote (most consistent in Congress)",
        "impact_if_loses": "-1 hard-R (Gallrein would vote pro-arms with Trump)",
        "note": "Trump endorsed Gallrein Oct 2025. Massie claims +17 lead in own internal polling. Rand Paul campaigning with him. Cook: Solid R general. Primary May 19.",
        "polymarket_url": None,
    },
    {
        "id": "paul-ky-senate",
        "name": "Rand Paul", "party": "R", "state": "KY", "chamber": "senate",
        "anti_intervention": True, "anti_intervention_level": "yes",
        "type": "incumbent",
        "status": "safe",  # not up for election in 2026
        "primary_date": None, "general_date": None,
        "general_opponent": None,
        "cook_rating": None,
        "win_prob": None,
        "impact_if_wins": None,
        "impact_if_loses": None,
        "note": "Term ends Jan 2029 — not on 2026 ballot. Safe for this cycle.",
    },
    # ── ANTI-ARMS CHALLENGERS IN COMPETITIVE RACES ───────────────────────────
    {
        "id": "casar-tx37",
        "name": "Greg Casar", "party": "D", "state": "TX", "chamber": "house", "district": 37,
        "anti_intervention": True, "anti_intervention_level": "yes",
        "type": "incumbent",  # running in redrawn TX-37 from TX-35
        "status": "pending_primary",
        "primary_date": "2026-03-03",  # TX primary was March 3
        "primary_result": "WON",  # safe D district
        "general_date": "2026-11-03",
        "cook_rating": "Safe Democratic",
        "win_prob": 0.95,
        "impact_if_wins": "+1 soft-D retained (TX-35 redrawn to TX-37)",
        "note": "Running in redrawn TX-37. Safe D seat.",
    },
    {
        "id": "tlaib-mi12",
        "name": "Rashida Tlaib", "party": "D", "state": "MI", "chamber": "house", "district": 12,
        "anti_intervention": True, "anti_intervention_level": "yes",
        "type": "incumbent",
        "status": "pending_primary",
        "primary_date": "2026-08-04",  # MI primary
        "primary_opponent": "TBD",
        "general_date": "2026-11-03",
        "cook_rating": "Safe Democratic",
        "win_prob": 0.80,  # won 2024 primary by large margin, but well-funded challenges possible
        "impact_if_wins": "+1 soft-D retained",
        "impact_if_loses": "Likely -1 if defeated by AIPAC-backed primary challenger",
        "note": "Won 2024 primary 58% vs AIPAC-backed challenger. May face renewed challenge in 2026.",
    },
    {
        "id": "omar-mn05",
        "name": "Ilhan Omar", "party": "D", "state": "MN", "chamber": "house", "district": 5,
        "anti_intervention": True, "anti_intervention_level": "yes",
        "type": "incumbent",
        "status": "pending_primary",
        "primary_date": "2026-08-11",  # MN primary
        "general_date": "2026-11-03",
        "cook_rating": "Safe Democratic",
        "win_prob": 0.78,
        "impact_if_wins": "+1 soft-D retained",
        "note": "Survived close 2024 primary 56%. AIPAC spent heavily against her. May face challenge again.",
    },
    {
        "id": "herrera-tx23",
        "name": "Brandon Herrera", "party": "R", "state": "TX", "chamber": "house", "district": 23,
        "anti_intervention": True, "anti_intervention_level": "yes",
        "type": "challenger",
        "status": "pending_general",
        "primary_date": "2026-03-03", "primary_result": "WON",
        "primary_pct": 43.0, "primary_opponent_pct": 41.0,
        "primary_opponent": "Tony Gonzales (withdrew Mar 5 amid ethics scandal)",
        "general_date": "2026-11-03",
        "general_opponent": "Katy Padilla Stout (D)",
        "cook_rating": "Solid Republican",
        "win_prob": 0.82,
        "impact_if_wins": "+1 anti-foreign-aid R in Congress (TX-23 open seat)",
        "impact_if_loses": "No change — seat stays R, Stout not anti-intervention",
        "note": "YouTube gun influencer 'The AK Guy'. Anti-AIPAC: 'I'm not anti-Israel, I'm anti-Israel buying US elections.' Lost TX-23 primary by 354 votes in 2024. Won 2026 primary 43%-41%; Gonzales withdrew Mar 5 amid sexual coercion scandal involving a staffer who died by suicide. AIPAC did not intervene this cycle. Cook: Solid R. General vs Katy Padilla Stout (D).",
        "polymarket_url": None,
    },
]

# ── Clerk House Vote XML scraper ───────────────────────────────────────────────
CLERK_XML_BASE = "http://clerk.house.gov/evs"

# Key 118th Congress votes for historical record
HISTORICAL_VOTE_XMLS = {
    "118-hr8034": ("2024", "152"),   # Israel Security Supplemental — Nay = anti-intervention
    "118-hr8035": ("2024", "153"),   # Ukraine/Israel combined — Nay = anti-intervention
}




def fetch_clerk_vote_xmls():
    """
    Scrapes Clerk of the House XML roll call files for historical votes.
    Returns dict: vote_id → {desc, date, nays: [{name_id, name, party, state}], ...}
    Free, no API key needed.
    """
    import xml.etree.ElementTree as ET
    results = {}

    for vote_id, (year, roll) in HISTORICAL_VOTE_XMLS.items():
        url = f"{CLERK_XML_BASE}/{year}/roll{roll.zfill(3)}.xml"
        print(f"  Fetching Clerk XML: roll {roll} ({year}) …")
        try:
            data = fetch_bytes(url)
            if not data:
                print(f"  Clerk XML fetch failed for {vote_id}")
                continue
            root = ET.fromstring(data)
            meta = root.find("vote-metadata")
            desc  = (meta.findtext("vote-desc") or "")[:100]
            date  = meta.findtext("action-date") or ""
            legis = meta.findtext("legis-num") or ""

            nays, yeas = [], []
            for rv in root.findall(".//recorded-vote"):
                leg  = rv.find("legislator")
                vote = (rv.findtext("vote") or "").strip().upper()
                if leg is None: continue
                entry = {
                    "name_id": leg.get("name-id", ""),
                    "name":    leg.text or "",
                    "party":   leg.get("party", ""),
                    "state":   leg.get("state", ""),
                }
                if "NAY" in vote or "NO" in vote:
                    nays.append(entry)
                elif "YEA" in vote or "AYE" in vote or "YES" in vote:
                    yeas.append(entry)

            results[vote_id] = {
                "id": vote_id, "desc": desc, "legis": legis,
                "date": date, "yeas": len(yeas), "nays": len(nays),
                "nay_members": nays,
                "url": f"https://clerk.house.gov/Votes/{year}{roll}",
            }
            r_nays = sum(1 for m in nays if m["party"]=="R")
            d_nays = sum(1 for m in nays if m["party"]=="D")
            print(f"  Roll {roll}: {desc[:50]} — Nay: {len(nays)} ({r_nays}R+{d_nays}D)")
            time.sleep(0.5)
        except Exception as e:
            print(f"  Clerk XML error for {vote_id}: {e}")

    return results

# ── Utilities ─────────────────────────────────────────────────────────────────
def _safe_url(url):
    """Redact api_key values from URLs before printing to logs."""
    return re.sub(r'(api_key=)[^&]+', r'\1***', url)

def fetch_json(url, headers=None, retries=2, circuit=None):
    """Fetch JSON from url. If circuit= is set, trips that circuit on 403."""
    if circuit and not _circuit_ok(circuit):
        return None
    req = urllib.request.Request(url, headers=headers or {})
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            print(f"  HTTP {e.code}: {_safe_url(url)[:90]}")
            if e.code == 429:
                time.sleep(10)
            elif e.code in (401, 403):
                if circuit: _circuit_trip(circuit, url)
                return None
            else:
                return None
        except Exception as e:
            if attempt < retries: time.sleep(2)
            else: print(f"  WARN: {_safe_url(url)[:90]} → {e}"); return None
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
        print(f"  HTML fetch failed: {_safe_url(url)} → {e}")
        return ""

def fetch_bytes(url):
    headers = {"User-Agent": "Mozilla/5.0 (compatible; AntiArmsTracker/3.0)"}
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.read()
    except Exception as e:
        print(f"  Bytes fetch failed: {_safe_url(url)} → {e}")
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
    if anti > pro: return "anti_intervention"
    if pro > anti: return "pro_arms"
    if anti > 0:   return "anti_intervention"
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
# 1. TRACKAIPAC — Democrat/Independent anti-intervention source
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
            "antiArms":True,"antiArmsLevel":"yes",
            "ps":"GENERAL" if "General Election" in text else "UPCOMING",
            "primaryDate":primary_date,"website":website,
            "source":"trackaipac","note":"Endorsed by Citizens Against AIPAC Corruption.",
            "opponent":"","incumbentParty":"",
        })
    print(f"  TrackAIPAC /endorsements: {len(challengers)} challengers")
    return challengers


# ─────────────────────────────────────────────────────────────────────────────
# 2. RLC LIBERTY INDEX — Republican anti-intervention source
# ─────────────────────────────────────────────────────────────────────────────

def fetch_rlc_liberty_index():
    """
    Downloads and parses the RLC Liberty Index PDF.
    Returns dict: name_lower → {name, score, rlc_level}
    Members scoring ≥ RLC_SOFT_THRESHOLD are classified as soft anti-intervention.
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
                "antiArms":True,"antiArmsLevel":"yes",
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
      Any: KNOWN_ANTI_INTERVENTION    → authoritative override
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
        known     = KNOWN_ANTI_INTERVENTION.get(name)

        anti_intervention  = ta_anti or rlc_anti or bool(known)
        if known:
            level = known["level"]
        elif ta_anti:
            level = "yes"
        elif rlc_anti:
            level = "yes"
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
            "antiArms":     anti_intervention,
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
        known = KNOWN_ANTI_INTERVENTION.get(name)
        level = known["level"] if known else "yes"
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
    hard = sum(1 for m in members if m.get("antiArmsLevel")=="yes")
    soft = anti - hard
    print(f"  Members: {len(members)} total — {anti} anti-intervention ({hard} {soft} soft)")
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
                if cls=="anti_intervention" and bid in members_by_id:
                    members_by_id[bid]["antiArms"]      = True
                    members_by_id[bid]["antiArmsLevel"]  = "yes"
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
        print(f"  {'🚫' if cls=='anti_intervention' else '🔫'} {btype.upper()} {bnum}: {btitle[:55]} ({cls})")

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
                    m["antiArmsLevel"] = "yes"
                    m["votes"].append({"bill":key,"vote":"No","desc":vote.get("description","")[:60]})
                    print(f"  🗳 {m['name']} voted No on pro-arms {key}")
                elif classification == "anti_intervention" and "yea" in voted:
                    m["antiArms"]      = True
                    m["antiArmsLevel"] = "yes"
                    m["votes"].append({"bill":key,"vote":"Yes","desc":vote.get("description","")[:60]})

            time.sleep(0.3)

    print(f"  House votes: {len(vote_records)} relevant roll calls found")
    return vote_records


def enrich_hard_classification(members, bills):
    """Congress.gov /member/{bioguideId}/cosponsored-legislation upgrade soft→hard."""
    if not CONGRESS_KEY: return members
    anti_bill_ids  = {b["id"] for b in bills if b.get("classification")=="anti_intervention"}
    anti_bill_nums = {b["number"].lower() for b in bills if b.get("classification")=="anti_intervention"}
    soft_members   = [m for m in members if m.get("antiArms") and m.get("antiArmsLevel")=="yes" and m.get("bioguide_id")]
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
                m["antiArmsLevel"] = "yes"
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
                                m["antiArms"]=True; m["antiArmsLevel"]="yes"
                                m["votes"].append({"bill":b.get("bill_number",""),"vote":"No","desc":desc})
                            elif cls=="anti_intervention" and "yea" in vtxt:
                                m["antiArms"]=True; m["antiArmsLevel"]="yes"
                                m["votes"].append({"bill":b.get("bill_number",""),"vote":"Yes","desc":desc})
                vote_records[str(rid)] = {"desc":desc,"bill":b.get("bill_number",""),"date":rc.get("date",""),"yeas":rc.get("yeas",0),"nays":rc.get("nays",0)}
                time.sleep(0.3)
            time.sleep(0.4)
    return vote_records


# ─────────────────────────────────────────────────────────────────────────────
# 6. FEC + POLYMARKET
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# CIRCUIT BREAKERS — module-level; trip on first 403, skip rest of that service
# ─────────────────────────────────────────────────────────────────────────────

_CIRCUIT_OPEN = {}  # service_name → bool

def _circuit_ok(service):
    return not _CIRCUIT_OPEN.get(service, False)

def _circuit_trip(service, url=""):
    if not _CIRCUIT_OPEN.get(service):
        _CIRCUIT_OPEN[service] = True
        print(f"  ⚡ Circuit tripped: {service} — skipping remaining calls to this service")


# ─────────────────────────────────────────────────────────────────────────────
# GROKIPEDIA SCRAPER — free, no API key, Grok-written content
#
# Grokipedia (grokipedia.com) auto-generates and continuously updates
# Wikipedia-style pages using Grok. No authentication required.
#
# URL patterns:
#   Person:  https://grokipedia.com/page/First_Last
#   Race:    https://grokipedia.com/page/2026_Statename_Xth_congressional_district_election
#   Senate:  https://grokipedia.com/page/2026_United_States_Senate_election_in_State
#
# Data extracted:
#   - description: first 2-3 substantive paragraphs (who they are, why they matter)
#   - primary_result, primary_date, primary_opponent (from race page)
#   - general_opponent, cook_rating (from race page)
#   - news_headline: most recent notable sentence from page
#
# Cached 24h. Runs in parallel (8 workers). Zero API cost.
# ─────────────────────────────────────────────────────────────────────────────

GROKIPEDIA_BASE = "https://grokipedia.com/page"
_GROKIPEDIA_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Referer":         "https://grokipedia.com/",
    "DNT":             "1",
}
_GROKIPEDIA_CACHE_TTL_S = 86400  # 24 hours


def _grokipedia_slug(name):
    """Convert 'Thomas Massie' → 'Thomas_Massie' (Title_Case, underscores)."""
    return "_".join(w.capitalize() for w in name.replace("'", "").replace(".", "").split())


def _grokipedia_race_slug(name, state, chamber, district=None):
    """
    Build race page slug.
    Examples:
      Thomas Massie, KY, house, 4  →  2026_Kentuckys_4th_congressional_district_election
      Ed Markey, MA, senate        →  2026_United_States_Senate_election_in_Massachusetts
    """
    state_names = {
        "AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California",
        "CO":"Colorado","CT":"Connecticut","DE":"Delaware","FL":"Florida","GA":"Georgia",
        "HI":"Hawaii","ID":"Idaho","IL":"Illinois","IN":"Indiana","IA":"Iowa",
        "KS":"Kansas","KY":"Kentucky","LA":"Louisiana","ME":"Maine","MD":"Maryland",
        "MA":"Massachusetts","MI":"Michigan","MN":"Minnesota","MS":"Mississippi",
        "MO":"Missouri","MT":"Montana","NE":"Nebraska","NV":"Nevada","NH":"New_Hampshire",
        "NJ":"New_Jersey","NM":"New_Mexico","NY":"New_York","NC":"North_Carolina",
        "ND":"North_Dakota","OH":"Ohio","OK":"Oklahoma","OR":"Oregon","PA":"Pennsylvania",
        "RI":"Rhode_Island","SC":"South_Carolina","SD":"South_Dakota","TN":"Tennessee",
        "TX":"Texas","UT":"Utah","VT":"Vermont","VA":"Virginia","WA":"Washington",
        "WV":"West_Virginia","WI":"Wisconsin","WY":"Wyoming",
    }
    state_full = state_names.get(state.upper(), state)
    # Possessive: "Kentucky" → "Kentuckys", "Texas" → "Texass" → fix edge cases
    possessive = state_full.rstrip("s") + "s"  # Kentucky→Kentuckys, Texas→Texass
    # Grokipedia seems to use just "Texass" etc — no apostrophe in URL slug

    if chamber == "senate":
        return f"2026_United_States_Senate_election_in_{state_full}"

    ordinal = {1:"1st",2:"2nd",3:"3rd"}.get(district, f"{district}th") if district else ""
    if ordinal:
        return f"2026_{possessive}_{ordinal}_congressional_district_election"
    else:
        return f"2026_{possessive}_congressional_district_election"


def _fetch_grokipedia(slug, circuit="grokipedia"):
    """Fetch a Grokipedia page by slug. Returns HTML string or empty string."""
    if not _circuit_ok(circuit):
        return ""
    url = f"{GROKIPEDIA_BASE}/{urllib.parse.quote(slug)}"
    try:
        req = urllib.request.Request(url, headers=_GROKIPEDIA_HEADERS)
        with urllib.request.urlopen(req, timeout=20) as r:
            raw = r.read()
            # Handle gzip
            if r.info().get("Content-Encoding") == "gzip":
                import gzip
                raw = gzip.decompress(raw)
            return raw.decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return ""  # page doesn't exist yet, not a service failure
        if e.code in (403, 429, 503):
            _circuit_trip(circuit, url)
        return ""
    except Exception:
        return ""


def _extract_grokipedia_text(html):
    """
    Extract clean paragraph text from a Grokipedia HTML page.
    Returns list of paragraph strings, longest/most substantive first.
    """
    if not html:
        return []
    # Strip script/style blocks
    html = re.sub(r'<script[^>]*>.*?</script>', ' ', html, flags=re.DOTALL|re.IGNORECASE)
    html = re.sub(r'<style[^>]*>.*?</style>',  ' ', html, flags=re.DOTALL|re.IGNORECASE)
    # Extract <p> tag contents
    paras = re.findall(r'<p[^>]*>(.*?)</p>', html, re.DOTALL|re.IGNORECASE)
    result = []
    for p in paras:
        # Strip HTML tags
        text = re.sub(r'<[^>]+>', ' ', p)
        # Decode HTML entities
        text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>') \
                   .replace('&quot;', '"').replace('&#39;', "'").replace('&nbsp;', ' ')
        # Collapse whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        # Skip nav/boilerplate paragraphs
        if len(text) < 80:
            continue
        if any(skip in text.lower() for skip in ['cookie', 'privacy policy', 'terms of', 'copyright', 'all rights']):
            continue
        result.append(text)
    return result


def _extract_race_facts(html, name=""):
    """
    Parse race-specific facts from a Grokipedia race page.
    Returns dict with primary_result, primary_date, primary_opponent,
    general_opponent, cook_rating.
    """
    facts = {}
    if not html:
        return facts
    text = re.sub(r'<[^>]+>', ' ', html)
    text = re.sub(r'\s+', ' ', text)
    tl = text.lower()

    # Primary result — look for "won the primary" or "lost the primary"
    if re.search(r'\b(won|advanced|won\s+the)\s+(republican|democratic)?\s*primary', tl):
        facts["primary_result"] = "WON"
    elif re.search(r'\b(lost|defeated\s+in|eliminated\s+in)\s+(the\s+)?(republican|democratic)?\s*primary', tl):
        facts["primary_result"] = "LOST"

    # Primary date — YYYY-MM-DD or "March 3" style
    date_m = re.search(r'primary\s+(?:is\s+)?(?:set\s+for\s+|scheduled\s+(?:for\s+)?)?'
                       r'((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:,\s*\d{4})?)',
                       text, re.IGNORECASE)
    if date_m:
        raw_date = date_m.group(1).strip()
        # Try to parse to YYYY-MM-DD
        for fmt in ("%B %d, %Y", "%B %d"):
            try:
                from datetime import datetime as _dt
                d = _dt.strptime(raw_date, fmt)
                year = d.year if d.year != 1900 else 2026
                facts["primary_date"] = f"{year}-{d.month:02d}-{d.day:02d}"
                break
            except ValueError:
                pass

    # Cook rating
    cook_m = re.search(
        r'Cook\s+Political[^,\.]*[:\s]+(Safe\s+[DR](?:emocratic|epublican)?|Likely\s+[DR](?:emocratic|epublican)?|Lean\s+[DR](?:emocratic|epublican)?|Toss[- ]?up)',
        text, re.IGNORECASE
    )
    if cook_m:
        facts["cook_rating"] = cook_m.group(1).strip()

    # General opponent — "faces [Name]" or "running against [Name]"
    opp_m = re.search(
        r'(?:faces|running against|opposed by|general election against)\s+([A-Z][a-zA-Z\s\-\.]{3,30}?)(?:\s*,|\s+in\s+the)',
        text, re.IGNORECASE
    )
    if opp_m:
        opp = opp_m.group(1).strip()
        if len(opp.split()) <= 5:  # sanity check — names shouldn't be 6+ words
            facts["general_opponent"] = opp

    # Primary opponent — "challenger [Name]" or "challenged by [Name]"
    primopp_m = re.search(
        r'(?:challenger|challenged by|primary opponent|faces\s+(?:a\s+)?challenge\s+from)\s+([A-Z][a-zA-Z\s\-\.]{3,30}?)(?:\s*,|\s*\.|\s+a\s)',
        text, re.IGNORECASE
    )
    if primopp_m:
        opp = primopp_m.group(1).strip()
        if len(opp.split()) <= 5:
            facts["primary_opponent"] = opp

    return facts


def scrape_grokipedia_candidate(name, state, chamber, district=None, existing_cache=None):
    """
    Scrape Grokipedia for a candidate's person page and their race page.
    Returns merged dict with description, race facts, and fetched_at.
    Cached 24h in grokipedia_cache.
    """
    cache_key = f"{name}|{state}|{chamber}"
    existing_cache = existing_cache or {}
    cached = existing_cache.get(cache_key, {})
    if _cache_fresh(cached):
        return cache_key, cached

    result = {}

    # ── Person page ──────────────────────────────────────────────────────────
    person_slug = _grokipedia_slug(name)
    person_html = _fetch_grokipedia(person_slug)
    paras = _extract_grokipedia_text(person_html)

    if paras:
        # First 2 substantive paragraphs, capped at 500 chars total
        desc = " ".join(paras[:2])[:500].rsplit(".", 1)[0] + "."
        result["description"] = desc
        # Last paragraph as news headline (most recent info often at end)
        if len(paras) >= 3:
            result["news_headline"] = paras[-1][:200].rsplit(".", 1)[0] + "."

    # ── Race page ─────────────────────────────────────────────────────────────
    race_slug  = _grokipedia_race_slug(name, state, chamber, district)
    race_html  = _fetch_grokipedia(race_slug)
    race_facts = _extract_race_facts(race_html, name)
    result.update(race_facts)

    # Also try extracting description from race page if person page was empty
    if not result.get("description") and race_html:
        race_paras = _extract_grokipedia_text(race_html)
        if race_paras:
            result["description"] = race_paras[0][:400].rsplit(".", 1)[0] + "."

    if result:
        result["fetched_at"]   = datetime.now(timezone.utc).isoformat()
        result["_source"]      = "grokipedia"
        result["person_slug"]  = person_slug
        result["race_slug"]    = race_slug
        print(f"  📖 Grokipedia: {name}"
              + (f" | cook: {result.get('cook_rating')}" if result.get("cook_rating") else "")
              + (f" | {result.get('primary_result','')}" if result.get("primary_result") else "")
              + (" | desc ✓" if result.get("description") else " | no desc"))
    else:
        print(f"  · Grokipedia: {name} — no page found yet")

    return cache_key, result


def scrape_grokipedia_all(priority_candidates, existing_cache):
    """
    Parallel Grokipedia scrape for all priority candidates.
    priority_candidates: {name: {state, chamber, party, district}}
    Returns updated grokipedia_cache dict.
    """
    print(f"\nGrokipedia scrape: {len(priority_candidates)} candidates (8 workers, no key needed) …")

    new_cache = dict(existing_cache)

    def _worker(item):
        name, info = item
        return scrape_grokipedia_candidate(
            name,
            info.get("state", ""),
            info.get("chamber", "house"),
            info.get("district"),
            existing_cache,
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_worker, item): item[0] for item in priority_candidates.items()}
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                key, result = fut.result()
                if result:
                    new_cache[key] = result
            except Exception as e:
                print(f"  ✗ Grokipedia worker error ({name}): {e}")

    found = sum(1 for k, v in new_cache.items() if v.get("description") and k not in existing_cache)
    total = sum(1 for v in new_cache.values() if v.get("description"))
    print(f"  Grokipedia: {found} new / {total} with descriptions / {len(new_cache)} total")
    return new_cache



# ─────────────────────────────────────────────────────────────────────────────
# GROK API ENRICHMENT ENGINE (optional, additive)
# Handles the one thing Grokipedia can't: win probability estimation.
#
# Grokipedia (above) covers descriptions + race facts for FREE.
# This layer adds:
#   A. grok_enrich_candidate() — win_prob estimate with reasoning via web_search
#      Only called for candidates where Grokipedia returned no cook_rating and
#      no market (Polymarket/PredictIt/Metaculus) exists yet.
#   B. grok_batch_descriptions() — fallback batch descriptions for any candidate
#      Grokipedia couldn't find (page doesn't exist yet).
#
# XAI_API_KEY is OPTIONAL. Pipeline degrades gracefully without it.
# Parallelised with ThreadPoolExecutor(max_workers=8).
# ─────────────────────────────────────────────────────────────────────────────

XAI_BASE  = "https://api.x.ai/v1"
GROK_MODEL_SEARCH  = "grok-4-1-fast"   # Responses API — has web_search
GROK_MODEL_BATCH   = "grok-4"          # Chat Completions — no web search, cheaper

_GROK_CACHE_TTL_S = 86400  # 24 hours


def _grok_responses(prompt, use_web_search=True, max_tokens=600):
    """
    Call xAI /v1/responses (Grok Responses API).
    Returns the assistant text content, or None on failure.
    """
    if not XAI_KEY or not _circuit_ok("xai"):
        return None

    payload = {
        "model": GROK_MODEL_SEARCH,
        "input": [{"role": "user", "content": prompt}],
        "max_output_tokens": max_tokens,
    }
    if use_web_search:
        payload["tools"] = [{"type": "web_search"}]

    body = json.dumps(payload).encode()
    headers = {
        "Content-Type":  "application/json",
        "Authorization": f"Bearer {XAI_KEY}",
    }
    try:
        req = urllib.request.Request(
            f"{XAI_BASE}/responses", data=body, headers=headers, method="POST"
        )
        with urllib.request.urlopen(req, timeout=45) as r:
            d = json.loads(r.read().decode())
            # Responses API: output is a list of content blocks
            for block in d.get("output", []):
                if block.get("type") == "message":
                    for part in block.get("content", []):
                        if part.get("type") == "output_text":
                            return part.get("text", "")
            return None
    except urllib.error.HTTPError as e:
        body_bytes = e.read()
        msg = body_bytes.decode(errors="replace")[:200]
        print(f"  Grok HTTP {e.code}: {msg}")
        if e.code in (401, 403, 429):
            _circuit_trip("xai", f"HTTP {e.code}")
        return None
    except Exception as ex:
        print(f"  Grok error: {ex}")
        return None


def _grok_chat(messages, max_tokens=2000):
    """
    Call xAI /v1/chat/completions (OpenAI-compatible).
    Used for batch description calls (no web search needed, cheaper).
    """
    if not XAI_KEY or not _circuit_ok("xai"):
        return None

    payload = {
        "model": GROK_MODEL_BATCH,
        "messages": messages,
        "max_tokens": max_tokens,
    }
    body = json.dumps(payload).encode()
    headers = {
        "Content-Type":  "application/json",
        "Authorization": f"Bearer {XAI_KEY}",
    }
    try:
        req = urllib.request.Request(
            f"{XAI_BASE}/chat/completions", data=body, headers=headers, method="POST"
        )
        with urllib.request.urlopen(req, timeout=60) as r:
            d = json.loads(r.read().decode())
            return d["choices"][0]["message"]["content"]
    except urllib.error.HTTPError as e:
        msg = e.read().decode(errors="replace")[:200]
        print(f"  Grok chat HTTP {e.code}: {msg}")
        if e.code in (401, 403, 429):
            _circuit_trip("xai", f"HTTP {e.code}")
        return None
    except Exception as ex:
        print(f"  Grok chat error: {ex}")
        return None


def _cache_fresh(entry):
    """True if grok_cache entry was fetched within TTL."""
    ts = entry.get("fetched_at", "")
    if not ts:
        return False
    try:
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(ts)).total_seconds()
        return age < _GROK_CACHE_TTL_S
    except Exception:
        return False


def _parse_grok_json(text):
    """
    Extract and parse a JSON object from Grok's response text.
    Grok sometimes wraps JSON in markdown fences — strip them.
    """
    if not text:
        return {}
    # Strip markdown code fences
    text = re.sub(r"^```(?:json)?\s*", "", text.strip(), flags=re.MULTILINE)
    text = re.sub(r"\s*```$", "", text.strip(), flags=re.MULTILINE)
    # Find first { ... } block
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if not m:
        return {}
    try:
        return json.loads(m.group())
    except Exception:
        # Try to salvage partial JSON by truncating at last complete value
        raw = m.group()
        for end in range(len(raw), 0, -1):
            try:
                return json.loads(raw[:end] + "}")
            except Exception:
                continue
        return {}


def grok_enrich_candidate(name, info, existing_cache):
    """
    Web-search enrichment for a single candidate/race.
    Returns a structured dict; cached 24h in grok_cache.

    Fields returned:
      primary_date, primary_result, primary_pct, primary_opponent,
      general_date, general_opponent, cook_rating,
      win_prob_estimate (float 0-1), win_prob_reasoning (str),
      polymarket_slug (str or null),
      description (2 sentences),
      news_headline (1 sentence, most recent relevant news),
      fetched_at (ISO timestamp)
    """
    cache_key = f"{name}|{info.get('state','')}|{info.get('chamber','')}"
    cached = existing_cache.get(cache_key, {})
    if _cache_fresh(cached):
        return cached

    # Skip win_prob estimation if Grokipedia already gave us a cook_rating
    # (cook_rating is sufficient to derive a probability without an API call)
    gk = info.get("_grokipedia", {})
    if gk.get("cook_rating") and not info.get("needs_win_prob"):
        return {}  # Grokipedia coverage is enough; don't burn API quota

    if not XAI_KEY:
        return {}

    chamber_label = "US Senate" if info.get("chamber") == "senate" else "US House"
    state = info.get("state", "")
    party = info.get("party", "?")
    district = info.get("district", "")
    district_str = f" District {district}" if district else ""

    prompt = f"""You are a US elections analyst. Research {name} ({party}, {state}{district_str}) — their 2026 {chamber_label} race.

Search for: primary date and result, general election opponent, Cook Political Report rating, current betting market odds if any.

Return ONLY a valid JSON object — no prose, no markdown fences:
{{
  "primary_date": "YYYY-MM-DD or null",
  "primary_result": "WON or LOST or RUNOFF or PENDING or null",
  "primary_pct": 0.0,
  "primary_opponent": "Name or null",
  "general_date": "YYYY-MM-DD or null",
  "general_opponent": "Name (party) or null",
  "cook_rating": "Safe D|Likely D|Lean D|Toss-up|Lean R|Likely R|Safe R or null",
  "win_prob_estimate": 0.0,
  "win_prob_reasoning": "1 sentence explaining the estimate",
  "polymarket_slug": "exact-slug from polymarket.com/event/SLUG or null",
  "description": "Sentence 1: who this person is politically. Sentence 2: what is electorally at stake for them in 2026.",
  "news_headline": "Most recent relevant news about their 2026 race, 1 sentence, or null"
}}"""

    text = _grok_responses(prompt, use_web_search=True, max_tokens=700)
    result = _parse_grok_json(text)

    if result:
        result["fetched_at"] = datetime.now(timezone.utc).isoformat()
        result["_source"] = "grok_web_search"
        # Clamp win_prob
        wp = result.get("win_prob_estimate")
        if wp is not None:
            try:
                result["win_prob_estimate"] = max(0.01, min(0.99, float(wp)))
            except Exception:
                result.pop("win_prob_estimate", None)
        print(f"  ✓ Grok enriched: {name}"
              + (f" (cook: {result.get('cook_rating')}, p={result.get('win_prob_estimate')})" if result.get('cook_rating') else ""))
    else:
        print(f"  ✗ Grok no data: {name}")

    return result


def grok_enrich_all(priority_candidates, existing_cache):
    """
    Parallel Grok web-search enrichment for priority candidates.
    priority_candidates: dict of name → {state, chamber, party, district}
    Returns: {cache_key → enriched_dict}
    """
    if not XAI_KEY:
        print("  Grok enrichment: XAI_API_KEY not set — skipping")
        return existing_cache

    print(f"\nGrok web enrichment: {len(priority_candidates)} priority candidates (8 workers) …")

    new_cache = dict(existing_cache)

    def _worker(item):
        name, info = item
        cache_key = f"{name}|{info.get('state','')}|{info.get('chamber','')}"
        result = grok_enrich_candidate(name, info, existing_cache)
        return cache_key, result

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_worker, item): item[0] for item in priority_candidates.items()}
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                key, result = fut.result()
                if result:
                    new_cache[key] = result
            except Exception as e:
                print(f"  ✗ Grok worker error ({name}): {e}")

    matched = sum(1 for k, v in new_cache.items() if v.get("description") and k not in existing_cache)
    print(f"  Grok enrichment: {matched} new / {len(new_cache)} total in cache")
    return new_cache


def grok_batch_descriptions(members, existing_descriptions):
    """
    Single Grok call (no web search, cheap) to generate a 2-sentence
    political description for every anti-intervention member who lacks one.

    Returns: {name: "description text"}
    """
    need = [m for m in members
            if m.get("antiArms") and m["name"] not in existing_descriptions]
    if not need:
        return existing_descriptions
    if not XAI_KEY:
        return existing_descriptions

    print(f"\nGrok batch descriptions: {len(need)} members without descriptions …")

    # Process in batches of 20 to stay within token limits
    result = dict(existing_descriptions)
    BATCH = 20
    for i in range(0, len(need), BATCH):
        batch = need[i:i+BATCH]
        member_list = "\n".join(
            f"- {m['name']} ({m.get('party','?')}-{m.get('state','?')}, "
            f"{'Senate' if m.get('chamber')=='senate' else 'House'},"
            f" anti-intervention {'incumbent' if m.get('level') else ''})"
            for m in batch
        )
        prompt = f"""You are a concise political analyst. For each US politician below, write exactly 2 sentences:
Sentence 1: who they are and their key policy positions.
Sentence 2: why they matter to anti-intervention / anti-arms-sales advocates.

Return ONLY a JSON object: {{"name": "description", ...}}

Members:
{member_list}"""

        text = _grok_chat([{"role": "user", "content": prompt}], max_tokens=BATCH * 80)
        parsed = _parse_grok_json(text)
        for name, desc in (parsed or {}).items():
            if isinstance(desc, str) and len(desc) > 20:
                result[name] = desc

    print(f"  Grok descriptions: {len(result) - len(existing_descriptions)} new")
    return result


def apply_grok_enrichment_to_races(races, grok_cache):
    """
    Merge Grok-enriched race data into RACES_2026 entries.
    Only fills fields that are None/missing — never overwrites confirmed data.
    Also adds win_prob if Grok has an estimate and no market exists.
    """
    enriched = 0
    for race in races:
        name    = race["name"]
        state   = race.get("state", "")
        chamber = race.get("chamber", "house")
        key     = f"{name}|{state}|{chamber}"
        g = grok_cache.get(key, {})
        if not g:
            continue

        changed = False
        # Fill in missing fields from Grok
        fill_map = [
            ("primary_date",    "primary_date"),
            ("primary_result",  "primary_result"),
            ("primary_pct",     "primary_pct"),
            ("primary_opponent","primary_opponent"),
            ("general_date",    "general_date"),
            ("general_opponent","general_opponent"),
            ("cook_rating",     "cook_rating"),
            ("note",            "news_headline"),   # Grok headline → note if missing
        ]
        for race_field, grok_field in fill_map:
            if race.get(race_field) is None and g.get(grok_field) is not None:
                race[race_field] = g[grok_field]
                changed = True

        # Description
        if not race.get("description") and g.get("description"):
            race["description"] = g["description"]
            changed = True

        # Polymarket slug hint from Grok
        if not race.get("polymarket_url") and g.get("polymarket_slug"):
            race["polymarket_slug_hint"] = g["polymarket_slug"]
            changed = True

        # Win probability — only if no market has assigned one
        if race.get("win_prob") is None and g.get("win_prob_estimate") is not None:
            race["win_prob"]         = g["win_prob_estimate"]
            race["win_prob_src"]     = "grok_estimate"
            race["win_prob_reasoning"] = g.get("win_prob_reasoning", "")
            changed = True

        if changed:
            enriched += 1
            print(f"  ↳ Grok→race: {name} (cook={g.get('cook_rating')}, p={race.get('win_prob')})")

    print(f"  Grok enriched {enriched}/{len(races)} race entries")
    return races


def apply_grok_descriptions_to_members(members, descriptions):
    """Copy Grok-generated descriptions into each member dict."""
    applied = 0
    for m in members:
        if m.get("antiArms") and not m.get("description"):
            desc = descriptions.get(m["name"])
            if desc:
                m["description"] = desc
                applied += 1
    if applied:
        print(f"  Applied {applied} Grok descriptions to members")
    return members



# ── Market validation helper (module-level — used by all three fetchers) ──────
_MARKET_DISQUALIFY_TERMS = [
    "president","presidential","white house","governor","gubernatorial",
    "2024","2028","2030","senate majority","house majority",
    "peruvian","peru","uk","british","canada","canadian","australian","french",
    "german","mexican","chinese","russian","european","israeli","ukrainian",
    "mayor","attorney general","secretary of state","comptroller","treasurer",
    "2028 democratic","2028 republican","democratic nomination for president",
    "republican nomination for president",
]

def validate_market_for_candidate(q_text, cand_name, cand_info):
    """
    Returns (True, "ok") only if this market question plausibly refers to
    this candidate's 2026 US congressional race.

    Rules:
      1. Full name in title  OR  (last name + correct state + correct chamber)
      2. No disqualifying terms (foreign country, wrong office, wrong year)
      3. Year is 2026 or absent — rejects 2024/2028/2030
    Module-level so fetch_poly, fetch_predictit, fetch_metaculus all share it.
    """
    q      = q_text.lower()
    parts  = cand_name.split()
    first  = parts[0].lower()
    last   = parts[-1].lower()
    state  = (cand_info.get("state") or "").lower()
    chamber = cand_info.get("chamber", "house")

    for bad in _MARKET_DISQUALIFY_TERMS:
        if bad in q:
            return False, f"disqualified by '{bad}'"

    has_bad_year = any(yr in q for yr in ["2024","2028","2030","2032"])
    if has_bad_year:
        return False, "wrong year in title"

    full_name_match = (first in q and last in q)

    chamber_words = ["senate","senator"] if chamber == "senate"                     else ["house","congress","congressional","representative","rep.","district","cd-"]
    state_match    = (state in q) or (cand_info.get("state","") in q_text)
    chamber_match  = any(w in q for w in chamber_words)
    contextual_match = (last in q and state_match and chamber_match)

    if not full_name_match and not contextual_match:
        return False, f"no match (last='{last}', state='{state}')"

    return True, "ok"

def fetch_fec(members):
    if not FEC_KEY: return {}
    print("\nFetching FEC finance …")
    results = {}
    for name, cid in KNOWN_FEC_IDS.items():
        url = f"{FEC_BASE}/candidate/{cid}/totals/?api_key={FEC_KEY}&cycle=2024&per_page=1"
        d   = fetch_json(url)
        res = (d or {}).get("results", [])
        t   = res[0] if res else None
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


def fetch_poly(members, races=None):
    """
    Fetches Polymarket win probabilities for anti-intervention members + 2026 races.

    TWO-PASS STRATEGY (no API key required — all public endpoints):

    Pass 1 — Catalog sweep:
        GET gamma-api.polymarket.com/events?tag=elections&active=true&limit=200
        GET gamma-api.polymarket.com/markets?tag=us-elections&active=true&limit=200
        Builds a name→market dict covering all active election markets at once.

    Pass 2 — Per-candidate fallback (only for those not found in pass 1):
        GET gamma-api.polymarket.com/markets?q=<name>&limit=8
        GET gamma-api.polymarket.com/events?q=<name>&limit=8

    For each matched market we get the YES token_id, then:
        GET clob.polymarket.com/midpoint?token_id=<yes_token>
        → returns {"mid": "0.623"} — the live probability

    Stored per candidate in data.json["poly"]:
        prob          float   live win probability 0–1
        token_id      str     YES-outcome token ID (browser uses this for batch refresh)
        no_token_id   str     NO-outcome token ID
        condition_id  str     market condition ID
        question      str     market question text
        slug          str     Polymarket event slug (for URL)
        volume        float   total volume USD
        volume_24h    float   24h volume USD
        url           str     direct market URL
        src           str     "polymarket_live" | "polymarket_gamma"
        fetched_at    str     ISO timestamp
    """
    if not _circuit_ok("polymarket"):
        print("  Polymarket circuit open — skipping")
        return {}

    GAMMA = "https://gamma-api.polymarket.com"
    CLOB  = "https://clob.polymarket.com"
    # Gamma API blocks CI/datacenter IPs with HTTP 403. Use CLOB /markets as
    # the catalog source instead — same data, different IP allowlist.
    CLOB_MARKETS = "https://clob.polymarket.com/markets"
    POLY_HEADERS = {
        "User-Agent":      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin":          "https://polymarket.com",
        "Referer":         "https://polymarket.com/",
    }
    print("\nFetching Polymarket (CLOB catalog + CLOB midpoint, no auth) …")
    results = {}

    # Build candidate list: anti-intervention incumbents + all race entries
    candidates = {}  # name → {name, type}
    for m in members:
        if m.get("antiArms") and m["name"] not in candidates:
            candidates[m["name"]] = {"name": m["name"], "type": "member"}
    for r in (races or []):
        if r.get("name") and r["name"] not in candidates:
            candidates[r["name"]] = {"name": r["name"], "type": "race"}
    if not candidates:
        return {}

    # ── PASS 1: catalog sweep across all election markets ─────────────────
    catalog = {}  # candidate_name → market dict

    def ingest_markets(market_list):
        """Index markets by candidate full-name match with strict validation."""
        for mk in market_list:
            q = (mk.get("question") or mk.get("title") or "")
            if not q:
                continue
            for name, info in candidates.items():
                if name in catalog:
                    continue  # already matched
                ok, reason = validate_market_for_candidate(q, name, info)
                if ok:
                    catalog[name] = mk

    # CLOB /markets bulk catalog — paginate through all active markets
    # (Gamma tag endpoints 403 on CI IPs; CLOB /markets is more permissive)
    next_cursor = ""
    page = 0
    max_pages = 15  # 100 markets/page → up to 1500 markets scanned
    while page < max_pages:
        url = CLOB_MARKETS + "?active=true&closed=false&limit=100"
        if next_cursor:
            url += f"&next_cursor={urllib.parse.quote(next_cursor)}"
        d = fetch_json(url, headers=POLY_HEADERS, circuit="polymarket") or {}
        items = d.get("data") or (d if isinstance(d, list) else [])
        if not items:
            break
        ingest_markets(items)
        next_cursor = d.get("next_cursor", "")
        if not next_cursor or next_cursor == "LTE=":  # LTE= = end of pagination
            break
        page += 1
        time.sleep(0.3)

    print(f"  Catalog: {len(catalog)} named candidates found in Gamma")

    # ── PASS 2: per-candidate search for those not in catalog ─────────────
    def gamma_search_candidate(name, info):
        """Search Gamma for a specific candidate not found in catalog.
        Uses full name search and validates every result against the candidate's
        known state, chamber, and year — never returns a false match."""
        # Search by full name first (most precise), then fall back to broader queries
        queries = [
            f'"{name}" 2026',
            f'{name} 2026',
            f'{name.split()[-1]} {info.get("state","")} 2026',
            f'{name.split()[-1]} 2026 {info.get("chamber","house")}',
        ]
        for q in queries:
            # Try CLOB search first (works on CI); fall back to Gamma if needed
            for base_url in [
                f"{CLOB_MARKETS}?q={urllib.parse.quote(q)}&limit=10",
                f"{GAMMA}/markets?q={urllib.parse.quote(q)}&limit=10",
            ]:
                d = fetch_json(base_url, headers=POLY_HEADERS) or {}
                items = d.get("data") or d if isinstance(d, list) else d.get("markets", [])
                if isinstance(items, dict): items = []
                for mk in items:
                    mq = mk.get("question") or mk.get("title") or ""
                    ok, reason = validate_market_for_candidate(mq, name, info)
                    if ok:
                        return mk
            time.sleep(0.2)
        return None

    # ── Process each candidate ─────────────────────────────────────────────
    def extract_tokens(mk):
        """Extract YES/NO token IDs from a Gamma market object."""
        tokens = mk.get("tokens") or mk.get("clobTokenIds") or []
        yes_tok, no_tok = None, None
        if isinstance(tokens, list):
            for tok in tokens:
                if isinstance(tok, dict):
                    outcome = (tok.get("outcome") or "").upper()
                    tid = tok.get("token_id") or tok.get("tokenId") or ""
                    if outcome == "YES": yes_tok = tid
                    if outcome == "NO":  no_tok  = tid
                elif isinstance(tok, str):
                    if not yes_tok: yes_tok = tok
                    elif not no_tok: no_tok = tok
        return yes_tok, no_tok

    def get_midpoint(token_id):
        """Live CLOB midpoint — the actual market probability."""
        if not token_id:
            return None
        d = fetch_json(f"{CLOB}/midpoint?token_id={token_id}", headers=POLY_HEADERS, circuit="polymarket")
        if d and "mid" in d:
            try: return float(d["mid"])
            except: pass
        d2 = fetch_json(f"{CLOB}/last-trade-price?token_id={token_id}", headers=POLY_HEADERS)
        if d2 and "price" in d2:
            try: return float(d2["price"])
            except: pass
        return None

    for name, cand in candidates.items():
        last = name.split()[-1].lower()
        try:
            # Find market — catalog first, then search
            mk = catalog.get(name)
            if not mk:
                mk = gamma_search_candidate(name, cand)
                if mk:
                    print(f"  ↳ {name}: found via search")
            if not mk:
                continue

            yes_tok, no_tok = extract_tokens(mk)
            cond_id = mk.get("conditionId") or mk.get("condition_id") or ""
            slug    = mk.get("slug") or ""

            # Get live probability
            prob = None
            if yes_tok:
                prob = get_midpoint(yes_tok)
                if prob is None and no_tok:
                    p_no = get_midpoint(no_tok)
                    if p_no is not None:
                        prob = round(1.0 - p_no, 4)

            # Fallback: lastTradePrice from Gamma (snapshot, not live)
            src = "polymarket_live"
            if prob is None:
                ltp = mk.get("lastTradePrice") or mk.get("last_trade_price") or mk.get("outcomePrices")
                if ltp is not None:
                    try:
                        if isinstance(ltp, str) and ltp.startswith("["):
                            ltp = json.loads(ltp)[0]
                        prob = round(float(ltp), 4)
                        src  = "polymarket_gamma"
                    except: pass

            if prob is None or not (0 <= prob <= 1):
                continue

            results[name] = {
                "prob":          prob,
                "src":           src,
                "question":      mk.get("question") or mk.get("title") or "",
                "condition_id":  cond_id,
                "token_id":      yes_tok or "",
                "no_token_id":   no_tok  or "",
                "slug":          slug,
                "volume":        float(mk.get("volume") or 0),
                "volume_24h":    float(mk.get("volume24hr") or mk.get("volume_24hr") or 0),
                "url":           f"https://polymarket.com/event/{slug}" if slug else "https://polymarket.com",
                "fetched_at":    datetime.now(timezone.utc).isoformat(),
            }
            marker = "◉" if src == "polymarket_live" else "◎"
            print(f"  {marker} {name}: {round(prob*100)}% ({src}) vol=${results[name]['volume']:,.0f}")

        except Exception as e:
            print(f"  WARN poly {name}: {e}")
        time.sleep(0.25)

    live  = sum(1 for v in results.values() if v.get("src") == "polymarket_live")
    gamma = sum(1 for v in results.values() if v.get("src") == "polymarket_gamma")
    print(f"  Polymarket total: {len(results)} ({live} live CLOB, {gamma} Gamma snapshot)")
    return results


# ─────────────────────────────────────────────────────────────────────────────
# FORECAST FALLBACK — PredictIt + Metaculus
# Called after fetch_poly(); fills in win_prob for any candidate/race still
# missing odds. Priority chain per candidate:
#   1. Polymarket CLOB live      (real money, highest resolution)
#   2. PredictIt lastTradePrice  (real money, political focus, party-level)
#   3. Metaculus community pred  (expert crowd, individual + party questions)
#   4. Manual estimate from RACES_2026 (static, written by hand)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_predictit(candidates, existing_poly):
    """
    Scrape PredictIt public API (no auth required) for 2026 election markets.

    Endpoint: GET https://www.predictit.org/api/marketdata/all/
    Updates every ~60 seconds. Non-commercial use only per PredictIt ToS.

    Two matching strategies:
      A. Direct contract match  — contract name contains candidate last name
         → returns that candidate's win probability directly
      B. Party-level market match — market title contains state (e.g. "Texas
         Senate 2026") → returns Democratic contract's lastTradePrice as a
         D-candidate's implied prob, or Republican contract for R-candidates.

    Returns dict keyed by candidate name:
        { prob, src, question, url, fetched_at }
    """
    if not _circuit_ok("predictit"):
        print("  PredictIt circuit open — skipping")
        return {}

    PREDICTIT_ALL = "https://www.predictit.org/api/marketdata/all/"
    results = {}

    # Only process candidates not already covered by Polymarket
    need = {name: info for name, info in candidates.items()
            if name not in existing_poly}
    if not need:
        print("  PredictIt: all candidates already have Polymarket coverage, skipping")
        return {}

    print(f"\nFetching PredictIt (public, no auth) for {len(need)} candidates …")

    data = fetch_json(PREDICTIT_ALL, circuit="predictit")
    if not data or "markets" not in data:
        print("  PredictIt: no data returned")
        return {}

    markets = data["markets"]
    print(f"  PredictIt: {len(markets)} active markets")

    def best_price(contract):
        """Use bestBuyYesCost if available, else lastTradePrice."""
        p = contract.get("bestBuyYesCost") or contract.get("lastTradePrice")
        try:
            p = float(p)
            return round(p, 4) if 0 < p <= 1 else None
        except (TypeError, ValueError):
            return None

    # Pre-index markets for fast lookup
    # Index by: set of words in market name + each contract name
    for mkt in markets:
        mkt_name  = (mkt.get("name") or mkt.get("shortName") or "").lower()
        mkt_url   = mkt.get("url") or f"https://www.predictit.org/markets/detail/{mkt.get('id','')}"
        contracts = mkt.get("contracts") or []

        for cand_name, cand_info in need.items():
            if cand_name in results:
                continue

            party = cand_info.get("party", "D")

            # Strategy A: validate the overall market title against this candidate
            mkt_title = mkt.get("name") or mkt.get("shortName") or ""
            ok, reason = validate_market_for_candidate(mkt_title, cand_name, cand_info)
            if not ok:
                # Also check individual contract names combined with market title
                ok = False
                for ct in contracts:
                    ct_full = f"{mkt_title} {ct.get('name','')}"
                    ok2, _ = validate_market_for_candidate(ct_full, cand_name, cand_info)
                    if ok2:
                        ok = True
                        break
            if not ok:
                continue

            # Strategy A: individual contract name contains candidate last name
            first = cand_name.split()[0].lower()
            last  = cand_name.split()[-1].lower()
            for ct in contracts:
                ct_name = (ct.get("name") or ct.get("shortName") or "").lower()
                # Require first OR (last + some identifying context)
                if first in ct_name or (last in ct_name and len(last) > 4):
                    p = best_price(ct)
                    if p is not None:
                        results[cand_name] = {
                            "prob":       p,
                            "src":        "predictit_contract",
                            "question":   f"{mkt_title} → {ct.get('name','')}",
                            "url":        mkt_url,
                            "fetched_at": datetime.now(timezone.utc).isoformat(),
                        }
                        print(f"  ◈ {cand_name}: {round(p*100)}% (PredictIt direct match)")
                        break

            if cand_name in results:
                continue

            # Strategy B: party-level market — only use if both state AND chamber
            # appear in the market title AND the market has exactly 2 contracts (R vs D)
            mkt_lower = mkt_title.lower()
            state_lower = (cand_info.get("state") or "").lower()
            chamber = cand_info.get("chamber","house")
            chamber_words = ["senate"] if chamber == "senate" else ["house","congressional","congress"]
            state_in_title = state_lower in mkt_lower or cand_info.get("state","") in mkt_title
            chamber_in_title = any(w in mkt_lower for w in chamber_words)
            two_contracts = len(contracts) == 2

            if state_in_title and chamber_in_title and two_contracts and "2026" in mkt_title:
                target = "democratic" if party == "D" else "republican"
                for ct in contracts:
                    ct_name = (ct.get("name") or "").lower()
                    if target in ct_name:
                        p = best_price(ct)
                        if p is not None:
                            results[cand_name] = {
                                "prob":       p,
                                "src":        "predictit_party",
                                "question":   f"{mkt_title} [party proxy for {cand_name}]",
                                "url":        mkt_url,
                                "fetched_at": datetime.now(timezone.utc).isoformat(),
                            }
                            print(f"  ◇ {cand_name}: {round(p*100)}% (PredictIt party proxy)")
                            break

    unmatched = [n for n in need if n not in results]
    print(f"  PredictIt: {len(results)} matched, {len(unmatched)} unmatched")
    if unmatched[:5]:
        print(f"    Unmatched sample: {unmatched[:5]}")
    return results


def fetch_metaculus(candidates, existing_poly, existing_predictit):
    """
    Fetch Metaculus community predictions for 2026 races.
    No API key required — all public questions are readable unauthenticated.

    Two passes:
      Pass 1 — 2026 Midterms tournament sweep (project_id=3349 or search by tag)
               Bulk-fetches all open questions in the tournament, indexes by
               candidate last name / state / chamber.
      Pass 2 — Per-candidate search for those not found in Pass 1.
               GET /api2/questions/?search=<name+2026>&status=open&limit=8

    Probability field: question.community_prediction.q2 (median) for binary
    questions, or aggregations.recency_weighted.latest.centers[0] in newer API.

    Returns dict keyed by candidate name:
        { prob, src, question, url, metaculus_id, fetched_at }
    """
    if not _circuit_ok("metaculus"):
        print("  Metaculus circuit open — skipping")
        return {}

    BASE = "https://www.metaculus.com/api2"
    results = {}

    # Only process candidates not covered by Polymarket or PredictIt
    need = {name: info for name, info in candidates.items()
            if name not in existing_poly and name not in existing_predictit}
    if not need:
        print("  Metaculus: all candidates covered by upstream sources, skipping")
        return {}

    print(f"\nFetching Metaculus (public, no auth) for {len(need)} candidates …")

    def extract_prob(q):
        """
        Extract probability from a Metaculus question dict.
        Handles both api2 (community_prediction) and newer aggregation formats.
        """
        # Newer format: aggregations.recency_weighted.latest
        try:
            agg = q.get("aggregations", {})
            rw  = agg.get("recency_weighted", {})
            lat = rw.get("latest", {})
            if lat and lat.get("centers"):
                return round(float(lat["centers"][0]), 4)
        except (TypeError, ValueError, KeyError):
            pass

        # api2 format: community_prediction dict
        try:
            cp = q.get("community_prediction") or {}
            # Binary question: q2 = median
            if cp.get("q2") is not None:
                return round(float(cp["q2"]), 4)
            # Fallback: full object with history
            hist = cp.get("history") or []
            if hist:
                last = hist[-1]
                if isinstance(last, dict) and last.get("q2") is not None:
                    return round(float(last["q2"]), 4)
        except (TypeError, ValueError, KeyError):
            pass

        # metaculus_prediction (their own model)
        try:
            mp = q.get("metaculus_prediction") or {}
            if mp.get("q2") is not None:
                return round(float(mp["q2"]), 4)
        except (TypeError, ValueError, KeyError):
            pass

        return None

    def search_questions(query, limit=10):
        """Search Metaculus questions by text."""
        url = f"{BASE}/questions/?search={urllib.parse.quote(query)}&status=open&type=forecast&limit={limit}"
        d = fetch_json(url, circuit="metaculus") or {}
        return d.get("results", [])

    # ── Pass 1: 2026 Midterms tournament bulk fetch ─────────────────────────
    # Tournament slug: midterms-2026, project ID discovered dynamically
    # Try fetching the tournament questions page
    catalog = {}  # last_name_lower → question dict

    def ingest_questions(questions):
        for q in questions:
            title = q.get("title") or q.get("url_title") or ""
            for cand_name, cand_info in need.items():
                if cand_name in catalog:
                    continue
                ok, _ = validate_market_for_candidate(title, cand_name, cand_info)
                if ok:
                    catalog[cand_name] = q

    # Bulk-fetch tournament
    for project_search in ["midterms-2026", "2026 midterm", "2026 congressional"]:
        url = f"{BASE}/questions/?search={urllib.parse.quote(project_search)}&status=open&type=forecast&limit=100"
        d   = fetch_json(url, circuit="metaculus") or {}
        ingest_questions(d.get("results", []))
        time.sleep(0.5)

    print(f"  Metaculus Pass 1: {len(catalog)} candidate names found in tournament")

    # ── Pass 2: per-candidate search for those not in catalog ───────────────
    for cand_name, cand_info in need.items():
        if cand_name in results:
            continue

        last  = cand_name.split()[-1].lower()
        state = cand_info.get("state", "")

        # Check catalog first (keyed by full name)
        q = catalog.get(cand_name)

        # If not found, search directly
        if not q:
            for query in [
                f"{cand_name} 2026",
                f"{last} {state} 2026",
                f"{last} congress 2026",
                f"{last} senate 2026",
            ]:
                qs = search_questions(query, limit=8)
                for candidate_q in qs:
                    title = candidate_q.get("title") or ""
                    ok, _ = validate_market_for_candidate(title, cand_name, cand_info)
                    if ok:
                        q = candidate_q
                        break
                if q:
                    break
                time.sleep(0.3)

        if not q:
            continue

        prob = extract_prob(q)
        if prob is None or not (0 < prob < 1):
            continue

        q_id  = q.get("id", "")
        slug  = q.get("slug") or q.get("url_title") or ""
        url   = f"https://www.metaculus.com/questions/{q_id}/" if q_id else "https://www.metaculus.com"

        results[cand_name] = {
            "prob":         prob,
            "src":          "metaculus",
            "question":     q.get("title") or q.get("url_title") or "",
            "metaculus_id": q_id,
            "url":          url,
            "fetched_at":   datetime.now(timezone.utc).isoformat(),
        }
        print(f"  △ {cand_name}: {round(prob*100)}% (Metaculus: '{q.get('title','')[:60]}')")
        time.sleep(0.3)

    unmatched = [n for n in need if n not in results]
    print(f"  Metaculus: {len(results)} matched, {len(unmatched)} unmatched")
    return results


def merge_forecast_sources(poly, predictit, metaculus, races):
    """
    Merge all forecast sources into a single poly dict and update races in-place.
    Priority: Polymarket > PredictIt > Metaculus > existing manual estimate.

    Also adds a 'win_prob_src_detail' field to races for the frontend to display.
    """
    merged = dict(poly)  # start with Polymarket

    # Layer in PredictIt for any not already covered
    for name, data in predictit.items():
        if name not in merged:
            merged[name] = data
        # Also upgrade party-proxy to contract match if available
        elif (merged[name].get("src") or "").startswith("predictit_party") \
             and data.get("src") == "predictit_contract":
            merged[name] = data

    # Layer in Metaculus for any still not covered
    for name, data in metaculus.items():
        if name not in merged:
            merged[name] = data

    # Wire into races
    src_priority = {
        "polymarket_live": 0, "polymarket_gamma": 1,
        "predictit_contract": 2, "predictit_party": 3,
        "metaculus": 4,
    }
    for race in races:
        name = race["name"]
        if name not in merged:
            continue
        mk   = merged[name]
        prob = mk.get("prob")
        src  = mk.get("src", "")
        if prob is None:
            continue
        # Only overwrite if new source is equal or better priority
        existing_src = race.get("win_prob_src", "manual")
        if src_priority.get(src, 99) <= src_priority.get(existing_src, 99):
            race["win_prob"]     = round(prob, 3)
            race["win_prob_src"] = src
            race["forecast_url"] = mk.get("url", "")
            print(f"  {src} → {name}: {round(prob*100)}%")

    return merged


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
            if nm.get("antiArms"): add(nk,nm,"pro_to_anti","First detected as anti-intervention")
            continue
        if om.get("antiArms") and not nm.get("antiArms"):
            add(nk,nm,"anti_to_pro","No longer flagged as anti-intervention")
        elif not om.get("antiArms") and nm.get("antiArms"):
            add(nk,nm,"pro_to_anti","Newly flagged as anti-intervention")
        elif om.get("antiArmsLevel")=="yes" and nm.get("antiArmsLevel")=="yes":
            add(nk,nm,"soft_to_hard",f"Upgraded to Hard via co-sponsorship or vote record")
        elif om.get("antiArmsLevel")=="yes" and nm.get("antiArmsLevel")=="yes":
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
# BALLOTPEDIA — race status, opponent names, primary results
# ─────────────────────────────────────────────────────────────────────────────

def scrape_ballotpedia_race(name, state, chamber, district=None):
    """
    Attempts to scrape basic race data from Ballotpedia for a candidate.
    Returns dict with opponent, primary_result, general_opponent fields.
    Non-critical — falls back gracefully if page not found.
    """
    # Construct Ballotpedia URL slug
    name_slug  = name.lower().replace(" ", "_").replace(".", "").replace("'", "")
    state_full = {
        "AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California",
        "CO":"Colorado","CT":"Connecticut","DE":"Delaware","FL":"Florida","GA":"Georgia",
        "HI":"Hawaii","ID":"Idaho","IL":"Illinois","IN":"Indiana","IA":"Iowa",
        "KS":"Kansas","KY":"Kentucky","LA":"Louisiana","ME":"Maine","MD":"Maryland",
        "MA":"Massachusetts","MI":"Michigan","MN":"Minnesota","MS":"Mississippi",
        "MO":"Missouri","MT":"Montana","NE":"Nebraska","NV":"Nevada","NH":"New_Hampshire",
        "NJ":"New_Jersey","NM":"New_Mexico","NY":"New_York","NC":"North_Carolina",
        "ND":"North_Dakota","OH":"Ohio","OK":"Oklahoma","OR":"Oregon","PA":"Pennsylvania",
        "RI":"Rhode_Island","SC":"South_Carolina","SD":"South_Dakota","TN":"Tennessee",
        "TX":"Texas","UT":"Utah","VT":"Vermont","VA":"Virginia","WA":"Washington",
        "WV":"West_Virginia","WI":"Wisconsin","WY":"Wyoming",
    }.get(state, state)

    # Ballotpedia uses Title_Case for politician pages: /Thomas_Massie
    # Race pages use state possessive: /Kentucky%27s_4th_Congressional_District_election,_2026
    title_slug = "_".join(p.capitalize() for p in name.replace("'","").replace(".","").split())

    # Try 1: politician's own page (most likely to exist)
    url = f"https://ballotpedia.org/{title_slug}"
    html = fetch_html(url)

    if not html or "does not exist" in html.lower()[:500] or "<title>Ballotpedia</title>" in html[:200]:
        # Try 2: race-specific page
        if chamber == "senate":
            url = f"https://ballotpedia.org/United_States_Senate_election_in_{state_full},_2026"
        else:
            ordinal = "1st" if district==1 else "2nd" if district==2 else "3rd" if district==3 else f"{district}th"
            url = f"https://ballotpedia.org/{state_full}%27s_{ordinal}_Congressional_District_election,_2026"
        html = fetch_html(url)
    if not html: return {}

    result = {}

    # Primary result
    if re.search(r'won\s+the\s+primary', html, re.IGNORECASE):
        result["primary_result"] = "WON"
    elif re.search(r'lost\s+the\s+primary', html, re.IGNORECASE):
        result["primary_result"] = "LOST"
    elif re.search(r'candidate\s+in\s+the\s+primary', html, re.IGNORECASE):
        result["primary_result"] = "PENDING"

    # Opponent in general
    opp_m = re.search(r'running\s+against\s+([A-Z][a-zA-Z\s\-\.]+?)\s+in\s+the\s+(?:general|November)', html, re.IGNORECASE)
    if opp_m: result["general_opponent"] = opp_m.group(1).strip()

    # Primary opponent  
    primopp_m = re.search(r'primary\s+(?:challenger|opponent|against)\s+([A-Z][a-zA-Z\s\-\.]+?)[\.,]', html, re.IGNORECASE)
    if primopp_m: result["primary_opponent"] = primopp_m.group(1).strip()

    # Cook rating
    cook_m = re.search(r'Cook\s+Political[^:]*:\s*([A-Za-z\s]+)(?:Democratic|Republican|\.|<)', html, re.IGNORECASE)
    if cook_m: result["cook_rating"] = cook_m.group(1).strip()

    return result


def enrich_races_with_ballotpedia(races):
    """
    Enriches RACES_2026 with live data from Ballotpedia.
    Only fetches for races where status is pending (not already confirmed).
    Updates: primary_result, general_opponent, primary_opponent, cook_rating.
    """
    print("\nEnriching races with Ballotpedia data …")
    enriched = 0
    for race in races:
        # Skip if already confirmed or not up this cycle
        if race.get("status") in ("safe", "confirmed"): continue
        if race.get("primary_result") == "WON" and race.get("general_opponent") not in (None, "TBD", ""):
            continue  # already have all the data we need

        bp = scrape_ballotpedia_race(
            race["name"], race.get("state",""), race.get("chamber","house"), race.get("district")
        )
        if not bp:
            time.sleep(0.5)
            continue

        # Merge — only overwrite if BP returned a value
        if bp.get("primary_result") and not race.get("primary_result"):
            race["primary_result"] = bp["primary_result"]
        if bp.get("general_opponent") and race.get("general_opponent") in (None, "TBD", ""):
            race["general_opponent"] = bp["general_opponent"]
        if bp.get("primary_opponent") and race.get("primary_opponent") in (None, "TBD", ""):
            race["primary_opponent"] = bp["primary_opponent"]
        if bp.get("cook_rating") and not race.get("cook_rating"):
            race["cook_rating"] = bp["cook_rating"]
        if bp:
            enriched += 1
        time.sleep(0.7)

    print(f"  Ballotpedia: enriched {enriched}/{len(races)} races")
    return races


def wire_polymarket_to_races(races, poly_data):
    """
    Matches Polymarket market data to races by candidate name.
    Updates win_prob and polymarket_url fields in-place.
    """
    if not poly_data:
        return races
    for race in races:
        name = race["name"]
        if name in poly_data:
            mk = poly_data[name]
            prob = mk.get("prob")
            if prob is not None:
                race["win_prob"]       = round(prob, 3)
                race["win_prob_src"]   = "polymarket"
                race["polymarket_url"] = mk.get("url", "")
                print(f"  Poly → {name}: {round(prob*100)}%")
    return races

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

    clerk_votes  = fetch_clerk_vote_xmls() if should_scrape else existing.get("clerk_votes", {})
    bills        = discover_bills(members_by_id) or existing.get("bills", [])
    house_votes  = fetch_house_votes(members_by_id) if CONGRESS_KEY else existing.get("house_votes", {})
    members      = enrich_hard_classification(members, bills)

    # ── AI classification pass ────────────────────────────────────────────────
    # Verify TrackAIPAC/RLC-flagged members who have no vote or bill evidence.
    # Uses Gemini → OpenRouter → Groq with cached results to avoid re-runs.
    existing_ai_cache = existing.get("ai_cache", {})
    members, ai_cache = ai_classify_members(members, existing_ai_cache)

    members_by_name = {m["name"].lower(): m for m in members}
    vote_records = fetch_legiscan_votes(members_by_name) or existing.get("vote_records", {})
    history      = detect_history_changes(old_members, members, ta_congress)

    fec_cands    = fetch_fec_candidates_2026(ta_endorsed, rlc_endorsed) if FEC_KEY else []
    challengers  = merge_challengers(ta_endorsed, rlc_endorsed, fec_cands)

    fec  = fetch_fec(members) or existing.get("fec", {})

    # ── Phase 2: Priority candidate list (used by both Grokipedia + Grok API) ──
    priority_candidates = {}
    for r in RACES_2026:
        if r.get("name") and r.get("status") not in ("safe",):
            priority_candidates[r["name"]] = {
                "state": r.get("state",""), "chamber": r.get("chamber","house"),
                "party": r.get("party","D"), "district": r.get("district"),
            }
    for c in challengers:
        n = c.get("name","")
        if n and (c.get("primary_date") or c.get("anti_intervention")):
            priority_candidates[n] = {
                "state": c.get("state",""), "chamber": c.get("chamber","house"),
                "party": c.get("party","D"), "district": c.get("district"),
            }
    for m in members:
        if m.get("antiArms") and m["name"] not in priority_candidates:
            priority_candidates[m["name"]] = {
                "state": m.get("state",""), "chamber": m.get("chamber","house"),
                "party": m.get("party","D"), "district": m.get("district"),
            }

    # ── Phase 2a: Grokipedia — free, no key, runs first ──────────────────────
    existing_grokipedia_cache = existing.get("grokipedia_cache", {})
    grokipedia_cache = scrape_grokipedia_all(priority_candidates, existing_grokipedia_cache)

    # Inject grokipedia data into priority_candidates for Grok API to check
    for name, info in priority_candidates.items():
        ck = f"{name}|{info.get('state','')}|{info.get('chamber','')}"
        gk = grokipedia_cache.get(ck, {})
        info["_grokipedia"] = gk
        # If Grokipedia has no cook_rating, flag for Grok API win_prob estimation
        if not gk.get("cook_rating") and not gk.get("win_prob_estimate"):
            info["needs_win_prob"] = True

    # ── Phase 2b: Grok API — only for win_prob where Grokipedia has nothing ──
    # Also used as description fallback for candidates with no Grokipedia page.
    existing_grok_cache = existing.get("grok_cache", {})
    grok_cache = grok_enrich_all(priority_candidates, existing_grok_cache)

    # ── Phase 2c: Batch descriptions fallback ────────────────────────────────
    # For incumbents not covered by Grokipedia or Grok individual enrichment.
    existing_descriptions = existing.get("descriptions", {})
    # Pre-fill descriptions from Grokipedia cache before calling batch API
    for name in list(priority_candidates.keys()):
        ck = f"{name}|{priority_candidates[name].get('state','')}|{priority_candidates[name].get('chamber','')}"
        gk_desc = grokipedia_cache.get(ck, {}).get("description")
        if gk_desc and name not in existing_descriptions:
            existing_descriptions[name] = gk_desc
    descriptions = grok_batch_descriptions(members, existing_descriptions)

    # ── Phase 3: Build race cards, merge Grokipedia + Grok data ─────────────
    races = [dict(r) for r in RACES_2026]
    races = enrich_races_with_ballotpedia(races)            # fills what it can
    races = apply_grok_enrichment_to_races(races, grokipedia_cache)  # Grokipedia first
    races = apply_grok_enrichment_to_races(races, grok_cache)        # Grok API fills gaps
    members = apply_grok_descriptions_to_members(members, descriptions)

    # ── Phase 4: Financial odds — circuit-broken, circuit resets per run ──────
    # forecast_candidates: all anti-intervention members + races + challengers
    forecast_candidates = {}
    for m in members:
        if m.get("antiArms"):
            forecast_candidates[m["name"]] = {
                "state": m.get("state",""), "party": m.get("party","D"),
                "chamber": m.get("chamber","house"),
            }
    for r in races:
        if r.get("name"):
            forecast_candidates[r["name"]] = {
                "state": r.get("state",""), "party": r.get("party","D"),
                "chamber": r.get("chamber","house"),
            }
    for c in challengers:
        if c.get("name"):
            forecast_candidates[c["name"]] = {
                "state": c.get("state",""), "party": c.get("party","D"),
                "chamber": c.get("chamber","house"),
            }

    # Use Grok-discovered Polymarket slugs as hints for fetch_poly
    for name, info in forecast_candidates.items():
        cache_key = f"{name}|{info.get('state','')}|{info.get('chamber','')}"
        slug = grok_cache.get(cache_key, {}).get("polymarket_slug")
        if slug:
            info["polymarket_slug_hint"] = slug

    poly            = fetch_poly(members, races=races) or existing.get("poly", {})
    predictit_data  = fetch_predictit(forecast_candidates, poly)
    metaculus_data  = fetch_metaculus(forecast_candidates, poly, predictit_data)

    # Overlay Grok win_prob_estimate as lowest-priority source
    for name, info in forecast_candidates.items():
        if name not in poly and name not in predictit_data and name not in metaculus_data:
            cache_key = f"{name}|{info.get('state','')}|{info.get('chamber','')}"
            g = grok_cache.get(cache_key, {})
            if g.get("win_prob_estimate") is not None:
                poly[name] = {
                    "prob":      g["win_prob_estimate"],
                    "src":       "grok_estimate",
                    "question":  g.get("win_prob_reasoning", ""),
                    "url":       "",
                    "fetched_at": g.get("fetched_at",""),
                }

    poly  = merge_forecast_sources(poly, predictit_data, metaculus_data, races)
    races = wire_polymarket_to_races(races, poly)

    anti = sum(1 for m in members if m["antiArms"])
    grok_matched  = sum(1 for v in grok_cache.values() if v.get("description"))
    odds_covered  = sum(1 for v in poly.values() if v.get("prob") is not None)

    data = {
        "meta": {
            "fetched_at":               datetime.now(timezone.utc).isoformat(),
            "trackaipac_scraped":        datetime.now(timezone.utc).isoformat() if should_scrape else last_scraped,
            "member_count":              len(members),
            "anti_intervention_count":   anti,
            "bill_count":                len(bills),
            "challenger_count":          len(challengers),
            "history_events":            len(history.get("events",[])),
            "grok_enriched":             grok_matched,
            "odds_covered":              odds_covered,
            "version":                   8,
            "sources": {
                "members":     "trackaipac.com + rlc.org Liberty Index",
                "bills":       "api.congress.gov (auto-discovered)",
                "house_votes": "api.congress.gov /house-vote",
                "challengers": "trackaipac.com + rlc.org endorsements + FEC",
                "votes":       "legiscan.com" if LEGISCAN_KEY else "pending key",
                "fec":         "api.open.fec.gov",
                "enrichment":  "grokipedia.com (free scrape) + xai Grok (win_prob fallback)",
                "descriptions":"xai Grok (batch, no web search)",
                "polymarket":  "polymarket.com CLOB",
                "predictit":   "predictit.org (public API, no auth)",
                "metaculus":   "metaculus.com (public API, no auth)",
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
        "congress_history":    CONGRESS_HISTORY,
        "races_2026":          races,
        "clerk_votes":         clerk_votes,
        "ai_cache":            ai_cache,
        "grok_cache":          grok_cache,
        "grokipedia_cache":    grokipedia_cache,
        "descriptions":        descriptions,
    }

    with open("data.json","w") as f:
        json.dump(data, f, indent=2)

    print(f"""
✓ data.json written (v8)
  Members:           {len(members)} ({anti} anti-intervention)
  Bills:             {len(bills)}
  House votes:       {len(house_votes)} relevant roll calls
  Challengers:       {len(challengers)}
  Grokipedia:        {sum(1 for v in grokipedia_cache.values() if v.get("description"))} descriptions from free scrape
  Odds covered:      {odds_covered} candidates (Poly/PredictIt/Metaculus/Grok)
  History events:    {len(history.get("events",[]))}
""")


if __name__ == "__main__":
    main()
