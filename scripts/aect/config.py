"""
Runtime configuration. Contains NO data about people, races, votes or bills —
only API locations, credentials (from env), cache lifetimes and the election
calendar arithmetic that every cycle shares.
"""
import os
from datetime import date

# ── Credentials (all optional; each missing key is reported, never fatal) ────
def _env(*names):
    for n in names:
        v = os.environ.get(n, "").strip()
        if v:
            return v
    return ""

CONGRESS_KEY   = _env("CONGRESS_KEY", "CONGRESS_API_KEY")
FEC_KEY        = _env("FEC_KEY", "FEC_API_KEY")
GEMINI_KEY     = _env("GEMINI_KEY", "GEMINI_API_KEY", "GOOGLE_API_KEY")
XAI_KEY        = _env("XAI_API_KEY", "XAI_KEY", "GROK_API_KEY")
OPENROUTER_KEY = _env("OPENROUTER_API_KEY", "OPENROUTER_KEY")
GROQ_KEY       = _env("GROQ_API_KEY", "GROQ_KEY")
LEGISCAN_KEY   = _env("LEGISCAN_KEY", "LEGISCAN_API_KEY")

KEYS = {
    "CONGRESS_KEY": CONGRESS_KEY, "FEC_KEY": FEC_KEY, "GEMINI_KEY": GEMINI_KEY,
    "XAI_API_KEY": XAI_KEY, "OPENROUTER_API_KEY": OPENROUTER_KEY,
    "GROQ_API_KEY": GROQ_KEY, "LEGISCAN_KEY": LEGISCAN_KEY,
}

# ── Election calendar (derived from today's date, never typed in) ─────────────
TODAY = date.today()

def congress_for_year(year: int) -> int:
    """The 1st Congress began in 1789; each Congress spans two years."""
    return (year - 1789) // 2 + 1

CURRENT_CONGRESS = congress_for_year(TODAY.year)
# The election cycle being contested is the even year that ends this Congress.
CYCLE = TODAY.year if TODAY.year % 2 == 0 else TODAY.year + 1
NEXT_CONGRESS = CURRENT_CONGRESS + 1
SESSION = 1 if TODAY.year % 2 == 1 else 2

def congress_years(congress: int):
    first = 1789 + (congress - 1) * 2
    return (first, first + 1)

# How many past Congresses to rebuild vote history for (historical tracking).
HISTORY_CONGRESSES = int(os.environ.get("AECT_HISTORY_CONGRESSES", "3"))

# ── Source locations ──────────────────────────────────────────────────────────
CLERK_MEMBERS_XML  = "https://clerk.house.gov/xml/lists/MemberData.xml"
CLERK_ROLL_XML     = "https://clerk.house.gov/evs/{year}/roll{roll:03d}.xml"
CLERK_ROLL_PAGE    = "https://clerk.house.gov/Votes/{year}{roll}"
SENATE_MEMBERS_XML = "https://www.senate.gov/general/contact_information/senators_cfm.xml"
SENATE_VOTE_MENU   = "https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_{congress}_{session}.xml"
SENATE_VOTE_XML    = "https://www.senate.gov/legislative/LIS/roll_call_votes/vote{congress}{session}/vote_{congress}_{session}_{vote:05d}.xml"
CONGRESS_API       = "https://api.congress.gov/v3"
FEC_API            = "https://api.open.fec.gov/v1"
TRACKAIPAC_CONGRESS     = "https://www.trackaipac.com/congress"
TRACKAIPAC_ENDORSEMENTS = "https://www.trackaipac.com/endorsements"
RLC_NEWS           = "https://rlc.org/category/news/national/"
RLC_INDEX_PAGES    = ["https://rlc.org/candidates/house-liberty-index-scorecards/",
                      "https://rlc.org/candidates/senate-liberty-index-scorecards/"]
RLC_ENDORSE_PAGE   = "https://rlc.org/news/{cycle}-national-endorsements/"
BALLOTPEDIA        = "https://ballotpedia.org"
WIKIPEDIA_SUMMARY  = "https://en.wikipedia.org/api/rest_v1/page/summary/{slug}"
GROKIPEDIA         = "https://grokipedia.com/page/{slug}"
PREDICTIT_ALL      = "https://www.predictit.org/api/marketdata/all/"
POLY_CLOB          = "https://clob.polymarket.com"
POLY_GAMMA         = "https://gamma-api.polymarket.com"
KALSHI_API         = "https://api.elections.kalshi.com/trade-api/v2"

# ── Cache lifetimes (hours) ───────────────────────────────────────────────────
TTL_ROSTER_H     = 4
TTL_FEC_H        = 12
TTL_ENRICH_H     = 72
TTL_AI_PERSON_H  = 24 * 14   # AI position research per candidate
TTL_TRACKERS_H   = 12

# ── Budgets (per run) so free API tiers are never exhausted ───────────────────
AI_BUDGET_PER_RUN      = int(os.environ.get("AECT_AI_BUDGET", "120"))
ENRICH_BUDGET_PER_RUN  = int(os.environ.get("AECT_ENRICH_BUDGET", "250"))
ROLL_FETCH_BUDGET      = int(os.environ.get("AECT_ROLL_BUDGET", "1500"))

# Wall-clock budget: AI work stops after this many minutes so a slow or
# rate-limited provider can never push the run past the workflow timeout.
import time as _time
RUN_STARTED = _time.time()
AI_DEADLINE_MIN = float(os.environ.get("AECT_AI_DEADLINE_MIN", "45"))

def ai_time_left():
    return (RUN_STARTED + AI_DEADLINE_MIN * 60) - _time.time()

USER_AGENT = ("Mozilla/5.0 (compatible; AECT/10.0; "
              "+https://github.com/LukeRDavis/anti-establishment-congressional-tracker)")

STATE_NAMES = {
    "AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California",
    "CO":"Colorado","CT":"Connecticut","DE":"Delaware","FL":"Florida","GA":"Georgia",
    "HI":"Hawaii","ID":"Idaho","IL":"Illinois","IN":"Indiana","IA":"Iowa","KS":"Kansas",
    "KY":"Kentucky","LA":"Louisiana","ME":"Maine","MD":"Maryland","MA":"Massachusetts",
    "MI":"Michigan","MN":"Minnesota","MS":"Mississippi","MO":"Missouri","MT":"Montana",
    "NE":"Nebraska","NV":"Nevada","NH":"New Hampshire","NJ":"New Jersey","NM":"New Mexico",
    "NY":"New York","NC":"North Carolina","ND":"North Dakota","OH":"Ohio","OK":"Oklahoma",
    "OR":"Oregon","PA":"Pennsylvania","RI":"Rhode Island","SC":"South Carolina",
    "SD":"South Dakota","TN":"Tennessee","TX":"Texas","UT":"Utah","VT":"Vermont",
    "VA":"Virginia","WA":"Washington","WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming",
    "DC":"District of Columbia","PR":"Puerto Rico","GU":"Guam","VI":"Virgin Islands",
    "AS":"American Samoa","MP":"Northern Mariana Islands",
}
STATE_ABBR = {v.lower(): k for k, v in STATE_NAMES.items()}
VOTING_STATES = {k for k in STATE_NAMES if k not in ("DC","PR","GU","VI","AS","MP")}


# ── What this tracker measures ────────────────────────────────────────────────
# This is the tracker's definition, not data: a member is classified by how
# they vote on military aid / arms sales to these recipients (the AIPAC lens).
# Which roll calls those are is decided each run by the AI judge from the
# official vote records; war-powers and other foreign-aid votes are still
# recorded and shown, but they do not decide the dot colour.
FOCUS_CATEGORY   = os.environ.get("AECT_FOCUS_CATEGORY", "arms_aid")
FOCUS_RECIPIENTS = {c.strip().lower() for c in
                    os.environ.get("AECT_FOCUS_RECIPIENTS", "Israel").split(",") if c.strip()}
