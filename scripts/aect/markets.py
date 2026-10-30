"""
Prediction markets, stored per person in markets.json.

  • PredictIt   /api/marketdata/all/        (one call, every market)
  • Polymarket  clob.polymarket.com/markets (paginated catalog; Gamma blocks CI IPs)
  • Kalshi      trade-api/v2/events         (paginated, nested markets)

Matching is name-and-race based: a market is attached to a person only if
their full name appears (or their last name plus their state/district), the
market is about a {CYCLE} congressional race, and it isn't about some other
office. Each match records which race stage it prices (primary / general /
nomination) so the site can label it.
"""
import re, time

from .config import PREDICTIT_ALL, POLY_CLOB, KALSHI_API, CYCLE, STATE_NAMES
from .net import get_json, stream, now_iso

OTHER_OFFICE = re.compile(
    r"\b(president|presidential|white house|governor|gubernatorial|mayor|attorney general|"
    r"secretary of state|lieutenant governor|speaker|majority leader|minority leader|running mate|"
    r"vice president|cabinet|ambassador|resign|expelled|censure|impeach|indicted|tweet|say\b|mention)",
    re.I)
WRONG_YEAR = re.compile(r"\b20(?:1\d|2[0-4]|2[7-9]|3\d)\b")


def _words(s):
    return set(re.findall(r"[a-z]+", (s or "").lower()))


def matches(text, person):
    t = text or ""
    tl = t.lower()
    if OTHER_OFFICE.search(t):
        return False
    if WRONG_YEAR.search(t) and str(CYCLE) not in t:
        return False
    w = _words(t)
    names = person["name"].split()
    if not names:
        return False
    first = names[0].lower()
    last = ((person.get("last_name") or names[-1]).lower().split() or [names[-1].lower()])[-1]
    if last not in w:
        return False
    if first in w:
        return True
    state = person.get("state", "")
    full = STATE_NAMES.get(state, "").lower()
    in_state = (full and full in tl) or re.search(rf"\b{state}\b", t)
    if person.get("chamber") == "house" and person.get("district") is not None:
        d = person["district"]
        in_race = re.search(rf"\b{state}-?0?{d}\b|\b{d}(?:st|nd|rd|th) (?:congressional )?district", t, re.I)
        return bool(in_state and in_race)
    return bool(in_state and "senate" in tl)


def stage(text):
    tl = (text or "").lower()
    if "runoff" in tl:
        return "runoff"
    if "primary" in tl or "nomination" in tl or "nominee" in tl:
        return "primary"
    return "general"


# ── Catalog loaders ───────────────────────────────────────────────────────────
def load_predictit():
    d = get_json(PREDICTIT_ALL, stream_name="market_predictit")
    out = []
    for m in (d or {}).get("markets", []):
        for c in m.get("contracts", []) or []:
            price = c.get("bestBuyYesCost") or c.get("lastTradePrice")
            try:
                price = float(price)
            except (TypeError, ValueError):
                continue
            if not 0 < price <= 1:
                continue
            out.append({"src": "predictit_contract", "market": m.get("name", ""),
                        "outcome": c.get("name", ""), "prob": round(price, 4),
                        "url": m.get("url") or f"https://www.predictit.org/markets/detail/{m.get('id','')}",
                        "market_id": m.get("id"), "contract_id": c.get("id")})
    stream("market_predictit", ok=bool(out), count=len(out), note=f"{len(out)} live contracts")
    return out


def load_polymarket(max_pages=60):
    out, cursor, pages = [], "", 0
    headers = {"Origin": "https://polymarket.com", "Referer": "https://polymarket.com/"}
    while pages < max_pages:
        url = f"{POLY_CLOB}/markets" + (f"?next_cursor={cursor}" if cursor else "")
        d = get_json(url, stream_name="market_polymarket", headers=headers)
        if not d:
            break
        for m in d.get("data", []) or []:
            if m.get("closed") or not m.get("active", True):
                continue
            q = m.get("question", "")
            if "congress" not in q.lower() and "house" not in q.lower() and "senate" not in q.lower() \
               and "primary" not in q.lower() and "district" not in q.lower():
                continue
            yes = next((t for t in m.get("tokens", []) or [] if (t.get("outcome") or "").lower() == "yes"), None)
            if not yes:
                continue
            try:
                price = float(yes.get("price"))
            except (TypeError, ValueError):
                continue
            out.append({"src": "polymarket_live", "market": q, "outcome": "Yes", "prob": round(price, 4),
                        "token_id": yes.get("token_id"), "condition_id": m.get("condition_id"),
                        "url": f"https://polymarket.com/event/{m.get('market_slug','')}"})
        cursor = d.get("next_cursor") or ""
        pages += 1
        if not cursor or cursor == "LTE=":
            break
        time.sleep(0.1)
    stream("market_polymarket", ok=pages > 0, count=len(out),
           note=f"{pages} catalog pages scanned · {len(out)} open congressional markets")
    return out


def load_kalshi(max_pages=40):
    out, cursor, pages = [], "", 0
    while pages < max_pages:
        url = f"{KALSHI_API}/events?status=open&with_nested_markets=true&limit=200" + \
              (f"&cursor={cursor}" if cursor else "")
        d = get_json(url, stream_name="market_kalshi")
        if not d:
            break
        for ev in d.get("events", []) or []:
            title = ev.get("title", "")
            cat = (ev.get("category") or "").lower()
            if cat and cat not in ("politics", "elections", "us elections", "u.s. politics"):
                continue
            for m in ev.get("markets", []) or []:
                outcome = m.get("yes_sub_title") or m.get("subtitle") or m.get("title") or ""
                price = None
                for f, scale in (("yes_ask_dollars", 1), ("last_price_dollars", 1),
                                 ("yes_ask", 100), ("last_price", 100)):
                    v = m.get(f)
                    try:
                        if v not in (None, "", 0, "0"):
                            price = float(v) / scale
                            break
                    except (TypeError, ValueError):
                        continue
                if price is None or not 0 < price <= 1:
                    continue
                out.append({"src": "kalshi", "market": f"{title} {ev.get('sub_title','')}".strip(),
                            "outcome": outcome, "prob": round(price, 4), "ticker": m.get("ticker"),
                            "url": f"https://kalshi.com/markets/{(ev.get('series_ticker') or '').lower()}"})
        cursor = d.get("cursor") or ""
        pages += 1
        if not cursor:
            break
        time.sleep(0.1)
    stream("market_kalshi", ok=pages > 0, count=len(out), note=f"{pages} pages · {len(out)} political markets")
    return out


# ── Matching ──────────────────────────────────────────────────────────────────
PRIORITY = {"polymarket_live": 0, "kalshi": 1, "predictit_contract": 2}


def match_people(people):
    """people: {key: person}. Returns {key: {"markets": [...], "best": {...}}}."""
    catalog = load_polymarket() + load_kalshi() + load_predictit()
    out = {}
    for key, p in people.items():
        hits = []
        for c in catalog:
            # multi-outcome markets (Kalshi/PredictIt) name the person in the outcome;
            # binary Polymarket markets name them in the question
            text = f"{c['market']} {c['outcome']}" if c["outcome"] not in ("Yes", "") else c["market"]
            if c["src"] != "polymarket_live" and not matches(c["outcome"] + " " + c["market"], p):
                continue
            if c["src"] == "polymarket_live" and not matches(text, p):
                continue
            # for outcome-style markets the outcome itself must be this person
            if c["src"] != "polymarket_live":
                ow = _words(c["outcome"])
                last = ((p.get("last_name") or p["name"]).lower().split() or [""])[-1]
                if not last or last not in ow:
                    continue
            hits.append({**c, "stage": stage(c["market"]), "question": f"{c['market']} → {c['outcome']}"
                         if c["outcome"] not in ("Yes", "") else c["market"], "fetched_at": now_iso()})
        if hits:
            hits.sort(key=lambda h: (PRIORITY.get(h["src"], 9), h["stage"] != "general"))
            out[key] = {"markets": hits[:6], "best": hits[0]}
    stream("markets", ok=bool(catalog), count=len(out),
           note=f"{len(catalog)} market outcomes loaded · {len(out)} tracked people matched")
    return out
