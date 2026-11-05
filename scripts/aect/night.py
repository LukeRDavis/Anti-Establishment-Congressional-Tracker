"""
Election night: averaged odds, poll-closing freeze, called results.

Used by the full pipeline (every 4 h) and by scripts/election_night.py, the light
job that runs every 15 minutes around a general election.

  • Odds. Every prediction market matched to a person (Polymarket, Kalshi,
    PredictIt) is reduced to one price per source for the stage being decided,
    then averaged over the sources that have a number.
  • Freeze. Poll-closing times come from The Green Papers' polling-hours page
    for the cycle (local hours with UTC equivalents, per state). A race's odds
    stop updating one minute before its state's first polls close; the last
    snapshot taken before then is kept.
  • Results. Wikipedia election boxes (aect.results) mark each general race
    won or lost once a winner is posted.

Nothing about any person or race is written here.
"""
import re
from datetime import datetime, timedelta, timezone

from .config import CYCLE, STATE_NAMES
from .net import get_text, strip_html, stream, load_cache, save_cache, now_iso, age_hours

_MONTH_RE = ("(?:January|February|March|April|May|June|July|August|September|"
             "October|November|December)")
POLL_HOURS = "https://www.thegreenpapers.com/G{yy}/PollingHours.phtml?format=g"
FREEZE_BEFORE = timedelta(minutes=1)
_MONTHS = {m: i for i, m in enumerate(["january", "february", "march", "april", "may", "june", "july",
                                         "august", "september", "october", "november", "december"], 1)}


def _parse_dt(s):
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


def poll_closes(general_date):
    """{state: ISO UTC time the state's first polls close} for the general election on general_date."""
    cache = load_cache("poll_closes")
    if cache.get("date") == general_date and age_hours(cache.get("at")) < 24 and cache.get("states"):
        stream("poll_closes", ok=True, count=len(cache["states"]), note="cached")
        return cache["states"]
    gd = datetime.fromisoformat(general_date).date()
    text = strip_html(get_text(POLL_HOURS.format(yy=str(CYCLE)[2:]), stream_name="poll_closes") or "")
    # cut the page into one segment per state (longest names first so "West Virginia" wins over "Virginia")
    marks = []
    for code, name in sorted(STATE_NAMES.items(), key=lambda kv: -len(kv[1])):
        # a state's heading is followed closely by its first election date
        for m in re.finditer(rf"\b{re.escape(name)}\b(?=[^0-9]{{0,40}}\d{{1,2}}\s+{_MONTH_RE}\s+\d{{4}})", text):
            if not any(a <= m.start() < b for a, b, _ in marks):
                marks.append((m.start(), m.end(), code))
    marks.sort()
    out = {}
    date_re = re.compile(r"(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|"
                         r"October|November|December)\s+(\d{4})", re.I)
    for i, (start, end, code) in enumerate(marks):
        seg = text[end: marks[i + 1][0] if i + 1 < len(marks) else len(text)]
        dates = list(date_re.finditer(seg))
        for j, dm in enumerate(dates):
            try:
                d = datetime(int(dm.group(3)), _MONTHS[dm.group(2).lower()], int(dm.group(1))).date()
            except Exception:
                continue
            if d != gd:
                continue
            body = seg[dm.end(): dates[j + 1].start() if j + 1 < len(dates) else len(seg)]
            if "general" not in body.lower()[:80]:
                continue
            closes = []
            for o, c in re.findall(r"\((\d{4})\s*[-–—]\s*(\d{4})\s*UTC\)", body):
                oh, ch = int(o[:2]) * 60 + int(o[2:]), int(c[:2]) * 60 + int(c[2:])
                day = gd + timedelta(days=1) if ch <= oh else gd
                closes.append(datetime(day.year, day.month, day.day, ch // 60, ch % 60, tzinfo=timezone.utc))
            if closes and code not in out:
                out[code] = min(closes).isoformat()
    if out:
        save_cache("poll_closes", {"date": general_date, "at": now_iso(), "states": out})
    elif cache.get("date") == general_date:
        out = cache.get("states", {})
    stream("poll_closes", ok=bool(out), count=len(out),
           note=f"first poll-closing time for {len(out)} states on {general_date}")
    return out


def odds_summary(entry, stage, same_party_rival=False):
    """One price per source for this stage, averaged over the sources that have one.
    A market on which PARTY wins the seat says nothing when both finalists share a party."""
    by_src = {}
    for h in (entry or {}).get("markets", []):
        if h.get("stage") != stage or h.get("prob") is None:
            continue
        if h.get("by_party") and same_party_rival:
            continue
        src = h["src"].split("_")[0]          # polymarket_live → polymarket
        if src not in by_src:                 # markets are pre-sorted best-first
            by_src[src] = {"src": src, "prob": h["prob"], "question": h.get("question", "")[:160],
                           "url": h.get("url", "")}
    if not by_src:
        return None
    probs = [s["prob"] for s in by_src.values()]
    return {"avg": round(sum(probs) / len(probs), 4), "n": len(probs),
            "sources": list(by_src.values()), "at": now_iso(), "stage": stage}


def apply(data, mk, people, old_data=None, now=None):
    """Add poll_close, odds (averaged; frozen at poll close), opponents and general_result
    to every race in data['races'], and record the freeze/result counts in meta."""
    now = now or datetime.now(timezone.utc)
    gd = (data.get("meta") or {}).get("general_date")
    closes = poll_closes(gd) if gd else {}
    old_races = {r.get("id"): r for r in (old_data or {}).get("races", [])}
    # last pre-close odds per race, kept in their own file so no other job's copy of
    # data.json can overwrite them
    snaps = load_cache("odds_snapshots") or {}
    # everyone still in each general race, so a visitor sees who the tracked candidate faces
    seats = {}
    for k, p in people.items():
        if not p.get("on_ballot") or p.get("primary_result") != "WON":
            continue
        ch = (p.get("seeking") or {}).get("chamber", p.get("chamber"))
        d = (p.get("seeking") or {}).get("district", p.get("district")) if ch == "house" else None
        seats.setdefault((p.get("state"), ch, d), []).append(k)
    frozen = called = 0
    for r in data.get("races", []):
        p = people.get(r["id"], {})
        close = closes.get(r.get("state"))
        r["poll_close"] = close
        stage = "general" if r.get("primary_result") == "WON" or r.get("status") == "pending_general" else "primary"
        key = (r.get("state"), r.get("chamber"), r.get("district") if r.get("chamber") == "house" else None)
        rivals = [k for k in seats.get(key, []) if k != r["id"]]
        fresh = odds_summary(mk.get(r["id"]), stage,
                             same_party_rival=any(people[k].get("party") == r.get("party") for k in rivals))
        old = (old_races.get(r["id"]) or {}).get("odds")
        cdt = _parse_dt(close) if close else None
        if stage == "general" and cdt and now >= cdt - FREEZE_BEFORE:
            # polls are about to close or closed: the latest snapshot taken BEFORE the close,
            # from the snapshot file or the previous data.json; a later price never qualifies
            valid = [o for o in (snaps.get(r["id"]), old)
                     if o and o.get("at") and _parse_dt(o["at"]) and _parse_dt(o["at"]) < cdt]
            best = max(valid, key=lambda o: o["at"]) if valid else None
            r["odds"] = {**{k: v for k, v in best.items() if k != "close"}, "frozen": True} if best else None
            frozen += 1
        else:
            r["odds"] = fresh
            if fresh and stage == "general":
                # latest pre-close price; the close time lets the cache merge reject later ones
                snaps[r["id"]] = {**fresh, "close": close}
        if r.get("odds"):
            r["win_prob"] = r["odds"]["avg"]
            r["win_prob_src"] = "+".join(s["src"] for s in r["odds"]["sources"])
        else:
            r["win_prob"], r["win_prob_src"] = None, ""   # no who-wins market: don't show a stale side bet
        r["opponents"] = [{"name": people[k]["name"], "party": people[k].get("party"),
                           "anti": bool(people[k].get("antiArms") and people[k].get("antiTier") == "hard")}
                          for k in rivals][:4]
        if cdt and now < cdt or p.get("primary_result") == "LOST":
            r.pop("general_result", None)     # polls still open / not on the November ballot
        elif p.get("general_result"):
            r["general_result"] = p["general_result"]
            r["general_result_source"] = p.get("general_result_source")
            r["general_result_by"] = p.get("general_result_by")
            called += 1
    save_cache("odds_snapshots", snaps)
    meta = data.setdefault("meta", {})
    meta["poll_closes"] = closes
    meta["election_night"] = {"updated_at": now_iso(), "frozen": frozen, "called": called,
                              "odds_sources": ["polymarket", "kalshi", "predictit"]}
    stream("election_night", ok=True, count=len(data.get("races", [])),
           note=f"{frozen} races frozen at poll close · {called} called")
    return data


def in_window(general_date, now=None, before_h=36, after_h=24 * 21):
    if not general_date:
        return False
    now = now or datetime.now(timezone.utc)
    gd = datetime.fromisoformat(general_date).replace(tzinfo=timezone.utc)
    return gd - timedelta(hours=before_h) <= now <= gd + timedelta(hours=after_h)


def refresh_every(general_date, now=None):
    """Seconds between election-night refreshes: every 10 minutes until three days after the
    election, then hourly while late counts and calls trickle in."""
    now = now or datetime.now(timezone.utc)
    gd = datetime.fromisoformat(general_date).replace(tzinfo=timezone.utc)
    return 600 if now <= gd + timedelta(hours=72) else 3600


def history(hist, data):
    """Log a tracked anti-establishment candidate winning (becomes) or a counted
    anti-establishment incumbent losing (stops). Each person is logged once per cycle."""
    events = hist.setdefault("events", [])
    seen = {(e.get("member"), e.get("change"), e.get("cycle")) for e in events}
    added = 0
    for r in data.get("races", []):
        res = r.get("general_result")
        if res not in ("WON", "LOST") or r.get("antiTier") not in (None, "hard"):
            continue
        change = ("elected" if r.get("type") == "challenger" else None) if res == "WON" else \
                 ("defeated" if r.get("type") == "incumbent" else None)
        if not change or (r["name"], change, CYCLE) in seen:
            continue
        events.append({"date": now_iso()[:10], "detected_at": now_iso(), "member": r["name"],
                       "party": r.get("party"), "state": r.get("state"), "chamber": r.get("chamber"),
                       "change": change, "cycle": CYCLE,
                       "trigger": ("won the general election" if res == "WON" else "lost the general election"),
                       "source": r.get("general_result_source") or "wikipedia"})
        added += 1
    return added


def cross_check(people, settled, closes, now=None):
    """Combine Wikipedia's result (already in p['general_result']) with settled markets.
      both agree        -> called, confirmed by both
      only one has it   -> called by that source (a market call is marked unconfirmed)
      they disagree     -> not called; flagged for review
    Nothing counts before the state's polls close or for someone who lost their primary."""
    now = now or datetime.now(timezone.utc)
    stats = {"both": 0, "wikipedia": 0, "market": 0, "conflict": 0}
    for k, p in people.items():
        if p.get("primary_result") == "LOST" or not p.get("on_ballot"):
            continue
        close = closes.get(p.get("state"))
        if not close or now < datetime.fromisoformat(close):
            continue
        wiki = p.get("wiki_result")
        m = settled.get(k) or {}
        mkt = m.get("result")
        if m.get("conflict"):
            mkt = None
        if wiki and mkt and wiki != mkt:
            p.pop("general_result", None)
            p["general_result_by"], p["result_conflict"] = None, f"Wikipedia says {wiki}, settled markets say {mkt}"
            stats["conflict"] += 1
            continue
        p.pop("result_conflict", None)
        if wiki and mkt:
            p["general_result"], p["general_result_by"] = wiki, "both"
            stats["both"] += 1
        elif wiki:
            p["general_result"], p["general_result_by"] = wiki, "wikipedia"
            stats["wikipedia"] += 1
        elif mkt:
            p["general_result"], p["general_result_by"] = mkt, "market"
            p["general_result_source"] = (m.get("sources") or [{}])[0].get("url")
            stats["market"] += 1
        else:
            p.pop("general_result", None)
            p["general_result_by"] = None
        p["market_settlement"] = m.get("sources")
    # a seat can't have more winners than it has seats: market-only calls that would break that
    # are withdrawn and flagged (House: 1 per district; Senate: up to 2 when a special is held)
    seats = {}
    for k, p in people.items():
        if p.get("general_result") == "WON":
            ch = (p.get("seeking") or {}).get("chamber", p.get("chamber"))
            d = (p.get("seeking") or {}).get("district", p.get("district")) if ch == "house" else None
            seats.setdefault((ch, p.get("state"), d), []).append(p)
    up = {}                                   # Senate seats on this year's ballot, per state
    for p in people.values():
        if p.get("incumbent") and p.get("chamber") == "senate" and not p.get("not_up_this_cycle"):
            up[p.get("state")] = up.get(p.get("state"), 0) + 1
    for (ch, st, d), ws in seats.items():
        if len(ws) > (1 if ch == "house" else max(1, up.get(st, 1))):
            for p in ws:
                if p.get("general_result_by") == "market":
                    p.pop("general_result", None)
                    p["general_result_by"] = None
                    p["result_conflict"] = "settled markets named more winners than this seat has"
                    stats["market"] -= 1
                    stats["conflict"] += 1
    stream("results_cross_check", ok=True, count=sum(stats.values()),
           note=f"called by both {stats['both']} · Wikipedia only {stats['wikipedia']} · "
                f"markets only {stats['market']} · conflicting {stats['conflict']}")
    return stats

