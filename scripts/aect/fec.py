"""
Step 2 — every person who registered a federal campaign for this cycle, plus
their money. This is the ground-truth candidate list: nobody enters the
tracker by hand and nobody is dropped because a tracker didn't list them.

Endpoints (OpenFEC):
  /candidates/search/            all H+S filers for the cycle, with committees
  /candidates/totals/            receipts / spending / cash for every filer
  /schedules/schedule_e/by_candidate/   outside spending for & against
"""
import re, time

from .config import FEC_API, FEC_KEY, CYCLE, TTL_FEC_H
from .net import get_json, stream, qs, load_cache, save_cache, now_iso, age_hours

_SUFFIX = re.compile(r"\b(JR|SR|II|III|IV|MR|MRS|MS|DR|HON|REV|PHD|MD|ESQ)\b\.?", re.I)


def fec_display_name(raw):
    """'NORWOOD, CALEB GABRIEL MR.' → ('Caleb Norwood', 'Caleb Gabriel Norwood')."""
    raw = _SUFFIX.sub("", raw or "").strip(" ,.")
    if "," in raw:
        last, rest = [p.strip() for p in raw.split(",", 1)]
    else:
        parts = raw.split()
        last, rest = (parts[-1], " ".join(parts[:-1])) if parts else ("", "")
    firsts = [w for w in rest.replace(".", " ").split() if len(w) > 1 or w.isalpha()]
    nick = re.search(r'"([^"]+)"', raw)
    first = nick.group(1) if nick else (firsts[0] if firsts else "")
    tc = lambda s: " ".join(w.capitalize() if not re.match(r"^(mc|mac|o')", w, re.I)
                            else w[:2].capitalize() + w[2:].capitalize() for w in s.split())
    last_tc = "-".join(tc(x) for x in last.split("-"))
    short = f"{tc(first)} {last_tc}".strip()
    full = f"{tc(' '.join(firsts))} {last_tc}".strip()
    return short, full


def _party(code):
    code = (code or "").upper()
    return {"DEM": "D", "REP": "R", "IND": "I", "DFL": "D", "NPA": "I", "NON": "I",
            "LIB": "L", "GRE": "G", "GRN": "G"}.get(code, code[:1] or "?")


def fetch_candidates(force=False):
    cache = load_cache("fec_candidates")
    if not FEC_KEY:
        stream("fec_candidates", ok=False, note="FEC_KEY not provided — using cached filer list"
               if cache.get("candidates") else "FEC_KEY not provided — no filer list")
        return cache.get("candidates", {})
    if not force and cache.get("cycle") == CYCLE and age_hours(cache.get("fetched_at")) < TTL_FEC_H:
        stream("fec_candidates", ok=True, count=len(cache["candidates"]), note="cache fresh")
        return cache["candidates"]

    out = {}
    for office in ("H", "S"):
        page, pages = 1, 1
        while page <= pages:
            url = f"{FEC_API}/candidates/search/?" + qs({
                "api_key": FEC_KEY, "election_year": CYCLE, "office": office,
                "per_page": 100, "page": page, "sort": "name", "is_active_candidate": "true"})
            d = get_json(url, stream_name="fec_candidates")
            if not d:
                break
            pages = d.get("pagination", {}).get("pages", 1)
            for c in d.get("results", []):
                cid = c.get("candidate_id")
                if not cid:
                    continue
                short, full = fec_display_name(c.get("name", ""))
                try:
                    dist = int(c.get("district") or 0) if office == "H" else None
                except ValueError:
                    dist = None
                out[cid] = {
                    "fec_id": cid, "name": short, "full_name": full, "fec_name": c.get("name", ""),
                    "party": _party(c.get("party")), "party_full": c.get("party_full", ""),
                    "state": c.get("state", ""), "district": dist,
                    "chamber": "house" if office == "H" else "senate",
                    "incumbent_challenge": c.get("incumbent_challenge"),   # I / C / O
                    "candidate_status": c.get("candidate_status"),
                    "has_raised_funds": c.get("has_raised_funds"),
                    "committee_ids": [p.get("committee_id") for p in c.get("principal_committees", [])
                                      if p.get("committee_id")],
                    "first_file_date": c.get("first_file_date"),
                    "last_file_date": c.get("last_file_date"),
                }
            page += 1
            time.sleep(0.15)
    if out:
        save_cache("fec_candidates", {"cycle": CYCLE, "fetched_at": now_iso(), "candidates": out})
        stream("fec_candidates", ok=True, count=len(out),
               note=f"{len(out)} active {CYCLE} House+Senate filers")
    else:
        stream("fec_candidates", ok=False, note="no filers returned — see errors")
        out = cache.get("candidates", {})
    return out


def fetch_totals(force=False):
    """Receipts / disbursements / cash for every filer this cycle (bulk, ~2 req/100 filers)."""
    cache = load_cache("fec_totals")
    if not FEC_KEY:
        stream("fec_finance", ok=False, note="FEC_KEY not provided")
        return cache.get("totals", {})
    if not force and cache.get("cycle") == CYCLE and age_hours(cache.get("fetched_at")) < TTL_FEC_H:
        stream("fec_finance", ok=True, count=len(cache["totals"]), note="cache fresh")
        return cache["totals"]
    out = {}
    for office in ("H", "S"):
        page, pages = 1, 1
        while page <= pages:
            url = f"{FEC_API}/candidates/totals/?" + qs({
                "api_key": FEC_KEY, "election_year": CYCLE, "office": office,
                "per_page": 100, "page": page, "sort": "-receipts", "election_full": "true"})
            d = get_json(url, stream_name="fec_finance")
            if not d:
                break
            pages = d.get("pagination", {}).get("pages", 1)
            for t in d.get("results", []):
                cid = t.get("candidate_id")
                if cid:
                    out[cid] = {
                        "raised": t.get("receipts"), "spent": t.get("disbursements"),
                        "cash": t.get("cash_on_hand_end_period"),
                        "individual": t.get("individual_itemized_contributions"),
                        "pac": t.get("other_political_committee_contributions"),
                        "coverage_end": t.get("coverage_end_date"),
                    }
            page += 1
            time.sleep(0.15)
    if out:
        save_cache("fec_totals", {"cycle": CYCLE, "fetched_at": now_iso(), "totals": out})
        stream("fec_finance", ok=True, count=len(out), note=f"totals for {len(out)} filers")
    else:
        stream("fec_finance", ok=False, note="no totals returned")
        out = cache.get("totals", {})
    return out


def fetch_outside_spending(fec_ids, budget=400):
    """Independent expenditures for/against each tracked candidate, with the spenders named."""
    cache = load_cache("fec_ie")
    if not FEC_KEY:
        stream("fec_outside_spending", ok=False, note="FEC_KEY not provided")
        return cache.get("ie", {})
    ie = cache.get("ie", {})
    stamps = cache.get("stamps", {})
    todo = [c for c in fec_ids if age_hours(stamps.get(c)) > TTL_FEC_H][:budget]
    for cid in todo:
        url = f"{FEC_API}/schedules/schedule_e/by_candidate/?" + qs({
            "api_key": FEC_KEY, "candidate_id": cid, "cycle": CYCLE, "per_page": 100})
        d = get_json(url, stream_name="fec_outside_spending")
        if d is None:
            break
        rec = {"support": 0.0, "oppose": 0.0, "spenders": []}
        for r in d.get("results", []):
            amt = float(r.get("total") or 0)
            side = "support" if r.get("support_oppose_indicator") == "S" else "oppose"
            rec[side] += amt
            rec["spenders"].append({"committee": r.get("committee_name", ""),
                                    "committee_id": r.get("committee_id", ""),
                                    "side": side, "amount": amt})
        rec["spenders"].sort(key=lambda s: -s["amount"])
        rec["spenders"] = rec["spenders"][:12]
        ie[cid] = rec
        stamps[cid] = now_iso()
        time.sleep(0.15)
    save_cache("fec_ie", {"ie": ie, "stamps": stamps})
    stream("fec_outside_spending", ok=bool(ie), count=len(ie),
           note=f"refreshed {len(todo)}, {len(ie)} candidates with outside-spending records")
    return ie
