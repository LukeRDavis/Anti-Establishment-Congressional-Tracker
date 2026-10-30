"""
Assemble one record per person (keyed by bioguide ID for members, FEC ID for
candidates) from every source, decide anti-interventionist status from
evidence, and emit:

  candidates.json  — the master per-person file (every field, every source)
  markets.json     — per-person prediction-market matches
  data.json        — the shape the website already reads
"""
import re
from datetime import date

from .config import CYCLE, CURRENT_CONGRESS, NEXT_CONGRESS, TODAY, STATE_NAMES


def _norm(s):
    import unicodedata
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z ]", "", s).strip()


def _last(name):
    parts = [p for p in _norm(name).split() if p not in ("jr", "sr", "ii", "iii", "iv")]
    return parts[-1] if parts else ""


def _past(iso):
    try:
        return date.fromisoformat(iso) <= TODAY
    except Exception:
        return False


def senate_class_up(cycle):
    return ["I", "II", "III"][((cycle - 2024) // 2) % 3]


def general_election_day(year):
    """Tuesday after the first Monday in November."""
    from datetime import timedelta
    d = date(year, 11, 1)
    first_monday = d + timedelta(days=(0 - d.weekday()) % 7)
    return (first_monday + timedelta(days=1)).isoformat()


def race_status(p):
    if p.get("ps") == "DEFEATED":
        return "defeated"
    if p.get("primary_result") == "WON":
        return "pending_general"
    if p.get("primary_date") and _past(p["primary_date"]):
        return "results_pending"
    return "pending_primary"


# ── People universe ───────────────────────────────────────────────────────────
def build_people(members, fec_cands, trackers):
    people = {}
    for m in members:
        key = f"BG:{m['bioguide_id']}" if m.get("bioguide_id") else f"SEN:{m['last_name']}|{m['state']}"
        people[key] = {**m, "key": key, "incumbent": True, "sources": [m.get("_source", "")]}

    # index members for linking their own FEC filing
    by_seat = {}
    for k, p in people.items():
        by_seat.setdefault((p["state"], p["chamber"], p.get("district")), []).append(k)
        by_seat.setdefault((p["state"], p["chamber"], None), []).append(k)

    inc_by_last = {}
    for k, p in people.items():
        inc_by_last.setdefault((p["state"], _last(p["name"])), []).append(k)

    for cid, c in fec_cands.items():
        if not (c.get("name") or "").strip():
            continue
        seat_keys = by_seat.get((c["state"], c["chamber"], c["district"] if c["chamber"] == "house" else None), [])
        linked = next((k for k in seat_keys if _last(people[k]["name"]) == _last(c["name"])), None)
        if not linked:
            # a sitting member filing for a different seat (House → Senate, or a redrawn district)
            first = (_norm(c["name"]).split() or [""])[0]
            linked = next((k for k in inc_by_last.get((c["state"], _last(c["name"])), [])
                           if (_norm(people[k]["name"]).split() or [""])[0][:3] == first[:3]), None)
        if linked:
            p = people[linked]
            p.setdefault("fec_ids", []).append(cid)
            p["sources"].append("fec")
            if (c["chamber"], c.get("district")) != (p["chamber"], p.get("district")):
                p["seeking"] = {"chamber": c["chamber"], "state": c["state"], "district": c.get("district")}
            continue
        key = f"FEC:{cid}"
        people[key] = {**c, "key": key, "incumbent": False, "fec_ids": [cid],
                       "last_name": (c["name"].split() or [""])[-1], "sources": ["fec"]}

    # tracker-listed names: attach to an existing person, or add if FEC hasn't got them
    name_index = {}
    for k, p in people.items():
        name_index.setdefault((_norm(p["name"]), p["state"]), k)
        name_index.setdefault((_last(p["name"]), p["state"], p["chamber"], p.get("district")), k)

    def attach(rec, tag):
        k = (name_index.get((_norm(rec["name"]), rec.get("state", "")))
             or name_index.get((_last(rec["name"]), rec.get("state", ""), rec.get("chamber"), rec.get("district"))))
        if not k:
            k = f"TR:{_norm(rec['name'])}|{rec.get('state','')}"
            people[k] = {"key": k, "name": rec["name"], "last_name": (rec["name"].split() or [""])[-1],
                         "party": rec.get("party", "?"), "state": rec.get("state", ""),
                         "district": rec.get("district"), "chamber": rec.get("chamber") or "house",
                         "incumbent": False, "sources": []}
            name_index[(_norm(rec["name"]), rec.get("state", ""))] = k
        p = people[k]
        p["sources"].append(tag)
        p.setdefault("tracker", {})[tag] = {x: rec[x] for x in rec if x not in ("name",)}
        if rec.get("website") and not p.get("website"):
            p["website"] = rec["website"]
        return k

    for rec in trackers.get("ta_congress", {}).values():
        k = attach(rec, "trackaipac_congress")
        p = people[k]
        p["lobby_total"] = rec.get("lobby_total", 0)
        p["pacs"] = rec.get("pacs", [])
        if p.get("party") in ("?", "", None) and rec.get("party"):
            p["party"] = rec["party"]
    for rec in trackers.get("ta_endorsed", []):
        attach(rec, "trackaipac_endorsed")
    for rec in trackers.get("rlc_endorsed", []):
        attach(rec, "rlc_endorsed")
    rlc = trackers.get("rlc_index", {})
    for p in people.values():
        r = rlc.get(_norm(p["name"]))
        if r:
            p["rlc_score"] = r["score"]
    return people


# ── Election status ───────────────────────────────────────────────────────────
def apply_status(people, enrichment, calendar, fec_cands):
    class_up = senate_class_up(CYCLE)
    filers = {cid for p in people.values() for cid in p.get("fec_ids", [])}
    for k, p in people.items():
        e = enrichment.get(k, {})
        for f in ("description", "description_source", "wikipedia_url", "ballotpedia_url"):
            if e.get(f):
                p[f] = e[f]
        if e.get("website") and not p.get("website"):
            p["website"] = e["website"]
        cal = calendar.get(p.get("state", ""), {})
        p["primary_date"] = e.get("primary_date") or cal.get("primary")
        p["runoff_date"] = cal.get("runoff")
        p["general_date"] = e.get("general_date") or general_election_day(CYCLE)

        # A primary result is only accepted when it names a date in this cycle that has passed
        pr, prd = e.get("primary_result"), e.get("primary_result_date")
        if pr and prd and _past(prd) and (not p["primary_date"] or _past(p["primary_date"])):
            p["primary_result"] = pr
            p["primary_result_source"] = e.get("primary_result_text", "")

        on_ballot = bool(p.get("fec_ids")) or (p.get("chamber") == "house" and p.get("incumbent"))
        if p["chamber"] == "senate" and p.get("incumbent") and not p.get("fec_ids"):
            on_ballot = p.get("senate_class") == class_up
        p["on_ballot"] = on_ballot

        if p.get("incumbent"):
            if e.get("retiring") or (fec_cands and not p.get("fec_ids") and p["chamber"] == "house"
                                     and p.get("primary_date") and _past(p["primary_date"])):
                p["ps"] = "RETIRING"
            if e.get("running_for_other_office") or p.get("seeking"):
                p["ps"] = "RETIRING"
                p["note"] = (f"Running for {e.get('running_for_other_office') or p['seeking']['chamber'].title()}"
                             + (f" ({p['seeking']['state']})" if p.get("seeking") else ""))
            if p.get("primary_result") in ("LOST", "WITHDREW"):
                p["ps"] = "DEFEATED"
            if not on_ballot and p["chamber"] == "senate":
                p["ps"] = "INCUMBENT"
                p["not_up_this_cycle"] = True
            p.setdefault("ps", "INCUMBENT")
        else:
            res = p.get("primary_result")
            if res == "WON":
                p["ps"] = "WON"
            elif res in ("LOST", "WITHDREW"):
                p["ps"] = "DEFEATED"
            elif p.get("primary_date") and _past(p["primary_date"]):
                p["ps"] = "RESULTS_PENDING"
            else:
                p["ps"] = "UPCOMING"
    return people


# ── Classification ────────────────────────────────────────────────────────────
def classify(people, positions):
    for k, p in people.items():
        rec = [v for v in p.get("vote_record", []) if v["significance"] == "major"]
        cur = [v for v in rec if v["congress"] == CURRENT_CONGRESS]
        basis = cur or rec
        anti_votes = [v for v in basis if v["anti"]]
        pos = positions.get(k, {})
        evidence = []
        level = None
        if anti_votes:
            ratio = len(anti_votes) / len(basis)
            level = "yes" if ratio >= 0.5 else "soft"
            latest = max(anti_votes, key=lambda v: v["date"])
            evidence.append(f"{latest['position']} on {latest['measure']} ({latest['date']}): {latest['summary']}")
            p["anti_vote_ratio"] = round(ratio, 2)
            p["anti_votes"] = len(anti_votes)
            p["relevant_votes"] = len(basis)
        elif pos.get("anti_interventionist"):
            level = "yes" if pos.get("strength") == "strong" else "soft"
            evidence += [f"{e['claim']} ({e['url']})" for e in pos.get("evidence", [])[:2]]
        p["antiArms"] = level is not None
        p["antiArmsLevel"] = level
        p["antiArmsBasis"] = ("votes" if anti_votes else "statements" if level else None)
        p["antiArmsEvidence"] = evidence
        if pos:
            p["position_research"] = pos
        if not p.get("note") and evidence:
            p["note"] = evidence[0][:180]
    return people


# ── Outputs ───────────────────────────────────────────────────────────────────
def _best_market(p, mk):
    hits = (mk or {}).get("markets") or []
    if not hits:
        return None
    want = "primary" if (p.get("primary_date") and not _past(p["primary_date"])) else "general"
    hits = sorted(hits, key=lambda h: (h["stage"] != want, {"polymarket_live": 0, "kalshi": 1}.get(h["src"], 2)))
    return hits[0]


def frontend_data(people, markets, fec_totals, fec_ie, history, relevant_votes, streams, meta_extra):
    members, candidates, races, poly, fec, descriptions = [], [], [], {}, {}, {}
    for k, p in people.items():
        if p.get("description"):
            descriptions[p["name"]] = p["description"]
        bm = _best_market(p, markets.get(k))
        if bm:
            poly[p["name"]] = {"prob": bm["prob"], "src": bm["src"], "question": bm["question"],
                               "url": bm["url"], "token_id": bm.get("token_id"),
                               "condition_id": bm.get("condition_id"), "stage": bm["stage"],
                               "fetched_at": bm["fetched_at"]}
        tot = {}
        for cid in p.get("fec_ids", []):
            t = fec_totals.get(cid) or {}
            for f in ("raised", "spent", "cash"):
                try:
                    v = float(t.get(f))
                except (TypeError, ValueError):
                    continue
                tot[f] = (tot.get(f) or 0) + v
            ie = fec_ie.get(cid)
            if ie:
                tot["ie_support"] = tot.get("ie_support", 0) + ie["support"]
                tot["ie_against"] = tot.get("ie_against", 0) + ie["oppose"]
                tot["ie_spenders"] = ie["spenders"][:6]
            tot["cid"] = cid
        if tot:
            fec[p["name"]] = tot

        base = {"name": p["name"], "party": p.get("party") or "?", "state": p.get("state", ""),
                "district": p.get("district"), "chamber": p.get("chamber", "house"),
                "antiArms": p["antiArms"], "antiArmsLevel": p["antiArmsLevel"],
                "antiArmsBasis": p["antiArmsBasis"], "evidence": p["antiArmsEvidence"],
                "ps": p.get("ps", "UPCOMING"), "note": p.get("note", ""),
                "website": p.get("website", ""), "fec_ids": p.get("fec_ids", []),
                "primaryDate": p.get("primary_date"), "primary_result": p.get("primary_result"),
                "endorsed_by": [s for s in p.get("sources", []) if s.endswith("endorsed")]}
        if p.get("incumbent"):
            members.append({**base, "bioguide_id": p.get("bioguide_id", ""), "caucus": p.get("caucus", ""),
                            "lobby_total": p.get("lobby_total", 0), "pacs": p.get("pacs", []),
                            "rlc_score": p.get("rlc_score"), "votes": p.get("vote_record", [])[-12:],
                            "anti_votes": p.get("anti_votes", 0), "relevant_votes": p.get("relevant_votes", 0),
                            "not_up_this_cycle": p.get("not_up_this_cycle", False)})
        elif p["antiArms"]:
            candidates.append({**base, "source": "+".join(sorted(set(p.get("sources", [])))) or "fec"})

        if p["antiArms"] and p.get("on_ballot") and p.get("ps") not in ("RETIRING",) and \
                not p.get("not_up_this_cycle"):
            races.append({
                "id": k, "name": p["name"], "party": p.get("party") or "?", "state": p.get("state", ""),
                "chamber": p.get("seeking", {}).get("chamber", p.get("chamber", "house")),
                "district": p.get("seeking", {}).get("district", p.get("district")),
                "type": "incumbent" if p.get("incumbent") else "challenger",
                "status": race_status(p),
                "primary_date": p.get("primary_date"), "primary_result": p.get("primary_result"),
                "runoff_date": p.get("runoff_date"), "general_date": p.get("general_date"),
                "win_prob": bm["prob"] if bm else None, "win_prob_src": bm["src"] if bm else "",
                "win_prob_stage": bm["stage"] if bm else None,
                "note": p.get("note", ""), "evidence": p["antiArmsEvidence"],
            })

    anti_members = [m for m in members if m["antiArms"]]
    return {
        "meta": {
            "fetched_at": meta_extra["fetched_at"], "version": 10, "cycle": CYCLE,
            "congress": CURRENT_CONGRESS, "next_congress": NEXT_CONGRESS,
            "member_count": len(members), "anti_intervention_count": len(anti_members),
            "anti_by_party": {pt: sum(1 for m in anti_members if (m.get("caucus") or m["party"]) == pt)
                              for pt in ("R", "D", "I")},
            "candidate_count": len(candidates), "odds_covered": len(poly),
            "relevant_votes": len(relevant_votes), "streams": streams,
            "sources": {
                "members": "clerk.house.gov MemberData.xml + senate.gov roster, checked against latest roll calls",
                "candidates": f"FEC {CYCLE} filings (all House + Senate filers)",
                "votes": "clerk.house.gov + senate.gov roll-call XML, relevance judged by AI with web search",
                "positions": "AI web research with cited sources",
                "finance": "OpenFEC totals + independent expenditures",
                "markets": "Polymarket CLOB · Kalshi · PredictIt",
                "bios": "Wikipedia · Ballotpedia · Grokipedia",
            },
            **{k: v for k, v in meta_extra.items() if k != "fetched_at"},
        },
        "members": members,
        "candidates": candidates,
        "trackaipac_endorsed": [], "rlc_endorsed": [],
        "races_2026": races, "races": races,
        "poly": poly, "fec": fec, "descriptions": descriptions,
        "congress_history": history,
        "relevant_votes": [{k2: v[k2] for k2 in ("chamber", "congress", "roll", "date", "legis", "summary",
                                                 "significance", "anti_position", "topic", "url")}
                           | {"party_anti": _party_anti(v)} for v in relevant_votes],
        "bills": [],
    }


def _party_anti(v):
    out = {}
    for pos, party, _ in (v.get("positions") or {}).values():
        if pos == v["anti_position"]:
            out[party[:1]] = out.get(party[:1], 0) + 1
    return out
