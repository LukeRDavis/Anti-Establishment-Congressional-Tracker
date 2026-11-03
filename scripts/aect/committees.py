"""
Committee rosters: who sits on each tracked committee, who chairs it, who is ranking member.

Source: the congress-legislators project's committee-membership-current.yaml and
committees-current.yaml (compiled from the House Clerk and Senate rosters and kept
current). Which committees are tracked is the plank definition in config.
"""
from .config import PLANK_COMMITTEES, ACTIVE_PLANKS
from .net import get_text, stream, load_cache, save_cache, now_iso, age_hours

RAW = "https://raw.githubusercontent.com/unitedstates/congress-legislators/main/{}"


def _yaml(text):
    try:
        import yaml
        return yaml.safe_load(text)
    except Exception as e:
        stream("committees", error=f"YAML parse failed: {e}")
        return None


def fetch():
    cache = load_cache("committees")
    if age_hours(cache.get("at")) < 12 and cache.get("committees"):
        stream("committees", ok=True, count=len(cache["committees"]), note="cached")
        return cache["committees"]
    roster = _yaml(get_text(RAW.format("committee-membership-current.yaml"), stream_name="committees") or "")
    meta = _yaml(get_text(RAW.format("committees-current.yaml"), stream_name="committees") or "") or []
    names = {c.get("thomas_id"): c.get("name") for c in meta if isinstance(c, dict)}
    out = []
    for plank in ACTIVE_PLANKS:
        for chamber, ids in PLANK_COMMITTEES.get(plank, {}).items():
            for cid in ids:
                rows = (roster or {}).get(cid) or []
                if not rows:
                    continue
                members = [{"bg": r.get("bioguide"), "name": r.get("name"), "side": r.get("party"),
                            "rank": r.get("rank"), "title": r.get("title") or ""} for r in rows]
                out.append({"id": cid, "chamber": chamber, "plank": plank,
                            "name": (names.get(cid) or cid).replace("Committee on ", "").replace("House ", "").replace("Senate ", "").removeprefix("the ").strip(),
                            "size": len(members), "majority": sum(m["side"] == "majority" for m in members),
                            "members": members})
    if out:
        save_cache("committees", {"at": now_iso(), "committees": out})
    else:
        out = cache.get("committees", [])
    stream("committees", ok=bool(out), count=len(out),
           note=f"{len(out)} committees for planks {', '.join(ACTIVE_PLANKS)}")
    return out
