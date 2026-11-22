"""AI reviews for past Congresses.

Past records never change, so each member who counts in a past Congress is reviewed once per judge
standard (JUDGE_VERSION) and the review is kept for good. The review sees what the rule saw: the
member's counted roll calls up to the end of that Congress. It is advisory: the rule decides who
counts in a past Congress, and the card shows where the AI agrees or disagrees."""
import glob
import json
import os

from . import ai, past
from . import establishment as E
from . import votes as V
from .config import ai_time_left, congress_years
from .net import load_cache, save_cache, now_iso

HV = 2          # history review version: 2 = with the anchor-rule reminder at the end of the prompt
PER_RUN = int(os.environ.get("AECT_HISTORY_REVIEWS", "8"))

SYSTEM_H = E.SYSTEM + """

THIS REVIEW IS OF A PAST CONGRESS. Judge the person as of the end of the Congress named, using only
the roll calls given (cast up to then) and the standard above. Later events are irrelevant to this
review. There is no dossier, money or discharge-petition data for past Congresses: do not hold their
absence against anyone, and leave leadership_support null. Votes against war-on-terror-era
authorizations were a radical stand at the time and weigh as such."""


def _ordinal(n):
    from .bridge import ordinal
    return ordinal(n)


def _prompt(c, m, rec):
    areas = V.area_record(rec)
    focus = sorted(rec, key=lambda v: v.get("date") or "")
    key = [v for v in focus if v.get("area") == V.ANCHOR or (v["anti"] and not v.get("party_line"))]
    rest = [v for v in focus if v not in key]
    picked = sorted(key[-28:] + rest[-max(0, 34 - len(key[-28:])):], key=lambda v: v.get("date") or "")
    lines = [f"{v.get('date')} [{V.AREA_LABEL.get(v.get('area'), 'other')}]{' [non-binding]' if V.non_binding(v) else ''}"
             f" {v.get('measure') or ''}: {v.get('position')} ({'anti' if v['anti'] else 'interventionist'}"
             f"{'; own party ' + str(v.get('own_party_share')) + '% same' if v.get('own_party_share') is not None else ''})"
             f" {(v.get('summary') or '')[:70]}" for v in picked]
    if len(picked) < len(focus):
        lines.insert(0, f"({len(focus) - len(picked)} older routine votes left out)")
    words = "; ".join(f"{x['label']}: {x['status']} ({x['anti']} of {x['votes']} anti)" for x in areas.values() if x["anti"])
    y = congress_years(c)
    b = m.get("bridge") or {}
    return "\n\n".join([
        f"Congress: the {_ordinal(c)} ({y[0]}–{y[1]})" if isinstance(y, (list, tuple)) else f"Congress: the {_ordinal(c)}",
        f"Person: {m['name']} ({m.get('party')}), {m.get('chamber')}, {m.get('state', '')}",
        f"The tracker's rule says: {m.get('bottom_line', '')}",
        f"First counted: the {_ordinal(b['since'])} Congress" if b.get("since") else "",
        "Areas by the end of this Congress: " + (words or "none engaged"),
        "Counted roll calls up to this Congress (oldest first):\n" + ("\n".join(lines) or "none"),
        E.ANCHOR_REMINDER,
    ])


def run():
    """Review the next past members who count; returns how many reviews landed."""
    if PER_RUN <= 0 or not ai.available_any():
        return 0
    files = sorted(glob.glob(os.path.join(past.OUT_DIR, "congress_*.json")))
    history = {}
    for f in files:
        with open(f) as fh:
            d = json.load(fh)
        history[d["congress"]] = d
    if not history:
        return 0
    cache = load_cache("history_judgments") or {}
    records, queue = {}, []
    for c in sorted(history):
        for m in history[c]["members"]:
            records.setdefault(m["bioguide_id"], []).extend(v for v in m.get("votes", []) if v.get("area"))
            j = cache.get(f"{c}-{m['bioguide_id']}")
            if m.get("antiArms") and not (j and j.get("v") == E.JUDGE_VERSION and j.get("hv") == HV):
                queue.append((c, m, list(records[m["bioguide_id"]])))
    queue.sort(key=lambda x: (-x[0], x[1]["name"]))      # the most recent Congresses first
    done = tried = 0
    for c, m, rec in queue:
        if done >= PER_RUN or tried >= PER_RUN * 3 or ai_time_left() < 120:
            break
        tried += 1
        res, _, provider = ai.ask_json(SYSTEM_H, _prompt(c, m, rec))
        if not (isinstance(res, dict) and res.get("tier") in ("hard", "soft", "none")):
            continue
        cache[f"{c}-{m['bioguide_id']}"] = {
            "v": E.JUDGE_VERSION, "hv": HV, "tier": res["tier"], "provider": provider,
            "bottom_line": (res.get("bottom_line") or "")[:240], "writeup": (res.get("writeup") or "")[:2400],
            "confidence": E._conf_index(res), "model": ai.LAST.get("model"), "judged_at": now_iso()}
        done += 1
    save_cache("history_judgments", cache)
    if done:
        past.rerate()
    print(f"  history reviews: {done} landed this run · {max(0, len(queue) - done)} past members still to review")
    return done
