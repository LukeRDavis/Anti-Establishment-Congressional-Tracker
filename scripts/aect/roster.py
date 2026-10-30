"""
Step 1 — who is in Congress right now.

Primary sources (no key needed):
  • House Clerk MemberData.xml  — official roster incl. vacancies & successors
  • Senate senators_cfm.xml      — Secretary of the Senate roster
Freshness checks:
  • The most recent House roll call lists every sitting Representative by
    bioguide ID, and the most recent Senate roll call lists every sitting
    Senator. Anyone on a roster but absent from the latest roll call is
    flagged (death/resignation the roster files haven't caught up with).
  • Congress.gov /member (when CONGRESS_KEY works) cross-checks both.
"""
import re
import xml.etree.ElementTree as ET

from .config import (CLERK_MEMBERS_XML, SENATE_MEMBERS_XML, CONGRESS_API, CONGRESS_KEY,
                     CURRENT_CONGRESS, SESSION, SENATE_VOTE_MENU, SENATE_VOTE_XML,
                     CLERK_ROLL_XML, TODAY)
from .net import get_bytes, get_json, stream, qs


def _district_num(raw):
    raw = (raw or "").strip()
    if raw.lower() in ("at large", "at-large"):
        return 0
    m = re.match(r"(\d+)", raw)
    return int(m.group(1)) if m else None


def fetch_house():
    raw = get_bytes(CLERK_MEMBERS_XML, stream_name="roster_house")
    if not raw:
        stream("roster_house", ok=False, note="Clerk MemberData.xml unavailable")
        return [], []
    root = ET.fromstring(raw)
    members, vacancies = [], []
    publish = root.get("publish-date", "")
    for m in root.iter("member"):
        info = m.find("member-info")
        if info is None:
            continue
        sd = (m.findtext("statedistrict") or "").strip()
        district_raw = (info.findtext("district") or "").strip()
        state_el = info.find("state")
        state = state_el.get("postal-code", "") if state_el is not None else sd[:2]
        bioguide = (info.findtext("bioguideID") or "").strip()
        footnote = (info.findtext("footnote") or "").strip()
        if not bioguide:
            pred = m.find("predecessor-info")
            vacancies.append({
                "chamber": "house", "state": state, "district": _district_num(district_raw),
                "reason": footnote,
                "predecessor": pred.findtext("pred-official-name") if pred is not None else "",
                "predecessor_bioguide": pred.findtext("pred-memindex") if pred is not None else "",
            })
            continue
        delegate = district_raw in ("Delegate", "Resident Commissioner")
        first = (info.findtext("firstname") or "").strip()
        last = (info.findtext("lastname") or "").strip()
        sworn = info.find("sworn-date")
        members.append({
            "bioguide_id": bioguide,
            "name": f"{first} {last}".strip(),
            "official_name": (info.findtext("official-name") or "").strip(),
            "first_name": first, "last_name": last,
            "party": (info.findtext("party") or "").strip() or "?",
            "caucus": (info.findtext("caucus") or "").strip(),
            "state": state, "district": _district_num(district_raw),
            "chamber": "house", "delegate": delegate,
            "sworn_date": sworn.get("date", "") if sworn is not None else "",
            "note": footnote,
            "_source": "clerk.house.gov MemberData.xml",
        })
    voting = [x for x in members if not x["delegate"]]
    stream("roster_house", ok=bool(voting), count=len(voting),
           note=f"{len(voting)} Representatives + {len(members)-len(voting)} delegates, "
                f"{len(vacancies)} vacancies (published {publish})")
    return members, vacancies


def fetch_senate():
    raw = get_bytes(SENATE_MEMBERS_XML, stream_name="roster_senate")
    if not raw:
        stream("roster_senate", ok=False, note="senators_cfm.xml unavailable")
        return []
    root = ET.fromstring(raw)
    out = []
    for m in root.iter("member"):
        bg = (m.findtext("bioguide_id") or "").strip()
        last = (m.findtext("last_name") or "").strip()
        first = (m.findtext("first_name") or "").strip()
        if not bg or not last:
            continue
        # "Angus S., Jr." → "Angus"; "Edward J." → "Edward"
        first_clean = re.split(r"[ ,]", first)[0]
        party = (m.findtext("party") or "").strip() or "?"
        out.append({
            "bioguide_id": bg, "name": f"{first_clean} {last}", "official_name": f"{first} {last}",
            "first_name": first_clean, "last_name": last, "party": party, "caucus": party,
            "state": (m.findtext("state") or "").strip(), "district": None,
            "chamber": "senate", "delegate": False,
            "senate_class": (m.findtext("class") or "").replace("Class ", "").strip(),
            "leadership": (m.findtext("leadership_position") or "").strip(),
            "website": (m.findtext("website") or "").strip(),
            "note": "", "_source": "senate.gov senators_cfm.xml",
        })
    upd = root.findtext("last_updated", "")
    stream("roster_senate", ok=len(out) >= 90, count=len(out),
           note=f"{len(out)} senators (file last updated {upd})")
    return out


def latest_house_roll_members(year, start_roll=None):
    """Find the newest House roll call and return {bioguide: party}."""
    lo, hi = 1, 2000
    # exponential probe then binary search for the last roll that exists
    probe = start_roll or 1
    last_ok = None
    while probe <= hi:
        if get_bytes(CLERK_ROLL_XML.format(year=year, roll=probe), stream_name="votes_house_xml", retries=1):
            last_ok = probe
            probe *= 2
        else:
            hi = probe - 1
            break
    if last_ok is None:
        return None, {}
    lo = last_ok
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if get_bytes(CLERK_ROLL_XML.format(year=year, roll=mid), stream_name="votes_house_xml", retries=1):
            lo = mid
        else:
            hi = mid - 1
    raw = get_bytes(CLERK_ROLL_XML.format(year=year, roll=lo), stream_name="votes_house_xml")
    present = {}
    if raw:
        root = ET.fromstring(raw)
        for rv in root.iter("recorded-vote"):
            leg = rv.find("legislator")
            if leg is not None and leg.get("name-id"):
                present[leg.get("name-id")] = leg.get("party", "")
    return lo, present


def latest_senate_roll_members():
    """Return (vote_number, [{last, state, party, first}]) from the newest Senate vote."""
    menu = get_bytes(SENATE_VOTE_MENU.format(congress=CURRENT_CONGRESS, session=SESSION),
                     stream_name="votes_senate_xml")
    if not menu:
        return None, []
    root = ET.fromstring(menu)
    nums = [int(v.findtext("vote_number") or 0) for v in root.iter("vote")]
    if not nums:
        return None, []
    n = max(nums)
    raw = get_bytes(SENATE_VOTE_XML.format(congress=CURRENT_CONGRESS, session=SESSION, vote=n),
                    stream_name="votes_senate_xml")
    if not raw:
        return n, []
    vroot = ET.fromstring(raw)
    out = []
    for m in vroot.iter("member"):
        out.append({"last": (m.findtext("last_name") or "").strip(),
                    "first": (m.findtext("first_name") or "").strip(),
                    "state": (m.findtext("state") or "").strip(),
                    "party": (m.findtext("party") or "").strip()})
    return n, out


def _congress_gov_members():
    if not CONGRESS_KEY:
        stream("congress_gov", ok=False, note="CONGRESS_KEY not provided to the workflow")
        return None
    out, offset = [], 0
    while True:
        url = f"{CONGRESS_API}/member?" + qs({"currentMember": "true", "limit": 250,
                                              "offset": offset, "format": "json",
                                              "api_key": CONGRESS_KEY})
        d = get_json(url, stream_name="congress_gov")
        if not d:
            break
        batch = d.get("members", [])
        out += batch
        if len(batch) < 250:
            break
        offset += 250
    if out:
        stream("congress_gov", ok=True, count=len(out), note=f"{len(out)} current members")
    else:
        stream("congress_gov", ok=False, note="no members returned — check the error body above "
               "(API_KEY_INVALID means the secret value is wrong; API_KEY_MISSING means it isn't passed)")
    return out or None


def fetch_current_members():
    house, vacancies = fetch_house()
    senate = fetch_senate()

    # Freshness check — House
    roll, present = latest_house_roll_members(TODAY.year)
    if roll and present:
        for m in house:
            if not m["delegate"] and m["bioguide_id"] not in present:
                m["roster_flag"] = f"absent from latest House roll call #{roll}"
        stale = [m["name"] for m in house if m.get("roster_flag")]
        if stale:
            stream("roster_house", note=f"{len(stale)} not on latest roll #{roll}: {', '.join(stale[:8])}")

    # Freshness check — Senate (roster file is updated far less often)
    vote_n, sen_present = latest_senate_roll_members()
    if sen_present:
        keys = {(p["last"].lower(), p["state"]) for p in sen_present}
        for s in senate:
            if (s["last_name"].lower(), s["state"]) not in keys:
                s["departed"] = True
                s["roster_flag"] = f"not on latest Senate roll call #{vote_n}"
        rkeys = {(s["last_name"].lower(), s["state"]) for s in senate}
        for p in sen_present:
            if (p["last"].lower(), p["state"]) not in rkeys:
                senate.append({
                    "bioguide_id": "", "name": f"{p['first']} {p['last']}", "first_name": p["first"],
                    "last_name": p["last"], "party": p["party"], "caucus": p["party"],
                    "state": p["state"], "district": None, "chamber": "senate", "delegate": False,
                    "note": f"seated after the senate.gov roster file was last updated (on roll #{vote_n})",
                    "_source": "senate.gov roll call",
                })
        gone = [s["name"] for s in senate if s.get("departed")]
        if gone:
            stream("roster_senate", note=f"no longer voting (roster file stale): {', '.join(gone)}")
        senate = [s for s in senate if not s.get("departed")]

    # Cross-check with Congress.gov when a working key exists
    cg = _congress_gov_members()
    if cg:
        cg_ids = {m.get("bioguideId") for m in cg}
        ours = {m["bioguide_id"] for m in house + senate if m.get("bioguide_id")}
        missing = cg_ids - ours
        if missing:
            stream("congress_gov", note=f"{len(missing)} current members in Congress.gov not in roster files")

    members = [m for m in house if not m["delegate"]] + senate
    for m in members:
        m["ps"] = "INCUMBENT"
    return members, vacancies, [m for m in house if m["delegate"]]
