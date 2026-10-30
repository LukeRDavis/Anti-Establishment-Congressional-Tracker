"""
Offline end-to-end test: replaces the network with fixtures and runs the
whole pipeline. Checks that (a) nothing crashes with keys missing, (b) a
Massie-amendment-shaped roll call classifies exactly its Aye voters,
(c) primary results are never accepted before the primary date,
(d) the stream report flags every missing source.

Run:  python scripts/tests/test_pipeline_offline.py
"""
import json, os, re, shutil, sys, tempfile, urllib.error
from datetime import date, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = os.path.dirname(HERE)
sys.path.insert(0, SCRIPTS)

tmp = tempfile.mkdtemp()
os.environ["AECT_CACHE_DIR"] = os.path.join(tmp, "data", "cache")
for k in ("CONGRESS_KEY", "FEC_KEY", "GEMINI_KEY", "XAI_API_KEY", "OPENROUTER_API_KEY", "GROQ_API_KEY"):
    os.environ.pop(k, None)

from aect import net, config  # noqa: E402

YEAR = config.TODAY.year
STATES = [s for s in config.VOTING_STATES]

# ── Synthetic House: 435 seats, party alternating; Massie at KY-04 ───────────
house = []
i = 0
for st in sorted(STATES):
    for d in range(1, 10):
        if len(house) >= 435:
            break
        i += 1
        house.append({"bg": f"H{i:06d}", "first": f"Rep{i}", "last": f"Last{i}",
                      "party": "D" if i % 2 else "R", "state": st, "district": d})
house[0].update(bg="M001184", first="Thomas", last="Massie", party="R", state="KY", district=4)
house[1].update(bg="K000389", first="Ro", last="Khanna", party="D", state="CA", district=17)


def member_xml():
    rows = []
    for m in house:
        rows.append(f"""<member><statedistrict>{m['state']}{m['district']:02d}</statedistrict><member-info>
<bioguideID>{m['bg']}</bioguideID><lastname>{m['last']}</lastname><firstname>{m['first']}</firstname>
<official-name>{m['first']} {m['last']}</official-name><party>{m['party']}</party><caucus>{m['party']}</caucus>
<state postal-code="{m['state']}"><state-fullname>{config.STATE_NAMES[m['state']]}</state-fullname></state>
<district>{m['district']}th</district><sworn-date date="20250103"/></member-info></member>""")
    rows.append("""<member><statedistrict>CA14</statedistrict><member-info><bioguideID/><lastname/>
<state postal-code="CA"/><district>14th</district><footnote>Vacancy due to resignation</footnote></member-info>
<predecessor-info><pred-official-name>Eric Swalwell</pred-official-name></predecessor-info></member>""")
    return f'<MemberData publish-date="test"><members>{"".join(rows)}</members></MemberData>'.encode()


senate = [{"bg": f"S{j:06d}", "first": f"Sen{j}", "last": f"SLast{j}", "party": "D" if j % 2 else "R",
           "state": sorted(STATES)[j // 2], "class": ["I", "II", "III"][j % 3]} for j in range(100)]
senate[0].update(first="Bernard", last="Sanders", party="I", state="VT")


def senate_xml():
    rows = "".join(f"""<member><last_name>{s['last']}</last_name><first_name>{s['first']}</first_name>
<party>{s['party']}</party><state>{s['state']}</state><class>Class {s['class']}</class>
<bioguide_id>{s['bg']}</bioguide_id></member>""" for s in senate)
    return f"<contact_information>{rows}<last_updated>test</last_updated></contact_information>".encode()


def roll_xml(roll):
    """Roll 1 = Massie-style amendment: Massie + every odd-indexed Democrat vote Aye."""
    votes = []
    for idx, m in enumerate(house):
        aye = (m["bg"] == "M001184") or (m["party"] == "D" and idx % 4 == 0)
        votes.append(f'<recorded-vote><legislator name-id="{m["bg"]}" party="{m["party"]}" '
                     f'state="{m["state"]}">{m["last"]}</legislator><vote>{"Aye" if aye else "No"}</vote></recorded-vote>')
    meta = (f"<vote-metadata><congress>{config.CURRENT_CONGRESS}</congress><session>2</session>"
            f"<rollcall-num>{roll}</rollcall-num><legis-num>H R 8595</legis-num>"
            f"<vote-question>On Agreeing to the Amendment</vote-question><amendment-num>8</amendment-num>"
            f"<amendment-author>Massie of Kentucky Part A Amendment No. 8</amendment-author>"
            f"<vote-result>Failed</vote-result><action-date>15-Jul-{YEAR}</action-date>"
            f"<vote-desc></vote-desc></vote-metadata>")
    return f"<rollcall-vote>{meta}<vote-data>{''.join(votes)}</vote-data></rollcall-vote>".encode()


def passage_xml(roll):
    meta = (f"<vote-metadata><congress>{config.CURRENT_CONGRESS}</congress><legis-num>H R 8595</legis-num>"
            f"<vote-question>On Passage</vote-question><vote-result>Passed</vote-result>"
            f"<action-date>15-Jul-{YEAR}</action-date><vote-desc>National Security, Department of State, "
            f"and Related Programs Appropriations Act</vote-desc></vote-metadata>")
    v = "".join(f'<recorded-vote><legislator name-id="{m["bg"]}" party="{m["party"]}" state="{m["state"]}">'
                f'{m["last"]}</legislator><vote>Yea</vote></recorded-vote>' for m in house)
    return f"<rollcall-vote>{meta}<vote-data>{v}</vote-data></rollcall-vote>".encode()


def senate_menu():
    return b"<vote_summary><votes></votes></vote_summary>"


BP_FUTURE = (date.today() + timedelta(days=30)).strftime("%B %-d, %Y")
BP_PAST_WRONG_CYCLE = "May 19, 2024"


def ballotpedia(path):
    if "calendar" in path.lower():
        return ("<p>" + " ".join(f"{config.STATE_NAMES[s]} primary {BP_FUTURE}." for s in STATES) + "</p>").encode()
    if "Thomas_Massie" in path:
        # A page that mentions a past-cycle win must NOT produce primary_result
        return (f"<p>Thomas Massie Kentucky. Massie won the Republican primary on {BP_PAST_WRONG_CYCLE}. "
                f"The primary will occur on {BP_FUTURE}.</p>").encode()
    return None


def router(url):
    if "MemberData.xml" in url:
        return member_xml()
    if "senators_cfm.xml" in url:
        return senate_xml()
    m = re.search(r"/evs/(\d{4})/roll(\d+)\.xml", url)
    if m:
        y, r = int(m.group(1)), int(m.group(2))
        if y == YEAR and r == 1:
            return roll_xml(1)
        if y == YEAR and r == 2:
            return passage_xml(2)
        return None
    if "vote_menu" in url:
        return senate_menu()
    if "ballotpedia.org" in url:
        return ballotpedia(url)
    return None


def fake_request(url, data=None, headers=None, timeout=30, method=None):
    body = router(url)
    if body is None:
        raise urllib.error.HTTPError(url, 404, "nf", {}, None)
    return body


net._request = fake_request

# Pre-seed one AI judgment (as if Gemini had judged roll 1 on an earlier run)
os.makedirs(os.environ["AECT_CACHE_DIR"], exist_ok=True)
with open(os.path.join(os.environ["AECT_CACHE_DIR"], "vote_judgments.json"), "w") as f:
    json.dump({f"house-{YEAR}-1": {"relevant": True, "anti_position": "Yea", "significance": "major",
                                    "topic": "Israel military aid", "summary": "Amendment to cut $3.3B FMF for Israel",
                                    "sources": [], "provider": "fixture"},
               f"house-{YEAR}-2": {"relevant": True, "anti_position": "Nay", "significance": "minor",
                                    "topic": "approps", "summary": "Final passage", "sources": [], "provider": "fixture"}}, f)

# run the pipeline with ROOT pointed at the temp dir
import fetch_data  # noqa: E402
fetch_data.ROOT = tmp
code = fetch_data.main()
assert code == 0, "pipeline exited non-zero"

data = json.load(open(os.path.join(tmp, "data.json")))
report = json.load(open(os.path.join(tmp, "pipeline_report.json")))
members = {m["name"]: m for m in data["members"]}

expected_d = sum(1 for idx, m in enumerate(house) if m["party"] == "D" and idx % 4 == 0)
anti = [m for m in data["members"] if m["antiArms"]]
anti_r = [m["name"] for m in anti if m["party"] == "R"]
print("\nanti members:", len(anti), "R:", anti_r, "expected D:", expected_d)
assert anti_r == ["Thomas Massie"], anti_r
assert sum(1 for m in anti if m["party"] == "D") == expected_d
assert members["Thomas Massie"]["antiArmsBasis"] == "votes"
assert members["Thomas Massie"].get("primary_result") is None, "past-cycle win leaked into this cycle"
assert data["meta"]["anti_by_party"]["R"] == 1
assert len(data["members"]) == 535, len(data["members"])
assert data["meta"]["vacancies"][0]["predecessor"] == "Eric Swalwell"
assert data["congress_history"][str(config.CURRENT_CONGRESS)]["hardR"] == 1
s = report["streams"]
assert s["roster_house"]["ok"] and s["roster_senate"]["ok"]
assert s["fec_candidates"]["ok"] is False and s["ai_positions"]["ok"] is False
for r in data["races_2026"]:
    assert r["primary_result"] is None
print("\nALL OFFLINE CHECKS PASSED")
shutil.rmtree(tmp)
