"""
House discharge petitions — who signed what, from the Clerk.

  clerk.house.gov/DischargePetition/DischargePetitions?page=N   list of petitions (this Congress)
  clerk.house.gov/DischargePetition/{id}                         one petition: measure, subject, signers

A discharge petition forces a measure to the floor over the Speaker and the Rules
Committee. Signing one is a recorded act of going around House leadership; a
member of the majority party who signs is going around their OWN leadership.
That is the signal fed to the anti-establishment judge. Nothing about any person
or petition is written here.
"""
import re

from .net import get_text, strip_html, stream, load_cache, save_cache, now_iso, age_hours

BASE = "https://clerk.house.gov"
TTL_H = 24


def fetch():
    """{bioguide_id: [{no, measure, about, url, signed, signers, success}]}"""
    cache = load_cache("discharge_petitions")
    if age_hours(cache.get("at")) < TTL_H and cache.get("petitions"):
        out = _by_member(cache["petitions"])
        stream("discharge_petitions", ok=True, count=len(cache["petitions"]), note="cached")
        return out
    ids = []
    for page in range(1, 30):
        html = get_text(f"{BASE}/DischargePetition/DischargePetitions?page={page}", stream_name="discharge_petitions")
        found = [i for i in re.findall(r"/DischargePetition/(\d{10})", html or "") if i not in ids]
        if not found:
            break
        ids += found
    old = {p["id"]: p for p in cache.get("petitions", [])}
    petitions = []
    for pid in ids:
        html = get_text(f"{BASE}/DischargePetition/{pid}", stream_name="discharge_petitions")
        if not html:
            if pid in old:
                petitions.append(old[pid])
            continue
        text = strip_html(html)
        no = int(pid[-2:])
        m = re.search(r"congress\.gov/bill/(\d+)/(\w+)/(\d+)", html, re.I)
        kinds = {"HR": "H.R.", "HRES": "H.Res.", "HJRES": "H.J.Res.", "HCONRES": "H.Con.Res.",
                 "S": "S.", "SRES": "S.Res.", "SJRES": "S.J.Res.", "SCONRES": "S.Con.Res."}
        measure = f"{kinds.get(m.group(2).upper(), m.group(2).upper())} {m.group(3)}" if m else ""
        about = ""
        a = re.search(r"(Providing for (?:the )?consideration of (?:the )?[\w ]{0,20}\([^)]{3,40}\)[^.]{0,220})", text) \
            or re.search(r"(Providing for (?:the )?consideration of [^.]{10,300})", text)
        if a:
            about = a.group(1).strip()
        signers = []
        for row in re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S | re.I):
            bg = re.search(r"(?:/Members/|index=)([A-Z]\d{6})", row)
            if not bg:
                continue
            cells = [strip_html(c) for c in re.findall(r"<td[^>]*>(.*?)</td>", row, re.S | re.I)]
            party = next((c for c in cells if c in ("Republican", "Democratic", "Independent")), "")
            date = next((re.search(r"\d{2}/\d{2}/\d{4}", c).group(0) for c in cells
                         if re.search(r"\d{2}/\d{2}/\d{4}", c)), "")
            signers.append({"bg": bg.group(1), "party": party[:1], "date": date})
        petitions.append({"id": pid, "no": no, "measure": measure, "about": about[:240],
                          "url": f"{BASE}/DischargePetition/{pid}", "signers": signers})
    if petitions:
        cache = {"at": now_iso(), "petitions": petitions}
        save_cache("discharge_petitions", cache)
    else:
        petitions = cache.get("petitions", [])
    stream("discharge_petitions", ok=bool(petitions), count=len(petitions),
           note=f"{len(petitions)} petitions · {sum(len(p['signers']) for p in petitions)} signatures")
    return _by_member(petitions)


def _by_member(petitions):
    out = {}
    for p in petitions:
        n = len(p["signers"])
        by_party = {}
        for s in p["signers"]:
            by_party[s["party"]] = by_party.get(s["party"], 0) + 1
        for s in p["signers"]:
            out.setdefault(s["bg"], []).append({
                "no": p["no"], "measure": p["measure"], "about": p["about"], "url": p["url"],
                "signed": s["date"], "signers": n, "signers_by_party": by_party,
                "own_party_signers": by_party.get(s["party"], 0)})
    return out
