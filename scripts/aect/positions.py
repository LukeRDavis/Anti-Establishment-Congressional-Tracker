"""
Step 4b — what each candidate has actually said.

For people without a voting record (challengers, open-seat candidates), the
evidence is their own public statements. An AI with live web search reads
their campaign site, interviews and coverage and reports a position ONLY
with source links. A claim with no source URL is discarded.

Also records whether the candidate publicly rejects AIPAC/pro-Israel-lobby
money, which the FEC outside-spending data can then corroborate or contradict.
"""
from . import ai
from .config import CYCLE, TTL_AI_PERSON_H, AI_BUDGET_PER_RUN, STATE_NAMES
from .net import stream, load_cache, save_cache, now_iso, age_hours, get_text, strip_html

SYSTEM = f"""You research U.S. congressional candidates' public positions for a tracker of anti-interventionist candidates in the {CYCLE} elections.

Anti-interventionist means the candidate has explicitly called for one or more of: cutting, conditioning, or ending U.S. military aid or arms sales to a foreign government (e.g. Israel, Ukraine, Saudi Arabia); an arms embargo; opposing a U.S. war or military strike; repealing or opposing war authorizations; withdrawing U.S. forces from a conflict.

Rules:
- Use web search. Prefer the candidate's own campaign website, social posts, interviews, debates, and reputable news.
- Only count EXPLICIT positions the candidate stated. Vague "peace" or "diplomacy" language without a policy position does not count.
- Every evidence item MUST include the URL where you found it. Do not include an item you cannot cite.
- If you find nothing explicit, return anti_interventionist false with an empty evidence list — do not guess.

Respond with JSON only:
{{"anti_interventionist": true|false, "strength": "strong"|"moderate"|"none",
  "rejects_aipac_money": true|false|null,
  "evidence": [{{"claim": "short paraphrase of what they said", "url": "https://...", "date": "YYYY-MM or empty"}}],
  "summary": "one sentence"}}"""


def _prompt(p, website_text):
    office = (f"U.S. Senate in {STATE_NAMES.get(p['state'], p['state'])}" if p["chamber"] == "senate"
              else f"U.S. House, {p['state']}-{p.get('district') or 'AL'}")
    lines = [f"Candidate: {p['name']} ({p.get('party','?')}), running in {CYCLE} for {office}."]
    if p.get("incumbent"):
        lines.append("This person is a sitting member of Congress.")
    if p.get("website"):
        lines.append(f"Campaign website: {p['website']}")
    if website_text:
        lines.append(f"Text from their campaign website (excerpt):\n{website_text[:2500]}")
    return "\n".join(lines)


TEXT_ONLY = """You read a U.S. congressional candidate's own campaign-website text and decide whether it states an explicit anti-interventionist position: cutting, conditioning or ending U.S. military aid or arms sales to a foreign government, an arms embargo, opposing a U.S. war or strike, or withdrawing U.S. forces.
Use ONLY the text provided. Do not use outside knowledge. Quote or closely paraphrase the sentence that states the position.
Respond with JSON only:
{"anti_interventionist": true|false, "strength": "strong"|"moderate"|"none", "rejects_aipac_money": true|false|null, "quote": "the sentence from the text, or empty", "summary": "one sentence"}"""


def research(people, order, ttl_h=TTL_AI_PERSON_H, budget=None):
    """
    people: {key: person}; order: keys in priority order.
    With a web-search AI: full research with cited sources.
    Without one: the candidate's own campaign-site text is read (that page is the citation).
    """
    cache = load_cache("positions")
    budget = AI_BUDGET_PER_RUN // 2 if budget is None else budget
    grounded = ai.available(search=True)
    if not grounded and not ai.available_any():
        stream("ai_positions", ok=False, count=len(cache), note="no AI provider available")
        return cache
    done = 0
    for key in order:
        if done >= budget:
            break
        if age_hours(cache.get(key, {}).get("researched_at")) < ttl_h:
            continue
        p = people[key]
        site_text = ""
        if p.get("website"):
            site_text = strip_html(get_text(p["website"], stream_name="campaign_sites", retries=0) or "")
        if grounded:
            result, sources, provider = ai.ask_json(SYSTEM, _prompt(p, site_text), search=True)
        else:
            if len(site_text) < 200:
                continue            # nothing of their own to read without web search
            result, sources, provider = ai.ask_json(
                TEXT_ONLY, f"Candidate: {p['name']}\nWebsite: {p['website']}\n\n{site_text[:6000]}")
            if isinstance(result, dict) and result.get("quote"):
                result["evidence"] = [{"claim": result["quote"][:300], "url": p["website"], "date": ""}]
        done += 1
        if not isinstance(result, dict):
            continue
        evidence = [e for e in (result.get("evidence") or [])
                    if isinstance(e, dict) and str(e.get("url", "")).startswith("http")]
        anti = bool(result.get("anti_interventionist")) and bool(evidence)
        cache[key] = {
            "anti_interventionist": anti,
            "strength": result.get("strength") if anti else "none",
            "rejects_aipac_money": result.get("rejects_aipac_money"),
            "evidence": evidence[:5], "summary": (result.get("summary") or "")[:300],
            "grounding": (sources or [])[:5], "provider": provider, "researched_at": now_iso(),
            "method": "web_search" if grounded else "campaign_site_text",
        }
    save_cache("positions", cache)
    yes = sum(1 for v in cache.values() if v.get("anti_interventionist"))
    stream("ai_positions", ok=True, count=len(cache),
           note=f"researched {done} this run ({'web search' if grounded else 'campaign-site text only'}) · "
                f"{len(cache)} total · {yes} with cited anti-interventionist positions")
    return cache
