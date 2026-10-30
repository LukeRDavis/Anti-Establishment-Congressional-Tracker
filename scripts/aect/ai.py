"""
AI layer. One entry point — ask_json() — that cascades across every configured
provider. When `search=True` the call is grounded in live web results (Gemini
Google Search grounding or xAI web_search), which is how the pipeline asks
AI to *contextualize* things the raw data can't explain on its own (what an
amendment actually did, what a candidate has said) while the numbers
themselves still come from the primary sources.
"""
import json, os, re

from .config import GEMINI_KEY, XAI_KEY, OPENROUTER_KEY, GROQ_KEY
from .net import post_json, stream, circuit_ok

GEMINI_MODELS = [m for m in os.environ.get(
    "AECT_GEMINI_MODELS", "gemini-2.5-flash,gemini-2.0-flash").split(",") if m]
XAI_MODEL        = os.environ.get("AECT_XAI_MODEL", "grok-4-1-fast")
OPENROUTER_MODEL = os.environ.get("AECT_OPENROUTER_MODEL", "meta-llama/llama-3.3-70b-instruct:free")
GROQ_MODEL       = os.environ.get("AECT_GROQ_MODEL", "llama-3.3-70b-versatile")

CALLS = {"gemini": 0, "xai": 0, "openrouter": 0, "groq": 0, "failed": 0}


def available(search=False):
    if search:
        return bool(GEMINI_KEY and circuit_ok("ai_gemini")) or bool(XAI_KEY and circuit_ok("ai_xai"))
    return any([GEMINI_KEY and circuit_ok("ai_gemini"), XAI_KEY and circuit_ok("ai_xai"),
                OPENROUTER_KEY and circuit_ok("ai_openrouter"), GROQ_KEY and circuit_ok("ai_groq")])


def parse_json_loose(text):
    if not text:
        return None
    t = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.M)
    for pat in (r"\{.*\}", r"\[.*\]"):
        m = re.search(pat, t, re.S)
        if m:
            try:
                return json.loads(m.group())
            except Exception:
                continue
    return None


def _gemini(system, prompt, search):
    if not GEMINI_KEY or not circuit_ok("ai_gemini"):
        return None, None
    for model in GEMINI_MODELS:
        url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
               f"{model}:generateContent?key={GEMINI_KEY}")
        payload = {
            "system_instruction": {"parts": [{"text": system}]},
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.1, "maxOutputTokens": 2048},
        }
        if search:
            payload["tools"] = [{"google_search": {}}]
        else:
            payload["generationConfig"]["responseMimeType"] = "application/json"
        resp = post_json(url, payload, stream_name="ai_gemini", trip_on=(401, 403))
        if not resp:
            continue
        try:
            cand = resp["candidates"][0]
            text = "".join(p.get("text", "") for p in cand["content"]["parts"])
            sources = []
            gm = cand.get("groundingMetadata") or {}
            for ch in gm.get("groundingChunks", []) or []:
                web = ch.get("web") or {}
                if web.get("uri"):
                    sources.append({"title": web.get("title", ""), "url": web["uri"]})
            CALLS["gemini"] += 1
            return parse_json_loose(text), sources
        except Exception as e:
            stream("ai_gemini", error=f"unexpected response shape: {e}")
    return None, None


def _xai(system, prompt, search):
    if not XAI_KEY or not circuit_ok("ai_xai"):
        return None, None
    headers = {"Authorization": f"Bearer {XAI_KEY}"}
    if search:
        payload = {"model": XAI_MODEL,
                   "input": [{"role": "system", "content": system},
                             {"role": "user", "content": prompt}],
                   "tools": [{"type": "web_search"}], "max_output_tokens": 2048}
        resp = post_json("https://api.x.ai/v1/responses", payload, headers, stream_name="ai_xai")
        if not resp:
            return None, None
        text, sources = "", []
        for block in resp.get("output", []) or []:
            if block.get("type") == "message":
                for part in block.get("content", []) or []:
                    if part.get("type") == "output_text":
                        text += part.get("text", "")
                        for a in part.get("annotations", []) or []:
                            if a.get("url"):
                                sources.append({"title": a.get("title", ""), "url": a["url"]})
        CALLS["xai"] += 1
        return parse_json_loose(text), sources
    payload = {"model": XAI_MODEL, "temperature": 0.1, "max_tokens": 2048,
               "messages": [{"role": "system", "content": system},
                            {"role": "user", "content": prompt}]}
    resp = post_json("https://api.x.ai/v1/chat/completions", payload, headers, stream_name="ai_xai")
    if not resp:
        return None, None
    CALLS["xai"] += 1
    return parse_json_loose(resp["choices"][0]["message"]["content"]), []


def _openai_compat(url, key, model, stream_name, system, prompt):
    if not key or not circuit_ok(stream_name):
        return None, None
    headers = {"Authorization": f"Bearer {key}",
               "HTTP-Referer": "https://lukerdavis.github.io/anti-establishment-congressional-tracker/",
               "X-Title": "AECT"}
    payload = {"model": model, "temperature": 0.1, "max_tokens": 2048,
               "messages": [{"role": "system", "content": system + "\nRespond with JSON only."},
                            {"role": "user", "content": prompt}]}
    resp = post_json(url, payload, headers, stream_name=stream_name)
    if not resp:
        return None, None
    try:
        return parse_json_loose(resp["choices"][0]["message"]["content"]), []
    except Exception:
        return None, None


def ask_json(system, prompt, *, search=False):
    """
    Returns (parsed_json, sources, provider) or (None, None, None).
    search=True requires a provider with live web grounding; without one the
    call is skipped rather than answered from stale model memory.
    """
    order = [("gemini", lambda: _gemini(system, prompt, search)),
             ("xai",    lambda: _xai(system, prompt, search))]
    if not search:
        order += [("openrouter", lambda: _openai_compat(
                      "https://openrouter.ai/api/v1/chat/completions", OPENROUTER_KEY,
                      OPENROUTER_MODEL, "ai_openrouter", system, prompt)),
                  ("groq", lambda: _openai_compat(
                      "https://api.groq.com/openai/v1/chat/completions", GROQ_KEY,
                      GROQ_MODEL, "ai_groq", system, prompt))]
    for name, fn in order:
        result, sources = fn()
        if result is not None:
            if name in ("openrouter", "groq"):
                CALLS[name] += 1
            return result, (sources or []), name
    CALLS["failed"] += 1
    return None, None, None
