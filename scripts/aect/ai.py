"""
AI layer. One entry point — ask_json() — that cascades across every configured
provider. When `search=True` the call is grounded in live web results (Gemini
Google Search grounding or xAI web_search), which is how the pipeline asks
AI to *contextualize* things the raw data can't explain on its own (what an
amendment actually did, what a candidate has said) while the numbers
themselves still come from the primary sources.
"""
import json, os, re

from .config import GEMINI_KEY, XAI_KEY, OPENROUTER_KEY, GROQ_KEY, ai_time_left
from .net import post_json, get_json, stream, circuit_ok, circuit_trip, LAST_HTTP_STATUS

_GEMINI_MODELS = None


def gemini_models():
    """
    Ask Google which models this key can use and pick the newest general
    'flash' model that supports generateContent. Model names are retired
    regularly, so none is written into the code. AECT_GEMINI_MODELS overrides.
    """
    global _GEMINI_MODELS
    if _GEMINI_MODELS is not None:
        return _GEMINI_MODELS
    override = [m for m in os.environ.get("AECT_GEMINI_MODELS", "").split(",") if m]
    if override:
        _GEMINI_MODELS = override
        return override
    names = []
    d = get_json(f"https://generativelanguage.googleapis.com/v1beta/models?pageSize=200&key={GEMINI_KEY}",
                 stream_name="ai_gemini")
    for m in (d or {}).get("models", []):
        n = m.get("name", "").split("/")[-1]
        if "generateContent" not in (m.get("supportedGenerationMethods") or []):
            continue
        if "flash" not in n or re.search(r"lite|image|tts|audio|live|embed|thinking|exp|8b", n):
            continue
        names.append(n)

    def version(n):
        nums = re.findall(r"\d+(?:\.\d+)?", n)
        return (float(nums[0]) if nums else 0, "preview" not in n, -len(n))
    names.sort(key=version, reverse=True)
    _GEMINI_MODELS = names[:3]
    stream("ai_gemini", note=f"models available: {', '.join(_GEMINI_MODELS) or 'none found'}")
    return _GEMINI_MODELS
XAI_MODEL        = os.environ.get("AECT_XAI_MODEL", "grok-4-1-fast")
_COMPAT_MODELS = {}


def _rank_chat_models(ids):
    """Prefer large general chat models; drop speech, guard, embedding and vision-only ids."""
    keep = [i for i in ids if not re.search(r"whisper|guard|tts|embed|vision|audio|ocr|moderation|"
                                              r"compound|playai|distil|rerank|coder|math", i, re.I)]

    def score(i):
        il = i.lower()
        size = max([float(x) for x in re.findall(r"(\d+(?:\.\d+)?)b\b", il)] or [0])
        pref = 2 if re.search(r"gpt-oss|llama-4|llama-3\.3|qwen3|deepseek|kimi|gemini|mistral-large", il) else 0
        return (pref, min(size, 500))
    return sorted(keep, key=score, reverse=True)


def compat_models(provider):
    """Ask Groq / OpenRouter which models this key can use right now (names are retired often)."""
    if provider in _COMPAT_MODELS:
        return _COMPAT_MODELS[provider]
    env = os.environ.get(f"AECT_{provider.upper()}_MODELS", "")
    if env:
        _COMPAT_MODELS[provider] = [m for m in env.split(",") if m]
        return _COMPAT_MODELS[provider]
    ids = []
    if provider == "groq" and GROQ_KEY:
        d = get_json("https://api.groq.com/openai/v1/models", stream_name="ai_groq",
                     headers={"Authorization": f"Bearer {GROQ_KEY}"})
        ids = [m.get("id", "") for m in (d or {}).get("data", []) if m.get("active", True)]
    elif provider == "openrouter" and OPENROUTER_KEY:
        d = get_json("https://openrouter.ai/api/v1/models", stream_name="ai_openrouter")
        for m in (d or {}).get("data", []):
            pr = m.get("pricing") or {}
            free = m.get("id", "").endswith(":free") or (str(pr.get("prompt")) in ("0", "0.0")
                                                          and str(pr.get("completion")) in ("0", "0.0"))
            if free and (m.get("context_length") or 0) >= 16000:
                ids.append(m["id"])
    _COMPAT_MODELS[provider] = _rank_chat_models(ids)[:4]
    stream(f"ai_{provider}", note=f"models available: {', '.join(_COMPAT_MODELS[provider]) or 'none'}")
    return _COMPAT_MODELS[provider]

CALLS = {"gemini": 0, "xai": 0, "openrouter": 0, "groq": 0, "failed": 0}
_DEAD_MODELS = {}   # {search_flag: {model, ...}} models that hit quota or were retired this run


def available(search=False):
    if ai_time_left() <= 0:
        return False
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
    models = [m for m in gemini_models() if m not in _DEAD_MODELS.get(search, set())]
    for model in models:
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
            if LAST_HTTP_STATUS.get("ai_gemini") in (404, 429):
                _DEAD_MODELS.setdefault(search, set()).add(model)   # quota/retired: skip it this run
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


def _openai_compat(url, key, provider, system, prompt):
    stream_name = f"ai_{provider}"
    if not key or not circuit_ok(stream_name):
        return None, None
    headers = {"Authorization": f"Bearer {key}",
               "HTTP-Referer": "https://lukerdavis.github.io/anti-establishment-congressional-tracker/",
               "X-Title": "AECT"}
    dead = _DEAD_MODELS.setdefault(provider, set())
    models = [m for m in compat_models(provider) if m not in dead]
    for model in models:
        payload = {"model": model, "temperature": 0.1, "max_tokens": 2048,
                   "messages": [{"role": "system", "content": system + "\nRespond with JSON only."},
                                {"role": "user", "content": prompt}]}
        resp = post_json(url, payload, headers, stream_name=stream_name)
        if not resp:
            if LAST_HTTP_STATUS.get(stream_name) in (400, 404, 429):
                dead.add(model)
            continue
        try:
            out = parse_json_loose(resp["choices"][0]["message"]["content"])
            if out is not None:
                return out, []
        except Exception:
            continue
    if compat_models(provider) and all(m in dead for m in compat_models(provider)):
        circuit_trip(stream_name, "every available model failed or is over quota")
    return None, None


def available_any():
    return available(search=False)


def ask_json(system, prompt, *, search=False, allow_ungrounded=False):
    """
    Returns (parsed_json, sources, provider) or (None, None, None).
    search=True asks for live web grounding. If no grounded provider answers
    and allow_ungrounded=True (only used when the prompt already carries the
    primary-source text the model needs), the non-search providers are tried.
    """
    order = [("gemini", lambda: _gemini(system, prompt, search)),
             ("xai",    lambda: _xai(system, prompt, search))]
    if search and allow_ungrounded:
        order += [("gemini", lambda: _gemini(system, prompt, False)),
                  ("xai",    lambda: _xai(system, prompt, False))]
    if not search or allow_ungrounded:
        order += [("groq", lambda: _openai_compat(
                      "https://api.groq.com/openai/v1/chat/completions", GROQ_KEY, "groq", system, prompt)),
                  ("openrouter", lambda: _openai_compat(
                      "https://openrouter.ai/api/v1/chat/completions", OPENROUTER_KEY, "openrouter",
                      system, prompt))]
    for name, fn in order:
        result, sources = fn()
        if result is not None:
            if name in ("openrouter", "groq"):
                CALLS[name] += 1
            return result, (sources or []), name
    if GEMINI_KEY and gemini_models() and all(
            m in _DEAD_MODELS.get(search, set()) for m in gemini_models()):
        circuit_trip("ai_gemini", "every available model is over quota or retired")
    CALLS["failed"] += 1
    return None, None, None
