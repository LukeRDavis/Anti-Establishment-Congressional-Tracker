#!/usr/bin/env python3
"""
Liveness check for every API the tracker uses: one tiny request each.
Prints status and a short detail per service; keys are never printed.
"""
import json, os, sys, time, urllib.request, urllib.error

TIMEOUT = 30


def call(url, data=None, headers=None):
    t = time.time()
    req = urllib.request.Request(url, data=json.dumps(data).encode() if data is not None else None,
                                 headers={"User-Agent": "AECT-healthcheck", "Content-Type": "application/json",
                                          **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
            return r.status, json.loads(r.read() or b"{}"), round(time.time() - t, 2)
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")[:200]
        return e.code, {"_error": body}, round(time.time() - t, 2)
    except Exception as e:
        return None, {"_error": str(e)[:200]}, round(time.time() - t, 2)


def key(*names):
    return next((os.environ[n] for n in names if os.environ.get(n)), "")


rows = []


def report(name, ok, code, secs, detail):
    rows.append((name, ok, code, secs, detail))
    print(f"{'OK  ' if ok else 'FAIL' if ok is False else 'SKIP'}  {name:22} HTTP {code}  {secs}s  {detail}")


def chat(name, url, k, model, extra_headers=None):
    code, d, s = call(url, {"model": model, "max_tokens": 5, "temperature": 0,
                            "messages": [{"role": "user", "content": "Reply with the word OK."}]},
                      {"Authorization": f"Bearer {k}", **(extra_headers or {})})
    txt = ((d.get("choices") or [{}])[0].get("message") or {}).get("content", "") if code == 200 else d.get("_error", "")
    report(f"{name} chat", code == 200, code, s, f"{model}: {txt.strip()[:80]!r}")


# Congress.gov
k = key("CONGRESS_KEY", "CONGRESS_API_KEY")
if k:
    code, d, s = call(f"https://api.congress.gov/v3/member?limit=1&format=json&api_key={k}")
    report("Congress.gov", code == 200, code, s, f"{(d.get('pagination') or {}).get('count', '?')} members indexed" if code == 200 else d.get("_error"))
else:
    report("Congress.gov", None, "-", 0, "no key")

# FEC
k = key("FEC_KEY", "FEC_API_KEY")
if k:
    code, d, s = call(f"https://api.open.fec.gov/v1/candidates/?per_page=1&api_key={k}")
    report("FEC", code == 200, code, s, f"{(d.get('pagination') or {}).get('count', '?')} candidates" if code == 200 else d.get("_error"))
else:
    report("FEC", None, "-", 0, "no key")

# Gemini
k = key("GEMINI_KEY", "GEMINI_API_KEY", "GOOGLE_API_KEY")
if k:
    code, d, s = call(f"https://generativelanguage.googleapis.com/v1beta/models?pageSize=200&key={k}")
    models = [m["name"].split("/")[-1] for m in d.get("models", [])
              if "generateContent" in (m.get("supportedGenerationMethods") or []) and "flash" in m["name"]
              and not any(x in m["name"] for x in ("image", "tts", "audio", "live", "lite"))]
    report("Gemini models", code == 200, code, s, f"{len(models)} flash models" if code == 200 else d.get("_error"))
    import re as _re
    def _v(n):
        nums = _re.findall(r"\d+(?:\.\d+)?", n)
        return (float(nums[0]) if nums else 0, "preview" not in n, -len(n))
    ranked = sorted([m for m in models if not _re.search(r"omni|robotics|computer|thinking|exp|8b", m)], key=_v, reverse=True)[:6]
    for m in ranked:                       # same order the tracker uses; first that answers wins
        code, d, s = call(f"https://generativelanguage.googleapis.com/v1beta/models/{m}:generateContent?key={k}",
                          {"contents": [{"parts": [{"text": "Reply with the word OK."}]}],
                           "generationConfig": {"maxOutputTokens": 5}})
        txt = "".join(p.get("text", "") for p in ((d.get("candidates") or [{}])[0].get("content") or {}).get("parts", [])) \
            if code == 200 else d.get("_error", "")[:90]
        report("Gemini chat", code == 200, code, s, f"{m}: {txt.strip()!r}")
        if code == 200:
            break
else:
    report("Gemini", None, "-", 0, "no key")

# OpenAI-compatible free tiers
for name, models_url, chat_url, names in [
        ("Groq", "https://api.groq.com/openai/v1/models", "https://api.groq.com/openai/v1/chat/completions", ("GROQ_API_KEY", "GROQ_KEY")),
        ("Mistral", "https://api.mistral.ai/v1/models", "https://api.mistral.ai/v1/chat/completions", ("MISTRAL_API_KEY", "MISTRAL_KEY")),
        ("Cerebras", "https://api.cerebras.ai/v1/models", "https://api.cerebras.ai/v1/chat/completions", ("CEREBRAS_API_KEY", "CEREBRAS_KEY"))]:
    k = key(*names)
    if not k:
        report(name, None, "-", 0, "no key")
        continue
    code, d, s = call(models_url, headers={"Authorization": f"Bearer {k}"})
    ids = [m.get("id") for m in d.get("data", [])] if code == 200 else []
    report(f"{name} models", code == 200, code, s, f"{len(ids)} models" if code == 200 else d.get("_error"))
    pick = next((i for i in ids if any(x in i for x in ("gpt-oss-120b", "mistral-small", "llama-3.3", "qwen"))), ids[0] if ids else None)
    if pick:
        chat(name, chat_url, k, pick)
        if rows[-1][2] == 429:               # a burst limit (another job may be using it): one retry
            time.sleep(5)
            chat(name, chat_url, k, pick)

# OpenRouter: key status shows whether the $10 tier (1,000 free requests/day) is active
k = key("OPENROUTER_API_KEY", "OPENROUTER_KEY")
if k:
    code, d, s = call("https://openrouter.ai/api/v1/key", headers={"Authorization": f"Bearer {k}"})
    info = d.get("data") or {}
    report("OpenRouter key", code == 200, code, s,
           f"is_free_tier={info.get('is_free_tier')} usage=${info.get('usage')} limit={info.get('limit')}"
           if code == 200 else d.get("_error"))
    code, d, s = call("https://openrouter.ai/api/v1/models")
    free = [m["id"] for m in d.get("data", []) if m.get("id", "").endswith(":free")]
    report("OpenRouter models", code == 200, code, s, f"{len(free)} free models")
    for m in free[:4]:
        chat("OpenRouter", "https://openrouter.ai/api/v1/chat/completions", k, m,
             {"HTTP-Referer": "https://lukerdavis.github.io/anti-establishment-congressional-tracker/", "X-Title": "AECT"})
        if rows[-1][1]:
            break
else:
    report("OpenRouter", None, "-", 0, "no key")

failed = [r for r in rows if r[1] is False]
print(f"\n{len(rows) - len(failed)} of {len(rows)} checks passed or skipped; {len(failed)} failed")
if os.environ.get("GITHUB_STEP_SUMMARY"):
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
        f.write("## API health\n\n| Check | Result | HTTP | Seconds | Detail |\n|---|---|---|---|---|\n")
        for n, ok, c, sec, det in rows:
            f.write(f"| {n} | {'✅' if ok else '❌' if ok is False else '·'} | {c} | {sec} | {str(det).replace('|', '/')[:150]} |\n")
sys.exit(0)
