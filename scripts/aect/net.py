"""
HTTP helpers, per-service circuit breakers, cache I/O and the stream-health
registry. Every fetch records its outcome against a named stream so the run
ends with a report of exactly which data arrived and which did not.
"""
import gzip, json, os, re, time, urllib.error, urllib.parse, urllib.request
from datetime import datetime, timezone

from .config import USER_AGENT

# ── Stream health registry ────────────────────────────────────────────────────
STREAMS = {}

def stream(name, *, ok=None, count=None, note=None, error=None):
    s = STREAMS.setdefault(name, {"ok": None, "count": 0, "requests": 0,
                                  "failures": 0, "errors": [], "notes": []})
    if ok is not None:
        s["ok"] = ok
    if count is not None:
        s["count"] = count
    if note:
        s["notes"].append(note)
        print(f"  [{name}] {note}")
    if error:
        s["failures"] += 1
        if len(s["errors"]) < 6 and error not in s["errors"]:
            s["errors"].append(error)
    return s

def _hit(name, failed=False, error=None):
    if not name:
        return
    s = stream(name)
    s["requests"] += 1
    if failed:
        stream(name, error=error)

# ── Circuit breakers ──────────────────────────────────────────────────────────
_OPEN = {}

def circuit_ok(service):
    return not _OPEN.get(service)

def circuit_trip(service, why):
    if service and not _OPEN.get(service):
        _OPEN[service] = why
        print(f"  ⚡ {service}: circuit opened ({why}) — skipping its remaining calls")
        stream(service, error=f"circuit opened: {why}")

# ── Requests ──────────────────────────────────────────────────────────────────
def redact(url):
    return re.sub(r'((?:api_key|key)=)[^&]+', r'\1***', url)

def _request(url, *, data=None, headers=None, timeout=30, method=None):
    h = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip"}
    h.update(headers or {})
    req = urllib.request.Request(url, data=data, headers=h, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
        if r.headers.get("Content-Encoding") == "gzip":
            raw = gzip.decompress(raw)
        return raw

def get_bytes(url, *, stream_name=None, headers=None, timeout=30, retries=2,
              trip_on=(401, 403), circuit=None, quiet_404=True):
    """GET returning bytes or None. Logs status + response body on failure."""
    circuit = circuit or stream_name
    if circuit and not circuit_ok(circuit):
        return None
    for attempt in range(retries + 1):
        try:
            raw = _request(url, headers=headers, timeout=timeout)
            _hit(stream_name)
            return raw
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read()[:300].decode(errors="replace").replace("\n", " ")
            except Exception:
                pass
            if e.code == 404 and quiet_404:
                _hit(stream_name)
                return None
            if e.code == 429 and attempt < retries:
                wait = int(e.headers.get("Retry-After", "0") or 0) or 8 * (attempt + 1)
                time.sleep(min(wait, 30))
                continue
            msg = f"HTTP {e.code} {redact(url)[:110]} :: {body[:160]}"
            print(f"    {msg}")
            _hit(stream_name, failed=True, error=msg)
            if e.code in trip_on:
                circuit_trip(circuit, f"HTTP {e.code}")
            return None
        except Exception as e:
            if attempt < retries:
                time.sleep(2 * (attempt + 1))
                continue
            msg = f"{type(e).__name__}: {e} :: {redact(url)[:110]}"
            print(f"    {msg}")
            _hit(stream_name, failed=True, error=msg)
            return None
    return None

def get_text(url, **kw):
    raw = get_bytes(url, **kw)
    return raw.decode("utf-8", errors="replace") if raw else ""

def get_json(url, **kw):
    raw = get_bytes(url, headers={"Accept": "application/json", **(kw.pop("headers", {}) or {})}, **kw)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception as e:
        _hit(kw.get("stream_name"), failed=True, error=f"bad JSON from {redact(url)[:90]}: {e}")
        return None

def post_json(url, payload, headers=None, *, stream_name=None, timeout=90, trip_on=(401, 403)):
    circuit = stream_name
    if circuit and not circuit_ok(circuit):
        return None
    data = json.dumps(payload).encode()
    h = {"Content-Type": "application/json", **(headers or {})}
    for attempt in range(3):
        try:
            raw = _request(url, data=data, headers=h, timeout=timeout, method="POST")
            _hit(stream_name)
            return json.loads(raw)
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read()[:300].decode(errors="replace").replace("\n", " ")
            except Exception:
                pass
            if e.code in (429, 500, 502, 503) and attempt < 2:
                time.sleep(6 * (attempt + 1))
                continue
            msg = f"HTTP {e.code} {redact(url)[:90]} :: {body[:200]}"
            print(f"    {msg}")
            _hit(stream_name, failed=True, error=msg)
            if e.code in trip_on or (e.code == 429 and attempt >= 2):
                circuit_trip(circuit, f"HTTP {e.code}")
            return None
        except Exception as e:
            if attempt < 2:
                time.sleep(3)
                continue
            _hit(stream_name, failed=True, error=f"{type(e).__name__}: {e}")
            return None
    return None

def qs(params):
    return urllib.parse.urlencode({k: v for k, v in params.items() if v is not None}, doseq=True)

# ── Cache files ───────────────────────────────────────────────────────────────
CACHE_DIR = os.environ.get("AECT_CACHE_DIR", "data/cache")

def load_cache(name, default=None):
    path = os.path.join(CACHE_DIR, f"{name}.json")
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return {} if default is None else default

def save_cache(name, obj):
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, f"{name}.json")
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, separators=(",", ":"), sort_keys=True, default=str)
    os.replace(tmp, path)

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def age_hours(ts):
    if not ts:
        return 1e9
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt).total_seconds() / 3600
    except Exception:
        return 1e9

def strip_html(html):
    html = re.sub(r'<(script|style|noscript)[^>]*>.*?</\1>', ' ', html, flags=re.S | re.I)
    text = re.sub(r'<[^>]+>', ' ', html)
    for a, b in (("&amp;", "&"), ("&nbsp;", " "), ("&#39;", "'"), ("&quot;", '"'),
                 ("&lt;", "<"), ("&gt;", ">"), ("&#8217;", "'"), ("&#8211;", "–")):
        text = text.replace(a, b)
    return re.sub(r'\s+', ' ', text).strip()


def parse_xml(raw, stream_name=None, what=""):
    """Parse XML defensively — a blocked request often returns an HTML page instead."""
    import xml.etree.ElementTree as ET
    if not raw:
        return None
    try:
        return ET.fromstring(raw)
    except ET.ParseError as e:
        head = raw[:160].decode(errors="replace").replace("\n", " ")
        stream(stream_name, error=f"not XML ({what}): {e} :: {head}")
        return None
