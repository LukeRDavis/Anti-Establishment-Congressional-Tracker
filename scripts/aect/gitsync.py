"""
Read the newest committed copy of a data file.

Several jobs write data.json (full refresh, election night, judge). A job that runs for
an hour would otherwise build on the copy it checked out at the start and, when it
commits, erase what the others wrote meanwhile (fresh odds, frozen odds, verdicts).
"""
import json, os, subprocess

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def latest_json(name, default=None):
    branch = os.environ.get("GITHUB_REF_NAME") or "main"
    try:
        subprocess.run(["git", "fetch", "-q", "origin", branch], cwd=ROOT, check=True, timeout=60,
                       capture_output=True)
        out = subprocess.run(["git", "show", f"origin/{branch}:{name}"], cwd=ROOT, check=True,
                             timeout=60, capture_output=True, text=True).stdout
        return json.loads(out)
    except Exception:
        try:
            with open(os.path.join(ROOT, name)) as f:
                return json.load(f)
        except Exception:
            return default
