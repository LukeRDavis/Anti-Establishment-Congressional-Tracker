#!/usr/bin/env python3
"""
Merge this job's per-person caches with the versions another job pushed meanwhile.

The full refresh and the hourly judge both write data/cache/establishment.json (and a
few other per-key caches). Before committing, each job runs this against origin/main
so that for every key the newest entry wins and neither job erases the other's work.

Usage: python scripts/merge_caches.py <ref>   (e.g. origin/main, after git fetch)
"""
import json, os, subprocess, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHES = ["establishment", "fec_employers", "member_bills", "headline_screen", "odds_snapshots", "vote_judgments"]


def _stamp(v):
    return (v or {}).get("judged_at") or (v or {}).get("at") or "" if isinstance(v, dict) else ""


def main(ref):
    for name in CACHES:
        rel = f"data/cache/{name}.json"
        path = os.path.join(ROOT, rel)
        try:
            theirs = json.loads(subprocess.run(["git", "show", f"{ref}:{rel}"], cwd=ROOT, capture_output=True,
                                               check=True, text=True).stdout)
        except Exception:
            continue
        try:
            with open(path) as f:
                mine = json.load(f)
        except Exception:
            mine = {}
        if not isinstance(mine, dict) or not isinstance(theirs, dict):
            continue
        merged, taken = dict(mine), 0
        for k, v in theirs.items():
            # an odds snapshot taken at or after its race's poll close is never a valid freeze
            close = (v.get("close") if isinstance(v, dict) else None) or \
                ((merged.get(k) or {}).get("close") if isinstance(merged.get(k), dict) else None)
            if close and _stamp(v) >= close:
                continue
            if k not in merged or _stamp(v) > _stamp(merged.get(k)):
                merged[k] = v
                taken += 1
        with open(path, "w") as f:
            json.dump(merged, f, separators=(",", ":"))
        print(f"{name}: kept {len(mine)} local, took {taken} newer from {ref}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "origin/main")
