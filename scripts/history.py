#!/usr/bin/env python3
"""Build past Congresses (since 2001) a few at a time; prints REMAINING=<n> for the workflow."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("AECT_AI_DEADLINE_MIN", "40")
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from aect import past

remaining = past.sync(max_congresses=int(os.environ.get("AECT_HISTORY_PER_RUN", "3")))
print(f"REMAINING={remaining}")
