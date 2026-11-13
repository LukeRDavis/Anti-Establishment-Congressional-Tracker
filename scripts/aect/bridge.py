"""The bridge: once an incumbent first counts as anti-establishment in a Congress, they keep counting
in every later Congress until something ends it. What ends it:

  * a vote for aid to Israel (the anchor) after they first counted, unless they turned back and held
    (two or more anti votes after it);
  * money from the Israel lobby or war profiteers after they first counted;
  * actively backing a party leader or Speaker candidate whose own record fails the standard
    (endorsing, nominating, whipping or campaigning for them, or serving in leadership under them;
    the routine opening-day party-line floor vote alone does not count).

Past Congresses (from Voteview) can only see votes; money and leadership support are judged for the
current and next Congress, where the AI judge researches them."""
import json
import os

from . import votes as V

PATH = os.path.join("data", "history", "bridges.json")


def ordinal(n):
    n = int(n)
    return f"{n}{'th' if 10 <= n % 100 <= 20 else {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')}"


def anchor_break(votes, since):
    """The vote that broke the bridge on the anchor: an interventionist vote on aid to Israel cast in
    a Congress after `since`, not followed by two or more anti votes on it. None if unbroken."""
    vs = sorted([v for v in votes if v.get("area") == V.ANCHOR and int(v.get("congress") or 0) > int(since)],
                key=lambda v: v.get("date") or "")
    pro = [i for i, v in enumerate(vs) if not v.get("anti")]
    if not pro:
        return None
    after = [v for v in vs[pro[-1] + 1:] if v.get("anti")]
    return None if len(after) >= 2 else vs[pro[-1]]


def break_words(v):
    what = v.get("measure") or v.get("legis") or "a measure"
    return f"voted for {V.AREA_LABEL[V.ANCHOR]} on {v.get('date', '')} ({what})"


def load():
    try:
        with open(PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def save(state):
    os.makedirs(os.path.dirname(PATH), exist_ok=True)
    with open(PATH, "w") as f:
        json.dump(state, f, separators=(",", ":"))
