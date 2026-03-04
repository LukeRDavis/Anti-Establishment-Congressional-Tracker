# Anti-Arms Congress Tracker

Interactive hemicycle map of every US Congress member who opposes arms sales to Israel — with live Polymarket odds, FEC finance data, challenger tracking, and P2P data sync via Gun.js.

**Live site:** https://lukerdavis.github.io/Anti-Arms-Congress-Tracker/

---

## Architecture

```
GitHub Actions (every 4 hours)
  └─ scripts/fetch_data.py
       ├─ reads FEC_KEY + POLY_KEY from GitHub Secrets (never exposed)
       ├─ fetches FEC campaign finance + Polymarket odds
       └─ writes data.json → committed to repo

GitHub Pages
  └─ serves index.html + data.json
       └─ index.html loads data.json on open
            └─ writes to Gun.js P2P graph → syncs to all open peers
                 └─ 60-second rate limit enforced across all peers via Gun timestamp
```

**API keys never touch the client.** The HTML only calls:
- `./data.json` (your own GitHub Pages, no key needed)
- Google Civic API (safe client-side if you restrict to your domain — see below)

---

## Setup (one-time, ~10 minutes)

### 1. Add GitHub Secrets

Go to your repo → **Settings → Secrets and variables → Actions → New repository secret**

Add these two secrets:

| Name | Value |
|------|-------|
| `FEC_KEY` | `R3Qtl007fPuyabNB6FaFIuNYjbfWsnDBKctlbJlQ` |
| `POLY_KEY` | `019cb8c0-76e7-7eae-96ee-555833a07879` |

> ⚠️ These keys are never written into any file in the repo. They only exist in GitHub's encrypted secrets vault and are injected as environment variables when the Action runs.

### 2. Enable GitHub Pages

Go to **Settings → Pages**
- Source: **Deploy from a branch**
- Branch: `main` / `(root)`
- Click Save

Your site will be live at `https://lukerldavis.github.io/Anti-Arms-Congress-Tracker/` within ~2 minutes.

### 3. Restrict the Google Civic API key

Go to [Google Cloud Console](https://console.cloud.google.com) → **APIs & Services → Credentials** → click your key → **Application restrictions → HTTP referrers** → add:

```
https://lukerldavis.github.io/*
```

This makes the Civic key safe to ship in the HTML — it won't work on any other domain.

### 4. Trigger the first data fetch manually

Go to **Actions → Refresh Data → Run workflow**

This runs `fetch_data.py` immediately and commits a populated `data.json`. Subsequent runs happen automatically every 4 hours.

---

## Files

| File | Purpose |
|------|---------|
| `index.html` | Entire app — self-contained, embeddable, no build step |
| `data.json` | Cache of FEC + Polymarket data, refreshed by Actions |
| `scripts/fetch_data.py` | Runs server-side in CI, holds API keys via env vars |
| `.github/workflows/fetch-data.yml` | Cron schedule + commit back to repo |

---

## Embedding

Paste this anywhere:

```html
<iframe
  src="https://lukerdavis.github.io/Anti-Arms-Congress-Tracker/"
  width="100%"
  height="900"
  style="border:none;border-radius:12px;"
  loading="lazy"
  title="Anti-Arms Congress Tracker">
</iframe>
```

---

## P2P / Gun.js behavior

- On load, the app checks Gun.js relay peers for a cached copy of `data.json`
- If peers have data fetched within the last 60 seconds, it uses that — **no API call made**
- If data is stale, it fetches `data.json` from GitHub Pages and broadcasts the result to all peers
- The 60-second rate limit is **global across all users** — enforced via a shared Gun timestamp
- If GitHub Pages goes down, peers who have opened the page recently continue serving each other

Gun relay peers used (public, free, no signup):
- `https://gun-us.herokuapp.com/gun`
- `https://gun-manhattan.herokuapp.com/gun`

---

## Updating member data

All member data is hardcoded in `index.html` (lines ~80–280). To update:
1. Edit `index.html` directly on GitHub or clone + push
2. GitHub Pages redeploys automatically within ~60 seconds
3. Gun peers pick up the new version on next page load

---

## Sources

- HR 3565 Block the Bombs Act co-sponsor list
- Senate Joint Resolutions of Disapproval (Nov 2024, Apr 2025)
- OpenFEC API — campaign finance, independent expenditures
- Polymarket — incumbent reelection odds
- Google Civic Information API — representative lookup by address
