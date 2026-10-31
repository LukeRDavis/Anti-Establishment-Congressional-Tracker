# Anti-Establishment Congressional Tracker (AECT)

Interactive hemicycle map of every US Congress member who opposes establishment politics by means of opposing foreign interventionism.

The goal of this tracker is Non-Partisan Coalition Building: Designed for progressives, constitutionalists, and libertarians to track anti-establishment progress across these shared values.

**Live site:** https://lukerdavis.github.io/Anti-Establishment-Congressional-Tracker/

## How the data is built (pipeline v10)

No person, race, vote or bill is typed into the code. `scripts/fetch_data.py` rebuilds everything every 4 hours (GitHub Actions):

1. **Members of Congress**: House Clerk `MemberData.xml` and the senate.gov roster, checked against the most recent roll calls so deaths and resignations are caught.
2. **Candidates**: every House and Senate filer with the FEC for the current cycle.
3. **General information**: Wikipedia, Ballotpedia (primary results are accepted only when dated in the current cycle) and Grokipedia, plus each state's primary calendar.
4. **Anti-interventionist evidence**:
   * **Votes.** Every roll call is pulled from clerk.house.gov and senate.gov. An AI with web search decides which votes concern foreign military aid, arms or war powers, and which side is the anti-interventionist one. Members are then classified by how they voted.
   * **Statements.** Candidates without a voting record are researched by AI with web search. A position counts only if it comes with a cited source.
   * **Trackers.** TrackAIPAC and RLC data set research priority and serve as corroboration. They are never the only evidence.
   * **Money.** FEC totals and outside spending for and against each candidate.
5. **Prediction markets**: Polymarket, Kalshi and PredictIt, matched by name and race.

Each run writes `pipeline_report.json`, also shown as a table in the Actions job summary, listing which data streams arrived and why any failed.

**Secrets used** (all optional; a missing one is reported, not fatal): `CONGRESS_KEY`, `FEC_KEY`, `GEMINI_KEY`, `XAI_API_KEY`, `OPENROUTER_API_KEY`, `GROQ_API_KEY`, `LEGISCAN_KEY`. Vote judgments and candidate research need `GEMINI_KEY` or `XAI_API_KEY`, because those two providers can search the web.

Phase 2 of AECT will involve the following:

**Fiscal Responsibility or Anti-Cantillon Deficit Inflationism** Track members who oppose deficit spending and inflationary policies, with options to filter for those prioritizing welfare-focused or zero-inflation approaches.

**Labor & Corporate Accountability or Pro-Good Labor:** Identify legislators supporting fair labor practices, anti-monopoly policies, and opposing exploitative foreign labor models (e.g., corporate global labor arbitrage).

**Environmental Stewardship or Pro Sustainable Nature and Agriculture:** Highlight support for regenerative agriculture, wilderness conservation, and ecological restoration.

