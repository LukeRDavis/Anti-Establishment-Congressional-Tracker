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

**Fiscal Responsibility or Anti-Cantillon Deficit Inflationism:** Track members who oppose deficit spending and inflationary policies, with options to filter for those prioritizing welfare-focused or zero-inflation approaches.

* **Who counts as anti-establishment on this plank.** A member or candidate counts when they are anti-deficit AND/OR:
  * **support zero or near-zero monetary inflation**, meaning caps on money-supply (M2) growth tied to real productivity and innovation rather than to credit expansion; AND/OR
  * **fight loose monetary policy and its link to big banks and debt-fueled corporate expansion**: cheap money passed through bank substructures (interest on reserve balances (IORB), prime-rate corporate lending) that funds expansion corporations have not earned, buybacks propped up by debt, and the price inflation that follows.
* **Who is establishment.** Members who back loose monetary policy and the debt-fueled corporate expansion it funds represent the corporatocratic, pro-big-bank side of monetary inflation, whatever their stated position on deficits.
* **Framework.** The plank draws on two documents by the author. The *Greed Premium* working paper finds that corporations funding expansion with new debt (about $799B a year, bridging an 18.4-point gap between actual and self-funding margins) inject demand without matching production, which shows up as price inflation. The *Inflation Control Amendment* draft caps annual monetary inflation at 2% and at a credit-adjusted innovation rate (about 0.5% a year).
* **How it is judged.** As with anti-arms, AI contextualizers decide from the record using general criteria, never names:
  * votes and sponsorships on Federal Reserve policy, IORB, bank regulation, bailouts, debt ceilings and deficit spending;
  * donors from the finance sector;
  * how specific and consistent the candidate's statements are.

**Labor & Corporate Accountability or Pro-Good Labor:** Identify legislators supporting fair labor practices, anti-monopoly policies, and opposing exploitative foreign labor models (e.g., corporate global labor arbitrage).

**Environmental Stewardship or Pro Sustainable Nature and Agriculture:** Highlight support for regenerative agriculture, wilderness conservation, and ecological restoration.

