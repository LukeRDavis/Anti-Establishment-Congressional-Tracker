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
   * **Votes.** Every roll call is pulled from clerk.house.gov and senate.gov. An AI with web search sorts each relevant vote into one of five areas and decides which side is the anti-interventionist one:
     1. aid to the State of Israel;
     2. military aid or arms to any country (legitimate lend-lease deals and mostly defensive arms for a country resisting a direct violation of its territorial sovereignty, as recognized under international law standards such as the Hague conventions, UN, ICC, ICJ and human rights organizations, are neutral; this never covers aid to Israel, which is its own area);
     3. sanctions on a country for reasons other than human rights, genocide or mass killing (sanctions aimed at those are neutral);
     4. restraining U.S. use of force that answers no imminent threat to American soil or citizens and has no declaration of war;
     5. war declarations and authorizations: voting against broad or open-ended ones, or to repeal them, is anti-interventionist; a narrow, time-limited one aimed at preventing genocide or mass killing is neutral.

     Non-military aid to other countries is neutral. Broad must-pass bills (defense authorizations, omnibus spending) count only when they carried particularly severe interventionist provisions.
   * **The standard.** Aid to Israel is the anchor: a sitting member must have voted anti-interventionist on it and stayed consistent from then on; someone who changed course counts once their anti votes have held (two or more, into their latest Congress). The other four areas are supporting evidence: a member can stand anywhere on them, and going back to the interventionist side in one lowers confidence without disqualifying. Military aid and sanctions are judged country by country: anti on arms to one country doesn't oblige anti on arms to another, but once anti on a given recipient a member should stay consistent on it. A vote cast with one's own party still counts as anti-interventionist; whether a member also votes that way against their own party is how principle is told from posturing. The floor for a sitting member is one real breaking vote (anti-interventionist against most of their own party), plus, when an anti-interventionist discharge petition is open, signing it.
   * **Statements.** Candidates without a voting record are researched by AI with web search. A position counts only if it comes with a cited source.
   * **Trackers.** TrackAIPAC and RLC data set research priority and serve as corroboration. They are never the only evidence.
   * **Money.** FEC totals and outside spending for and against each candidate.
   * **Leadership defiance.** House discharge petitions and their signers, from the Clerk. Signing one while your own party holds the majority means going around your own Speaker.
   * **The judge.** Free-tier AI models act as a realistic operative for the coalition and ask one question about every member and running candidate: a reliable anti-establishment ally, or playing political games? Each verdict comes with a bottom line and a public write-up naming the exact model and the time it ran. A conservative rule decides until the AI reaches someone.
5. **Prediction markets**: Polymarket, Kalshi (including its who-wins series for every House and Senate race) and PredictIt, matched by name and race or by party nominee. Each race's odds are the average over the sources that have a price.
6. **Committee rosters**: chairs, ranking members and members of the committees tracked for each plank, from the congress-legislators project.

**Election night.** `scripts/election_night.py` runs every 15 minutes, but only in the window from 36 hours before to 96 hours after a general election. It refreshes the odds and the Wikipedia result boxes, freezes each race's odds one minute before its state's polls close (poll-closing times from The Green Papers), and marks races won or lost.

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

