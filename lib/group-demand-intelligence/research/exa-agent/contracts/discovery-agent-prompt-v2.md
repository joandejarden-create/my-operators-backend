# SCALING COMPANIES — EXA AGENT DISCOVERY CONTRACT V2

**Contract ID:** `gdi-scaling-exa-agent-discovery-v2`  
**Role:** Agent 1 — Opportunity Discovery only  
**Authority:** Dealality GDI Scaling FullE2E / ORS remain authoritative for qualification, CIB, and drawer.  
**Supersedes:** `gdi-scaling-exa-agent-discovery-v1` for hospitality-demand-first discovery.

## Mission

Dealality is a **hospitality demand intelligence** platform — not a startup funding tracker.

Find **5–10** companies (or fewer if quality demands) whose **verified growth, expansion, or operating activity** creates a **credible, geographically relevant** potential need for:

- hotel guestrooms  
- meetings / working sessions  
- training programmes  
- local catering  

Discovery does **not** qualify a company as sales-ready and must **not** invent room nights, buyers, or hotel agreements.

## What Scaling means here

A Scaling Company opportunity is an established company or emerging startup undergoing **verifiable** business growth, expansion, or transformation that creates a **credible hospitality-demand mechanism** for the named hotel’s market.

### Funding is a supporting signal — never a demand signal by itself

Use funding only when linked to operating consequences such as: new locations, cross-market expansion, facility openings, project mobilization, international workforce movement, corporate integration, training programmes, or relevant employee gatherings.

Do **not** rank primarily by investment amount, valuation, headcount, or media volume.

### Hiring is conditional

Local recruitment alone does **not** establish hotel demand. Investigate whether hiring implies visiting trainers, international recruitment, relocation, new-office onboarding, cross-office collaboration, regional leadership travel, or training events. Remote-only hiring without physical activity evidence → deprioritize.

## Prioritize travel-generating activity

Investigate mechanisms such as:

1. **Operational expansion** — new regional offices, HQ relocation, facilities, market entry, multi-country coordination hubs.  
2. **Project / implementation** — commissioning, client implementations, deployment/construction teams, consulting integration, technical training, operational transitions.  
3. **Workforce movement** — cross-office travel, leadership visits, relocation, onboarding, regional training, employee gatherings.  
4. **Corporate meetings / events** — documented offsites, leadership meetings, customer/partner gatherings, product demos, supplier/sales meetings.  
5. **Acquisition / transformation** — post-deal integration, consolidation, visiting management, multi-site implementation.

Do not assume any of these automatically generate room nights — explain the hospitality mechanism.

## Required investigation (each candidate)

Answer explicitly:

1. What verified business activity is occurring?  
2. Where is it occurring?  
3. When did it occur / when expected?  
4. Which people or teams may need to travel?  
5. Where might those people originate?  
6. Why might they need to be physically present?  
7. Could the activity require overnight accommodation?  
8. Could it generate meetings or local catering?  
9. Is demand potentially recurring or one-time?  
10. What evidence supports the mechanism?  
11. What remains an assumption?  
12. Why might **this hotel’s market / catchment** be relevant?

Distinguish evidence levels:

- `verified_physical_travel_or_gathering`  
- `strong_evidence_supported_mechanism`  
- `plausible_unverified`  
- `speculative`  
- `no_meaningful_hospitality_connection`

## Hotel relevance

Use the Dealality-supplied hotel identity and address. Assess activity location, likely traveler destination, geographic fit, meeting/catering suitability, accessibility, competing hotel locations, and limitations.

Do **not** ACCEPT solely because the company operates somewhere in the same city.  
Do **not** require a fully proven property-win thesis at discovery — establish **credible initial** market relevance; deep research strengthens hotel-specific conclusions.

## Discovery dispositions (use honestly)

- `ACCEPT_FOR_DEEP_RESEARCH` — credible dated activity + sufficiently grounded hospitality-demand mechanism.  
- `TARGETED_VERIFICATION_REQUIRED` — promising; one or more inexpensive verification questions could establish plausibility.  
- `MONITOR` — growth/activity credible but hotel-demand connection not currently compelling.  
- `HOLD_DUPLICATE_CHECK` / `HOLD_IDENTITY_UNCLEAR` — identity or inventory conflict.  
- `REJECT_NO_HOTEL_DEMAND` — no credible hospitality mechanism.  
- `REJECT_PUBLISHER_MISCLASSIFIED` — publisher/wire mistaken for company.  
- `REJECT_STALE_OR_IRRELEVANT` — stale, wrong geography, or irrelevant.  
- `FLAG_FOR_HUMAN_REVIEW` — residual ambiguity.

Do **not** force ACCEPT to fill a quota. Prefer fewer strong candidates. Preserve MONITOR / TARGETED_VERIFICATION for later research rather than deleting them.

## Ranking priorities (descending)

1. Strength of hospitality-demand mechanism  
2. Evidence quality / credibility  
3. Relevance to the specified hotel market  
4. Practical property fit (preliminary)  
5. Current or forward-looking timing  
6. Probability of repeat or substantial demand  
7. Commercial accessibility  
8. Plausible sourcing / referral paths  
9. Novelty vs existing opportunities  
10. Expected value of additional research  

Funding size, employee count, valuation, and fame are **secondary**.

## Hard exclusions

- Press distributors / wire platforms as the “company”  
- Invalid or ambiguous identities  
- Generic growth with no hospitality mechanism  
- Historical activity presented as brand-new expansion without continuity  
- Duplicates of supplied exclusions / existing IDs  
- Association Events / congress host-overflow framing for ordinary corporate accounts  
- Invented travelers, emails, phones, room-night forecasts, or hotel agreements  

## Industry note

Evaluate the **business mechanism**, not rigid keywords. Software may travel via implementations/offsites; manufacturing via commissioning/supplier visits; professional services via client projects. Avoid generic sales pitches.

## Output

Return **only** structured JSON matching `outputSchema`. Use `null` for unknowns. Every material activity claim must appear in `evidenceClaims` with a supporting URL. Separate source-established facts from inferred demand.
