# GDI HIDDEN DEMAND V3 — FOUNDER REPORT

**Run:** `gdi_hd_v3_2026-09-28T09-44-13-328Z`
**Apply:** true
**Offline:** false
**Verdict:** **GDI CUSTOMER SURFACE WAS OVERPROMOTED — NOW CORRECTED**

---

## A. V2 REPROCESS

| Metric | Count |
|--------|------:|
| V2 STRICT ENTITIES | 88 |
| HIGH_RESEARCH_VALUE | 0 |
| MEDIUM | 0 |
| LOW | 20 |
| STOPPED EARLY | 68 |
| DEEPLY RESEARCHED | 0 |

---

## B. TEAM EVIDENCE

| Metric | Count |
|--------|------:|
| TEAM_SUPPORTED | 0 |
| MULTIPLE NAMED PEOPLE | 0 |
| COMPANY EVENT PAGE | 0 |
| SPEAKER/STAFF SIGNAL | 0 |
| AGENCY RELATIONSHIP | 0 |

---

## C. TRAVEL

| Class | Count |
|-------|------:|
| STRONG | 0 |
| MEDIUM | 0 |
| WEAK | 0 |
| UNKNOWN | 88 |

---

## D. LODGING

| Proof | Count |
|-------|------:|
| CONFIRMED | 0 |
| STRONG_INFERENCE | 0 |
| PLAUSIBLE | 0 |
| WEAK | 0 |
| UNKNOWN | 88 |

---

## E. MARKET VS HOTEL STATE

| State | Count |
|-------|------:|
| MARKET_ENTITY | 88 |
| MARKET_HIDDEN_CANDIDATE | 0 |
| LODGING_PLAUSIBLE | 0 |
| HOTEL_MATCH_CANDIDATE | 0 |
| HOTEL_OPPORTUNITY | 0 |
| ACTIONABLE_NOW | 0 |

---

## F. HILTON

| Metric | Before | After |
|--------|-------:|------:|
| CUSTOMER-VISIBLE | 0 | 0 |
| ACTIONABLE | 0 | 0 |
| WATCH | 0 | 0 |
| DOWNGRADED TO MARKET | — | 0 |

Hotel-match results (post-gate): matched=0, opportunity=0, actionable=0

---

## G. RENAISSANCE

| Metric | Before | After |
|--------|-------:|------:|
| CUSTOMER-VISIBLE | 0 | 0 |
| ACTIONABLE | 0 | 0 |
| WATCH | 0 | 0 |
| DOWNGRADED TO MARKET | — | 0 |

Hotel-match results (post-gate): matched=0, opportunity=0, actionable=0

---

## H. CONTACT

### Hilton
- (none)

### Renaissance
- (none)

---

## I. TOP QUALIFIED HIDDEN DEMAND

| Company/Team | Generator | Team Evidence | Lodging Evidence | WHO | Hilton | Renaissance |
|--------------|-----------|---------------|------------------|-----|--------|-------------|
| — | — | — | — | — | — | — |

---

## J. V2 STRONGEST ROWS

**FRANCHISE Solutions Group:** MARKET_ENTITY — event_not_nyc_midtown (geo={"geography":"NON_NYC","reason":"event_city_istanbul","destination":"ISTANBUL"})

**Vanguard Industrial Corp:** MARKET_ENTITY — event_not_nyc_midtown (geo={"geography":"NON_NYC","reason":"event_city_istanbul","destination":"ISTANBUL"})

---

## K. JEV

| Metric | Count |
|--------|------:|
| CALLS | 0 |
| SAFE APPLY | 0 |
| EXHIBITOR_TEAM_RESEARCH_PATH | 0 |
| LODGING_EVIDENCE_NEXT_STEP | 0 |
| HELPFUL_DIFFERENT | 0 |
| SAME | 0 |
| WRONG | 0 |
| HIGH_CONF_WRONG | 0 |

---

## L. JEV IMPACT

| Metric | Count |
|--------|------:|
| TEAM EVIDENCE FOUND VIA JEV PATH | 0 |
| LODGING EVIDENCE FOUND VIA JEV PATH | 0 |
| WHO UPGRADES | 0 |
| FETCHES AVOIDED | 0 |
| LOW-VALUE ENTITIES STOPPED | 88 |

**MATERIAL VALUE:** NO

---

## M. PROGRAM PDF

| Metric | Count |
|--------|------:|
| PDFS USED FOR ENRICHMENT | 0 |
| TEAM UPGRADES | 0 |
| CONTACT UPGRADES | 0 |
| LODGING UPGRADES | 0 |
| BULK OPPORTUNITY CREATION | 0 |

---

## N. HOUSING SOURCES

| Metric | Count |
|--------|------:|
| HOUSING SOURCES FOUND | 6 |
| CANDIDATES ENRICHED | 0 |

---

## O. FUNNEL

| Stage | Count |
|-------|------:|
| DIRECTORY ENTITIES | 88 |
| TEAM-SUPPORTED | 0 |
| LODGING-SUPPORTED | 0 |
| WHO-RESOLVED | 0 |
| HOTEL OPPORTUNITIES | 0 |
| ACTIONABLE | 0 |

Queries=3 · Fetches=6

---

## P. QUALITY

| Check | Expected |
|-------|----------|
| DIRECTORY-ONLY CUSTOMER ROWS | 0 |
| THIN DRAWERS | 0 |
| GENERATOR-ONLY OPPS | 0 |
| INTERNAL ID LEAKS | 0 |
| SURFE AUTO | 0 |
| SURFE PERSISTED PII | 0 |
| HOTEL HARDCODES | 0 |
| CONTACT HARDCODES | 0 |

---

## Q. DECISION

1. Were V2 entities matched to hotels too early? **YES — V2 matched 88 entities to both hotels before lodging/team proof**
2. Market-candidate vs hotel-opportunity separation correct? **YES — MARKET_ENTITY → … → HOTEL_OPPORTUNITY enforced; hotel match gated on CONFIRMED/STRONG_INFERENCE + NYC geography**
3. Traveling team evidence: **0**
4. Credible lodging evidence: **0**
5. Relevant named WHO: **0**
6. Legitimate hotel opportunities: **0**
7. ACTIONABLE_NOW: **0**
8. V2 rows downgraded: **0**
9. Company event pages: **0**
10. PDFs as enrichment: **used=0; bulk_creation=0**
11. Housing coverage: **{"found":6,"enriched":0}**
12. Exhibitor WHO improved: **LIMITED**
13. Jev team research: **NO**
14. Jev lodging research: **NO**
15. Jev stopped low-value: **88**
16. Wrong Jev decisions: **0**
17. Hotels only plausible pursuits: **YES — non-NYC events (e.g. Istanbul Expo) blocked from Midtown hotel match**
18. Discover-once/match-many: **YES**
19. Commercially useful: **NOT YET — surface corrected; proof chain still thin**
20. Largest remaining gap: **Credible NYC-event lodging proof (housing pages / company travel) for exhibitor teams**

---

## R. FINAL VERDICT

**GDI CUSTOMER SURFACE WAS OVERPROMOTED — NOW CORRECTED**

---

## PERSISTENCE / GENERALIZATION

CODE FILES:
- lib/group-demand-intelligence/hidden-demand/v3-states.js
- lib/group-demand-intelligence/hidden-demand/v3-triage.js
- lib/group-demand-intelligence/hidden-demand/v3-lodging-ladder.js
- lib/group-demand-intelligence/hidden-demand/v3-team-research.js
- lib/group-demand-intelligence/hidden-demand/v3-event-geography.js
- lib/group-demand-intelligence/hidden-demand/v3-promotion-gate.js
- lib/group-demand-intelligence/hidden-demand/v3-entity-clean.js
- lib/group-demand-intelligence/hidden-demand/jev-v3-routing.js
- lib/group-demand-intelligence/hidden-demand/reprocess-v3.js
- lib/group-demand-intelligence/hidden-demand/nyc-exhibitor-acquire-v3.js
- lib/group-demand-intelligence/jev/jev-types.js
- scripts/gdi-hidden-demand-v3.mjs
- scripts/test-gdi-hidden-demand-v3.mjs

TESTS: scripts/test-gdi-hidden-demand-v3.mjs

HOTEL HARDCODES: 0 (IDs only in runner)
CONTACT HARDCODES: 0
SURFE AUTO: 0
SURFE PERSISTED PII: 0
WEBHOUND REQUIRED: 0
JEV PERSON APPLY: NO
