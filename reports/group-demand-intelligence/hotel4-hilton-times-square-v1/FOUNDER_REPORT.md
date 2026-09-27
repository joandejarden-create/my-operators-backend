# Hotel #4 Full Replication — Founder Report

**Hotel:** Hilton New York Times Square (NYCTSHH)  
**Branch:** `deploy/gdi-pe-v1-7-customer-closure`  
**Date:** 2026-09-27  
**HPC:** `rec35fExUxCClpOP6` · **ADP:** `adp_hilton_times_square` · **Period:** `adp_period_adp_hilton_times_square_20260927133046_20958b`

---

## A. HOTEL

| Field | Value |
|---|---|
| CANONICAL NAME | Hilton New York Times Square |
| HPC ID | `rec35fExUxCClpOP6` |
| HOTEL CODE | NYCTSHH |
| ADDRESS | 234 West 42nd Street, New York, New York 10036, USA |
| ROOMS | 478 |
| OWNER | UNKNOWN (no current first-party / deed evidence persisted) |
| OPERATOR | UNKNOWN (Hilton brand-managed appearance only — not asserted as operator of record) |
| OWNERSHIP CONFIDENCE | LOW / UNKNOWN |

---

## B. HPC

| Field | Value |
|---|---|
| EXISTING MATCH | NO_EXISTING_CANONICAL_MATCH |
| DUPLICATE DECISION | CREATE_NEW (identity `ind_hilton_us_nyctshh`) |
| FIELDS COMPLETED | Name, Brand, Address, City, State, Postal, Country, Lat/Lon, Rooms, Official URL, Phone, Market/Submarket, identity key, source provenance |
| EVENT-SPACE INTERPRETATION | Formal: 1 meeting room / 300 sq ft (Private Dining, conference 15). Social: 42nd And Sky Bar & Lounge 2,000 sq ft / reception 100 — **not** flattened into formal meeting total. Hilton “300 sq ft total event space” = formal inventory only. |
| HPC READY | **PASS** |

---

## C. ADP PROFILE

| Field | Value |
|---|---|
| TERRITORIES | 8 intents (Business, Leisure, Couples, Family, Meetings & Groups, Wellness, Adventure, Celebrations) |
| VERIFIED ATTRIBUTES | 24 stewarded attributes (fixture) · monitored reality set 8 |
| SCENARIOS | 50 (generic parity pack — no NYC market pack) |
| SCENARIO VERSION | standard / measurement contract `e4d85401…` |
| PEERS | 5 CORE (Honors Midtown + Marquis / Westin / Sheraton / Embassy / HGI lens by territory) |
| PEER ADEQUACY | **PASS** (ADEQUATE) |

---

## D. ADP RUN

| Field | Value |
|---|---|
| EXPECTED CALLS | 200 |
| SUCCESS | 200 |
| FAILED | 0 |
| COST | **$6.50** |
| RUNTIME | ~27.4 min (13:30:46 → 13:58:11Z) |
| PERIOD ID | `adp_period_adp_hilton_times_square_20260927133046_20958b` |
| CERTIFIED | **true** |

---

## E. ADP RESULTS

| Field | Value |
|---|---|
| AI CONSIDERATION | **3.5%** (7/200 observations) |
| SCENARIO PRESENCE | **12%** (6/50 scenarios) |
| PROPERTY REALITY | **62.5%** recognized (5/8 monitored; gap score 37.5%) |
| TOP TERRITORIES | Family 40% · Leisure 25% · Business 20% |
| WEAKEST TERRITORIES | Couples 0% · Meetings & Groups 0% · Celebration 0% · Wellness 0% · Adventure 0% |
| OBSERVED COMPETITORS | Marquis (87) · Knickerbocker (55) · Westin TS (41) · Sheraton TS (26) · Civilian · Margaritaville · Hard Rock · Archer · Baccarat · W NYC TS |
| DISPLACEMENT | Marquis top alternative — 31 scenarios where subject absent; peer overlap 100% on declared set |

---

## F. ADP FINDINGS

| Finding | Evidence | Root Cause | Action | Completion Criteria |
|---|---|---|---|---|
| Improve AI representation of pet-friendly | 0/7 recognition on pet_friendly | Attribute absent from AI answers despite stewarded fixture | Publish/verify pet policy on Hilton property page + OTAs; retest attribute scenarios | Pet-friendly recognition >0 on next official period |
| Address displacement by New York Marriott Marquis | Marquis 87 appearances; 31 scenarios subject absent | Stronger Times Square convention / large-meeting narrative | Differentiate on Sky Lobby / room size / Honors + limited-meeting lodging motions; retest vs Marquis | Subject consideration ↑ and Marquis-only displacement scenarios ↓ |
| Strengthen overall AI authority signals | Demand capture 12% (<40%) | Thin third-party / structured authority vs Midtown peers | Structured data + authoritative Midtown lodging content across leisure/business/family | Scenario presence > prior 12% on next certified period |

---

## G. GDI INITIALIZATION

| Field | Value |
|---|---|
| FIT | 11 |
| TARGETS | 22 |
| DISCOVERY MODES | OPEN_UNIVERSE + WEEKLY lane harvest (KNOWN_TARGET / EVENT_SERIES / FUTURE_CALENDAR / HOUSING / OVERFLOW / SPORTS / CORPORATE / GOVERNMENT / PRIVATE_EVENTS / VENUE_PARTNERSHIPS covered in seed lanes) |
| WEEKLY_READY | true |

---

## H. GDI LIVE RESEARCH

| Field | Value |
|---|---|
| QUERIES | 22 (budget cap 40; stratified) |
| FETCHES | 34 discovery + 103 contact |
| CANDIDATES | 16 |
| VALID | 1 VALID_WATCH · 15 INVALID at hygiene · research watchlist 16 |
| DQ | LA 2028 Olympics → OUTSIDE_CATCHMENT (customer-hidden) |
| COST | Discovery ~SERP/OpenAI within ≤$8 envelope · Contact research fetches 103 · Webhound **0** |
| WEBHOUND REQUIRED | **0** |

---

## I. GDI OPPORTUNITIES (customer-visible)

| Bucket | Count |
|---|---:|
| ACTIONABLE | **0** |
| WATCH / FUTURE_WATCH | **15** (all WATCHLIST; 14 FUTURE_CYCLE + 1 OVERFLOW_HOUSING) |
| HOUSING | 0 dedicated HOUSING priority (overflow motion on IJCAI) |
| OVERFLOW | 1 (IJCAI-ECAI 2026) |

---

## J. TOP CUSTOMER OPPORTUNITIES (sample)

| Opportunity | Motion | Timing | Hotel Fit | Contact | Why Now |
|---|---|---|---|---|---|
| CDA Annual Meeting 2026 | FULL_HOTEL_RFP | 2026-05-02 | 52 | NAMED_PARTIAL | Advance hotel planning for attendees |
| IJCAI-ECAI 2026 | OVERFLOW | 2026-08-15 | 52 | NAMED_PARTIAL | International overflow / lodging |
| FIFA World Cup 2026 (NYC host) | SPORTS | 2026-06-08 | 66 | ORG_PATH / NAMED_PARTIAL | Citywide lodging surge watch |
| Scale Healthcare Leadership Conference | FULL_HOTEL_RFP | 2026-10-12 | 58 | NAMED_PARTIAL | Advance accommodations |
| New York Forum on Economic Sanctions 2026 | OTHER | 2026-12-02 | 58 | NAMED_PARTIAL | Advance bookings |
| USCBS Annual Meeting & Conference | FULL_HOTEL_RFP | 2026-03-01 | 57 | ORG_PATH | Near-term cycle |
| Fearless Leadership Symposium | GOVERNMENT_SCIENTIFIC | 2026-04-17 | 57 | ORG_PATH | 2026 planning |
| Corporate Meetings Midtown / TS | ASSOCIATION_CONFERENCE | TBD | 58 | NAMED_PARTIAL | Ongoing Midtown corporate |

Stewardship note: some open-universe titles remain thin (e.g. NYC Hotel Week, venue-list style rows). Treat as WATCH; next cycle should tighten commercial-motion evidence before ACTIONABLE promotion.

---

## K. CONTACT INTELLIGENCE

| Tier | Count |
|---|---:|
| NAMED_DIRECT | 0 |
| NAMED_PARTIAL | 7 |
| FUNCTIONAL | 0 |
| ORG_PATH | 9 |
| NO_CONTACT | 0 |
| WHO_RESOLVED_HOW_MISSING | 3 (batch) |

Surfe auto: **0** · Surfe persisted PII: **0**

---

## L. JEV

| Metric | Value |
|---|---:|
| CALLS | 80 |
| SAFE APPLY | 0 (routes SAME as deterministic — no override needed) |
| SAME | 78 |
| HELPFUL_DIFFERENT | 0 |
| WRONG | 0 |
| HIGH_CONF_WRONG | 0 |
| UNKNOWN | 2 |
| PERSON APPLY | **NO** |

Impact: Jev confirmed source/stop routing; no quality degradation; person ranking advisory only.

---

## M. DRAWER QUALITY

| Metric | Value |
|---|---:|
| COMPLETE (drawerReadiness.ok) | 16/16 after re-enrich |
| UNKNOWN_AFTER_RESEARCH | acceptable unknowns preserved |
| THIN | 0 after `promoteQualified` / customer enrichment path |
| INTERNAL ID LEAKS | 0 expected / none observed in customer fields |

---

## N. CUSTOMER UI

| Surface | Result |
|---|---|
| ADP | **PASS** — published snapshot + certified period |
| GDI | **PASS** — Airtable-canonical bag; 15 customer-visible |
| FILTERS | **PASS** (share/auth isolation + hotelId binding) |
| DRAWERS | **PASS** (enrichment v1 + drawerReadiness) |

---

## O. DURABILITY

| Check | Result |
|---|---|
| SURVIVES RESTART | **PASS** (Airtable persistence + ADP filesystem published) |
| SURVIVES CLEAN PROCESS | **PASS** (cache invalidate + reload IDs match) |
| LOCAL RUNTIME DEPENDENCY | **NO** for customer-visible GDI (Airtable primary) |

---

## P. ISOLATION

| Hotel | Result |
|---|---|
| BETHESDA | **PASS** |
| RENAISSANCE | **PASS** |
| W ROME | **PASS** |
| HILTON TS | **PASS** |
| CROSS-HOTEL LEAKS | **0** (`test-gdi-hotel4-four-hotel-isolation.mjs` 7/7) |

---

## Q. COST

| Lane | Cost |
|---|---|
| HPC | ~$0 (official pages + Playwright; no paid ownership databases) |
| ADP | **$6.50** |
| GDI discovery | within ≤$8 envelope (22 queries / 34 fetches) |
| CONTACT | fetch-heavy; Surfe $0 |
| TOTAL | **~$6.50 + SERP/OpenAI contact** (no Webhound) |

---

## R. MANUAL INTERVENTIONS

1. **DATA STEWARDSHIP** — Event-space formal vs lounge separation locked from Hilton first-party pages  
2. **DATA STEWARDSHIP** — Ownership/operator left UNKNOWN (no guess)  
3. **GENERIC PRODUCT GAP** — Geo catchment was DMV-hardcoded; fixed to hotel territory keywords  
4. **GENERIC PRODUCT GAP** — Research orchestrator saved without customer enrichment; now enriches before persist  
5. **EXPECTED HUMAN REVIEW** — Open-universe WATCH list needs sales triage before ACTIONABLE promotion  
6. **AUTOMATION DEBT** — Duplicate FIFA World Cup rows should collapse on next dedupe cycle  

Count: **6**

---

## S. GENERALIZATION

| Category | Count / notes |
|---|---|
| HOTEL-SPECIFIC PROD HARDCODES | **0** (NYC/Hilton facts in config/fixture/HPC only) |
| GENERIC FIXES | 2 (geo catchment; orchestrator enrichment) |
| REUSABLE IMPROVEMENTS | Hotel-config territory as core-market signal; promote/enrich path on research save; Hotel #4 first-cycle + isolation runners |

---

## T. DECISION

1. HPC bound? **YES** (`rec35fExUxCClpOP6`)  
2. Ownership evidenced? **NO** — correctly UNKNOWN  
3. Event-space interpretation correct? **YES**  
4. ADP scenario parity? **YES** (50 / certified)  
5. Peer pack stewardship? **YES** (ADEQUATE)  
6. Full ADP baseline? **YES** (200/200)  
7. Entity/attribute/citation/displacement? **YES** (pipelines ran; reality 62.5%; competitors observed)  
8. Findings concrete? **PARTIAL** — displacement concrete; “authority signals” still broad  
9. GDI NYC demand without copied targets? **YES**  
10. Adapted to high-room / limited-meeting? **PARTIAL** — config/fit band correct; discovery still watch-heavy  
11. Housing/overflow/room-block motions? **PARTIAL** — 1 overflow; sports housing watches; no ACTIONABLE_NOW  
12. Contact V2 standards? **YES**  
13. Jev improve orchestration? **MARGINAL** (SAME confirmation; no HELPFUL_DIFFERENT)  
14. Jev degrade quality? **NO**  
15. Drawers mature parity? **YES** after re-enrich  
16. Survive restart? **YES**  
17. Cross-hotel leaks? **0**  
18. Hotel-specific prod logic? **NO**  
19. Ready for hotel #5? **CONDITIONAL** — ADP yes; GDI needs one stronger ACTIONABLE cycle  
20. Largest remaining generic gap? **Open-universe → ACTIONABLE conversion for lodging-primary (limited meeting) hotels without thin venue-list / mega-event noise**

---

## U. FINAL VERDICT

**HOTEL #4 PASSES — MINOR STEWARDSHIP GAPS REMAIN**

ADP certified and durable. GDI pipeline, contact V2, drawers, geo DQ, and isolation work. Remaining gaps: ownership UNKNOWN, zero ACTIONABLE_NOW, and open-universe noise requiring one more market cycle before claiming GDI sales-ready parity with Bethesda.

---

## PERSISTENCE / GENERALIZATION

| Field | Value |
|---|---|
| HPC ID | `rec35fExUxCClpOP6` |
| ADP PROPERTY ID | `adp_hilton_times_square` |
| ADP PERIOD | `adp_period_adp_hilton_times_square_20260927133046_20958b` |
| GDI HOTEL ID | `rec35fExUxCClpOP6` |
| CODE FILES | `hilton-times-square-baseline-period-001-v1.js`, peer/affiliation/entity registries, `enrich-gdi-opportunity-for-customer.js`, `research-orchestrator.js`, hotel4 scripts |
| CONFIG FILES | `config/.../rec35fExUxCClpOP6.json`, ADP fixture, alias map |
| TESTS | `test-gdi-hotel4-four-hotel-isolation.mjs` (+ existing 3-hotel isolation) |
| HOTEL-SPECIFIC PROD HARDCODES | 0 |
| BENCHMARK HARDCODES | 0 |
| SURFE AUTO | 0 |
| SURFE PERSISTED PII | 0 |
| WEBHOUND REQUIRED | 0 |
| JEV PERSON APPLY | NO |
