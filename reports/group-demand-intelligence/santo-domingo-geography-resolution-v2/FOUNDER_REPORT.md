# Santo Domingo Geography Resolution + Venue-TBD Applicability V2 — Founder Report

**Branch:** `deploy/gdi-pe-v1-7-customer-closure`  
**Preflight HEAD:** `9f1498a17130bcfd8f19b7bd7868b988cd8acec7`  
**FINAL SHA:** `2b91132252dd63fc8a3bd6c85856e05725abd009`  
**Shadow only:** yes · **Apply:** no · **Cron:** HELD · **Deploy:** NOT_RUN

---

## A. EXECUTIVE RESULT

PRIORITY OPEN/TBD: **12**

LOCATION STATES (priority 16 = 12 OPEN/TBD + 4 lodging-secondary):

| State | Count |
|-------|------:|
| SUBMARKET_KNOWN | 0 |
| METRO_WIDE | 0 |
| VENUE_TBD | 0 |
| HOTEL_TBD | 0 |
| LOCATION_UNKNOWN_RESEARCH_INCOMPLETE | 0 |
| LOCATION_UNKNOWN_PUBLIC_DATA_CEILING | 4 |
| WRONG_MARKET | 12 |

JW SHADOW READY: **0**  
JW MATCHED NEEDS DATA: **4**  
RADISSON SHADOW READY: **0**  
RADISSON MATCHED NEEDS DATA: **4**  
BOTH: **4** · JW ONLY: **0** · RADISSON ONLY: **0** · NEITHER: **12**

---

## B. V1 → V2 COMPARISON

| Metric | Value |
|--------|------:|
| V1 PAIRS | 86 |
| V1 NOT_APPLICABLE | 86 |
| V2 REACH HOTEL FIT | 4 |
| V2 TRUE NOT_APPLICABLE | 12 |
| V2 NEEDS LOCATION DATA | 0 |
| FALSE HOLDS RECOVERED | 4 |

---

## C. PRIORITY-12 MATRIX

| Market Opp | Location State | Evidence | JW Geo | JW Fit | JW Final | Radisson Geo | Radisson Fit | Radisson Final |
|---|---|---|---|---|---|---|---|---|
| Organizacion reunion anual RFAI | WRONG_MARKET | Spain procurement URL | NONE | — | NOT_APPLICABLE | NONE | — | NOT_APPLICABLE |
| 908 empleos Alojamiento | WRONG_MARKET | jobs listing | NONE | — | NOT_APPLICABLE | NONE | — | NOT_APPLICABLE |
| Alojamiento Portales Gubernamentales | WRONG_MARKET | non-event seed | NONE | — | NOT_APPLICABLE | NONE | — | NOT_APPLICABLE |
| Tipos de planes de alojamiento | WRONG_MARKET | content/guide | NONE | — | NOT_APPLICABLE | NONE | — | NOT_APPLICABLE |
| Tabla posiciones baloncesto | WRONG_MARKET | sports standing | NONE | — | NOT_APPLICABLE | NONE | — | NOT_APPLICABLE |
| Photo Humanidades Puerto Rico | WRONG_MARKET | Puerto Rico | NONE | — | NOT_APPLICABLE | NONE | — | NOT_APPLICABLE |
| México vs Alemania/Australia | WRONG_MARKET | wrong country | NONE | — | NOT_APPLICABLE | NONE | — | NOT_APPLICABLE |
| #12Guerreras puertorriqueño | WRONG_MARKET | Puerto Rico | NONE | — | NOT_APPLICABLE | NONE | — | NOT_APPLICABLE |
| Baloncesto venezolano | WRONG_MARKET | Venezuela | NONE | — | NOT_APPLICABLE | NONE | — | NOT_APPLICABLE |
| Special Olympics Puerto Rico | WRONG_MARKET | Puerto Rico | NONE | — | NOT_APPLICABLE | NONE | — | NOT_APPLICABLE |
| Karol G tour | WRONG_MARKET | concert/tour | NONE | — | NOT_APPLICABLE | NONE | — | NOT_APPLICABLE |
| Bad Bunny concierto | WRONG_MARKET | concert | NONE | — | NOT_APPLICABLE | NONE | — | NOT_APPLICABLE |

**Lodging-secondary (not in OPEN/TBD-12; unlocked fit):**

| Market Opp | Location State | JW/Rad Geo | Final | Class |
|---|---|---|---|---|
| ADTS 8.º Congreso | PUBLIC_DATA_CEILING | PLAUSIBLE / PLAUSIBLE | HOTEL_MATCHED_NEEDS_MORE_DATA | BOTH |
| Congreso Turismo de Salud (IG) | PUBLIC_DATA_CEILING | PLAUSIBLE / PLAUSIBLE | HOTEL_MATCHED_NEEDS_MORE_DATA | BOTH |
| Congreso Turismo de Salud (congresoadts.com) | PUBLIC_DATA_CEILING | PLAUSIBLE / PLAUSIBLE | HOTEL_MATCHED_NEEDS_MORE_DATA | BOTH |
| II Congreso Dominicano programa | PUBLIC_DATA_CEILING | PLAUSIBLE / PLAUSIBLE | HOTEL_MATCHED_NEEDS_MORE_DATA | BOTH |

---

## D. FALSE-HOLD ANALYSIS

| Reason | Count |
|--------|------:|
| TRUE GEO NONFIT (wrong market) | 12 |
| SUBMARKET NOT FOUND | 0 |
| VENUE TBD | 0 |
| HOTEL TBD | 0 |
| RESEARCH INCOMPLETE | 0 |
| PUBLIC DATA CEILING | 4 |

V1’s 86/86 NOT_APPLICABLE was mostly **metro-only UNKNOWN gate**, not true geo non-fit. Of the priority set: 12 were never SD lodging demand; 4 were recoverable ceiling cases that V1 never allowed into fit.

---

## E / F. JW & RADISSON SHADOW READY / MATCHED

**Shadow ready:** none.

**Matched needs data (both hotels, identical gaps):** 4 ADTS / Turismo de Salud / Congreso Dominicano rows — geo PLAUSIBLE, fit 53, gaps `room_demand_peak`, `who_contact`. No WHO, no action path, no summary QA yet.

---

## G. DIFFERENTIATION

BOTH_HOTELS: **4** (ceiling + lodging-supported congress; no submarket → no JW/Radisson split)  
JW_ONLY / RADISSON_ONLY: **0**  
NEITHER: **12** (OPEN/TBD junk)  
NEEDS_MORE_DATA: **0** at market-class rollup (pair finals are MATCHED_NEEDS_DATA under BOTH)

Strongest driver: **absence of SUBMARKET_KNOWN** → both hotels enter as PLAUSIBLE together. Piantini/Naco differentiation only appears in scenario tests, not live corpus.

---

## H. JEV

| Metric | Value |
|--------|------:|
| MARKET LOCATION ACTIONS | 5 |
| LOCATION BLOCKERS RESOLVED | 0 |
| PAIR EVALUATIONS UNLOCKED | 8 |
| HOTEL-PAIR ACTIONS | 0 |
| PAIR BLOCKERS RESOLVED | 0 |
| STATE ADVANCES | 0 |
| QUERIES | 5 |
| FETCHES | 31 |
| WRONG ROUTES | 0 |
| PAIR EVALS / JEV ACTION | 1.6 |

Jev mainly exhausted incomplete → ceiling; did not invent venue-TBD. Seed wrong-market filter prevented wasted Jev on junk.

---

## I. HI DEPENDENCY

| Hotel | Commercial Profile | Event Spaces | Demand Nodes |
|-------|--------------------|--------------|--------------|
| JW | POPULATED | NOT_RESEARCHED | POPULATED (Piantini / Blue Mall) |
| Radisson | POPULATED | NOT_RESEARCHED | POPULATED (Naco / Tiradentes) |

Missing HI prevent fit evaluation? **NO** (event spaces gap flagged; not treated as non-fit).

---

## J. WATCH

MARKET WATCH BEFORE: **24** · AFTER: **24**  
No new venue/hotel/housing/RFP watches (no affirmative TBD discovered).

---

## K. NYC REGRESSION

Renaissance: **11** · Hilton: **10** · NOW NOW: **0** (selective unchanged)  
Brooklyn safeguard: **PASS** · Times Square specific: **PASS**  
Duplicate source packets: **0**  
VENUE_TBD PLAUSIBLE is SD-script-scoped — not applied to NYC corpus.

---

## L. DIRECT ANSWERS

1. Was V1's geography gate too strict? **Yes** for genuine metro/TBD/ceiling SD demand — but the OPEN/TBD-12 were mostly wrong-market noise, not TBD.
2. Genuinely venue/hotel TBD among 12? **0** (affirmative TBD required; none found).
3. Needed better location research? **0 of 12** (wrong market); **4 secondary** hit public-data ceiling after bounded research.
4. Hotel pairs reaching product-fit evaluation? **8** (4 × 2 hotels).
5. BOTH? **4**
6. JW-only? **0**
7. Radisson-only? **0**
8. Truly neither? **12** of OPEN/TBD
9. Venue-TBD reach hotel fit? **Yes in ontology/tests/scenarios; no live VENUE_TBD in corpus.**
10. Metro-wide without blind fanout? **Yes** — scenarios + NYC Brooklyn/Times Square PASS; bare metro still insufficient without V2 status.
11. Precise submarket overrides broad geo? **Yes** (Piantini STRONG vs Naco DIRECT in scenarios).
12. Jev distinguish missing evidence vs TBD? **Partially** — routed incomplete→ceiling; did not fabricate TBD.
13. Jev advance hotel-specific fit research? **No** (0 hotel-pair actions; gaps remain room_demand/WHO).
14. Incomplete HI block any pair? **NO**
15. Any pair strict-ready in shadow? **NO**
16. Santo Domingo valid second-market proof? **Partial** — geography V2 model validated; yield/ready not.
17. Market-first ready for portfolio migration? **NO**
18. Exact blocker? **(a)** V1 discovery quality (OPEN/TBD polluted), **(b)** no affirmative venue/hotel TBD live examples, **(c)** matched pairs lack room demand + WHO for strict ready, **(d)** event-space HI still NOT_RESEARCHED.

---

## FINAL VERDICT

**GEOGRAPHY V2 PASSES — VENUE-TBD / METRO MATCHING WORKS, MORE READY YIELD NEEDED**

---

## PERSISTENCE / SAFETY

No broad discovery · No new market · No Webhound · No Surfe AUTO · No Bethesda/NYC mutation · No production deploy · No cron · No portfolio migration · Shadow only

**FINAL SHA:** `2b91132252dd63fc8a3bd6c85856e05725abd009`  
**PUSH:** pending
