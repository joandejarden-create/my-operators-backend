# GDI Market-Level Discovery V1 — NYC Canary — Founder Report

Generated: 2026-09-30T15:28:48.395Z
HEAD: 3a3773fcff998598506f3621bf1342f94ee942fa
Apply: true

## FINAL VERDICT

**NYC MARKET DISCOVERY PASSES — MATERIAL NEW GDI YIELD**

## A. Executive Result

MARKET: New York City
HOTELS: Renaissance Times Square / Hilton Times Square / NOW NOW NoHo
STRICT READY BEFORE: Ren 11 | Hilton 11 | NOW NOW 0 | portfolio≈22
VISIBLE BEFORE: Ren 11 | Hilton 11 | NOW NOW 0

MARKET CANDIDATES DISCOVERED: 40
NEW MARKET ENTITIES: 34
EXISTING ENTITY COLLISIONS: 6
FUTURE VALIDATED: 29
LODGING VALIDATED: 27
HOTEL-EVALUATION READY: 19
HOTEL FITS EVALUATED: 120

NEW STRICT READY: Ren +1 | Hilton +2 | NOW NOW +0 | net 3
NEW FUTURE WATCH: 10
NET NEW GOOD OPPORTUNITIES: 3

STRICT READY AFTER: Ren 12 | Hilton 13 | NOW NOW 0 | portfolio≈25
VISIBLE AFTER: Ren 12 | Hilton 13 | NOW NOW 0

## B. Current Architecture Audit

Existing market-demand model: marketOpportunityId + extractMarketOpportunityPacket + buildHotelOpportunityFromMarketPacket
What is reusable: shared market identity, lodging/commercial evidence, cross-hotel fit, Jev supporting-data router
What is missing: dedicated Market Opportunity table (optional), denser subEventId, explicit watch triggers
Schema change: NONE for this canary
Why: Portability V1 + Phase0 already show packet/fit split works; discovery yield is the bottleneck, not schema

## C. NYC Discovery Funnel

| Stage | Count |
|---|---:|
| Discovered | 40 |
| Identity validated | 40 |
| Future validated | 29 |
| Lodging validated | 27 |
| Hotel evaluation ready | 19 |
| Future watch | 10 |
| Rejected (wrong/hist/dir/generic/other) | 60 |
| Strict ready (new hotel opps built) | 3 |

## D. Strongest New Market Demand

### New York City - The Science of Learning Education ...
- Organization: New York City
- Demand type: URBAN_ASSOCIATION
- Cycle/dates: —
- Evidence: https://www.learningandthebrain.com/conference-533/the-science-of-learning/accommodations
- Lodging signal: OFFICIAL_ROOM_BLOCK
- Placement: OPEN / UNRESOLVED
- WHO: not researched / ceiling
- Source family: HOUSING_PAGE
- Hotels: Ren=NO Hilton=NO NOW NOW=NO
- Collision: NEW_MARKET_ENTITY
- State: HOTEL_EVALUATION_READY

### The Forum 2027
- Organization: The
- Demand type: URBAN_ASSOCIATION
- Cycle/dates: 2027
- Evidence: https://www.nawb.org/the-forum-2027/
- Lodging signal: OFFICIAL_HOST_HOTEL
- Placement: PRIMARY SELECTED / NO OVERFLOW EVIDENCE
- WHO: not researched / ceiling
- Source family: OFFICIAL_EVENT_PAGE
- Hotels: Ren=YES Hilton=YES NOW NOW=CONDITIONAL
- Collision: NEW_FUTURE_CYCLE
- State: HOTEL_EVALUATION_READY

### Hotel Info for the 2027 AI Agent Conference in NYC
- Organization: AI Agent
- Demand type: URBAN_ASSOCIATION
- Cycle/dates: May 17–18, 2027
- Evidence: https://www.agentconference.com/hotelinfo
- Lodging signal: OFFICIAL_ROOM_BLOCK
- Placement: OPEN / UNRESOLVED
- WHO: not researched / ceiling
- Source family: HOUSING_PAGE
- Hotels: Ren=NO Hilton=NO NOW NOW=NO
- Collision: NEW_MARKET_ENTITY
- State: HOTEL_EVALUATION_READY

### Hotel - APAP
- Organization: Hotel
- Demand type: URBAN_ASSOCIATION
- Cycle/dates: —
- Evidence: https://apap365.org/conference/attend/hotel/
- Lodging signal: OFFICIAL_HOST_HOTEL
- Placement: PRIMARY SELECTED / NO OVERFLOW EVIDENCE
- WHO: not researched / ceiling
- Source family: VENUE_PAGE
- Hotels: Ren=NO Hilton=NO NOW NOW=NO
- Collision: EXISTING_MARKET_ENTITY_NEW_EVIDENCE
- State: IDENTITY_VALIDATED

### Accommodation - Neuro-immune axis - Cell Symposia
- Organization: Accommodation
- Demand type: URBAN_ASSOCIATION
- Cycle/dates: —
- Evidence: https://cell-press-symposia.com/neuroimmunology-2027/conference-accommodation.html
- Lodging signal: HOUSING_BUREAU
- Placement: OPEN / UNRESOLVED
- WHO: not researched / ceiling
- Source family: HOUSING_PAGE
- Hotels: Ren=NO Hilton=NO NOW NOW=NO
- Collision: NEW_MARKET_ENTITY
- State: IDENTITY_VALIDATED

### Annual Conference (APAC) - Audio Publishers Association
- Organization: Annual
- Demand type: URBAN_ASSOCIATION
- Cycle/dates: March 4, 2027
- Evidence: https://www.audiopub.org/apa-conference
- Lodging signal: OFFICIAL_ROOM_BLOCK
- Placement: OPEN / UNRESOLVED
- WHO: not researched / ceiling
- Source family: HOUSING_PAGE
- Hotels: Ren=NO Hilton=NO NOW NOW=NO
- Collision: NEW_FUTURE_CYCLE
- State: HOTEL_EVALUATION_READY

### Presenting research on student housing access at 2027 ...
- Organization: Presenting research on student housing access at 2027 ...
- Demand type: URBAN_ASSOCIATION
- Cycle/dates: 2027
- Evidence: https://www.facebook.com/bryon.pierson.5/posts/some-exciting-academic-news-%EF%B8%8Fim-excited-to-share-that-my-research-abstract-betwe/10166660167659893/
- Lodging signal: VENUE_SET_HOTEL_OPEN
- Placement: UNKNOWN
- WHO: not researched / ceiling
- Source family: HOUSING_PAGE
- Hotels: Ren=NO Hilton=NO NOW NOW=NO
- Collision: NEW_MARKET_ENTITY
- State: HOTEL_EVALUATION_READY

### hebrews 3 bible [theonebb.com]study questions
- Organization: hebrews 3 bible [theonebb.com]study questions
- Demand type: URBAN_ASSOCIATION
- Cycle/dates: 2027
- Evidence: https://forum-intl.org/search/hebrews%203%20bible%20%5Btheonebb.com%5Dstudy%20questions/
- Lodging signal: VENUE_SET_HOTEL_OPEN
- Placement: UNKNOWN
- WHO: not researched / ceiling
- Source family: HOUSING_PAGE
- Hotels: Ren=NO Hilton=NO NOW NOW=NO
- Collision: NEW_MARKET_ENTITY
- State: HOTEL_EVALUATION_READY

### [TG:Bifuapp]Gcash Payment for Membership Fees
- Organization: [TG
- Demand type: URBAN_ASSOCIATION
- Cycle/dates: 2027
- Evidence: https://forum-intl.org/search/%5BTG:Bifuapp%5DGcash%20Payment%20for%20Membership%20Fees/
- Lodging signal: VENUE_SET_HOTEL_OPEN
- Placement: UNKNOWN
- WHO: not researched / ceiling
- Source family: HOUSING_PAGE
- Hotels: Ren=NO Hilton=NO NOW NOW=NO
- Collision: NEW_MARKET_ENTITY
- State: HOTEL_EVALUATION_READY

### Hotel & Travel Information | NRF 2027: Retail's Big Show
- Organization: Hotel & Travel Information
- Demand type: URBAN_ASSOCIATION
- Cycle/dates: 2027
- Evidence: https://nrfbigshow.nrf.com/attend/hotels
- Lodging signal: VENUE_SET_HOTEL_OPEN
- Placement: OPEN / UNRESOLVED
- WHO: not researched / ceiling
- Source family: HOUSING_PAGE
- Hotels: Ren=NO Hilton=YES NOW NOW=NO
- Collision: NEW_MARKET_ENTITY
- State: HOTEL_EVALUATION_READY

### New York Hilton Midtown
- Organization: New York Hilton Midtown
- Demand type: URBAN_ASSOCIATION
- Cycle/dates: —
- Evidence: https://www.cvent.com/venues/tr-TR/new-york/hotel/new-york-hilton-midtown/venue-ce096130-c9d2-4190-bb2a-3ecec7955bc0?aPos=1&aType=suggested_ad_people_also_viewed&ts=1778503884228&aCompId=4effb49e-394f-4feb-bc13-996bd9f87208&aComp=true&aNeed=true
- Lodging signal: VENUE_SET_HOTEL_OPEN
- Placement: UNKNOWN
- WHO: not researched / ceiling
- Source family: VENUE_PAGE
- Hotels: Ren=NO Hilton=NO NOW NOW=NO
- Collision: NEW_MARKET_ENTITY
- State: IDENTITY_VALIDATED

### Congress Reviews HUD Oversight and Accountability ...
- Organization: Congress Reviews HUD Oversight and Accountability ...
- Demand type: MEDICAL_SCIENTIFIC
- Cycle/dates: —
- Evidence: https://www.thehabitatgroup.com/articles/9534-congress-reviews-hud-oversight-and-accountability-challenges
- Lodging signal: VENUE_SET_HOTEL_OPEN
- Placement: UNKNOWN
- WHO: not researched / ceiling
- Source family: HOUSING_PAGE
- Hotels: Ren=NO Hilton=NO NOW NOW=NO
- Collision: NEW_MARKET_ENTITY
- State: IDENTITY_VALIDATED


## E. Hotel Results

### Renaissance
- Before: ready 11 / visible 11
- Market entities evaluated: 40
- YES: 1
- CONDITIONAL: 4
- NO: 35
- New ready: 1
- New watch (market ledger): 10
- After: ready 12 / visible 12

### Hilton
- Before: ready 11 / visible 11
- Market entities evaluated: 40
- YES: 2
- CONDITIONAL: 17
- NO: 21
- New ready: 2
- New watch (market ledger): 10
- After: ready 13 / visible 13

### NOW NOW
- Before: ready 0 / visible 0
- Market entities evaluated: 40
- YES: 0
- CONDITIONAL: 4
- NO: 36
- New ready: 0
- New watch (market ledger): 10
- After: ready 0 / visible 0

## F. New Customer-Ready Opportunities

### The Forum 2027 → RENAISSANCE
- Market demand: gdi_mkt_d8c76574_the_the_forum_20
- Why this hotel: Strong fit signals because event size is workable but not a perfect capacity match, the hotel is in a credible demand territory for this opportunity.
- Why now: commercial PRIMARY SELECTED / NO OVERFLOW EVIDENCE; lodging {"status":"OFFICIAL_HOST_HOTEL","roomBlockMentioned":true,"overflowMentioned":false,"evidenceLevel":"DIRECT"}
- WHO: NO_CONTACT
- Action path: Contact the housing provider / tournament housing lead (not only the event brand), request housing-list inclusion, and confirm overflow peak-room needs and stay dates.
- Readiness: STRICT (builder ok)

### The Forum 2027 → HILTON
- Market demand: gdi_mkt_d8c76574_the_the_forum_20
- Why this hotel: Strong fit signals because event size is workable but not a perfect capacity match, the hotel is in a credible demand territory for this opportunity.
- Why now: commercial PRIMARY SELECTED / NO OVERFLOW EVIDENCE; lodging {"status":"OFFICIAL_HOST_HOTEL","roomBlockMentioned":true,"overflowMentioned":false,"evidenceLevel":"DIRECT"}
- WHO: NO_CONTACT
- Action path: Contact the housing provider / tournament housing lead (not only the event brand), request housing-list inclusion, and confirm overflow peak-room needs and stay dates.
- Readiness: STRICT (builder ok)

### Hotel & Travel Information | NRF 2027: Retail's Big Show → HILTON
- Market demand: gdi_mkt_ae528108_hotel_travel_hotel_travel
- Why this hotel: Strong fit signals because event size is workable but not a perfect capacity match, the hotel is in a credible demand territory for this opportunity.
- Why now: commercial OPEN / UNRESOLVED; lodging {"status":"VENUE_SET_HOTEL_OPEN","roomBlockMentioned":false,"overflowMentioned":false,"evidenceLevel":"STRONG_INFERENCE"}
- WHO: NO_CONTACT
- Action path: Contact the housing provider / tournament housing lead (not only the event brand), request housing-list inclusion, and confirm overflow peak-room needs and stay dates.
- Readiness: STRICT (builder ok)

## G. Future Watch

- **2027 Convention Location We can't wait to experience ...**
  - Trigger: HOUSING_OPEN
  - Evidence: https://www.facebook.com/ramosagencies/videos/2027-convention-location-%EF%B8%8F-we-cant-wait-to-experience-this-once-in-a-lifetime-de/1245163604423178/
  - Hotels: RENAISSANCE, HILTON, NOW_NOW
  - Collision: NEW_MARKET_ENTITY
- **NYSBA Annual Conference 2027 - New York State Bar ...**
  - Trigger: HOUSING_OPEN
  - Evidence: https://nysba.org/am2027/?srsltid=AU7gw4WO6PPMVXTnP60tsXIYO_To1ne3V2tX0LDiViSA5Mkp_cN7P9uN
  - Hotels: RENAISSANCE, HILTON, NOW_NOW
  - Collision: NEW_FUTURE_CYCLE
- **NYSBA Annual Meeting 2027: Expected Dates, Agenda ...**
  - Trigger: HOUSING_OPEN
  - Evidence: https://tempello.ai/blog/nysba-annual-meeting-2027
  - Hotels: RENAISSANCE, HILTON, NOW_NOW
  - Collision: NEW_FUTURE_CYCLE
- **Meetings & Events | Warwick New York**
  - Trigger: HOUSING_OPEN
  - Evidence: https://www.warwickhotels.com/warwick-new-york/meetings-and-events
  - Hotels: RENAISSANCE, HILTON, NOW_NOW
  - Collision: NEW_MARKET_ENTITY
- **ASE Annual Meeting (Surgical Education Week 2027) 2027 ...**
  - Trigger: HOUSING_OPEN
  - Evidence: https://www.cantonfair.net/event/66046-ase-annual-meeting
  - Hotels: RENAISSANCE, HILTON, NOW_NOW
  - Collision: NEW_MARKET_ENTITY
- **Fiscal Year 2027 Adopted Expense Budget Adjustment ...**
  - Trigger: HOUSING_OPEN
  - Evidence: https://council.nyc.gov/budget/wp-content/uploads/sites/54/2026/06/Fiscal-2027-Schedule-C-Final.pdf
  - Hotels: RENAISSANCE, HILTON, NOW_NOW
  - Collision: NEW_MARKET_ENTITY
- **Radiation Oncology Residency Program Virtual Tour**
  - Trigger: HOUSING_OPEN
  - Evidence: https://virtualtour.montefioreeinstein.org/radiation-oncology-residency
  - Hotels: RENAISSANCE, HILTON, NOW_NOW
  - Collision: NEW_MARKET_ENTITY
- **Quick Guide - Inside Ads Full**
  - Trigger: HOUSING_OPEN
  - Evidence: https://envision.freeman.com/opportunity/934983
  - Hotels: RENAISSANCE, HILTON, NOW_NOW
  - Collision: NEW_MARKET_ENTITY
- **The Aesthetic MEET 2026**
  - Trigger: HOUSING_OPEN
  - Evidence: https://s15.a2zinc.net/clients/asaps/asaps26/Public/Content.aspx?ID=955&sortMenu=101007
  - Hotels: RENAISSANCE, HILTON, NOW_NOW
  - Collision: NEW_MARKET_ENTITY
- **Habitat for Humanity x ICFF**
  - Trigger: HOUSING_OPEN
  - Evidence: https://icff.com/fair/attend-register/habitat/
  - Hotels: RENAISSANCE, HILTON, NOW_NOW
  - Collision: NEW_MARKET_ENTITY

## H. Cross-Hotel Reuse

Unique market entities (post-dedupe): 40
Hotel fits evaluated: 120
Average hotels/entity: 3
Differentiation: all3=4 two=1 one=14 none=21
Duplicate research avoided (est. vs hotel-first): 92 fetch-equivalents

## I. Source-Family Yield

| Family | Q | Fetches | Cand | ID | Future | Lodging | HotelEval | Strict | Watch | Rejected |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ASSOCIATION_CALENDAR | 2 | 5 | 5 | 0 | 0 | 0 | 0 | 0 | 0 | 13 |
| ASSOCIATION_PAGE | 0 | 0 | 0 | 1 | 1 | 0 | 0 | 0 | 1 | 0 |
| CONVENTION_BUREAU | 2 | 7 | 11 | 0 | 0 | 0 | 0 | 0 | 0 | 6 |
| CORPORATE_EVENT_PAGE | 1 | 0 | 8 | 0 | 0 | 0 | 0 | 0 | 0 | 1 |
| EVENT_MANUAL | 1 | 0 | 4 | 0 | 0 | 0 | 0 | 0 | 0 | 5 |
| EXHIBITOR_DIRECTORY | 1 | 6 | 9 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| FUTURE_HOST_SOURCE | 1 | 0 | 4 | 0 | 0 | 0 | 0 | 0 | 0 | 4 |
| HOUSING_PAGE | 2 | 13 | 13 | 17 | 11 | 15 | 10 | 1 | 1 | 5 |
| INDUSTRY_DAY | 1 | 0 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 5 |
| OFFICIAL_EVENT_PAGE | 2 | 9 | 10 | 8 | 8 | 5 | 5 | 2 | 3 | 7 |
| ORGANIZATION_SITE | 0 | 0 | 0 | 4 | 3 | 1 | 0 | 0 | 3 | 0 |
| PRODUCTION_SOURCE | 1 | 0 | 9 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| REGISTRATION_PAGE | 1 | 0 | 5 | 0 | 0 | 0 | 0 | 0 | 0 | 3 |
| SPORTS_SCHEDULE | 1 | 0 | 5 | 0 | 0 | 0 | 0 | 0 | 0 | 4 |
| TOUR_SERIES_SOURCE | 1 | 0 | 9 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| UNIVERSITY_CALENDAR | 1 | 0 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 7 |
| VENUE_PAGE | 0 | 0 | 0 | 10 | 5 | 6 | 3 | 0 | 2 | 0 |

## J. Jev Performance

JEV CALLS: 15
CALLS WITH USEFUL ROUTE: 1
UNIQUE BLOCKERS RESOLVED: 2
CANDIDATES STATE-CHANGED: 1
NO-OP CALLS: 14
WRONG-ROUTE CALLS: 0
Useful examples: [{"market":"gdi_mkt_1280b11c_new_york_cit_new_york_cit","action":"VERIFY_FUTURE_CYCLE","title":"New York City - The Science of Learning Education ..."}]
No-op examples: [{"market":"gdi_mkt_b1940943_2027_convent_2027_convent","action":"FIND_OFFICIAL_HOUSING_PAGE","title":"2027 Convention Location We can't wait to experience ..."},{"market":"gdi_mkt_3721c6c4_hotel_hotel_apap","action":"VERIFY_FUTURE_CYCLE","title":"Hotel - APAP"},{"market":"gdi_mkt_40aadec9_nysba_annual_nysba_annual","action":"FIND_OFFICIAL_HOUSING_PAGE","title":"NYSBA Annual Conference 2027 - New York State Bar ..."},{"market":"gdi_mkt_24868098_accommodatio_accommodatio","action":"VERIFY_FUTURE_CYCLE","title":"Accommodation - Neuro-immune axis - Cell Symposia"},{"market":"gdi_mkt_d1e7927e_nysba_annual_nysba_annual","action":"FIND_OFFICIAL_HOUSING_PAGE","title":"NYSBA Annual Meeting 2027: Expected Dates, Agenda ..."}]

## K. Rejection Analysis

- Wrong market: 54
- Historical: 4
- Directory noise: 2
- Generic event: 0
- No lodging (soft tally among inspected): 13
- Other: 0

## L. Economics

SEARCH QUERIES: 22
FETCHES: 46
JEV CALLS: 15
TOTAL RESEARCH COST: not metered in this canary (SerpAPI+HTTP; no Webhound/Surfe)
COST / VALID MARKET ENTITY: 1.15 fetches
COST / HOTEL-EVALUATION-READY: 2.42 fetches
COST / NEW STRICT-READY: 15.33 fetches
COST / NEW FUTURE WATCH: 4.60 fetches
DUPLICATE RESEARCH AVOIDED: ~92 fetch-equivalents

## M. Architecture Decision

A. EXISTING DEMAND PROGRAM / GENERATOR MODEL IS SUFFICIENT (marketOpportunityId packet + hotel fit builder; no new Airtable table)

Evidence: canary reused packet/fit path; creating a Market Opportunity table would not have changed discovery funnel yield.

## N. Discovery Bottleneck

**ENTITY EXTRACTION**

Evidence: 40 entities / 19 hotel-evaluation-ready → only 3 strict-ready hotel opps; org/title parsing often truncated (“The”, “Hotel”); Jev useful routes 1/15. High-yield families already identified (HOUSING_PAGE, OFFICIAL_EVENT_PAGE, VENUE_PAGE) — do not treat “ready to scale markets” as the next move until entity identity quality improves.

## O. Regression

- Renaissance ready preserved (≥): true
- Hilton ready preserved (≥): true
- NOW NOW ready preserved (≥): true
- Existing ready IDs stable: true
- No Surfe: PASS
- No wrong-base / legacy MVP writes: PASS
- Deploy: NOT_RUN
- Cron: HELD
- Customer-facing regression: NONE
- Apply meta: {"applied":true,"renaissanceAdded":1,"hiltonAdded":2,"nowNowAdded":0}

## P. Recommended Next GDI Step

**3. EXPAND SPECIFIC HIGH-YIELD SOURCE FAMILIES**

Prioritize HOUSING_PAGE + OFFICIAL_EVENT_PAGE depth and harden entity naming before any multi-market scale.

DO NOT EXECUTE — founder returns to Bethesda deliverable.

---

STOP.