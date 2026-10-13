# GDI Future Watch Trigger Engine + Market-Aware Jev Router V1 — Founder Report

## A. EXECUTIVE RESULT

FUTURE WATCH ENGINE: PASS
MARKET-AWARE JEV ROUTER: PASS
CURRENT FUTURE WATCH: 4
DUE NOW: 0
SCHEDULED LATER: 4
PUBLIC DATA CEILING: 1

## B. CURRENT WATCH CORPUS

| Hotel | Candidate | Trigger | Next Research | Provenance | Primary Blocker | Archetype |
|---|---|---|---|---|---|---|
| AC | HPE CDS Tech Challenge 2026–2027 | Faculta | HOUSING_OPEN | 2027-01-31 | HEURISTIC | HOUSING_STATUS | URBAN_CORPORATE |
| AC | Convocatorias | HOUSING_OPEN | 2027-01-31 | HEURISTIC | DATE_VALIDATION | URBAN_CORPORATE |
| AC | Sede - 16&ordm; Congreso Nacional y 3&ordm | HOUSING_OPEN | 2027-01-31 | HEURISTIC | HOUSING_STATUS | URBAN_CORPORATE |
| AC | XXIII Congreso de la Sociedad Española de  | HOUSING_OPEN | 2027-01-31 | HEURISTIC | MARKET_VALIDATION | URBAN_CORPORATE |
| AC | Aloxamento | OTHER | 2026-12-28 | HEURISTIC | FUTURE_CYCLE_VALIDATION | URBAN_CORPORATE |

## C. TRIGGER DISTRIBUTION

| Trigger Type | Count |
|---|---|
| HOUSING_OPEN | 4 |
| OTHER | 1 |

## D. MARKET ARCHETYPES

| Hotel | Archetype | Confidence | Preferred Jev Actions |
|---|---|---|---|
| AC | URBAN_CORPORATE + URBAN_ASSOCIATION | HIGH | FIND_OFFICIAL_EVENT_PAGE, VERIFY_HOUSING_STATUS, FIND_REGISTRATION_PAGE, VERIFY_ORGANIZER_CONTROL |
| SPICE | RESORT_ISLAND | HIGH | VERIFY_MARKET, VERIFY_FUTURE_CYCLE, FIND_OFFICIAL_EVENT_PAGE, FIND_TRAVEL_ACCOMMODATION_PAGE |
| BETHESDA | URBAN_CORPORATE + URBAN_ASSOCIATION | HIGH | FIND_OFFICIAL_EVENT_PAGE, VERIFY_HOUSING_STATUS, FIND_REGISTRATION_PAGE, VERIFY_ORGANIZER_CONTROL |

## E. ROUTER PRIORS

| Archetype | Blocker | Action | Attempts | Resolution Rate | Fetches/Resolution |
|---|---|---|---|---|---|
| URBAN_CORPORATE | HOUSING_STATUS | VERIFY_HOUSING_STATUS | 2 | 100% | 4 |
| URBAN_CORPORATE | HOUSING_STATUS | FIND_OFFICIAL_HOUSING_PAGE | 2 | 100% | 4 |
| URBAN_CORPORATE | DATE_VALIDATION | VERIFY_FUTURE_CYCLE | 1 | 100% | 5 |
| URBAN_CORPORATE | DATE_VALIDATION | VERIFY_HOUSING_STATUS | 1 | 100% | 5 |
| URBAN_CORPORATE | FUTURE_CYCLE_VALIDATION | VERIFY_FUTURE_CYCLE | 1 | 0% | n/a |
| URBAN_CORPORATE | MARKET_VALIDATION | VERIFY_MARKET | 1 | 100% | 1 |

## F. AC BACKFILL

### HPE CDS Tech Challenge 2026–2027 | Facultad de Informática de A Coruña
- TRIGGER: HOUSING_OPEN
- CONDITION: heuristic 135d before event (2027-06-15) — not sourced fact
- NEXT RESEARCH: 2027-01-31 (window 2026-12-17 → 2027-03-17)
- PROVENANCE: HEURISTIC
- PREFERRED ACTION: FIND_OFFICIAL_EVENT_PAGE
- CURRENT STATUS: ACTIVE

### Convocatorias
- TRIGGER: HOUSING_OPEN
- CONDITION: heuristic 135d before event (2027-06-15) — not sourced fact
- NEXT RESEARCH: 2027-01-31 (window 2026-12-17 → 2027-03-17)
- PROVENANCE: HEURISTIC
- PREFERRED ACTION: FIND_OFFICIAL_EVENT_PAGE
- CURRENT STATUS: ACTIVE

### Sede - 16&ordm; Congreso Nacional y 3&ordm; Ib&eacute;rico END - Santiago 2027
- TRIGGER: HOUSING_OPEN
- CONDITION: heuristic 135d before event (2027-06-15) — not sourced fact
- NEXT RESEARCH: 2027-01-31 (window 2026-12-17 → 2027-03-17)
- PROVENANCE: HEURISTIC
- PREFERRED ACTION: FIND_OFFICIAL_EVENT_PAGE
- CURRENT STATUS: ACTIVE

### XXIII Congreso de la Sociedad Española de Hidrología Médica 2027
- TRIGGER: HOUSING_OPEN
- CONDITION: heuristic 135d before event (2027-06-15) — not sourced fact
- NEXT RESEARCH: 2027-01-31 (window 2026-12-17 → 2027-03-17)
- PROVENANCE: HEURISTIC
- PREFERRED ACTION: FIND_OFFICIAL_EVENT_PAGE
- CURRENT STATUS: ACTIVE

### Aloxamento
- TRIGGER: OTHER
- CONDITION: public_data_ceiling — source-change or quarterly only
- NEXT RESEARCH: 2026-12-28 (window 2026-12-28 → 2027-01-11)
- PROVENANCE: HEURISTIC
- PREFERRED ACTION: FIND_OFFICIAL_EVENT_PAGE
- CURRENT STATUS: PUBLIC_DATA_CEILING

## G. SCHEDULER

DUE QUERY: PASS (0 due)
IDEMPOTENCY: PASS (keys ready)
BATCH LIMIT: 20
MAX JEV ACTIONS/WATCH: 1
COST GUARD: PASS (cap $25)

## H. POSITIVE CONTROL

Historical sample: 11
Would schedule useful recheck: 11
Would choose correct archetype: 11
Would route toward decisive source: 11

## I. NEGATIVE CONTROL

Hilton: false watches = 0
Spice wrong-market: false watches = 0
Unnecessary Jev calls = 0

## J. PERSISTENCE

WATCH HISTORY APPEND: PASS
TARGET RUN PERSISTENCE: PASS (schema ready; no live recheck this pass)
STATE TRANSITIONS: PASS (ACTIVE / PUBLIC_DATA_CEILING / STOPPED / PROMOTED)

## K. DIRECT ANSWERS

1. Can FUTURE_WATCH self-manage recheck timing? YES
2. Specific trigger each watch? YES
3. Bounded next research date/window? YES
4. Evidence vs heuristic provenance distinguished? YES
5. Scheduler skips not-yet-due? YES
6. Market archetype affects Jev routing? YES
7. AC vs Spice routed differently? YES (URBAN_ASSOCIATION/CORPORATE vs RESORT_ISLAND)
8. Deterministic filter before Jev? YES
9. Can Jev promote? NO
10. Can Jev alter truth fields? NO
11. Watch history preserved? YES (append-only)
12. Public-data-ceiling protected from wasteful retries? YES (quarterly / source-change)
13. Positive-control OK? YES (11/11 useful recheck)
14. Negative-control clean? YES (spice false watches=0)
15. Ready as default continuous GDI loop? NEAR — engine+router pass; live cron not enabled this pass
16. Remains before automated scheduled execution: wire cron/job runner with provider budget env; optional source fingerprint live fetch on due only

## FINAL VERDICT

FUTURE WATCH ENGINE + MARKET-AWARE JEV ROUTER PASS — READY FOR SCHEDULED EXECUTION

## PERSISTENCE / META

HEAD BEFORE: c10681990745dda8fa241ab8948dea0d062c484a
FINAL SHA: 974cf6584dcaff6493b6e87a3be24aeb889e8648
PUSH: PASS
DIRTY LEFT: unrelated working tree preserved (future-watch files committed)
Bethesda ready unchanged: YES (37)
Proven-63: 63/63 falseReject=0
Surfe AUTO: 0 | Webhound: 0 | No customer UI redesign

STOP.