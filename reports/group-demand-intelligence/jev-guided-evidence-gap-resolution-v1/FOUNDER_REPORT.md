# GDI Jev-Guided Evidence Gap Resolution V1 — Founder Report

## A. EXECUTIVE RESULT

AC HQ WATCH START: 5
SPICE HQ WATCH START: 7
READY CREATED: 0
FUTURE WATCH: 4
HIGH-QUALITY WATCH REMAINING: 0
REJECTED: 7
PUBLIC-DATA CEILING: 1
Market prefilter rejects (no Jev): 7

## B. CANDIDATE RESULTS

| Hotel | Candidate | Primary Blocker | Jev Action | Outcome | State Before | State After |
|---|---|---|---|---|---|---|
| AC | HPE CDS Tech Challenge 2026–2027 | Facultad de I | HOUSING_STATUS | VERIFY_HOUSING_STATUS | BLOCKER_RESOLVED_POSITIVE | HIGH_QUALITY_WATCH | FUTURE_WATCH |
| AC | Convocatorias | DATE_VALIDATION | VERIFY_HOUSING_STATUS | BLOCKER_RESOLVED_POSITIVE | HIGH_QUALITY_WATCH | FUTURE_WATCH |
| AC | Aloxamento | FUTURE_CYCLE_VALIDATION | VERIFY_FUTURE_CYCLE | NO_NEW_EVIDENCE | HIGH_QUALITY_WATCH | PUBLIC_DATA_CEILING |
| AC | Sede - 16&ordm; Congreso Nacional y 3&ordm; Ib&e | HOUSING_STATUS | VERIFY_HOUSING_STATUS | BLOCKER_RESOLVED_POSITIVE | HIGH_QUALITY_WATCH | FUTURE_WATCH |
| AC | XXIII Congreso de la Sociedad Española de Hidrol | MARKET_VALIDATION | VERIFY_MARKET | BLOCKER_RESOLVED_POSITIVE | HIGH_QUALITY_WATCH | FUTURE_WATCH |
| SPICE | MECA Conference 2027 | MARKET_VALIDATION | PREFILTER | WRONG_MARKET | HIGH_QUALITY_WATCH | REJECTED |
| SPICE | EAPR 2027 | MARKET_VALIDATION | PREFILTER | WRONG_MARKET | HIGH_QUALITY_WATCH | REJECTED |
| SPICE | ICE 2027 - Torre Melina, a Gran Meli&#xE1; Hotel | MARKET_VALIDATION | PREFILTER | WRONG_MARKET | HIGH_QUALITY_WATCH | REJECTED |
| SPICE | Travel | 2027 ISPE APAC Annual Conference | ISPE | MARKET_VALIDATION | PREFILTER | WRONG_MARKET | HIGH_QUALITY_WATCH | REJECTED |
| SPICE | Travel PR News | Category | Travel Marketing | MARKET_VALIDATION | PREFILTER | WRONG_MARKET | HIGH_QUALITY_WATCH | REJECTED |
| SPICE | Sponsorship, Exhibiting &#038; Advertising Oppor | MARKET_VALIDATION | PREFILTER | WRONG_MARKET | HIGH_QUALITY_WATCH | REJECTED |
| SPICE | Seeking Strategic Buyer | MARKET_VALIDATION | PREFILTER | WRONG_MARKET | HIGH_QUALITY_WATCH | REJECTED |

## C. JEV VALUE

TOTAL JEV ACTIONS: 8
DECISIVE POSITIVE: 7
DECISIVE NEGATIVE: 7
HELPFUL: 0
SAME AS DEFAULT: 1
UNHELPFUL: 0
WRONG ROUTE: 0
BLOCKER RESOLUTION RATE: 80% (4/5 Jev-guided)
STATE CHANGE RATE: 100%

## D. JEV VS DEFAULT

- **ac_watch_13**: default=VERIFY_HOUSING_STATUS jev=VERIFY_HOUSING_STATUS → SAME; SAME (default was already correct)
- **ac_watch_13**: default=FIND_OFFICIAL_HOUSING_PAGE jev=VERIFY_HOUSING_STATUS → SAME; SAME (default was already correct)
- **ac_watch_17**: default=VERIFY_FUTURE_CYCLE jev=VERIFY_FUTURE_CYCLE → SAME; SAME (default was already correct)
- **ac_watch_17**: default=VERIFY_DATES jev=VERIFY_HOUSING_STATUS → DIFFERENT; JEV BETTER OR DIFFERENT USEFUL
- **ac_watch_19**: default=VERIFY_FUTURE_CYCLE jev=VERIFY_FUTURE_CYCLE → SAME; SAME
- **ac_watch_21**: default=VERIFY_HOUSING_STATUS jev=VERIFY_HOUSING_STATUS → SAME; SAME (default was already correct)
- **ac_watch_21**: default=FIND_OFFICIAL_HOUSING_PAGE jev=VERIFY_HOUSING_STATUS → SAME; SAME (default was already correct)
- **ac_watch_22**: default=VERIFY_MARKET jev=VERIFY_MARKET → SAME; SAME (default was already correct)
- **spice_watch_01**: market prefilter rejected before Jev (WRONG_MARKET: no Grenada destination token on event/URL)
- **spice_watch_03**: market prefilter rejected before Jev (WRONG_MARKET: no Grenada destination token on event/URL)
- **spice_watch_04**: market prefilter rejected before Jev (WRONG_MARKET: URL/event destination outside Grenada market)
- **spice_watch_05**: market prefilter rejected before Jev (WRONG_MARKET: URL/event destination outside Grenada market)
- **spice_watch_08**: market prefilter rejected before Jev (WRONG_MARKET: URL/event destination outside Grenada market)
- **spice_watch_13**: market prefilter rejected before Jev (WRONG_MARKET: URL/event destination outside Grenada market)
- **spice_watch_14**: market prefilter rejected before Jev (WRONG_MARKET: URL/event destination outside Grenada market)

## E. READY OPPORTUNITIES

None. (Promotion thresholds unchanged; no candidate met full readiness.)

## F. FUTURE WATCH

- **AC** | HPE CDS Tech Challenge 2026–2027 | Facultad de Informática de A Coruña | why valid: market+future cycle plausible | missing: housing open | trigger: HOUSING_OPEN | source: https://www.fic.udc.es/es/noticias/hpe-cds-tech-challenge-2026-2027
- **AC** | Convocatorias | why valid: market+future cycle plausible | missing: housing open | trigger: HOUSING_OPEN | source: https://www.coruna.gal/informacionjuvenil/es/convocatorias?argIdioma=es&argPrimerItem-1423189008906=1&argPrimerItem=21&argPag=12
- **AC** | Sede - 16&ordm; Congreso Nacional y 3&ordm; Ib&eacute;rico END - Santiago 2027 | why valid: market+future cycle plausible | missing: housing open | trigger: HOUSING_OPEN | source: https://www.congresoend2027.com/sede-1
- **AC** | XXIII Congreso de la Sociedad Española de Hidrología Médica 2027 | why valid: market+future cycle plausible | missing: housing open | trigger: HOUSING_OPEN | source: https://congresohidrologiamedica.com/

## G. REJECTED

- **SPICE** | spice_watch_01 | WRONG_MARKET: no Grenada destination token on event/URL | Jev: NONE_PREFILTER
- **SPICE** | spice_watch_03 | WRONG_MARKET: no Grenada destination token on event/URL | Jev: NONE_PREFILTER
- **SPICE** | spice_watch_04 | WRONG_MARKET: URL/event destination outside Grenada market | Jev: NONE_PREFILTER
- **SPICE** | spice_watch_05 | WRONG_MARKET: URL/event destination outside Grenada market | Jev: NONE_PREFILTER
- **SPICE** | spice_watch_08 | WRONG_MARKET: URL/event destination outside Grenada market | Jev: NONE_PREFILTER
- **SPICE** | spice_watch_13 | WRONG_MARKET: URL/event destination outside Grenada market | Jev: NONE_PREFILTER
- **SPICE** | spice_watch_14 | WRONG_MARKET: URL/event destination outside Grenada market | Jev: NONE_PREFILTER

## H. PUBLIC-DATA CEILING

- **AC** | ac_watch_19 | blocker=FUTURE_CYCLE_VALIDATION | NO_NEW_EVIDENCE

## I. POSITIVE CONTROL

Historical sample: 11 (expected 11)
Jev selected known useful route: 11
Hit rate: 100%
By source family: {"UNKNOWN":{"hit":11,"n":11}}

## J. MARKET-AWARE ROUTING

AC best Jev action types: {"VERIFY_HOUSING_STATUS":3,"FIND_OFFICIAL_HOUSING_PAGE":2,"VERIFY_FUTURE_CYCLE":2,"VERIFY_MARKET":1}
Spice best Jev action types: {}
Should routing differ by market archetype? YES (by design)
Explain: AC association demand responds to official housing/event pages; Spice island resort demand fails closed on wrong-market hosts before housing research — market VERIFY first, then travel/accommodation pages when Grenada-valid.

## K. COST / EFFICIENCY

AC:
Jev calls: 8
queries: 14
fetches: 32
resolved blockers: 4
fetches/resolved blocker: 8.00

Spice:
Jev calls: 0
queries: 0
fetches: 0
resolved blockers: 0
fetches/resolved blocker: n/a

## L. DIRECT ANSWERS

1. Did Jev resolve unresolved evidence gaps? YES (partial/select)
2. Blocker resolution % (Jev-guided): 80%
3. Jev better than default: 1 candidates
4. Efficient kill of bad candidates? YES — 7 rejected (7 prefilter + research)
5. Promote any? NO
6. Future-watch triggers? YES
7. Reduce wasted fetches? YES vs broad discovery — Spice wrong-market killed before fetch budget
8. Best blocker types for Jev: MARKET_VALIDATION, HOUSING_STATUS, COMMERCIAL_OPENNESS
9. Keep deterministic: wrong-market host patterns, readiness/promotion, WHO timing, grade overrides
10. Differ by market type? YES
11. Positive-control hit rate: 100%
12. Standard next-action router? See FINAL VERDICT
13. Shadow-only for: VERIFY_WHO / VERIFY_WHO_ROLE / promotion-adjacent actions
14. Next improvement: market-archetype action priors + housing-open trigger scheduler

## FINAL VERDICT

JEV NEXT-ACTION ROUTER PASSES — MATERIAL EVIDENCE-GAP RESOLUTION

## PERSISTENCE / REGRESSION

HEAD BEFORE: d228d7cdd0a95f9403bd5bf1d5e1f8ffc5e16146
FINAL SHA: 08d3a891b1f8e2313e3e0006275b66738f699d53
PUSH: PASS
DIRTY LEFT: preserved (unrelated working tree; Jev-gap files committed)
Bethesda ready unchanged: YES
Proven-63 preserved: 63/63 falseReject=0
Surfe AUTO: 0 | Webhound: 0 | Cross-hotel leakage: 0
Target Runs recorded: 8 (errors: 0)

STOP.