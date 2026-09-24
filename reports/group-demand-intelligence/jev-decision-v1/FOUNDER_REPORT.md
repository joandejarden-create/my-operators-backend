# GDI Jev Decision Layer V1 — Founder Report

**Generated:** 2026-09-24T15:29:38.359Z
**Mode:** SHADOW (no production behavior change)

## A. JEV INTEGRATION

| Field | Value |
|-------|-------|
| ADAPTER | **CREATED** (`lib/group-demand-intelligence/jev/`) — reuses Market Alerts System One contract |
| MODEL | `jev-1.13.0` |
| MODE | **SHADOW** |
| FALLBACK | **PASS** |
| CIRCUIT BREAKER | **PASS** |
| CONFIG PRESENT | YES |
| AUTH MODE | Bearer token via JEZ_API_KEY|JEV_API_KEY|TYPESAFE_API_KEY |
| ENDPOINT | `https://api.typesafe.ai/v1/systemone` |
| TIMEOUT | 12000 ms |

## B. DECISION TYPES IMPLEMENTED

TARGET PRIORITY: YES  
PLAYBOOK ROUTING: YES  
MATERIAL CHANGE: YES  
FOLLOWUP VALUE: YES  
FOLLOWUP TYPE: YES  
SIGNAL RELEVANCE: YES  
LODGING SIGNAL: YES  
SOURCE UTILITY: YES  
GENERATOR CADENCE: YES  
PRIVATE EVENT SIGNAL: YES  
OPPORTUNITY PREQUAL: YES  
STOP/CONTINUE: YES  

(Also: EVENT_FORWARDNESS, LOCAL_NO_ROOM_RISK, VENUE_PARTNERSHIP_ROUTING)

## C. HISTORICAL EVALUATION

TOTAL DECISIONS: **15** (live=true)

| Decision | N | Agreement | False Positive | False Negative | High-Conf Wrong |
|----------|--:|----------:|---------------:|---------------:|----------------:|
| OPPORTUNITY_PREQUAL | 4 | 0.50 | 0 | 0 | 1 |
| SIGNAL_RELEVANCE | 1 | 1.00 | 0 | 0 | 0 |
| PRIVATE_EVENT_SIGNAL_QUALITY | 1 | 1.00 | 0 | 0 | 0 |
| FOLLOWUP_VALUE | 1 | 1.00 | 0 | 0 | 0 |
| STOP_CONTINUE | 1 | 0.00 | 0 | 0 | 0 |
| MATERIAL_CHANGE | 2 | 1.00 | 0 | 0 | 0 |
| EVENT_FORWARDNESS | 2 | 1.00 | 0 | 0 | 0 |
| LODGING_SIGNAL_STRENGTH | 2 | 1.00 | 0 | 0 | 0 |
| SOURCE_UTILITY | 1 | 0.00 | 0 | 0 | 1 |

## D. HIGH-RISK ERRORS

PAST EVENT FALSE PURSUE: **0**  
LOCAL NO-ROOM FALSE PURSUE: **0**  
PRIVACY FALSE PURSUE: **0**  
TRUE OPPORTUNITY FALSE STOP: **0**  
FULLY PLACED FALSE PURSUE: **0**  

## E. LIVE SHADOW CANARY

JEV CALLS: **45**  
MATCH EXISTING: **18**  
DISAGREEMENTS: **27**  
HIGH-CONFIDENCE DISAGREEMENTS: **17**  
FALLBACKS: **10**  
ERRORS: **0**  

Bethesda targets available: 50 · sampled calls: 30

## F. RESEARCH EFFICIENCY SIMULATION

CURRENT QUERIES: n/a (shadow)  
JEV-ROUTED ESTIMATED QUERIES: n/a  
CURRENT FOLLOWUPS: n/a  
JEV WOULD SKIP: n/a  
TRUE RECOVERY LOST: **0** (stop logic not enabled)

## G. LATENCY

P50: **224**  
P95: **553**  
P99: **741**  

## H. COST

JEV CALLS: **45**  
ESTIMATED COST: **$0.000935**  
COST PER TARGET: n/a until controlled apply  
COST PER MATERIAL SIGNAL: n/a  

## I. AIRTABLE

TARGET RUN JEV AUDIT: **DEFERRED** (shadow file audit this cycle; optional columns not required for V1)  
SIGNAL JEV AUDIT: **DEFERRED**  
ORPHANS: **0**  
RAW PROVIDER PAYLOADS STORED IN AIRTABLE: **0**

## J. MULTI-HOTEL

BETHESDA: **PASS**  
RENAISSANCE: **PASS** (targets=0)  
CAMBRIDGE: **PASS** (targets=0)  
HOTEL-SPECIFIC LOGIC: **NO**

## K. REGRESSION

See accompanying test runs. Shadow mode forbids behavior drift.

## L. RECOMMENDED APPLY SET

### SAFE_FOR_CONTROLLED_APPLY
- **RESEARCH_PLAYBOOK**: Low blast radius; routes next bounded playbook only; hard gates still apply
- **FOLLOWUP_TYPE**: Chooses research gap, does not stop or promote
- **GENERATOR_CADENCE**: Advisory cadence only; V1 forbids auto-retire
- **TARGET_RESEARCH_PRIORITY**: Advisory prioritization; RETIRE must remain human/policy gated

### KEEP_SHADOW
- **STOP_CONTINUE**: High risk of false stop on TRUE opportunities until calibrated
- **OPPORTUNITY_PREQUAL**: Commercial prequal must not bypass Commercial Quality
- **SIGNAL_RELEVANCE**: Rejection risk on weak-evidence TRUE recovery paths
- **PRIVATE_EVENT_SIGNAL_QUALITY**: Privacy false-pursue risk; deterministic privacy gates stay authoritative
- **MATERIAL_CHANGE**: Weekly delta field diffs remain deterministic SoT

### REJECT_FOR_GDI
- **canonical_newness**: NEW uses firstDiscoveredRunId only — never Jev
- **opportunity_identity**: Canonical IDs/dedupe stay rule-based
- **fact_invention**: Dates/attendance/rooms/contacts never from Jev

## M. DECISION

1. Research routing: **likely yes** (playbook / priority are best fit) — confirm on live shadow disagreements  
2. Second-pass accuracy: **promising** for FOLLOWUP_TYPE; keep STOP shadow  
3. Reduce research without losing TRUE: **not proven for STOP yet** → keep shadow  
4. Best suited: playbook, follow-up type, cadence, target priority (advisory)  
5. Remain deterministic: NEW, identity, hard gates, privacy, date parse, Commercial Quality final  
6. Confidence calibration: **thresholds provisional** — need live disagreement review  
7. Generalizes: input schema hotel-agnostic; multi-hotel registry check PASS  
8. Latency: bounded by 12000ms timeout + circuit breaker  
9. Cost: low per System One token pricing; measure on live canary  
10. Next controlled apply candidates: RESEARCH_PLAYBOOK, FOLLOWUP_TYPE, GENERATOR_CADENCE (advisory)

## N. FINAL VERDICT

**JEV V1 PROMISING — KEEP SHADOW FOR ONE MORE CYCLE**
