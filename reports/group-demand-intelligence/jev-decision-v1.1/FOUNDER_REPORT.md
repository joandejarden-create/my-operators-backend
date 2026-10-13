# GDI Jev Decision Layer V1.1 — Founder Report

**Generated:** 2026-09-24T16:19:14.440Z  
**Mode:** SHADOW (no production behavior change)  
**Verdict:** JEV V1.1 IMPROVED — ONE MORE SHADOW CYCLE REQUIRED

## A. V1 CLARIFICATION

| | |
|--|--|
| OFFLINE HISTORICAL CASES | **292** (separate from live) |
| LIVE V1 CALLS | **45** |
| LIVE V1 DISAGREEMENTS | **27** |
| HIGH-CONF DISAGREEMENTS | **17** |
| V1 FALLBACKS | **10** |

V1 fallback explanation: V1 marked `fallbackUsed` on LOW_CONFIDENCE and HARD_GATE policy outcomes even when the provider returned HTTP 200 with a valid choice (`errors=0`). V1.1 splits `technicalFallback` vs `policyFallback`.

### Offline historical by decision type

| Decision | N | Agreement (label baseline) |
|----------|--:|---------------------------:|
| OPPORTUNITY_PREQUAL | 7 | 1.00 |
| SIGNAL_RELEVANCE | 1 | 1.00 |
| PRIVATE_EVENT_SIGNAL_QUALITY | 1 | 1.00 |
| FOLLOWUP_VALUE | 1 | 1.00 |
| STOP_CONTINUE | 3 | 1.00 |
| MATERIAL_CHANGE | 2 | 1.00 |
| EVENT_FORWARDNESS | 2 | 1.00 |
| LODGING_SIGNAL_STRENGTH | 2 | 1.00 |
| SOURCE_UTILITY | 2 | 1.00 |
| TARGET_RESEARCH_PRIORITY | 64 | 1.00 |
| RESEARCH_PLAYBOOK | 83 | 0.37 |
| GENERATOR_CADENCE | 61 | 1.00 |
| VENUE_PARTNERSHIP_ROUTING | 1 | 1.00 |
| FOLLOWUP_TYPE | 61 | 1.00 |
| LOCAL_NO_ROOM_RISK | 1 | 1.00 |

Sources: `real_core=25`, `expanded_seed=240`, `live_adjudicated=27`, `synthetic=0`

## B. ADJUDICATION (V1 disagreements)

JEV_CORRECT: **0**  
CURRENT_GDI_CORRECT: **7**  
BOTH_ACCEPTABLE: **4**  
INSUFFICIENT_EVIDENCE: **16**

## C. HIGH-CONFIDENCE DISAGREEMENTS (V1)

HIGH_CONF_JEV_CORRECT: **0**  
HIGH_CONF_GDI_CORRECT: **6**  
HIGH_CONF_BOTH: **2**  
HIGH_CONF_INSUFFICIENT: **13**

## D. ROOT CAUSES OF JEV ERRORS

MISSING_CONTEXT: **0**  
AMBIGUOUS_CHOICES: **0**  
INPUT_TOO_THIN: **21**  
POLICY_CONTEXT_MISSING: **0**  
CONFIDENCE_MISCALIBRATED: **1**  
OTHER: **0**

Dominant cause: V1 canary always set `missingFields=["lodgingEvidence"]`, steering playbook to `LODGING_HOUSING`.

## E. CURRENT GDI ERRORS EXPOSED BY JEV

ROUTING_TOO_GENERIC: **0** (V1 sample)  
PREMATURE_STOP: **0**  
BAD_PRIORITY: **0**  
BAD_CADENCE: **0**  
OTHER: **0**

Live R2 showed **6** JEV_CORRECT (gap-specific lodging/future routing) and **50** BOTH_ACCEPTABLE — type-default GDI routing is often acceptable but generic.

## F. FALLBACKS

V1 FALLBACKS: **10** (LOW_CONFIDENCE / POLICY_REJECT miscounted as fallback)  
V1.1 TECHNICAL FALLBACKS: **0**  
V1.1 POLICY FALLBACKS: **51**  

CAUSE BREAKDOWN: `LOW_CONFIDENCE=36`, `POLICY_REJECT=15`

## G. CALIBRATION

| Confidence Bin | N | Accuracy* | Jev Correct | GDI Correct | Both | Insufficient |
|----------------|--:|----------:|------------:|------------:|-----:|-------------:|
| below_0.50 | 22 | 0.64 | 1 | 0 | 13 | 8 |
| 0.50–0.59 | 19 | 0.68 | 2 | 1 | 11 | 5 |
| 0.60–0.69 | 17 | 0.82 | 0 | 0 | 14 | 3 |
| 0.70–0.79 | 14 | 0.71 | 2 | 0 | 8 | 4 |
| 0.80–0.89 | 13 | 0.62 | 1 | 0 | 7 | 5 |
| 0.90–1.00 | 29 | 0.03 | 0 | 6 | 1 | 22 |

\*Among adjudicated disagreements only. **0.90+ is not trustworthy** (V1 input-bias cluster).

## H. GOLDEN DATASET

TOTAL LABELED: **292**  
REAL HISTORICAL (core): **25**  
EXPANDED SEED: **240**  
LIVE ADJUDICATED: **27**  
SYNTHETIC: **0**

## I. MULTI-HOTEL

| Hotel | Targets | Source | Agreement |
|-------|--------:|--------|-----------|
| Bethesda | 12 | research_registry | 0.40 |
| Renaissance | 12 | gdi_opportunities | 0.37 |
| Cambridge | 8 | gdi_opportunities | 0.68 |

HOTEL-SPECIFIC LOGIC: **NO**

## J. DECISION-TYPE RESULTS (live R2)

| Decision | N | Agreement | Jev OK | GDI OK | High-conf wrong | Fallback | Status |
|----------|--:|----------:|-------:|-------:|----------------:|----------|--------|
| RESEARCH_PLAYBOOK | 32 | 0.44 | 3 | 0 | 0 | tech=0/pol=10 | KEEP_SHADOW |
| FOLLOWUP_TYPE | 32 | 0.31 | 3 | 0 | 0 | tech=0/pol=12 | KEEP_SHADOW |
| TARGET_RESEARCH_PRIORITY | 32 | 0.34 | 0 | 0 | 0 | tech=0/pol=3 | KEEP_SHADOW |
| GENERATOR_CADENCE | 32 | 0.97 | 0 | 0 | 0 | tech=0/pol=12 | KEEP_SHADOW |
| STOP_CONTINUE | 7 | 0.29 | 0 | 0 | 0 | tech=0/pol=3 | KEEP_SHADOW |
| OPPORTUNITY_PREQUAL | 7 | 0.43 | 0 | 0 | 0 | tech=0/pol=2 | KEEP_SHADOW |
| SIGNAL_RELEVANCE | 8 | 0.25 | 0 | 0 | 0 | tech=0/pol=2 | KEEP_SHADOW |
| MATERIAL_CHANGE | 10 | 0.00 | 0 | 0 | 0 | tech=0/pol=7 | KEEP_SHADOW |

Live R2 adjudication (disagreements only): JEV_CORRECT **6** · GDI_CORRECT **0** · BOTH **50** · INSUFFICIENT **31**  
High-conf R2: JEV **3** · GDI **0** · BOTH **14** · INSUFFICIENT **18**

## K. RESEARCH OUTCOME VALIDATION

JEV ROUTE PRODUCED BETTER EVIDENCE: **0**  
CURRENT ROUTE PRODUCED BETTER EVIDENCE: **3**  
NO DIFFERENCE: **14**  
UNKNOWN: **143**

## L. EFFICIENCY SIMULATION

QUERIES CURRENT: **32**  
QUERIES JEV-ROUTED: **32**  
QUERIES SAVED: **0**  
MATERIAL SIGNALS LOST: **3**  
MATERIAL SIGNALS GAINED: **0**

(STOP not applied.)

## M. LATENCY / COST

CALLS: **160**  
P50: **223** ms  
P95: **263** ms  
P99: **657** ms  
ESTIMATED COST: **$0.004962**

## N. AIRTABLE AUDIT

TARGET RUN JEV FIELDS: **MAPPED** (write only when schema exposes optional columns)  
DECISIONS PERSISTED: **0** this cycle (shadow file audit + compact objects ready)  
ORPHANS: **0**  
RAW PAYLOADS STORED: **0**

## O. SAFE APPLY RECOMMENDATION

| Decision | Status | Why |
|----------|--------|-----|
| RESEARCH_PLAYBOOK | KEEP_SHADOW | Agreement 0.44; outcome proxy does not show superior evidence recovery yet |
| FOLLOWUP_TYPE | KEEP_SHADOW | Agreement 0.31; only 3 clear Jev-correct adjudications |
| TARGET_RESEARCH_PRIORITY | KEEP_SHADOW | Agreement 0.34; mostly BOTH/INSUFFICIENT |
| GENERATOR_CADENCE | KEEP_SHADOW | 0.97 agreement is mostly KEEP identity — advisory value unproven |
| STOP_CONTINUE | KEEP_SHADOW | High-risk false-stop class |
| OPPORTUNITY_PREQUAL | KEEP_SHADOW | Must not bypass Commercial Quality |
| SIGNAL_RELEVANCE | KEEP_SHADOW | Rejection risk |
| MATERIAL_CHANGE | KEEP_SHADOW | Weekly delta remains deterministic |

Closest future candidates after one more outcome-linked cycle: RESEARCH_PLAYBOOK → FOLLOWUP_TYPE → GENERATOR_CADENCE (advisory only).

## P. DECISION

1. High-conf V1 disagreements were **mostly insufficient / GDI-correct under input bias**, not mostly Jev-correct.
2. Wrong/non-comparable high-conf calls: **INPUT_TOO_THIN** (forced lodging missing).
3. Input/choice refinement helped R2 (tech fallbacks 0; some JEV_CORRECT on real gaps) but did not clear apply gates.
4. Fallback accounting improved for legitimate reasons (split technical vs policy); thresholds not lowered.
5. Multi-hotel generalization: **yes for cohorts** (registry + opportunity-derived); agreement still varies by archetype.
6. Routing better than generic? **Not yet proven** by outcome proxy (JEV better 0 / current better 3 / mostly UNKNOWN).
7. Safe for controlled apply now: **none**.
8. Remain shadow: **all** decision types this cycle.
9. Confidence calibrated? **No** — especially 0.90+.
10. Another shadow cycle necessary? **YES**.

## Q. FINAL VERDICT

**JEV V1.1 IMPROVED — ONE MORE SHADOW CYCLE REQUIRED**

---

## PERSISTENCE / GENERALIZATION

CODE FILES CHANGED:
- `lib/group-demand-intelligence/jev/jev-decision-service.js`
- `lib/group-demand-intelligence/jev/jev-types.js`
- `lib/group-demand-intelligence/jev/jev-observability.js`
- `lib/group-demand-intelligence/jev/jev-config.js`
- `lib/group-demand-intelligence/jev/jev-choice-filter.js` (new)
- `lib/group-demand-intelligence/jev/jev-adjudication.js` (new)
- `lib/group-demand-intelligence/jev/jev-cohort.js` (new)
- `lib/group-demand-intelligence/jev/jev-airtable-audit.js` (new)
- `lib/group-demand-intelligence/jev/historical-dataset.js`
- `lib/group-demand-intelligence/jev/index.js`
- `lib/group-demand-intelligence/research-coverage/airtable-field-map.js`
- `scripts/gdi-jev-shadow-eval-v1.1.mjs` (new)
- `scripts/test-gdi-jev-decision-v1.1.mjs` (new)
- `package.json`

FIXTURES ADDED:
- `fixtures/group-demand-intelligence/jev/adjudicated-disagreements-v1.json`
- `fixtures/group-demand-intelligence/jev/historical-decisions-expanded-v1.1.json`

ADJUDICATED CASES ADDED: **27**

TESTS:
- `npm run test:gdi-jev-decision-v1`
- `npm run test:gdi-jev-decision-v1.1`

HOTEL-SPECIFIC LOGIC: **NO**  
HARD-CODED ORGANIZATIONS: **NO**  
HARD-CODED DOMAINS: **NO**

GIT SHA: `b4352b8e05f739f8f93ab471d78c8213c3ba632e`  
WORKING TREE CLEAN: **NO**
