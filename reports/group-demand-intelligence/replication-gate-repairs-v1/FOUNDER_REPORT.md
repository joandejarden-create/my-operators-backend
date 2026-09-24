# ADP + GDI Replication Gate Repairs V1 — Founder Report

**Date:** 2026-09-24  
**Branch:** `deploy/gdi-pe-v1-7-customer-closure`  
**Hotel (seed validation only):** Renaissance New York Times Square `recG66DQJKP2c0UNh`  
**Canonical base:** `appa2cE7FTRmIbB32`  
**Not done this cycle:** full Renaissance ADP baseline, full weekly discovery, Surfe, Jev apply

## Verdict

**REPLICATION GATE PASSES — ADP PACK STEWARDSHIP STILL REQUIRED**

GDI Fit + Research Target seed is now generic, fail-closed on Airtable base routing, Bethesda-pilot-quarantined by default, dry-run → apply → recreate-proof for Renaissance without hotel-name switches.

---

## A. CHECKPOINT

| Field | Value |
|-------|-------|
| BRANCH | `deploy/gdi-pe-v1-7-customer-closure` |
| START SHA | `3bd76bf0ee66ddbde672dda9e20e07eff1c025cc` (Replication Readiness V2) |
| COMMITS | (see git log after push) |
| PUSH | pending at report write |

## B. CANONICAL AIRTABLE

| Check | Result |
|-------|--------|
| ADP BASE CONFIG | PASS (fail-closed; accepts `ADP_AIRTABLE_BASE_ID` → intelligence → GDI → verified-canonical `AIRTABLE_BASE_ID` only) |
| GDI BASE CONFIG | PASS (`AIRTABLE_GDI_BASE_ID` → canonical) |
| FAIL-CLOSED | PASS |
| LEGACY WRITE ATTEMPTS | 0 (guard throws on `appvtnDurnMSjINP6`) |

Local env snapshot (no secrets): GDI/intelligence SET_CANONICAL; product `AIRTABLE_BASE_ID` SET_NONCANONICAL and no longer used silently.

## C. ADP PERSISTENCE POLICY

| Field | Value |
|-------|-------|
| PRIMARY SOT | FILESYSTEM_SNAPSHOT |
| LIVE OVERLAY | AIRTABLE_PUBLISHED_REPORTS |
| HISTORY AIRTABLE | DISABLED |
| SECOND-HOTEL BLOCKER | NO — snapshot history sufficient for this replication phase |

## D. PILOT LOGIC

| Metric | Count |
|--------|-------|
| Bethesda production-reachable paths before | ~8 (orchestrator seeds/deepen/DMV + canary seeds + contact packs) |
| Default production paths after (arbitrary hotel) | 0 |
| Pilot optional (explicit flag / pilot hotel) | retained for Bethesda reference |
| Production required | 0 Bethesda-specific |

Renaissance config already had `bethesdaSeedPathAllowed: false` / `dmvExpansionAllowed: false`; orchestrator now uses `isPilotReferenceLogicEnabled`.

## E. RENAISSANCE ADP INPUTS

| Class | Items |
|-------|-------|
| AVAILABLE | profile, census link, rooms, meeting, brand, website, market, GDI hotel config, certified cohort presence |
| DERIVABLE | Fit rows, Research Targets (this cycle), portable generator seeds |
| MANUAL | competitor/peer pack stewardship, scenario commercial review, share authorization |
| BLOCKING | none for GDI seed gate |

## F. ROOT CAUSE (why Fit/Targets were 0)

Hotel Demand Generator Fit rows were never created for Renaissance. Research Target backfill only builds targets for generators with a **hotel-scoped Fit**. Bethesda had Fits from the Bethesda canary seed path; Renaissance did not. Not an ID mismatch — missing generic onboard seed.

## G. FIT GENERATION

| Metric | Count |
|--------|-------|
| DRY-RUN FITS | 8 |
| APPLIED FITS | 8 |
| INVALID / REJECTED | 0 |

## H. RESEARCH TARGET GENERATION

| Metric | Count |
|--------|-------|
| DRY-RUN TARGETS | 16 |
| APPLIED TARGETS | 16 |

**By type:** DEMAND_GENERATOR 8 · PROGRAM 8  

**By priority:** HIGH 4 · MEDIUM 12 · LOW 0  

Baseline seed status = ACTIVE monitoring (not weekly NEW / not opportunity).

## I. RECREATE TEST

| Metric | Value |
|--------|-------|
| FIT FIRST / RECREATED | 8 / 8 |
| FIT OVERLAP | 100% |
| TARGET FIRST / RECREATED | 16 / 16 |
| TARGET OVERLAP | 100% |
| MANUAL HOTEL-SPECIFIC INJECTION | NO |

Method: dry-run equivalence (safest; no production delete).

## J. WEEKLY READINESS

**STATUS:** WEEKLY_READY  

Structural fields present for orchestrator / lane router consumption. Full live weekly discovery **not** run this cycle.

## K. JEV SEED SHADOW

| Metric | Value |
|--------|-------|
| CALLS | 12 |
| AGREEMENT (finalPolicy vs deterministic) | 12 / 100% |
| JEV BETTER / CURRENT BETTER | 0 / 0 |
| UNKNOWN / HIGH-CONF WRONG | 0 / 0 (quality still UNKNOWN without research outcomes) |
| APPLY READY | NO |

Jev sometimes *selected* alternate playbooks (e.g. LODGING_HOUSING) but shadow policy kept deterministic seed routing.

## L. ADP JEV

| Field | Value |
|-------|-------|
| SHADOW ADAPTER EXISTS | NO |
| LOW-RISK INTEGRATION POINTS | EVIDENCE_UTILITY, SOURCE_PRIORITY, FOLLOWUP_RESEARCH_TYPE |
| PRODUCTION ADP BEHAVIOR CHANGED | NO |

## M. MANUAL INTERVENTIONS

| Field | Value |
|-------|-------|
| PREVIOUS ESTIMATE | 12–18 |
| NOW EXPECTED | ~6–8 |
| LEGITIMATE STEWARDSHIP | competitor/peer approval, commercial scenario review, quality certification, share authorization |
| AUTOMATION GAPS CLOSED | Fit seed, Target seed, base fail-closed, pilot quarantine |

## N. SECURITY / ISOLATION

| Check | Result |
|-------|--------|
| RENAISSANCE WRITES HOTEL-SCOPED | PASS |
| BETHESDA DATA LEAK | 0 |
| WRONG-BASE WRITES | 0 |
| SECRETS | PASS |

## O. PERFORMANCE

| Metric | Value |
|--------|-------|
| SEED RUNTIME (apply) | ~23s |
| AIRTABLE READS (est.) | 40 |
| AIRTABLE WRITES | 40 |
| JEV CALLS (shadow dry) | 12 |
| N+1 | present (per-entity find+upsert); acceptable for onboard seed size |

## P. REGRESSION

| Suite | Result |
|-------|--------|
| Replication gate V1 | PASS (17) |
| GDI waterstone portability | PASS |
| GDI research coverage V1 | PASS |
| ADP / Share / CSV / Contact / Surfe | not re-run full matrix this cycle (no Surfe; no ADP output change) |

## Q. DECISION ANSWERS

1. Canonical Airtable routing fail-closed? **YES**
2. Bethesda pilot removed from generic default? **YES**
3. Renaissance Fits generically? **YES**
4. Renaissance Targets generically? **YES**
5. Recreatable from canonical hotel inputs? **YES** (100% overlap)
6. Weekly orchestrator has structural needs? **YES** (WEEKLY_READY; live weekly not run)
7. ADP still manual? **Peer/scenario stewardship + certification**
8. Manual interventions before Bethesda parity? **~6–8**
9. Stewardship vs automation debt? **Mostly legitimate stewardship; seed automation debt closed**
10. Jev useful for seed priority/playbook? **Shadow agreement on policy path; quality UNKNOWN**
11. Move any Jev beyond shadow? **NO**
12. Ready for Second Hotel Replication V1? **YES for GDI seed gate; ADP pack stewardship still required before declaring full parity**

## R. FINAL VERDICT

**REPLICATION GATE PASSES — ADP PACK STEWARDSHIP STILL REQUIRED**

## Persistence

| Field | Value |
|-------|-------|
| BETHESDA-SPECIFIC DEFAULT LOGIC | NO (quarantined) |
| RENAISSANCE-SPECIFIC PRODUCTION LOGIC | NO |
| HARD-CODED HOTEL DOMAINS ADDED | NO (portable national templates only) |
| JEV PRODUCTION BEHAVIOR CHANGED | NO |

Evidence: `reports/group-demand-intelligence/replication-gate-repairs-v1/`
