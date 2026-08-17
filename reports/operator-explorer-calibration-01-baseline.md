# Operator Explorer Calibration-01 — Baseline

**Mode:** Dry-run only · No Airtable writes  
**Branch:** `app-shell-left-nav`  
**Commit:** `3c88c0b4e22a35052e450d00c5e2f1b9e417c040`  
**Generated:** 2026-08-10T15:04:33.286Z

## Universe

| Metric | Value |
| ------ | ----: |
| Entities | 27 |
| Track 1 | 12 |
| Track 2 | 15 |
| Existing Masters | 17 |
| Provisional entities | 10 |

## Pre-research Airtable (read-only snapshot of Masters in set)

| Object | Count (in-scope Masters) |
| ------ | -----------------------: |
| Claims | 25 |
| Market Presence | 40 |
| Case Studies | 35 |

## Feature flags (unchanged)

- `OPERATOR_FIT_ENGINE_V2=0`
- `OPERATOR_FIT_INTERNAL_PILOT=0`
- `ENABLE_OWNER_OPERATOR_WRITES=0`
- Owner pilot **disabled**

## Protected modules

- Operator Explorer quality baseline: Arbor + Hotel Equities
- Brand Explorer Active/Live freeze (separate)
- Company Validated do-not-overwrite

## Dummy/test exclusions

Nine Beta/Dummy Masters remain excluded (Antillano Norte + synthetic In Review set).

## Known pre-existing issues

- Cenote Azul public footprint historically weak
- Track 2 Core 5 profiles thinner than Arbor/HE gold bar
- Case Study `situation` / `branded_independent` taxonomy pollution
- Conversion experience flat field ~0% on Active universe
- Webhound Track 2 enrichment session running in parallel (sidecar; not production write)

## Validators noted for regression (do not change Fit)

Run when applying later: operator explorer OS/gates, Fit readiness tests, Phase 5E, companies validators — **not executed as blockers for this dry-run research package**.
