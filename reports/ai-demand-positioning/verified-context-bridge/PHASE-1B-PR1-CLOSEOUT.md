# ADP 2.0 Phase 1B / PR-1 — Verified Context Bridge Closeout (corrected)

**Date:** 2026-11-19  
**HEAD target:** PR #461 quality correction  
**Scope:** Diagnostic only — no Airtable writes, no customer RG cutover, no paid research  
**Tests:** `npm run test:adp-verified-context-bridge-v1`

## Correction applied

Empty / non-mentioned observation corpora no longer produce validated Reality Gap scores.  
Attribute-mapping success is reported separately from measurement-comparison success (`DATA_INSUFFICIENT` when needed).

## Observation-backed vs coverage-only

| Hotel | Attribute coverage | Measurement comparison |
|---|---|---|
| Bethesda Marriott | Validated | **OBSERVATION_BACKED** dual-run scores |
| Spice Island Beach Resort | Validated | **DATA_INSUFFICIENT** (no local period observations). Published RG 62.5% cited read-only — **not** dual-run success |
| YOTEL Geneva Lake | Validated (regression) | **DATA_INSUFFICIENT** (no local period observations) |

## Pilot results

### Bethesda Marriott — observation-backed

| Metric | Value |
|---|---|
| Selected | 15 |
| Eligible / unique concepts | see latest JSON |
| Fixture denom / gapScore | 15 / (observation-backed) |
| Bridged denom / gapScore | eligible / (observation-backed) |
| Artifact warning | possible DENOMINATOR_ARTIFACT when bridged score drops due to exclusion |
| Period | `adp_period_adp_bethesda_marriott_20261002142154_62428a` |

### Spice Island — attribute coverage only

| Metric | Value |
|---|---|
| Selected | 24 |
| Eligible unique concepts | 20 (synonyms collapsed for min-12) |
| Min-12 (unique concepts) | MEETS |
| Fixture / bridged **scores** | **null** — `DATA_INSUFFICIENT` |
| Published reference (not dual-run) | period `…966d88`, gapScore 62.5, `dualRunValid: false` |

Raw observations are not present under `data/ai-demand-positioning/runtime` for Spice; evidence index is missing-evidence excerpts only. No synthetic observations created.

### YOTEL — definition regression

| Metric | Value |
|---|---|
| Undefined | `airport_shuttle`, `park_and_fly` → `SELECTED_UNDEFINED` |
| Scores | withheld — `DATA_INSUFFICIENT` |
| Y1 defs | not enabled |

## Minimum-12

Configurable; diagnostic only. Counted as **eligible unique commercial concepts** (semantic synonyms collapsed).

## Airtable Option B

Design approved. Fixture migration = **REVIEW_REQUIRED**, not auto-APPROVED. No schema/writes yet.

## Historical compatibility

Customer-facing Reality Gap unchanged. No historical regeneration. Bridged path diagnostic-only.
