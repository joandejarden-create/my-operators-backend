# HE 5.0 — Existing Infrastructure Inventory (Phase 0)

**Generated:** 2026-11-19T23:05:38.151Z  
**PR:** #463 · Branch `feat/he-beth-phase3-sources-evidence-v1`  
**Apply:** blocked (`applyAuthorized: false`)

## What already works

| Capability | Module / surface | Status |
| --- | --- | --- |
| HPC identity collision + Mullen cohort | `hpc-population/*`, `he:mullen-pilot-hpc-identity-resolution` | WORKS (read-only) |
| Shared budget reserve/settle | `research-engine-v1/budget-ledger.js` + M1 shared ledger | WORKS |
| HI graph contract + upsert planner | `airtable-graph-*` | WORKS (dry-run) |
| Bethesda Phase 3 evidence + stakeholder dry-runs | `reports/hotel-explorer/phase3*`, `phase4-quality/*` | WORKS (staged) |
| Demand Anchors / TI GET APIs | `he-demand-access-read-v1`, `api/hotel-explorer-demand-access` | WORKS (US sparse honest empty) |
| Guest Sentiment + Market Alerts news | HE-QL-038 surfaces | WORKS (rights-gated / empty news honest) |
| Founder Preview proposed vs live | `api/hotel-explorer-founder-preview.js` | WORKS |
| Provider capability matrix | P3A `provider-capability-matrix.js` | WORKS (no auto paid) |
| HE 12-tab shell | `public/js/hotel-explorer.js` | WORKS |

## Partial

| Capability | Gap |
| --- | --- |
| Bethesda HI graph | Staged dry-runs not applied |
| Year built / renovation on HPC | Verified free; not persisted |
| Seven-hotel HPC CREATE | Identity packets ready; not created |
| US Demand Anchors near Bethesda | Infrastructure connected; few anchors |
| Guest Reputation live | Staged founder-preview only |
| Market Alerts Bethesda news | Genuine absence |

## Missing / out of scope this pilot

- Production Airtable apply (founder gate)
- Automatic Surfe
- Unrestricted paid enrichment
- Per-hotel frontend coding
- Duplicate hotel databases

## Reuse rule

Do **not** recreate HPC population, budget ledgers, or HI graph planners — extend via `lib/hotel-intelligence/he-5-0-full-e2e/`.
