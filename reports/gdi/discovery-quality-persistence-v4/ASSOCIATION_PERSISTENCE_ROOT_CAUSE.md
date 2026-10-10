# AssociationScout Persistence Root Cause

## Counts
| Metric | Value |
|--------|------:|
| ASSOCIATION DETAIL ROWS PRODUCED (SCOUT_YIELD) | 48 |
| ASSOCIATION DETAIL ROWS PERSISTED (ASSOCIATION_RESULTS.csv) | 0 |
| ASSOCIATION DETAIL ROWS LOST | 48 |

## Drop location
**scripts/gdi-discovery-expansion-v3-2026-10-03.mjs report writer — rowsForScout() never called for SCOUT_FAMILY.ASSOCIATION**

Pipeline:
`query → SERP → hitToCandidate → newCandidates[] → scoutYield counts → report writer`

- Produced: `orchestrator` incremented `scoutStats[AssociationScout].candidates` and included rows in `newCandidates` (contributing to TOTAL NEW CANDIDATES = 156).
- Persisted detail CSVs: Procurement, Medical, TourDmc, University, HiddenDemand, etc. via `rowsForScout`.
- **AssociationScout was omitted** from `rowsForScout` writes. Only `ASSOCIATION_SERIES_RESULTS.csv` (series subset) was written.
- Research-pool reconstruction read detail CSVs only → Association detail rows invisible → completion never saw them.

## Root cause
Silent report-writer omission of `ASSOCIATION_RESULTS.csv` — not a scout/query failure.

## Fix
1. V3 script now writes `ASSOCIATION_RESULTS.csv` via `rowsForScout(SCOUT_FAMILY.ASSOCIATION)`.
2. Future runs must assert every scout family with candidates has a detail CSV or explicit rejection ledger (no silent loss).

Silent loss: **YES** (before fix).
