# Mexico VIC Combined 4-Family Baseline

**Status:** `mexico_vic_4family_baseline_locked_staging_ready`  
**Locked at:** 2026-08-04T23:13:45.164Z

## Contents

| File | Purpose |
|------|---------|
| 01_combined_4family_index.json | Slim combined record index (666) |
| 02_family_summary.json | Per-family comparison |
| 03_source_lineage_map.json | Wave traceability |
| 04_property_identity_summary.json | Property Identity V1 |
| 05_temporal_affiliation_summary.json | Temporal Affiliation V1 |
| 06_completeness_by_family.json | Core / material / eligible |
| 07_data_eligible_index.json | Data-eligible subset |
| 08_brand_coverage_by_family.json | Brands found independently |
| 09_cross_family_steward_queue.json | Steward queue (no auto-merge) |
| 10_rejected_fuzzy_matches.json | Rejected fuzzy / insufficient |
| 11_marriott_steward_overlay.json | Brand Unconfirmed overlay |
| 12_brand_explorer_completion_readiness.json | BE readiness (no activation) |
| 13_staging_migration_readiness.json | Staging vs production |
| 14_freeze_manifest.json | Lock manifest |
| 15_freeze_hash.txt | Combined freeze hash |

## Rules

- Per-wave freeze artifacts are **not** modified.
- Marriott steward decisions are **overlay only**.
- Cross-family fuzzy auto-merges: **0**.
- Staging only — no Airtable / BE activation / production overwrite / Webhound.
