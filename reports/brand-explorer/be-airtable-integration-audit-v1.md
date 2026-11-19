# Brand Explorer Airtable integration audit v1

- Contract: `BE_AIRTABLE_INTEGRATION_SOT_V1`
- Generated: 2026-11-19T11:28:05.790Z
- Dry-run: true
- Airtable writes: false
- Designated footprint authority: **hpc**
- Transitional footprint authority: **hotel_census**

## Gaps
- HPC brand rollup not yet wired as primary BE censusSummary path
- TAB_CONTRACTS on main still 7 tabs vs atelier 10 (factory restore Phase 2)
- ADP/GDI brand-level read contracts reserved only

## Live census probe
_Skipped (pass `--live` for GET-only Hotel Census probe)._

## STOP
- No bulk brand population
- No production Airtable writes
- No ADP/GDI workstream modifications
