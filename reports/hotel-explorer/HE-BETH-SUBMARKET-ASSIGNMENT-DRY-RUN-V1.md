# Bethesda submarket assignment — Airtable dry-run (NOT APPLIED)

**Status:** `DRY_RUN_ONLY` · `applyAuthorized: false`  
**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Evidence:** `reports/hotel-explorer/HE-BETH-HPC-SUBMARKET-COVERAGE-V1.json`

## Findings (read-only)

- Exact Hotel Census match for submarket label `Pooks Hill / Rockville Pike`: **0**
- City=`Bethesda` / country=`United States` Scout coverage in this runtime: **0 census rows loaded** (`censusRecordsLoaded: 0` in Scout report — US Hotel Census may be out of current Scout cache scope or credentials path)
- UI correctly shows empty aggregates with explicit coverage language (no fabricated market totals)

## Proposed methodology (not applied)

| Field | Proposed value |
| --- | --- |
| Canonical market ID | TBD — provisional `Washington DC / Montgomery County` |
| Canonical submarket ID | TBD |
| Submarket label | `Pooks Hill / Rockville Pike` (provisional) |
| Source | HE founder pilot / property context |
| Assignment method | `provisional_corridor_label` |
| Verification status | `UNVERIFIED_PROVISIONAL` |
| Geographic boundary | Not verified — no official Dealality polygon |
| Exception handling | Show provisional badge; never present as official STR/Dealality census boundary |

## Required before any Airtable write

1. Founder review of this dry-run + coverage JSON  
2. Explicit apply authorization  
3. Stewardship workflow via canonical HPC / Hotel Census fields only  
4. Idempotent dry-run CSV/JSON of records to patch  

**No production Airtable writes were executed in this task.**
