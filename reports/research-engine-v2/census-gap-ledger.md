# Census Gap Ledger

**Updated:** 2026-08-09
**Source:** missing-field-source-strategy-controller-v1 (Railway resume)

## Snapshot (pre field-pass / mid-run)

- Live Hotel Property Census: **2561**
- Address complete before enrichment: **846**
- Missing address eligible: **1820**
- Discovery units completed: **234**
- Discovery timed out: **1** (Hilton/Mexico)
- Discovered cache: **1245**
- Discovery status: `official_directory_discovery_partial_network_remaining`

## Policy

- Write target: Hotel Property Census only (`tbl9aY5ijiuIzzWam`)
- Medium fields: internal-only
- Direct DataForSEO coordinates: held
- Owner/operator/dates/Recent Momentum/Company Validated/Brand Verified: held

## Status

- `production_census_v4_full_build_resumed_after_railway_discovery_fix_complete`
- `production_census_v4_full_build_partial_source_remaining`
- `production_census_v4_full_build_partial_network_remaining`

Live field-write tallies update on each Railway field strategy pass (`51-missing-field-source-strategy-last.json` on volume).
