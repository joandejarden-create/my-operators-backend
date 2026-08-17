# Production Census Autopilot Runner

**Status:** `production_census_cala_discovery_mode_ready_needs_source_adapter`  
**Target writes:** Deal Capture Platform → Hotel Property Census only (`tbl9aY5ijiuIzzWam`)  
**Brand Setup / Brand Explorer:** read-only; never patched  
**VIC:** evidence only  

## Queues

Includes `source_discovery` — CALA Discovery + Insert Mode (controlled insert approval bundles; no Airtable writes unless founder-approved apply).

See:

- `docs/data-intelligence/production-census-cala-discovery-mode.md`
- `docs/data-intelligence/production-census-autopilot-runner.md`
- `docs/data-intelligence/production-census-source-of-truth.md`

## Commands

```bash
npm run census:autopilot -- --region CALA --scope active-brand-setup --mode controlled \
  --strategy fastest-safe --queue source_discovery --run-until-complete --batch-size 250
```
