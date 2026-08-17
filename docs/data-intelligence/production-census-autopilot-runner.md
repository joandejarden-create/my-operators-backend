# Production Census Autopilot Runner

**Status:** `production_census_cala_discovery_mode_ready_needs_source_adapter`  
**Target writes:** Deal Capture Platform → **Hotel Property Census** only (`tbl9aY5ijiuIzzWam`)  
**Brand Setup active control list:** read-only Active/Live brands; never patched  
**VIC source claims:** evidence lineage only; never written  
**Brand Explorer:** untouched  

Canonical SoT: `docs/data-intelligence/production-census-source-of-truth.md`  
Discovery mode: `docs/data-intelligence/production-census-cala-discovery-mode.md`

## Scopes & strategies

| Flag | Meaning |
| --- | --- |
| `--scope active-brand-setup` | Control list = Active/Live Brand Setup brands |
| `--scope parent-company` | Filter by `--parent-company` (legacy) |
| `--strategy fastest-safe` | Score/order queues for max safe High writes |
| *(no `--queue`)* | **Execute all eligible queues** in fastest-safe order |
| `--queue <id>` | Targeted queue only |
| `--batch-size` | Chunk size only |
| `--run-until-complete` | Full eligible scope |
| `--max-records` | Sample/test cap only |
| `--resume <run-id>` | Continue from checkpoint |

## Commands

```bash
npm run census:autopilot -- --region CALA --scope active-brand-setup --mode plan \
  --strategy fastest-safe

# CALA discovery + insert approval bundle (no Airtable writes)
npm run census:autopilot -- --region CALA --scope active-brand-setup --mode controlled \
  --strategy fastest-safe --queue source_discovery --run-until-complete --batch-size 250

# Default controlled: multi-queue fastest-safe (no Airtable writes)
npm run census:autopilot -- --region CALA --scope active-brand-setup --mode controlled \
  --strategy fastest-safe --run-until-complete --batch-size 250

# Targeted queue
npm run census:autopilot -- --region CALA --scope active-brand-setup --mode controlled \
  --strategy fastest-safe --queue description_extraction --run-until-complete --batch-size 250

npm run census:autopilot -- --region CALA --scope active-brand-setup --mode apply \
  --strategy fastest-safe --run-until-complete --batch-size 100 \
  --approval-bundle reports/research-engine-v2/autopilot/<run-id>/approval-bundle.json \
  --confirm-safe-writes --confirm-write-to-production-census \
  --confirm-no-brand-explorer-writes --confirm-no-owner-operator \
  --confirm-no-date-writes --confirm-no-recent-momentum \
  --confirm-no-company-validation --confirm-webhound-not-production-source \
  --confirm-approval-bundle-bound --enable-production-writes
```

## Run folder artifacts

- `active-brand-setup-control-list.json`
- `brand-to-census-match-report.{md,json}` — **Matched Active / Live Brand Setup brands to production Hotel Property Census records.**
- `queue-priority-plan.{md,json}`
- `queue-execution-report.{md,json}` — per-queue executed / exhausted / soft-deferred
- `approval-bundle.json` — multi-queue High would-writes (controlled)
- `plan`, `batches/`, `checkpoint.json`, `runtime-metrics.json`
- `apply-summary` (apply), steward / webhound / blocked / summary

## Guardrails

- Write target must be Hotel Property Census (`tbl9aY5ijiuIzzWam`) or apply stops with `blocked_wrong_census_target`
- Geocode without provider → soft-route; other queues continue  
- Exhausted High proposals → continue to next queue  
- Max 2 retries per source; blocked families stop retrying  
- Batch runtime 3× overage → safety stop  
- Webhound candidates only (never production)  
- Full BE gates not run on Hotel Property Census dry-runs  

## Related

- `docs/data-intelligence/production-census-source-of-truth.md`
- `docs/data-intelligence/production-census-cala-discovery-mode.md`
- `docs/data-intelligence/production-census-active-brand-setup-scope.md`
- `docs/data-intelligence/production-census-autopilot-operating-model.md`
- `reports/research-engine-v2/production-census-autopilot-queue-orchestration-fix.md`
