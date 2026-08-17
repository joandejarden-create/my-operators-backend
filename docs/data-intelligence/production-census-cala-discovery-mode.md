# Production Census — CALA Discovery + Insert Mode

**Status:** `production_census_cala_discovery_mode_ready_needs_source_adapter`

## Purpose

Let Autopilot discover missing CALA hotels for Active/Live Brand Setup brands, dedupe against production **Hotel Property Census** (`tbl9aY5ijiuIzzWam`), and create insert approval bundles. Enrichment queues then run against the expanded Census after founder-approved insert apply.

## Source of truth

| Layer | Role |
| --- | --- |
| Brand Setup Active/Live | Read-only control list |
| Hotel Property Census | Only production write / insert target |
| VIC source claims | Evidence / dedupe only — never insert source of truth |
| legacy Census | Blocked |

## Commands

```bash
# Controlled discovery (no Airtable writes)
npm run census:autopilot -- --region CALA --scope active-brand-setup --mode controlled \
  --strategy fastest-safe \
  --queue source_discovery \
  --run-until-complete \
  --batch-size 250

# Parent-company scoped
npm run census:autopilot -- --region CALA --parent-company Hilton --mode controlled \
  --strategy fastest-safe \
  --queue source_discovery \
  --run-until-complete \
  --batch-size 250
```

Insert apply is implemented but **must not be run until founder approval**. Future:

```bash
ALLOW_CENSUS_AUTOPILOT_APPLY=1 \
CONFIRM_WRITE_TO_PRODUCTION_CENSUS=1 \
CONFIRM_NO_BRAND_EXPLORER_WRITES=1 \
CONFIRM_NO_OWNER_OPERATOR_WRITES=1 \
npm run census:autopilot -- --region CALA --scope active-brand-setup --mode apply \
  --strategy fastest-safe --queue source_discovery --run-until-complete --batch-size 100 \
  --approval-bundle reports/research-engine-v2/autopilot/<run-id>/approval-bundle.json \
  --confirm-safe-writes --confirm-write-to-production-census \
  --confirm-no-brand-explorer-writes --confirm-no-owner-operator \
  --confirm-no-date-writes --confirm-no-recent-momentum \
  --confirm-no-company-validation --confirm-webhound-not-production-source \
  --enable-production-writes
```

## Modules

| Module | Role |
| --- | --- |
| `production-census-cala-region-config.js` | CALA countries + adapter readiness plan |
| `census-autopilot-source-discovery.js` | Discover → match → insert approval bundle |
| `census-autopilot-discovery-insert-apply.js` | Approval-bundle-bound insert apply (built; not run here) |

## Adapter readiness (honest)

| Country | Hilton | Choice | Marriott | IHG |
| --- | --- | --- | --- | --- |
| Mexico | Ready (locations directory) | Ready (regional JSON-LD) | Needs listing adapter | Needs listing adapter |
| Other CALA | Needs adapter | Needs adapter | Needs adapter | Needs adapter |

VIC remains evidence-only for all countries.

## Match hierarchy

1. Official property ID / brand code  
2. Exact official URL  
3. Name + brand + city + country  
4. Address (probable / duplicate risk if name conflicts)  
5. Approved soft-brand aliases  

No fuzzy auto-insert. New inserts require **High** identity confidence.

## Insert field rules

Allowed core identity + High address/coords/rooms/descriptions when source-supported.  
Forbidden: owner/operator/developer/dates, Recent Momentum, Company Validated, Brand Verified, Brand Status, Brand Explorer / Brand Setup fields.

Default: `Production Use Status = Census Only / Not Owner-Facing`.

## Run artifacts

Under `reports/research-engine-v2/autopilot/<run-id>/`:

- `source-of-truth-check.json`
- `active-brand-discovery-control-list.json`
- `discovery-source-report.{md,json}`
- `discovered-properties.csv`
- `brand-to-census-match-report.{md,json}`
- `new-property-candidates.json`
- `duplicate-risk.json`
- `steward-review-queue.json`
- `webhound-candidates.json`
- `approval-bundle.json`
- `summary.md`

## Related

- `docs/data-intelligence/production-census-source-of-truth.md`
- `docs/data-intelligence/production-census-autopilot-runner.md`
- `docs/data-intelligence/production-census-autopilot-operating-model.md`
