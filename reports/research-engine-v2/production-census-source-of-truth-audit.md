# Production Census Source of Truth — Autopilot Audit

**Status:** `production_census_source_of_truth_locked_ready_for_autopilot`

**Generated:** 2026-08-05T22:42:45.260Z

## Verdict

Autopilot production writes are hard-locked to **Deal Capture Platform → Hotel Property Census (`tbl9aY5ijiuIzzWam`).**
Apply fails closed with `blocked_wrong_census_target` for legacy Census, VIC, Brand Setup, Brand Explorer, staging, or ambiguous "Census" targets.

## Canonical config

```json
{
  "baseName": "Deal Capture Platform",
  "tableName": "Hotel Property Census",
  "tableId": "tbl9aY5ijiuIzzWam",
  "role": "production_property_table",
  "allowedWriteTarget": true,
  "envBaseResolver": "AIRTABLE_BASE_ID_ALT (Deal Capture Platform)"
}
```

## Read / write rules

- Read Brand Setup active control list: yes (write: no)
- Read/write Hotel Property Census allowlisted fields: yes
- Read VIC source claims: yes (write: no)
- Write legacy / VIC / BE / Brand Setup: blocked

## Module audit

| Path | Hotel Property Census | table id | VIC refs | ambiguous match lines |
| --- | ---: | ---: | ---: | ---: |
| `lib/research-engine-v2/census-autopilot-runner.js` | 3 | 0 | 2 | 0 |
| `lib/research-engine-v2/census-autopilot-planner.js` | 0 | 0 | 0 | 0 |
| `lib/research-engine-v2/census-autopilot-queue-router.js` | 0 | 0 | 0 | 0 |
| `lib/research-engine-v2/census-autopilot-queue-orchestrator.js` | 0 | 0 | 1 | 0 |
| `lib/research-engine-v2/census-autopilot-apply-guard.js` | 1 | 1 | 0 | 0 |
| `lib/research-engine-v2/census-autopilot-family-directory-adapters.js` | 0 | 0 | 2 | 0 |
| `lib/research-engine-v2/census-cache-manager.js` | 0 | 0 | 0 | 0 |
| `lib/research-engine-v2/census-processing-gates.js` | 0 | 0 | 0 | 0 |
| `lib/research-engine-v2/census-autopilot-brand-census-matcher.js` | 7 | 0 | 0 | 0 |
| `lib/research-engine-v2/census-autopilot-batch-engine.js` | 0 | 0 | 0 | 0 |
| `lib/research-engine-v2/census-autopilot-field-allowlist.js` | 1 | 0 | 0 | 0 |
| `lib/research-engine-v2/census-autopilot-approval-bundle-apply.js` | 1 | 0 | 0 | 0 |
| `lib/research-engine-v2/census-autopilot-address-asset-preflight-apply.js` | 1 | 0 | 2 | 0 |
| `lib/research-engine-v2/census-autopilot-choice-address-resourcing.js` | 1 | 0 | 5 | 0 |
| `lib/research-engine-v2/census-autopilot-property-name-cleanup-apply.js` | 1 | 0 | 0 | 0 |
| `lib/research-engine-v2/production-census-source-of-truth.js` | 6 | 2 | 9 | 1 |
| `lib/research-engine-v2/production-census-write.js` | 13 | 1 | 33 | 0 |
| `scripts/census-autopilot.mjs` | 3 | 0 | 1 | 0 |
| `docs/data-intelligence/production-census-autopilot-runner.md` | 4 | 2 | 1 | 0 |
| `docs/data-intelligence/production-census-autopilot-operating-model.md` | 4 | 1 | 1 | 0 |
| `docs/data-intelligence/production-census-source-of-truth.md` | 6 | 2 | 6 | 1 |

### Ambiguous / missing notes

- `census-queue-registry.js` — **not present**; routing is `census-autopilot-queue-router.js` + orchestrator.
- VIC references in family adapters / choice resourcing are **read-only claim lineage** (correct).
- Vague "Census" remaining in identifiers (`census-autopilot-*`, run folder names) is naming only; write target is SoT-locked.

## Residual risks

- Module/file names still use census-autopilot-* prefix (acceptable; write target is locked by SoT)
- production-census-write.js can still write supporting tables in non-Autopilot census write path — Autopilot apply paths assert Hotel Property Census only
- Historical run summaries under reports/ may still say vague Census — new runs use precise terminology

## Locks applied

- lib/research-engine-v2/production-census-source-of-truth.js
- apply guard guardProductionCensusWriteTarget + applyPreflight
- batch-engine / memory adapter fail closed
- approval-bundle / address-asset / choice / name-cleanup apply assert SoT
- scripts/census-autopilot.mjs live meta table id check

## Change impact

**High** — Autopilot write-target governance. Rollback: revert `production-census-source-of-truth.js` + apply-guard/batch-engine wiring.
