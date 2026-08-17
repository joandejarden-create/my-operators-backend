# Brand Explorer Complete Build Orchestrator v2

One-command Brand Explorer build/remediation pipeline with **single-brand**, **multi-brand**, and **all-active** queue modes.

## Modes

| Mode | Command |
|------|---------|
| Single brand | `npm run brand-explorer-complete-build -- --brand tribute-portfolio --dry-run --target-quality active-profile` |
| Multi-brand | `npm run brand-explorer-complete-build -- --brands tribute-portfolio,curio-collection,kimpton --dry-run --target-quality active-profile` |
| All active | `npm run brand-explorer-complete-build -- --all-active --dry-run --target-quality active-profile` |

Default is **dry-run / read-only**. No Airtable writes unless `--apply-approved --approve-brand-explorer-complete-build`.

## Flags

| Flag | Purpose |
|------|---------|
| `--brand <slug>` | Single brand (default: `tribute-portfolio`) |
| `--brands a,b,c` | Comma-separated brand slugs |
| `--all-active` | All brands in `ACTIVE_BRAND_AUDIT_TARGETS` |
| `--max-concurrency 1` | Queue concurrency (default **1**) |
| `--max-concurrency 2` | Dry-run only; capped at 2 |
| `--dry-run` | Read-only (default) |
| `--apply-approved` | Run gated apply commands per brand (requires approval flag) |
| `--target-quality active-profile` | Quality gate target |
| `--stop-on-critical` | Halt brand pipeline on critical defects |
| `--continue-through-warnings` | Continue past non-critical halts |

## Per-brand outputs

Each brand is processed independently:

- `reports/brand-explorer-complete-build-<brand-slug>.md`
- `reports/brand-explorer-complete-build-<brand-slug>.json`

Includes: target resolution metadata, required-section status, Final QA, visual QA, governance, blockers, next writer, apply commands, active-profile readiness.

## Brand target resolution (v28C)

`resolveBrandTarget` resolution order:

1. Airtable record ID (`rec…`)
2. `ACTIVE_BRAND_AUDIT_TARGETS` (six active brands)
3. Exact Brand Name match (live Brand Setup - Brand Basics)
4. Normalized slug match (live Brand Basics)
5. v28B expansion backlog `proposedSlug` match
6. Live Brand Basics fallback (ambiguous → error with candidates)

Expansion Wave 1 slugs resolve via `expansion_backlog` when using v28B `proposedSlug` inputs (complete-build orchestrator v28C).

Per-brand reports include:

- `inputTarget`
- `resolvedBrandName`
- `resolvedRecordId`
- `resolvedSlug`
- `resolutionSource` (`active_registry` | `expansion_backlog` | `live_brand_basics` | `record_id` | `exact_name`)

## Batch outputs

When more than one brand is queued:

- `reports/brand-explorer-complete-build-batch.md`
- `reports/brand-explorer-complete-build-batch.json`

Aggregate buckets:

- brands ready / almost ready / blocked
- missing source evidence
- needing fact approval
- needing UI/copy repair
- carryover risk
- safe for apply-approved
- not safe for apply

Legacy primary report remains:

- `reports/brand-explorer-complete-build-orchestrator.{md,json}`

## Apply-approved safety

Apply is **skipped per brand** when any guardrail fails:

- Company Validated would change
- Critical Final QA defects remain
- Wrong-brand copy/carryover detected
- Pending / internal / FDD facts would surface externally
- Required sections not ready
- Required images missing
- Source governance insufficient
- Human / founder / legal review needed

Apply mode forces `--max-concurrency 1`. Founder/legal gated writers are never auto-applied.

## Rate limits

Brands run through a queue. Default concurrency is **1** with a short inter-brand delay. Use `--max-concurrency 2` only for dry-run batches.

## Active brand targets

From `ACTIVE_BRAND_AUDIT_TARGETS`:

- tribute-portfolio
- curio-collection
- kimpton
- radisson-blu
- radisson
- ascend
