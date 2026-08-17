# Production Census Autopilot — Queue Orchestration Fix

**Status:** `production_census_autopilot_queue_orchestration_fixed_ready_for_multi_queue_controlled`  
**Date:** 2026-08-05  
**Latest multi-queue controlled run:** `reports/research-engine-v2/autopilot/2026-08-05_21-13-16-CALA-active-brands`  
**Targeted description run:** `reports/research-engine-v2/autopilot/2026-08-05_21-13-30-CALA-active-brands`

## 1. What was wrong

Controlled mode with **no `--queue`** only loaded the **rooms_keys** live dry-run in `scripts/census-autopilot.mjs` (`loadLiveAutopilotContext`).

The fastest-safe **priority plan** listed description → amenities → radar → address → name → property type → rooms → geocode, but the CLI never executed those queues unless `--queue` was passed. Exhausted rooms High proposals (0 after the 5 avid applies) made Autopilot look idle even though other queues were eligible.

## 2. What code changed

| Area | Change |
| --- | --- |
| `lib/research-engine-v2/census-autopilot-queue-orchestrator.js` | **New** multi-queue orchestrator: resolve queues, run adapters in fastest-safe order, soft-defer geocode, build multi-queue approval bundle + queue-execution report |
| `scripts/census-autopilot.mjs` | Default path calls orchestrator; `--queue` targets one queue |
| `lib/research-engine-v2/census-autopilot-runner.js` | Removed rooms-only live fallback; writes multi-queue `approval-bundle.json` + `queue-execution-report.*` |
| `lib/research-engine-v2/census-autopilot-approval-bundle-apply.js` | `loadMultiQueueApprovalBundleProposals` for apply consumption |
| `lib/research-engine-v2/production-census-population-lane-2.js` | Expose full `proposals` (with patch/sources) for orchestration |
| `scripts/test-census-autopilot.mjs` | Orchestrator + multi-queue bundle loader tests |

## 3. Default no-queue behavior

```bash
npm run census:autopilot -- --region CALA --scope active-brand-setup --mode controlled \
  --strategy fastest-safe --run-until-complete --batch-size 250
```

1. Build active-brand-setup scope + Census match
2. Score queues with fastest-safe
3. Execute all eligible queues in scored order
4. Skip/exhaust queues with 0 High proposals (continue)
5. Soft-defer `coordinate_resolution` without provider/terms
6. One multi-queue `approval-bundle.json`
7. **No Airtable writes** in controlled mode

Optional: `AUTOPILOT_DESC_FETCH_LIMIT` caps description page fetches (default 80).

## 4. Targeted `--queue` behavior

```bash
npm run census:autopilot -- ... --queue description_extraction ...
```

Runs **only** that queue. Smoke confirmed: `queues_executed: ["description_extraction"]` only.

## 5–7. Smoke test queues (run `2026-08-05_21-13-16`)

| Queue | Status | High |
| --- | --- | ---: |
| description_extraction | executed_exhausted | 0 |
| amenities_extraction | executed_exhausted | 0 |
| radar_public_readiness | executed_exhausted | 0 |
| address_confirmation | executed_exhausted | 0 |
| property_name_cleanup | executed_exhausted | 0 |
| property_type_asset_context | **executed** | **1** |
| rooms_keys | executed_exhausted | 0 |
| coordinate_resolution | **soft_deferred** | 0 |

- **Skipped:** none
- **Soft-deferred:** `coordinate_resolution` (geocode provider/storage terms)
- Description ran **first**; rooms was not the default sole queue

## 8. Approval bundle summary

- Run: `2026-08-05_21-13-16-CALA-active-brands`
- Records proposed: **1** (`Asset Context` = Urban on avid hotels Queretaro Centro Sur)
- Queues executed: 7; soft-deferred: geocode
- `airtable_writes: false`
- Apply can load via `loadMultiQueueApprovalBundleProposals`

## 9. Runtime metrics

- Orchestrator runtime ≈ **193s** (with `AUTOPILOT_DESC_FETCH_LIMIT=3` for smoke)
- Active brands: 62; Census: 666; schema v1.1.4 ready
- Airtable writes: **false**

## 10. Validation results

- `npm run test:census-autopilot` — **PASS**
- `npm run dealality:batch-learning-audit` — **PASS** (`dealality_batch_learning_system_ready`)
- Brand Setup / Brand Explorer: untouched
- Owner/operator/date / Company Validated / Brand Verified / Recent Momentum: blocked

## 11. Recommended next command

Review the multi-queue approval bundle, then (when founder approves) apply **bundle-bound** only:

```bash
npm run census:autopilot -- --region CALA --scope active-brand-setup --mode apply \
  --strategy fastest-safe --run-until-complete --batch-size 250 \
  --approval-bundle reports/research-engine-v2/autopilot/2026-08-05_21-13-16-CALA-active-brands/approval-bundle.json \
  --confirm-safe-writes --confirm-write-to-production-census \
  --confirm-no-brand-explorer-writes --confirm-no-owner-operator \
  --confirm-no-date-writes --confirm-no-recent-momentum \
  --confirm-no-company-validation --confirm-webhound-not-production-source \
  --confirm-approval-bundle-bound --enable-production-writes
```

Or re-run controlled with a higher description fetch budget:

```bash
npm run census:autopilot -- --region CALA --scope active-brand-setup --mode controlled \
  --strategy fastest-safe --run-until-complete --batch-size 250
```

**Note:** Description High proposals are currently blocked by `official_page_blocked` on sampled Hilton pages and `already_enriched` / fetch deferrals — not by queue orchestration.

---

### Change impact

- **Classification:** Medium (read-path / orchestration; controlled writes remain off)
- **Rollback:** Revert orchestrator + CLI `loadLiveAutopilotContext` to rooms-only branch
- **Regression:** Retest controlled no-queue + `--queue description_extraction`; confirm no Census writes

### Data contract snapshot

- **Tables:** Hotel Property Census (read for dry-run; no write in controlled)
- **Mapping:** `QUEUE_FIELD_SETS` in orchestrator + existing queue maps
- **UI/output:** `approval-bundle.json`, `queue-execution-report.{md,json}`, `dry-run.json`
