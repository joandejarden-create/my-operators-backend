# Brand Explorer Batch Factory 2.0

**Contract:** `BE_FACTORY_2_0_BATCH_V1`  
**Status:** Staging + portfolio review enabled. Production Presentation writeback remains founder-gated per exact candidate.

## Purpose

Stop per-brand founder interruption during research. Queue all Active/Live Brand Basics brands, research/stage/certify automatically, then review the portfolio together in Admin Resources → **Brand Explorer Batch Review**.

## Hard rules

- Curio (`receQkxgjlezsc1xg`) and Ascend (`reclkgOzvAcBheUSo`) are **SKIPPED_PROTECTED** — no reprocessing.
- Autograph Factory 1.0 candidate is preserved and processed first.
- No generic approval token — each brand requires its exact `candidateHash`.
- Approval records tokens + write-plan fingerprints only; **does not** write production Presentation.
- Paid research budget defaults to `$0` until explicitly raised per brand.
- Missing-brand creation is out of scope until the existing inventory is processed.

## Commands

```bash
npm run be-factory-2-queue-build          # Airtable Active/Live → durable queue
npm run be-factory-2-process-batch -- --limit 5
npm run be-factory-2-process-batch -- --force --brand rec…
npm run be-factory-2-confirm-batch-ids
npm run be-factory-2-named-batch-five     # AC / Moxy / Vignette / Canopy / EVEN depth batch
npm run be-factory-2-dashboard
npm run test:be-factory-2-batch-v1
npm run test:be-factory-2-named-batch-five-v1
```

## Named batch five (2026-11-20)

Brands: AC Hotels, Moxy, Vignette, Canopy, EVEN.  
Identities confirmed against live Brand Basics. Depth candidates staged with exact-six loyalty cards and Commercial Engine gap-fill.  
**Disposition:** `NEEDS_REMEDIATION` until verified fee exhibits are attached (paid budget still $0).  
Autograph remains `READY_FOR_REVIEW`; Curio/Ascend remain protected.  
Review package: `reports/brand-explorer/factory-2-0/13-named-batch-five-consolidated-review.md`.

## Durable storage

- `data/brand-explorer/factory-2-0/queue.json`
- `data/brand-explorer/factory-2-0/brands/<brandId>/stage.json` (+ candidate, certification, write-plan, spend)
- `data/brand-explorer/factory-2-0/approvals/`
- `data/brand-explorer/factory-2-0/portfolio-dashboard.json`
- Mirrored reports under `reports/brand-explorer/factory-2-0/`

## Admin UI

- Route: `/app#/brand-explorer-batch-review`
- API: `/api/brand-explorer-batch-review/portfolio` (+ brand, queue, approve)
- Auth: internal runbook admin

## Status vocabulary

`QUEUED` · `RESEARCH_IN_PROGRESS` · `READY_FOR_REVIEW` · `NEEDS_REMEDIATION` · `APPROVED` · `PUBLISHED` · `SKIPPED_PROTECTED` · `BLOCKED`

CURRENT Presentation baselines are staged as `NEEDS_REMEDIATION` until a Factory-depth candidate closes gaps. Only Autograph (exact candidate + permanent standards) and future Curio-parity candidates reach `READY_FOR_REVIEW`.
