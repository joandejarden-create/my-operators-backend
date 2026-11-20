# Factory 2.0 — Founder Batch Dashboard

**Generated:** 2026-11-20  
**Contract:** `BE_FACTORY_2_0_BATCH_V1`  
**Production Presentation writes:** OFF  
**Customer publication:** OFF

## Portfolio totals

| Metric | Count |
|--------|------:|
| Total Active/Live queued | 68 |
| Eligible (excl. protected) | 66 |
| SKIPPED_PROTECTED (Curio/Ascend) | 2 |
| Staged this cycle | 9 |
| READY_FOR_REVIEW | 1 |
| NEEDS_REMEDIATION | 8 |
| RESEARCH_IN_PROGRESS | 0 |
| APPROVED | 0 |
| PUBLISHED | 0 |
| BLOCKED | 0 |
| Research spend (USD) | 0 |
| Expected publication writes (if later approved) | 168 |

## First five brands (priority batch)

| # | Brand | Status | Note |
|---|-------|--------|------|
| 1 | Autograph Collection | READY_FOR_REVIEW | Factory 1.0 exact candidate preserved (`d7510b9b…`) |
| 2 | Radisson Blu by Choice | NEEDS_REMEDIATION | CURRENT baseline staged; awaiting Factory-depth candidate |
| 3 | Comfort Inn & Suites | NEEDS_REMEDIATION | CURRENT baseline staged |
| 4 | Radisson Individuals by Choice | NEEDS_REMEDIATION | CURRENT baseline staged |
| 5 | Country Inn & Suites by Choice | NEEDS_REMEDIATION | CURRENT baseline staged |

## Review surface

- Admin Resources → **Brand Explorer Batch Review**
- URL: `/app#/brand-explorer-batch-review`
- Compare / Proposed links available per row
- Bulk approval binds **per-brand candidate hashes** (no generic token)
- Approval does **not** execute Airtable Presentation overwrite

## Next automated steps (no founder interruption required)

1. Continue `npm run be-factory-2-process-batch -- --limit 5` across remaining eligible inventory.
2. Gap-fill research / Factory candidate builders per brand until Curio-parity gates pass → READY_FOR_REVIEW.
3. Founder portfolio review when a reviewable cohort is READY.
4. Exact-candidate batch approval → authorized writeback path only after per-brand tokens.

## Authorization boundary

This cycle authorized research, staging, certification, dry-run write plans, and Admin review UI.  
It did **not** authorize unreviewed production Presentation overwrite or customer publication.
