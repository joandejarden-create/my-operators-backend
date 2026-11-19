# Brand Explorer Phase 2F — Founder review package

**STOP for founder review.** Curio and Ascend are **not** founder-approved.

## Local URL

```text
http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1
```

Admin Resources → **Brand Explorer Comparison** (`/app#/brand-explorer-compare`, admin/internal).

## What changed vs Phase 2E

1. Content & Presentation Contract (`configs/brand-explorer-content-presentation-contract-v1.json`)
2. Visual quality gates that **fail** the prior “99%” candidates on founder screenshot classes
3. Presentation repair (titles, economics differentiation, scenario imagery from official evidence)
4. Comparison Studio: Section-locked / Changes only / Full profile + quality filters + title-matched collections
5. Regression fixtures/tests for founder failure classes

## Scorecard (automated visual gates — not founder approval)

See `reports/brand-explorer/phase2f-visual-quality/00-before-after-scorecard.json`.

| Brand | Before visual pass | After visual pass | Status |
|---|---:|---:|---|
| Curio | ~0.71 | 1.00 automated visual (still **not** founder-approved) | PENDING_FOUNDER_REVIEW |
| Ascend | ~0.76 | ~0.96 — **blocked** on missing distinct property imagery | NOT_FOUNDER_APPROVED |

Failure registers: `07-*-failure-register.json`

**Open Ascend blocker:** official page capture did not yield distinct non-logo property photos. Galleries correctly fail rather than repeating social stubs / logos.

## Safety

Airtable writes: **0** · Paid research: **0** · Publication: **none** · Mass population: **none**

PR #447 / #451 preserved as historical context; Phase 2F ships on `feat/be-phase2f-visual-quality-v1`.
