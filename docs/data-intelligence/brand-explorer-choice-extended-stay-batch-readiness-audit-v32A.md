# Brand Explorer Choice Extended-Stay Batch Readiness Audit v32A

Read-only batch planning pass for **WoodSpring Suites**, **Everhome Suites**, and **Suburban Studios** under Choice Hotels. No activation, no Airtable writes, no image approval/materialization.

## Command

```bash
npm run brand-explorer-choice-extended-stay-batch-readiness-audit -- \
  --brands woodspring-suites,everhome-suites,suburban-studios --dry-run
```

Optional flags: `--include-final-qa`, `--include-complete-build` (embeds slower per-brand pipeline in audit).

## Factory lessons enforced

1. Slug + record ID must resolve to the same v31N expansion scoring path.
2. Durable Source Page URL only — no temporary Airtable attachment URLs.
3. Recent Momentum uses event-supporting press/trade sources; property listings belong in Openings.
4. Brand Asset Registry must reach Tribute-level completeness before materialization.
5. Company Validated untouched; activation one brand at a time.

## Batch feasibility ranking (2026-07-13 dry-run)

| Rank | Brand | Record ID | Contract | Feasibility | Fastest path |
|------|-------|-----------|----------|-------------|--------------|
| 1 | Everhome Suites | `recqkkrsevi4r9ibj` | 88 | needs_manual_review | Source + registry + internal-language cleanup; momentum/openings largely present |
| 2 | WoodSpring Suites | `recsOd51NzRPYsMko` | 63 | needs_manual_review | v32B source capture first; zero openings/momentum/registry |
| 3 | Suburban Studios | `reclcjg5Foa9Vs5TC` | 63 | needs_manual_review | v32B source capture + v31A partial backfill continuation |

All three: **v31N resolver path aligned** (slug and record ID → `expansion_backlog`).

## Recommended repair sequence

1. **v32B** — Choice extended-stay source capture writer (multi-brand)
2. **v32C** — Brand Asset Registry normalization writer (per brand)
3. **v32D** — presentation backfill writer (per brand)
4. **v32E** — openings/momentum rebuild writer (per brand)
5. **v32F** — approved asset materialization writer (per brand)
6. **v32G** — final QA + complete-build per brand (activation one at a time)

## Per-brand pipeline verification

```bash
npm run brand-explorer-final-qa-auditor -- --brand <slug> --dry-run
npm run brand-explorer-complete-build -- --brand <slug> --dry-run --target-quality active-profile
```

| Brand | Final QA | Complete Build |
|-------|----------|----------------|
| woodspring-suites | blocked (46) | blocked |
| everhome-suites | blocked (51) | blocked |
| suburban-studios | not_ready (52) | not ready |

## Report

`reports/brand-explorer-choice-extended-stay-batch-readiness-audit.json`
