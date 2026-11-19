# Brand Explorer — Founder Requirements Reconciliation V1

**Status:** RECONCILIATION COMPLETE · product NOT complete · Gate 2 BLOCKED  
**Reports:** `reports/brand-explorer/founder-requirements-reconciliation/`

## Principle

A passing contract test is not success. Success is knowing every founder requirement is captured, tracked, correctly implemented, or **explicitly left open** — and cannot silently disappear.

## Commands

```bash
node scripts/be-founder-requirements-reconciliation-v1.mjs
npm run test:be-founder-requirements-coverage-v1
npm run test:be-founder-architecture-corrections-v1
npm run test:be-permanent-content-standards-v1
```

## Status vocabulary

`NOT_CAPTURED` → `CAPTURED` → `CONTRACT_ENCODED` → `IMPLEMENTED` → `AUTOMATED_TESTED` → `BROWSER_VERIFIED` → `FOUNDER_APPROVED` · plus `BLOCKED`

## Coverage dimensions (independent)

A docs · B contract · C production code · D pipeline data · E regression test · F real app · G founder approval

## Lifecycle conflict

See `07-conflicting-requirements-decision-register.json` — production baseline **6** retained; founder **7** request kept OPEN; no invented stage.
