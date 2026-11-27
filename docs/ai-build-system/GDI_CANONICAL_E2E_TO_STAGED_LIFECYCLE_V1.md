# GDI Canonical E2E → STAGED Lifecycle V1

**Status:** Permanent (founder decision 2026-11-27)  
**Contract:** `GDI_CANONICAL_E2E_TO_STAGED_LIFECYCLE_V1`  
**Module:** `lib/group-demand-intelligence/publication-package/gdi-canonical-e2e-to-staged-lifecycle-v1.js`

## Sequence

```
FULL E2E → STAGED → FOUNDER APPROVAL → CURRENT
```

Eliminate intermediate founder approval gates between commercial qualification and STAGED.

| Stage | Meaning | Auto? |
|---|---|---|
| FULL E2E | Five-stage research + FQO + hotel-wide reconcile | Authorized run |
| STAGED | Complete deduplicated qualified hotel inventory for local founder review on `:8080` | Automatic after authorized E2E when qualified results exist |
| FOUNDER APPROVAL | Explicit founder decision | Manual |
| CURRENT | Customer-facing publication | Only after founder approval |

## Rules

1. Successfully qualified results from an authorized E2E run progress to STAGED without a separate founder authorization step.
2. Proposed Qualified may remain an internal technical classification — never a founder UI gate or inventory selector.
3. Do not stage HOLD, continuation, incomplete, speculative, or rejected rows as qualified.
4. STAGED must represent the complete qualified inventory (not only newest additions), with opportunity identity and historical package versions preserved.
5. STAGED creation/update must never modify CURRENT.
6. Production customers see CURRENT only (auth + hotel ACL preserved).
7. DryRun validates package construction; Live activates the STAGED pointer. Budget controls remain enforced.
8. New persistence schemas or materially different write mechanisms require separate review — reuse approved publication package store.

## PowerShell

Canonical orchestrator (no competing runner):

```powershell
.\scripts\gdi\Invoke-GdiFullE2E.ps1 -HotelId <rec...> -Mode DryRun
```

Mandatory completion artifacts include `06-staged-progression.json` and STAGED inventory validation. An E2E run is not operationally complete until successfully qualified results are visible in STAGED (Live) or validated for staging (DryRun).

## Local founder review

Authorized founder session on port **8080** displays STAGED through the normal GDI application (Group & Demand Opportunities, List/Tiles, Executive Summary, Research Receipt, drawers, filters, exports, Commercial Priorities when eligible). No Proposed/HOLD selectors, no inventory URL parameters required.

## Gates

- `npm run test:gdi-canonical-e2e-to-staged-lifecycle-v1`
- `npm run test:gdi-cumulative-integrated-staging-non-regression-v1`
- `scripts/validate-gdi-cumulative-integrated-staging-v1.ps1`

## Integration Controller

Accept only when: lifecycle docs present, auto-stage wired into Full E2E, STAGED ≠ CURRENT mutation, additive non-regression retained, and open GDI PRs coordinated via IC merge (product chats do not merge `main`).
