# Phase 1.1 — Local handoff

## Isolation (do not break)

| Checkout | Role |
|----------|------|
| `/agent/repos/my-operators-backend-adp-audit` | **Only** ADP worktree — PR `#364` branch `cursor/adp-hotel-explorer-audit-1ed2` |
| `/agent/repos/my-operators-backend` | Primary GDI — leave untouched |

## Reproduce gates

```bash
cd /agent/repos/my-operators-backend-adp-audit

npm run test:adp-e2e-persistence-contract-v1
npm run test:adp-permanent-full-e2e-certification-v1
npm run test:adp-phase1-radar-he-gates
npm run test:adp-customer-surface-contract

# Layer B hard requirement (EXIT 2 if durable missing)
ADP_PERMANENCE_REQUIRE_LAYER_B=1 npm run test:adp-permanent-full-e2e-certification-v1
```

## Reports (this phase)

All under `reports/adp-hotel-explorer-baseline-audit/phase-1.1/`:

1. `CERTIFICATION_BASELINE_AUTHORITY_RECONCILIATION.md` (+ `.json`)
2. `DURABLE_FIXTURE_ARCHITECTURE.md`
3. `PHASE1_1_LAYER_B_DURABLE_EVIDENCE_PROCESS.md`
4. `E2E_PERSISTENCE_GATE_RESULTS.md`
5. `CUSTOMER_SURFACE_ROOT_CAUSE_MATRIX.md` (+ `.json`)
6. `RADAR_HOTEL_EXPLORER_VALIDATION.md`
7. `PHASE1_1_GATE_MATRIX.md` (+ `.json`)
8. `PHASE2_FULL_E2E_PRIORITIES.md`
9. `FOUNDER_STOP_PHASE1_1.md` (this package summary)

## Browser smoke (when Airtable available)

```bash
PORT=8091 node server.js
# open:
# http://127.0.0.1:8091/deal-capture-radar-with-ranked-list.html?hotelExplorer=recLuxvwwxID7U2B8&internalPreview=1
```

Bethesda = internal preview only.

## Stop conditions

- Do **not** merge or deploy PR `#364` until founder review.
- Do **not** write production Airtable or start paid research.
- Do **not** commit production durable packages into Git.
