# Phase 2 — Full E2E priorities & remaining risks

**Do not start Phase 2 work in this PR without founder authorization.**

## Priorities

1. **Materialize Layer B durable evidence** on a governed machine (Railway volume / measured-dates export) and run read-only reconciliation.
2. **BPP measurement cycles** for `MEASUREMENT_NOT_ATTEMPTED` / freeze peer sets (`PEER_SET_NOT_FROZEN`) — no fake READY.
3. **Evidence pack self-containment** — restore runtime periods or bake present/missing packs so EMPTY_EVIDENCE clears without invention.
4. **Bethesda territory rates** — governed re-cert or pin to certified period; never silent constant flip.
5. **Inventory CSV completion** for the 7 published hotels missing classification rows.
6. **Locate / archive original certification freeze** if it exists outside this tree; otherwise formally retire reconstruction as inventory-only.
7. **Browser HE smoke with live census** once Airtable credentials are available in the ADP worktree env.
8. **Paid hotel remeasurement / new publication** — founder-gated only (explicitly out of Phase 1.1).

## Risks

| Risk | Why it matters |
|------|----------------|
| Treating reconstructed baseline as cert authority | Would falsely unlock comparability / customer claims |
| Committing durable production packages to Git | Secrets / leakage / false “CI green = production proven” |
| Weakening evidence EMPTY asserts | Customer surfaces show unsupported claims |
| Grandfathering Bethesda rates | Breaks historical certification contract |
| Running HE smoke without census | False confidence that Radar HE “works” end-to-end |
| Parallel GDI checkout interference | Contaminates ADP isolation |
