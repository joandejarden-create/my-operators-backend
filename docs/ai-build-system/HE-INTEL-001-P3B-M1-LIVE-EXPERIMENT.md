# HE-INTEL-001 P3B-M1 — First live ownership research experiment

**Manifest:** `HE_INTEL_001_P3B_M1_LIVE_OWNERSHIP_EXPERIMENT_V1`  
**Paid spend:** Proposed **$5 / $2.50 per hotel** — **NOT AUTHORIZED** until separate founder approval.

## Purpose

Determine which research method (or hybrid) most accurately and economically identifies actual hotel owners, relevant decision-makers, and verified contact pathways for three pilot hotels.

Infrastructure from P1–P3B-M1 (gateway, graph contracts, durable ledger, ownership classifier) is reused. No competing framework.

## Phase 1 — No-spend readiness (this PR)

```bash
npm run test:he-intel-001-p3b-m1-live-experiment-v1
npm run dry-run:he-intel-001-p3b-m1-live-experiment
```

Produces:

- Exact three-hotel research instructions
- Methods A–E plans (native = `NOT_EXECUTED_COST_UNKNOWN`; hybrid = labeled simulation by default)
- Mode A independent discovery protocol (Mode B optional)
- Evidence adjudication procedure
- Quality scorecard (cost separate; numerical pass ≠ `FULL_ACTIONABLE`)
- Financial controls and exact paid command shape

## Phase 2 — Paid execution (separate founder approval)

```bash
npm run run:he-intel-001-p3b-m1-live-experiment -- \
  --founder-auth FOUNDER_AUTH_HE_INTEL_001_P3B_M1_PAID_V1 \
  --confirm-paid-execution \
  --authorized-budget-usd 5 \
  --enable-live-network \
  --mode independent-discovery \
  --founder-spend-authorized
```

Until `--founder-spend-authorized` is given after **explicit** founder spend approval, the entrypoint refuses. Prior PR merges and auth-flag shape alone are not permission to spend.

## Hotels

| Hotel | HPC ID |
|---|---|
| Bethesda Marriott | `recLuxvwwxID7U2B8` |
| Wyndham San Diego Bayside | `recnPB3Q02NO8Ma1o` |
| The Ritz-Carlton, Mexico City | `recvVZ5unh4d3Krzi` |

Staging only — no canonical HI graph / HPC Owner-Operator / Radar Status / customer publication.
