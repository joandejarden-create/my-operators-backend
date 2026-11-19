# HE-INTEL-001 P3B-M1 — First live ownership experiment (no-spend readiness)

**Status:** `AWAITING_INTEGRATION_CONTROLLER` — STOP before paid execution  
**PR:** [#443](https://github.com/joandejarden-create/my-operators-backend/pull/443) — OPEN  
**HEAD:** `c978ea814c3487041cb27915010f17b52ef542d8`  
**Workspace:** `C:\Dev\dealality\ws-hotel-explorer`  
**Branch:** `feat/he-intel-001-p3b-m1-live-experiment-readiness`  
**Baseline:** PR #439 merged at `8a3cd20eb33ee377ba38fafbe10f7c498ba4c7dc` (on `origin/main`)

## What this delivers

Final **no-spend** experiment package for founder approval of the paid run:

| Item | Location |
|---|---|
| Manifest JSON | `reports/hotel-explorer/he-intel-001-research-benchmark/p3b-m1-live-experiment-manifest.json` |
| Readiness JSON | `reports/hotel-explorer/he-intel-001-research-benchmark/p3b-m1-live-experiment-readiness.json` |
| Protocol doc | `docs/ai-build-system/HE-INTEL-001-P3B-M1-LIVE-EXPERIMENT.md` |
| Code | `lib/.../benchmark-p3b-m1/live-experiment-*.js`, `quality-scorecard-v1.js`, `adjudication-protocol-v1.js` |

## Hotels (exact)

1. Bethesda Marriott — `recLuxvwwxID7U2B8`  
2. Wyndham San Diego Bayside — `recnPB3Q02NO8Ma1o`  
3. The Ritz-Carlton, Mexico City — `recvVZ5unh4d3Krzi`  

## Methods

| Method | When authorized |
|---|---|
| A Exa | Execute (discovery + contents, bounded) |
| B Parallel Search | Execute |
| C Parallel Task | Execute (full lifecycle; create ≠ result) |
| D Dealality native | **NOT_EXECUTED_COST_UNKNOWN** (USD unbounded) |
| E Hybrid | **NOT_EXECUTED_LABELED_SIMULATION_ONLY** by default (separate arm/budget if later authorized) |

Mode A independent discovery is primary. Mode B optional only if budget/priors allow without sacrificing Mode A.

## Financial controls

- Hard cap **$5** / per-hotel **$2.50** — **NOT AUTHORIZED**  
- List-price expected provider ~**$0.204** (core) / ~**$0.429** (w/ optional pro) — **not a max-charge guarantee**  
- Durable ledger + claim/lease + PENDING_RECONCILE + unknown-cost fail-closed  
- Surfe/PDL forbidden; staging only; no canonical writes  

## Exact paid command (after separate spend approval)

```bash
npm run run:he-intel-001-p3b-m1-live-experiment -- --founder-auth FOUNDER_AUTH_HE_INTEL_001_P3B_M1_PAID_V1 --confirm-paid-execution --authorized-budget-usd 5 --enable-live-network --mode independent-discovery --founder-spend-authorized
```

This readiness PR still refuses dispatch even with the spend flag (shape validation only).

## Tests

```bash
npm run test:he-intel-001-p3b-m1-live-experiment-v1
npm run dry-run:he-intel-001-p3b-m1-live-experiment
npm run test:he-intel-001-p3b-m1-live-activation-v1
npm run test:he-intel-001-ownership-research-objective-v1
npm run dealality:ci-precheck
```

## Next step for founder

1. Review manifest + this report  
2. Integration merges this PR  
3. Explicitly authorize the **$5 paid experiment** (separate message)  
4. Then execute Mode A comparison and return dossiers / comparison / recommendation  

**STOP BEFORE PAID EXECUTION.**

`AWAITING_INTEGRATION_CONTROLLER`
