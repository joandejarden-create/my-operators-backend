# HE-INTEL-001 P3B-M1 Live Activation

**Contract:** `HE_INTEL_001_P3B_M1_LIVE_BENCHMARK_V1` · version `1.1.0-m1-live-activation`  
**Phase:** P3B-M1 Live Activation  
**Paid spend:** Proposed **$5** / **$2.50 per hotel** — **NOT AUTHORIZED** for actual expenditure.

## What this phase delivers

1. **Phase A — live execution blockers resolved**
   - Durable shared-budget ledger (file-backed, restart-safe)
   - Multi-worker atomic reserve via file lock + shared run cap
   - Combined provider / model-token / orchestration accounting
   - `PENDING_RECONCILE` for timeouts / ambiguous billing (no auto-release)
   - No duplicate billable execution after interrupt/resume
   - Operation-scoped adapter authorization (not shared mutable boolean)
   - Fail-closed when cost cannot be bounded (`UNKNOWN` → refuse)

2. **Phase B — providers wired**
   - Exa + Parallel Search/Task transports connected behind P1 execution gateway
   - No direct adapter bypass
   - Live network hard-gated (`--enable-live-network` / `HE_INTEL_P3B_M1_LIVE_NETWORK=1` + founder auth)
   - Surfe / PDL forbidden

3. **Phase C/D — experiment framework**
   - Three-hotel request manifest (Bethesda Marriott, Wyndham San Diego Bayside, The Ritz-Carlton, Mexico City)
   - Ownership scoring via `HE_INTEL_001_OWNERSHIP_RESEARCH_OBJECTIVE_V1` (v1.1.0)
   - Comparable method report skeleton + recommendation hook
   - Valid URL alone ≠ ownership verification

## Commands

```bash
npm run test:he-intel-001-p3b-m1-live-activation-v1
npm run dry-run:he-intel-001-p3b-m1-live-activation
# Paid dispatch — still refused while $5 not authorized:
npm run run:he-intel-001-p3b-m1-live -- --founder-auth FOUNDER_AUTH_HE_INTEL_001_P3B_M1_PAID_V1 --confirm-paid-execution --authorized-budget-usd 5 --enable-live-network
```

## Precise authorized execution command (when founder later authorizes spend)

```bash
npm run run:he-intel-001-p3b-m1-live -- --founder-auth FOUNDER_AUTH_HE_INTEL_001_P3B_M1_PAID_V1 --confirm-paid-execution --authorized-budget-usd 5 --enable-live-network
```

Until founder explicitly authorizes the proposed $5, the entrypoint exits with `P3B_M1_PAID_SPEND_NOT_AUTHORIZED_BY_FOUNDER`.

## Write guarantees

- Staging-only benchmark findings
- No canonical HI graph writes (`appCCUsuGsE1ifoLk` production mutate forbidden)
- No HPC Owner/Operator / Radar Status changes
- No customer publication / deployment
