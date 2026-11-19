# HE-INTEL-001 P3B-M1 — Live benchmark readiness

**Contract:** `HE_INTEL_001_P3B_M1_LIVE_BENCHMARK_V1`  
**Paid budget:** proposed **$5.00** hard cap / **$2.50** per hotel — **NOT AUTHORIZED**  
**Surfe / PDL:** forbidden  
**Canonical graph writes:** forbidden (staging only)

## Pilot hotels
1. Bethesda Marriott — `recLuxvwwxID7U2B8`  
2. Wyndham San Diego Bayside — `recnPB3Q02NO8Ma1o`  
3. The Ritz-Carlton, Mexico City — `recvVZ5unh4d3Krzi`  

Six-hotel P3A cohort retained for subsequent evaluation.

## What this PR delivers
- Gateway-compatible adapters for Exa, Parallel Search/Task, Dealality native
- Atomic shared-budget ledger (run + per-hotel caps; no concurrent bypass)
- Durable operation IDs (resume without duplicate spend)
- Cross-arm discovery isolation
- New-intel scoring (excludes seed identity facts)
- Provider availability probe without exposing credentials
- Exact planned paid operations + expected costs
- Founder authorization gate: `FOUNDER_AUTH_HE_INTEL_001_P3B_M1_PAID_V1`

## Commands
```bash
npm run test:he-intel-001-p3b-m1-readiness-v1
npm run dry-run:he-intel-001-p3b-m1-readiness
# Paid entrypoint refuses without founder auth:
npm run run:he-intel-001-p3b-m1-live
```

## Proposed paid command (do not run until authorized)
```bash
npm run run:he-intel-001-p3b-m1-live -- --founder-auth FOUNDER_AUTH_HE_INTEL_001_P3B_M1_PAID_V1 --confirm-paid-execution --authorized-budget-usd 5
```

Readiness PR still exits non-zero even if auth shape is present — live transports are not wired here.
