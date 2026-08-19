# Benchmark Cohort Integrity Audit V1

> **Audit version:** `benchmark_cohort_integrity_audit_v1`  
> **Scope:** INTERNAL inspect-only. No engine, API, UI, or aggregation-method change.  
> **Module:** `lib/ai-visibility/competitive-moat/benchmark-cohort-integrity-audit.js`

## Why this exists

`BRAND_AI_PRESENCE_INDEX_PILOT_PASS` certified sample size and redaction. It did **not** certify that each subject is compared against the right commercial competitors.

**Principle:** benchmark relevance first; sample size second.

## What the audit inspects

- Exact included peers and Presence values used
- Excluded plausible peers and why
- Autograph and Curio deep dives
- Symmetric comparability
- Core expected competitor coverage
- Common-cohort filter funnel
- Effect of the 7 internal additions

## Current Presence denominator (inspected, not changed)

Pilot Presence = unique grains where the brand is mentioned ÷ **UNION** of grains from the subject and all cohort peers.

That is not an intersection common-cohort. Widely mentioned brands approach 1.0; sparse internal collections sit near 0.09–0.15 and pull the median down.

## Commands

```bash
npm run benchmark-cohort-integrity:audit
npm run test:benchmark-cohort-integrity-audit
```

## Outputs

- `reports/ai-visibility/benchmark-cohort-integrity-audit-v1.json`
- `reports/ai-visibility/benchmark-cohort-integrity-audit-v1.md` (founder table)

## Readiness

AI Presence Index remains **READY_FOR_INTERNAL_REVIEW**. Do not customer-launch until cohort remediation. Do not run mean-vs-median calibration until competitors are certified.
