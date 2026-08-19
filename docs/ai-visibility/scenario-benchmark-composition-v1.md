# Scenario Benchmark Composition Remediation V1

> **Result:** `BRAND_AI_SCENARIO_BENCHMARK_REMEDIATION_PASS`  
> **Next:** `BRAND_SCENARIO_INDEX_FINAL_CERTIFICATION`  
> **Policy:** `CORE_BENCHMARK_PLUS_SECONDARY_CONTEXT`  
> **Customer index rendering:** OFF  
> **Report:** `reports/ai-visibility/scenario-benchmark-composition-v1.json`  
> **Gates:** `npm run test:scenario-benchmark-composition-v1` · `npm run scenario-benchmark-composition:run`

CORE commercial peers define the competitive benchmark. SECONDARY peers are additional observed context and do **not** enter the denominator. No weights. No headline index. No UI activation.

## Policy

| Policy | Role |
|--------|------|
| CORE_ONLY | Diagnostic + production denominator |
| CORE_PLUS_SECONDARY | Diagnostic only (previous mixed median) |
| CORE_BENCHMARK_PLUS_SECONDARY_CONTEXT | **Recommended** — same index as CORE_ONLY, SECONDARY listed as context |

## Lifestyle peer remediation

| Brand | Decision |
|-------|----------|
| Design Hotels | KEEP_SECONDARY |
| Radisson RED | KEEP_SECONDARY |
| Preferred Hotels & Resorts | MOVE_CONDITIONAL |
| Even Hotels | NON_COMPARABLE |

## Branded residences

`REDESIGN_REQUIRED` — all current residences scenario indices SUPPRESSED. Do not salvage the mixed-architecture 700-class candidates.

## Existing tabs (contract only)

Reuse **Competitive / Peer Analysis** and **Questions Missing Watchlist**. No new tab. No numeric index in the customer UI until final certification. Full peer matrices remain INTERNAL_ONLY.

## Not in this phase

- Coverage wave ($4.74) — not run
- Headline / mean vs median certification
- Customer field activation on live `/api` benchmark reads
