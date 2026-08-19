# Scenario Benchmark Certification Expansion Audit V2

> **Result:** `BRAND_AI_SCENARIO_CERTIFICATION_EXPANSION_AUDIT_PASS`  
> **Next:** `WAIT_FOR_NEXT_BRAND_LONGITUDINAL_WAVE`  
> **Report:** `reports/ai-visibility/scenario-benchmark-certification-expansion-audit-v2.json`  
> **Gates:** `npm run test:scenario-benchmark-certification-expansion-audit-v2` · `npm run scenario-benchmark-certification-expansion-audit:run`

Read-only ranking of the 42 non-certified CORE-first scenario rows. Frozen customer values remain Autograph 103, Tapestry 103, Ascend 67. No UI changes. No provider calls.

## Frozen baseline

| Row | Index | Diff |
|-----|-------|------|
| Autograph soft-brand | 103 | 0 |
| Tapestry soft-brand | 103 | 0 |
| Ascend soft-collection | 67 | 0 |

## Path to 6 then 8

Target 6 = current 3 + Curio / Tribute / Vignette soft-brand after the next Brand longitudinal period (provider-conflict gate; 4-provider soft-brand prompts already in `BRAND_LONGITUDINAL_COHORT_V1`).

Target 8 adds Ascend + Vignette owner-flexibility on the same period (`p_cala_affiliation_flexibility_v1`).

Special incremental cost: **$0**. Do not union period-1 responses into frozen soft-brand grains.

## Not in this phase

- Lifestyle numeric index (split scenario first)
- Branded residences (REDESIGN_REQUIRED)
- Distribution / loyalty (prompt design)
- Independent-conversion OpenAI-only coverage wave (rejected; period-1 responses already stored)
