# V4 US HPC Pilot Selection — Criteria & Dry-Run

**Status:** Criteria locked · Dry-run only · No HPC creates · No schema apply  
**Gate:** PR #495 must merge before V4 implementation  

## Priorities (deterministic)

1. Verified physical-property identity (name + city + US state; brand/hotel lexeme)
2. Full address or distinctive geo evidence (from CMBS schedule snippet when present)
3. At least one verified hotel contract milestone (not loan maturity)
4. Future 2027–2040 milestones preferred; historical retained with status caution
5. Brand/alias evidence when available
6. Ownership/management counterparty mentions when available
7. Hotel Explorer enrichment potential (branded + address + milestone)

## Dispositions

| Class | Meaning |
|---|---|
| LINK_EXISTING | Live HPC collision verified match |
| CREATE_READY | Strong evidence + HPC verified absent — still needs founder manifest approval |
| RESEARCH_REQUIRED | Promising but incomplete geo/status/collision |
| REJECT_AMBIGUOUS | Non-hotel, generic, portfolio-wide, or identity collision |

**Do not force CREATE_READY to reach 25–50.**

## This dry-run

| Metric | Value |
|---|---:|
| Findings loaded | 481 |
| US hotels aggregated | 305 |
| LINK_EXISTING | 0 |
| CREATE_READY | 50 |
| RESEARCH_REQUIRED | 87 |
| REJECT_AMBIGUOUS | 8 |
| Pending collision (pre) | 210 |
| Shortlist (non-reject, ≤50) | 50 |
| HPC check run | true |

Shortlist capped at 50; CREATE_READY only after collision + founder manifest approval

## Top 15 shortlist

1. **Fairfield Inn & Suites Lebanon** (Lebanon, TN) · score 78 · CREATE_READY
2. **Home2 Suites - Lithia Springs, GA** (Lithia Springs, GA) · score 78 · CREATE_READY
3. **Residence Inn - Lynchburg** (Lynchburg, VA) · score 78 · CREATE_READY
4. **Springhill Suites - New Smyrna Beach** (New Smyrna Beach, FL) · score 78 · CREATE_READY
5. **SpringHill Suites Newark** (Newark, DE) · score 78 · CREATE_READY
6. **Anaheim Marriott Suites** (Garden Grove, CA) · score 75 · CREATE_READY
7. **AVR Embassy Suites Fort Worth** (Fort Worth, TX) · score 75 · CREATE_READY
8. **Candlewood Suites - Terre Haute IN** (Terre Haute, IN) · score 75 · CREATE_READY
9. **Candlewood Suites Sayre** (Sayre, PA) · score 75 · CREATE_READY
10. **Candlewood Suites Texarkana** (Texarkana, TX) · score 75 · CREATE_READY
11. **Candlewood Suites Watertown** (Evans Mills, NY) · score 75 · CREATE_READY
12. **Comfort Inn & Suites Paris** (Paris, TX) · score 75 · CREATE_READY
13. **Comfort Suites Buda Austin South** (Buda, TX) · score 75 · CREATE_READY
14. **Comfort Suites Gastonia** (Gastonia, NC) · score 75 · CREATE_READY
15. **Comfort Suites Raleigh-Durham Airport** (Durham, NC) · score 75 · CREATE_READY

## Next after #495 merge

1. Re-run with `--hpc-check`
2. Build founder creation manifest from CREATE_READY + LINK_EXISTING
3. Explicit approval per CREATE row before apply
