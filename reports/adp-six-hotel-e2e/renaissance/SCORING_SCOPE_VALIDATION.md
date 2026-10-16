# Scoring & customer language validation — `…2ff16a`

## Aggregation contract

- **320** provider observations (80 scenarios × 4 providers)  
- **80** scenario outcomes (present if ≥1 comparable provider mentions subject)  
- Scenario presence **45% = 36/80**  
- Consideration **26.9% = 86/320**  
- Family intent **0/6** in tested sample — finding in sample, **not** universal invisibility  

## Entity matching

Aliases retain Renaissance Times Square variants; exclusions include Midtown/Chelsea/Harlem and Westin Times Square (Broadway Ballroom property).

Branded vs unprompted: measurement uses existing ADP contract; labels show actual provider/model metadata from execution.

## Scope caveat (65 → 80)

Prior edition used **65** scenarios; current uses **80**.  
**Do not claim improvement from 40% → 45%** without matched-scenario analysis.  
Trends UI now shows **Not comparable scope** across historical vs current when `comparableScopeNote` is present.

## Actions reconciliation

| Surface | Actions |
|---------|---------|
| Current report | 2 — walking distance to Broadway; Marquis displacement (28 absent scenarios) |
| Monthly review | Expanded 5-action management agenda (pet policy, source audit, etc.) |

They differ in detail/agenda breadth but do not contradict: both center Marquis displacement / inclusion inconsistency; monthly adds operational source-hygiene steps. Customer-entered progress is not overwritten by regeneration (no action-store reset performed).

## Dry-run prior bug (fixed)

Runtime stub `…fdc3db` (`DRY_RUN_COMPLETE`, 0 raw responses) was incorrectly selected as prior → **+45 pts** false delta.  
Shared guard: `isNonComparableDevelopmentPeriod` in longitudinal comparability.
