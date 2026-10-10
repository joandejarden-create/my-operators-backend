# Downstream Guard QA — ADP / GDI (Phase 2)

## ADP

| Check | Result |
|-------|--------|
| Module | `applyCventDiscoveryOnlyAdpGuard` in `build-adp-hotel-attributes.js` |
| Cvent venue sourceUrl → usedInAdp | **false** |
| Historical rows deleted | **0** |
| Status stamp | `NEEDS_SOURCE_REVIEW` + `sourceRole=DISCOVERY_ONLY` |
| Verified missingCritical treats Cvent-only as missing | **yes** |

## GDI

| Check | Result |
|-------|--------|
| Module | `isHiValueScoringEligible` in `hi-enriched-hotel-profile-v1.js` |
| Cvent venue HI overlay → verified rooms/meeting fit | **excluded (null)** |
| Config capability (first-party hotel JSON) | still used when HI blocked |
| Cvent event-platform scoring | **still allowed** (`canUseForScoring` ok for event fields) |
| Automatic corpus re-score | **not run** |

## Cross-checks

- `canDisplayToCustomer(cvent venue)` = false
- `canUseForScoring(cvent venue hotel fact)` = false
- `canUseForScoring(cvent event registration)` = true
- Bethesda / share tokens / ADP baselines: **untouched**
