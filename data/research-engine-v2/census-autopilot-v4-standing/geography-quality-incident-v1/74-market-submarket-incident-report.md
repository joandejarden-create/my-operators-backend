# Market / Submarket Taxonomy Incident

**V4: PAUSED** · **Corrections: DRY-RUN ONLY — not applied**

## Root cause

`resolveDealalityMarket` used `COUNTRY_DEFAULT_MARKET[country] || country`, writing Country into Market for most of the 400-key cohort. Secondary `market || country` fallbacks existed in canonical geography / expansion proposals — **removed**.

## Counts (400)

| Class | n |
| --- | ---: |
| VALID_MARKET | 85 |
| COUNTRY_AS_MARKET | 205 |
| STATE_AS_MARKET | 36 |
| CITY_AS_MARKET | 66 |
| Blank Submarkets | 244 |
| Safe Market corrections (dry-run) | 22 |
| Safe Submarket corrections (dry-run) | 12 |
| N/A classifications | 9 |
| Steward | 183 |

## Q54–87

See `74-market-submarket-answers.json`.

## Gates

- Country cannot auto-populate Market: **NO** (except explicit single-market island allowlist)
- Submarket before valid Market: **NO**
- Forced Submarket for completeness: **NO**
- STR / Cvent / legacy geography: **NO**

## Artifacts 64–73

Produced under geography-quality-incident-v1/.
