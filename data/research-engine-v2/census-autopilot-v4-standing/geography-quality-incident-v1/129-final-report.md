# Geography Incident V2 — Final Report

**V4 PAUSED · Incremental dry-run NOT applied**

## Verdicts

| | |
| --- | --- |
| CONSOLIDATED REPAIR | **PASS** |
| MARKET | **NOT READY** (58.5%) |
| SUBMARKET | **PARTIAL** (55.1% applicable) |
| GEOGRAPHY QUALITY | **SAFE WITH BOUNDED UNKNOWNS** |
| V4 | **NEEDS MORE WORK** |

## Consolidated repair
- Pilot A PASS → full safe apply
- Updated **326** · Already **127** · Stale **6** · Blocked **2**
- Expected/actual **100%** · Safety violations **0**

## Market
- Invalid Country/State/City-as-Market remaining: **0 / 0 / 0**
- Deterministic coverage after clears + curated vNext2: **234/400 (58.5%)**
- Coverage vs pre-clear 70.5%: drop expected because UNKNOWN > WRONG
- Curated vNext2 Markets: **12** (no road-name / fake Markets)

## Residual
- Post-repair unresolved freeze: **165**
- SerpApi searches: **19** (insufficient-geography only)

Incremental dry-run: `127-final-incremental-geography-dry-run.json` — **DO NOT APPLY**
