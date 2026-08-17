# DEALALITY_DISCOVERY_FACTORY_V1_COMPLETE

**Generated:** 2026-08-10T10:48:57.737Z  
**Factory version:** `discovery-factory-v1`  
**Airtable writes:** **0** (locked)

## Safety

```
ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES=0
ENABLE_HBX_CENSUS_WRITES=0
```

- No production census writes
- No Brand Explorer writes
- No automatic merges
- No schema changes
- Discoveries staged locally under `data/hotel-intelligence/discovery-factory/`

## Discovery confidence model

| Tier | Identity confidence | Stage status | Manual review |
| --- | --- | --- | --- |
| **A** | ≥ 0.90 + strong name + city conf ≥ 0.85 + no multi-city + match=new | `READY_FOR_IMPORT` | No |
| **B** | 0.70–0.89 (or soft risks) | `REVIEW_REQUIRED` | Yes |
| **C** | < 0.70, ambiguous, duplicate, missing city/country | `REJECTED` / `MATCHED_EXISTING` | No (drop or already known) |

City inference upgrades: URL slug decode, accent alias canonicalization (São Paulo, San José, Ciudad de Panamá, …), name↔URL corroboration boost, multi-city conflict demotion.

## Country dashboard

Persistent: `data/hotel-intelligence/discovery-factory/country-dashboard.json`

| Rank | Country | Census | Est. universe | Coverage % | Candidates | Ready | Review | Rejected | Dup % | Priority |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | Brazil | 494 | 5336 | 9.3% | 4842 | 91 | 156 | 0 | 1.2 | 84.6 |
| 2 | Argentina | 129 | 917 | 14.1% | 788 | 0 | 0 | 0 | — | 48.7 |
| 3 | Mexico | 2181 | 3614 | 60.3% | 1433 | 0 | 0 | 0 | — | 45.2 |
| 4 | Peru | 57 | 360 | 15.8% | 303 | 0 | 0 | 0 | — | 42.9 |
| 5 | Turks and Caicos | 0 | 57 | 0% | 57 | 0 | 0 | 0 | — | 41.9 |
| 6 | Chile | 65 | 376 | 17.3% | 311 | 0 | 0 | 0 | — | 41.8 |
| 7 | Martinique | 0 | 64 | 0% | 64 | 0 | 0 | 0 | — | 41.2 |
| 8 | Bonaire | 0 | 51 | 0% | 51 | 0 | 0 | 0 | — | 41.1 |
| 9 | U.S. Virgin Islands | 0 | 51 | 0% | 51 | 0 | 0 | 0 | — | 41.1 |
| 10 | Montserrat | 0 | 50 | 0% | 50 | 0 | 0 | 0 | — | 41.1 |
| 11 | Guadeloupe | 0 | 32 | 0% | 32 | 0 | 0 | 0 | — | 40.9 |
| 12 | Anguilla | 0 | 25 | 0% | 25 | 0 | 0 | 0 | — | 40.9 |
| 13 | Saint Vincent and the Grenadines | 0 | 25 | 0% | 25 | 0 | 0 | 0 | — | 40.9 |
| 14 | Saint Barthélemy | 0 | 108 | 0% | 108 | 0 | 0 | 0 | — | 40.8 |
| 15 | Haiti | 0 | 19 | 0% | 19 | 0 | 0 | 0 | — | 40.8 |

## Updated discovery queue

| Rank | Country | Priority score | Gap | Why |
| ---: | --- | ---: | ---: | --- |
| 1 | Brazil | 84.6 | 4842 | gap≈4842; coverage 9.3%; strategic 1; sources 1; HBX unavailable |
| 2 | Argentina | 48.7 | 788 | gap≈788; coverage 14.1%; strategic 0.85; sources 1; HBX unavailable |
| 3 | Mexico | 45.2 | 1433 | gap≈1433; coverage 60.3%; strategic 0.95; sources 1 |
| 4 | Peru | 42.9 | 303 | gap≈303; coverage 15.8%; strategic 0.75; sources 1; HBX unavailable |
| 5 | Turks and Caicos | 41.9 | 57 | gap≈57; coverage 0%; strategic 0.55; sources 1; HBX unavailable |
| 6 | Chile | 41.8 | 311 | gap≈311; coverage 17.3%; strategic 0.7; sources 1; HBX unavailable |
| 7 | Martinique | 41.2 | 64 | gap≈64; coverage 0%; strategic 0.5; sources 1; HBX unavailable |
| 8 | Bonaire | 41.1 | 51 | gap≈51; coverage 0%; strategic 0.5; sources 1; HBX unavailable |
| 9 | U.S. Virgin Islands | 41.1 | 51 | gap≈51; coverage 0%; strategic 0.5; sources 1; HBX unavailable |
| 10 | Montserrat | 41.1 | 50 | gap≈50; coverage 0%; strategic 0.5; sources 1; HBX unavailable |
| 11 | Guadeloupe | 40.9 | 32 | gap≈32; coverage 0%; strategic 0.5; sources 1; HBX unavailable |
| 12 | Anguilla | 40.9 | 25 | gap≈25; coverage 0%; strategic 0.5; sources 1; HBX unavailable |

## Brazil validation

Country run: **Brazil** · Batch `factory_brazil_250_2026-08-10T10-44-50-064Z`

| Metric | Value |
| --- | ---: |
| Candidates processed | 250 |
| READY_FOR_IMPORT (Tier A) | 91 |
| REVIEW_REQUIRED (Tier B) | 156 |
| Rejected (Tier C) | 0 |
| Matched existing | 3 |
| Duplicate rate % | 1.2 |
| Avg identity confidence | 0.907 |
| Avg processing ms | 990 |
| Tier A % | 36.4 |
| Review burden % | 62.4 |

### Sample QA

- Verdict: **APPROVE_EXPANSION_SCALING**
- Quality pass: **true**
- Sample size: 25
- City present: 100%
- Canonical dhl_id: 100%
- Confidence ≥ 0.70: 100%

Sample rows:
- grand hyatt sao paulo · São Paulo · READY_FOR_IMPORT · conf 0.99 · dhl_06FYPHMBMWBRMCR55PJQ98S8CH
- brasil tropical hotel residence · Fortaleza · READY_FOR_IMPORT · conf 0.968 · dhl_06FYPHWQGGQGAP2BWR0C1GYAQM
- hotel araguaia goiania · Goiânia · READY_FOR_IMPORT · conf 0.99 · dhl_06FYPJ1WHRBJB4TYFDEQS2HYA7
- ritz suites hotel · Maceió · READY_FOR_IMPORT · conf 0.968 · dhl_06FYPJ81GCS71AW2WA1R6KWYBH
- hotel mocambique · Florianópolis · READY_FOR_IMPORT · conf 0.968 · dhl_06FYPJCGM4NJCNMJ2YQVKS6ZFW
- real palace hotel · Rio de Janeiro · READY_FOR_IMPORT · conf 0.968 · dhl_06FYPJDNY0G8KZFS81PCWFE5HX
- hotel gran corona · São Paulo · READY_FOR_IMPORT · conf 0.968 · dhl_06FYPJEMY43F6Y4P35KVQ7Z1WP
- tropical barra hotel · Rio de Janeiro · READY_FOR_IMPORT · conf 0.968 · dhl_06FYPJFDAWBTD8ZGM224P48H7H

## Batch recommendations

Do **not** auto-import.

1. **Brazil +500** — factory batch, validate Tier A ≥15% and dup rate <5%
2. **Brazil +1,000** — validate again
3. **Brazil remaining holds** — checkpoint dashboard
4. Auto-select next country by priority score (currently **Argentina**) — still no import
5. Import gate separate: only `READY_FOR_IMPORT` with explicit ENABLE flag + founder approval

## Expected hotel growth

| | Value |
| --- | ---: |
| Production now | 5956 |
| If import Tier A from this batch | 6047 |
| Milestone 10k remaining | 4044 (59.6%) |
| Milestone 12.5k remaining | 6544 (47.6%) |
| Milestone 15k remaining | 9044 (39.7%) |

## Estimated review reduction

- Prior Brazil-250 baseline review burden: **98.8%**
- Factory review burden this batch: **62.4%**
- Reduction: **36.4 percentage points**

## Progress toward 10,000 / 12,500 / 15,000

Production unchanged until explicit import approval. Tier A staging creates the safe import queue.

## Highest-value next country

**Argentina** (priority 48.7) — gap≈788; coverage 14.1%; strategic 0.85; sources 1; HBX unavailable

After Brazil scaling ladder completes, re-rank dashboard and take the then-#1 country automatically (still no auto-import).
