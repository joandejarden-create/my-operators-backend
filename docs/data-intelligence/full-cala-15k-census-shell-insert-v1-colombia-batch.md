# Full CALA 15K Census Shell Insert — Colombia Batch

**Status:** `production_census_full_cala_15k_shell_insert_v1_colombia_batch_apply_complete`

## Colombia batch
| | |
| --- | ---: |
| Census before | **3757** |
| Inserted | **500** (cap) |
| Census after (est.) | **4257** |
| Colombia eligible before | **1149** |
| Colombia remaining eligible | **649** |
| Errors | **0** |

## Source mix (this batch)
| | |
| --- | ---: |
| HBX-only shells | **383** |
| Cvent + HBX shells | **117** |
| Cvent-only shells | **0** (HBX-backed preferred first within country) |
| HBX Hotel Codes on shells | **500** |
| Candidate Brand writes | **161** |
| Current Brand writes | **0** |
| Brand Family writes | **0** |

## Dedupe
- existing_match_high skipped (universe): **3047**
- HBX Hotel Code field index hits: **1408**
- In-plan name+country skips: **283**

## Shell contract
Census Only / Not Owner-Facing · Hold · HR Required · Discovered — pending enrichment · smart Canonical casing · Discovery/Candidate provenance on insert · no Current Brand / Brand Family / Family·Source Family from unvalidated data

## Cumulative country batches (do not rollback)
| Country | Inserts | Remaining |
| --- | ---: | ---: |
| Dominican Republic | 416 | 0 |
| Costa Rica | 500 | ~371 |
| Panama | 280 | 0 |
| Colombia | 500 | 649 |

## Next
Mexico (then remaining CALA). CR remainder still deferred.

## Artifacts
- `reports/research-engine-v2/full-cala-15k-census-shell-insert-v1-colombia-batch.{md,json}`
- Checkpoint: `data/research-engine-v2/full-cala-15k-census-shell/full-cala-15k-checkpoint.json`
