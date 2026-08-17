# CALA Census ↔ Benchmark Coverage Reconciliation

**Status:** `production_census_benchmark_coverage_reconciliation_complete_aggregate_only`
**Generated:** 2026-08-11T21:19:49.702Z

## Policy

- Benchmark role: **BENCHMARK_ONLY** (licensing unconfirmed)
- Dealality table: Hotel Property Census (`tbl9aY5ijiuIzzWam`) — **read-only**
- No property-level benchmark persistence
- No Census inserts from this reconciliation
- Wording: **potential coverage gap** (not “missing hotels”)

## Totals

| Metric | Value |
| --- | ---: |
| DEALALITY_CENSUS_COUNT | 15007 |
| BENCHMARK_CENSUS_COUNT (Open) | 14938 |
| Geographies compared | 52 |
| BENCHMARK_ALIGNED | 7 |
| POSSIBLE_MINOR_GAP | 9 |
| POSSIBLE_MODERATE_GAP | 8 |
| POSSIBLE_MAJOR_GAP | 15 |
| ZERO_DEALALITY_BENCHMARK_NONZERO | 5 |
| DEALALITY_HIGHER_THAN_BENCHMARK | 6 |

## Top 15 geographic discovery priorities

| Geography | Dealality | BENCHMARK_COUNT | Ratio | Gap class | Priority |
| --- | ---: | ---: | ---: | --- | ---: |
| Cuba | 0 | 295 | 0 | ZERO_DEALALITY_BENCHMARK_NONZERO | 1425 |
| Saint Martin | 0 | 36 | 0 | ZERO_DEALALITY_BENCHMARK_NONZERO | 1158 |
| Sint Maarten | 0 | 41 | 0 | ZERO_DEALALITY_BENCHMARK_NONZERO | 1151 |
| Bonaire | 0 | 33 | 0 | ZERO_DEALALITY_BENCHMARK_NONZERO | 1149 |
| Montserrat | 0 | 3 | 0 | ZERO_DEALALITY_BENCHMARK_NONZERO | 1094 |
| Belize | 12 | 334 | 0.036 | POSSIBLE_MAJOR_GAP | 874 |
| Puerto Rico | 49 | 165 | 0.297 | POSSIBLE_MAJOR_GAP | 691 |
| Guatemala | 13 | 115 | 0.113 | POSSIBLE_MAJOR_GAP | 671 |
| Nicaragua | 7 | 132 | 0.053 | POSSIBLE_MAJOR_GAP | 645 |
| El Salvador | 10 | 81 | 0.123 | POSSIBLE_MAJOR_GAP | 603 |
| Cayman Islands | 22 | 57 | 0.386 | POSSIBLE_MAJOR_GAP | 589 |
| Honduras | 12 | 49 | 0.245 | POSSIBLE_MAJOR_GAP | 588 |
| Curaçao | 5 | 44 | 0.114 | POSSIBLE_MAJOR_GAP | 584 |
| Grenada | 4 | 39 | 0.103 | POSSIBLE_MAJOR_GAP | 567 |
| Saint Kitts and Nevis | 3 | 25 | 0.12 | POSSIBLE_MAJOR_GAP | 567 |

## Top city / destination discovery priorities

| Geography | City/destination | Dealality | Benchmark | Gap | Instruction |
| --- | --- | ---: | ---: | --- | --- |
| Cuba | Havana | 0 | 58 | HIGH | SEARCH Havana / Cuba MORE DEEPLY |
| Cuba | Varadero | 0 | 54 | HIGH | SEARCH Varadero / Cuba MORE DEEPLY |
| U.S. Virgin Islands | Charlotte Amalie | 0 | 24 | HIGH | SEARCH Charlotte Amalie / U.S. Virgin Islands MORE DEEPLY |
| Belize | Caye Caulker | 0 | 37 | HIGH | SEARCH Caye Caulker / Belize MORE DEEPLY |
| Cuba | Santiago De Cuba | 0 | 16 | HIGH | SEARCH Santiago De Cuba / Cuba MORE DEEPLY |
| Cuba | Cayo Santa Maria | 0 | 12 | HIGH | SEARCH Cayo Santa Maria / Cuba MORE DEEPLY |
| Argentina | Mar De Las Pampas | 0 | 11 | HIGH | SEARCH Mar De Las Pampas / Argentina MORE DEEPLY |
| Cuba | Holguin | 0 | 11 | HIGH | SEARCH Holguin / Cuba MORE DEEPLY |
| Cuba | Trinidad | 0 | 11 | HIGH | SEARCH Trinidad / Cuba MORE DEEPLY |
| Mexico | Cuautla | 0 | 10 | HIGH | SEARCH Cuautla / Mexico MORE DEEPLY |
| Mexico | Hidalgo Del Parral | 0 | 10 | HIGH | SEARCH Hidalgo Del Parral / Mexico MORE DEEPLY |
| Bahamas | Paradise | 0 | 9 | HIGH | SEARCH Paradise / Bahamas MORE DEEPLY |
| Cuba | Cayo Coco | 0 | 9 | HIGH | SEARCH Cayo Coco / Cuba MORE DEEPLY |
| U.S. Virgin Islands | St John | 0 | 9 | HIGH | SEARCH St John / U.S. Virgin Islands MORE DEEPLY |
| Mexico | Ixmiquilpan | 0 | 8 | HIGH | SEARCH Ixmiquilpan / Mexico MORE DEEPLY |
| Mexico | Acuna | 0 | 8 | HIGH | SEARCH Acuna / Mexico MORE DEEPLY |
| Cuba | Ciego De Avila | 0 | 8 | HIGH | SEARCH Ciego De Avila / Cuba MORE DEEPLY |
| Cuba | Camaguey | 0 | 8 | HIGH | SEARCH Camaguey / Cuba MORE DEEPLY |
| Mexico | Tonala | 0 | 7 | HIGH | SEARCH Tonala / Mexico MORE DEEPLY |
| Mexico | San Nicolas De Los Garza | 0 | 7 | HIGH | SEARCH San Nicolas De Los Garza / Mexico MORE DEEPLY |
| Mexico | Tequila | 0 | 7 | HIGH | SEARCH Tequila / Mexico MORE DEEPLY |
| Mexico | Iguala | 0 | 7 | HIGH | SEARCH Iguala / Mexico MORE DEEPLY |
| Cuba | La Habana | 0 | 7 | HIGH | SEARCH La Habana / Cuba MORE DEEPLY |
| Cuba | Baracoa | 0 | 7 | HIGH | SEARCH Baracoa / Cuba MORE DEEPLY |
| Cuba | Caibarien | 0 | 7 | HIGH | SEARCH Caibarien / Cuba MORE DEEPLY |

## Next recommended action

Run independent multi-source discovery for TOP geographic / city-destination priorities (SerpAPI, HBX where quota allows, official/public directories). Do not import or copy legacy Hotel Census records. Do not treat BENCHMARK_COUNT as true hotel inventory.

PROPERTY_LEVEL_BENCHMARK_DATA_PERSISTED: **NO**  
BENCHMARK_RECORDS_WRITTEN_TO_DEALALITY: **0**  
BENCHMARK_USED_AS_PRODUCTION_PROVENANCE: **NO**
