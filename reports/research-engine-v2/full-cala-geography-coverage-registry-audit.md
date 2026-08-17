# Full CALA Geography Coverage Registry Audit

**AUDIT_STATUS:** `production_census_full_cala_geography_coverage_registry_audit_complete`  
**Production writes:** **false**  
**Census:** **15485**  
**Generated:** 2026-08-11T21:46:08.375Z

## Executive return

| Field | Value |
| --- | ---: |
| CANONICAL_GEOGRAPHIES_TOTAL | 52 |
| GEOGRAPHIES_WITH_CENSUS_RECORDS | 51 |
| GEOGRAPHIES_WITH_ZERO_CENSUS_RECORDS | 1 |
| GEOGRAPHIES_HBX_SEARCHED | 33 |
| GEOGRAPHIES_CVENT_SEARCHED | 49 |
| GEOGRAPHIES_NOT_YET_SEARCHED | 2 |
| GEOGRAPHIES_WITH_SOURCE_GAPS | 5 |
| GEOGRAPHIES_WITH_NORMALIZATION_PROBLEMS | 7 |
| HOLD_CONCENTRATION_TOP_1 | 44.8% |
| HOLD_CONCENTRATION_TOP_3 | 66.7% |
| HOLD_CONCENTRATION_TOP_5 | 74.7% |
| NEXT_RECOMMENDED_ACTION | PROCEED_SOURCE_GAP_DISCOVERY_BY_GEOGRAPHY_QUEUE |
| FOUNDER_DECISION_REQUIRED | NO |



## Critical distinction

Zero source records ≠ searched-and-found-zero. HBX Wave1 searched only 5 countries. Cvent registry seeded 49 (Cuba excluded after empty probe). BES islands Sint Eustatius/Saba are not in Cvent registry.

- HBX Wave1 searched: Mexico, Dominican Republic, Colombia, Costa Rica, Panama, Argentina, Bolivia, Brazil, French Guiana, Guyana, Paraguay, Suriname, Venezuela, Anguilla, Antigua and Barbuda, Aruba, Bahamas, Bonaire, Cayman Islands, Cuba, Curaçao, Guadeloupe, Haiti, Martinique, Montserrat, Saint Barthélemy, Saint Lucia, Saint Martin, Saint Vincent and the Grenadines, Sint Maarten, Turks and Caicos Islands, U.S. Virgin Islands, Bermuda
- Cvent registry seeded ~49; excluded after probe: Cuba

## Macro Census distortion check

| Bucket | Count |
| --- | ---: |
| Mexico | 2724 |
| Central America ex-Mexico | 1372 |
| Caribbean | 1822 |
| South America ex-Brazil | 3825 |
| Brazil | 5742 |
| other_or_unresolved | 0 |

## Top 15 Census geographies

1. **Brazil** — 5742
2. **Mexico** — 2724
3. **Argentina** — 974
4. **Colombia** — 967
5. **Costa Rica** — 748
6. **Dominican Republic** — 654
7. **Peru** — 591
8. **Chile** — 528
9. **Panama** — 325
10. **Ecuador** — 307
11. **Jamaica** — 225
12. **Uruguay** — 167
13. **Bolivia** — 126
14. **Barbados** — 113
15. **Puerto Rico** — 88

## Zero-record geographies

- Cuba

## HOLD concentration

- Top 1: **44.8%** — Brazil (3444)
- Top 3: **66.7%**
- Top 5: **74.7%**

A large HOLD universe dominated by Brazil/Mexico does **not** prove Caribbean + rest-of-LATAM discovery coverage.

## Top 10 source-gap discovery priorities

| Rank | Geography | Status | Tourism | Census | HBX/Cvent searched | Action |
| --- | --- | --- | --- | ---: | --- | --- |
| 1 | Sint Eustatius | NOT_YET_SEARCHED | C | 12 | NO/NO | queue_first_pass_multi_source_discovery |
| 2 | Saba | NOT_YET_SEARCHED | C | 6 | NO/NO | queue_first_pass_multi_source_discovery |
| 3 | Cuba | NO_SOURCE_STOCK | S | 0 | YES/YES | confirm_search_evidence_then_alternate_sources |
| 4 | British Virgin Islands | SOURCE_GAP | A | 3 | NO/YES | expand_hbx_and_official_parent_discovery |
| 5 | Saint Kitts and Nevis | SOURCE_GAP | A | 19 | NO/YES | expand_hbx_and_official_parent_discovery |
| 6 | Barbados | NORMALIZATION_PROBLEM | S | 113 | UNKNOWN/YES | normalize_parent_country_encodings_before_discovery |
| 7 | Jamaica | NORMALIZATION_PROBLEM | S | 225 | UNKNOWN/YES | normalize_parent_country_encodings_before_discovery |
| 8 | Bonaire | SOURCE_GAP | A | 21 | YES/YES | expand_hbx_and_official_parent_discovery |
| 9 | Saint Martin | SOURCE_GAP | A | 35 | YES/YES | expand_hbx_and_official_parent_discovery |
| 10 | Sint Maarten | SOURCE_GAP | A | 25 | YES/YES | expand_hbx_and_official_parent_discovery |

## Bermuda

{
  "status": "SCOPE_REVIEW",
  "recommendation": "Do not silently include Bermuda in CALA shell/enrichment until founder decides. Reasonable hospitality-commercial inclusion candidate; not in prior Cvent LATAM registry.",
  "census_count": 14,
  "CVENT_SEARCHED": "NO",
  "HBX_SEARCHED": "YES"
}

## Full geography coverage matrix

| Geography | ISO | Region | Census | HBX | Cvent | Other | HOLD | Shells | HBX searched? | Cvent searched? | Norm issue? | Status | Next action |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | --- | --- |
| Sint Eustatius | BQ | Caribbean | 12 | 0 | 0 | 0 | 0 | 12 | NO | NO | NO | NOT_YET_SEARCHED | queue_first_pass_multi_source_discovery |
| Saba | BQ | Caribbean | 6 | 0 | 0 | 0 | 0 | 6 | NO | NO | NO | NOT_YET_SEARCHED | queue_first_pass_multi_source_discovery |
| Cuba | CU | Caribbean | 0 | 0 | 0 | 0 | 0 | 0 | YES | YES | NO | NO_SOURCE_STOCK | confirm_search_evidence_then_alternate_sources |
| British Virgin Islands | VG | Caribbean | 3 | 0 | 25 | 0 | 20 | 0 | NO | YES | NO | SOURCE_GAP | expand_hbx_and_official_parent_discovery |
| Saint Kitts and Nevis | KN | Caribbean | 19 | 0 | 18 | 0 | 0 | 16 | NO | YES | NO | SOURCE_GAP | expand_hbx_and_official_parent_discovery |
| Bonaire | BQ | Caribbean | 21 | 0 | 51 | 0 | 51 | 21 | YES | YES | NO | SOURCE_GAP | expand_hbx_and_official_parent_discovery |
| Saint Martin | MF | Caribbean | 35 | 0 | 100 | 0 | 98 | 35 | YES | YES | NO | SOURCE_GAP | expand_hbx_and_official_parent_discovery |
| Sint Maarten | SX | Caribbean | 25 | 0 | 100 | 0 | 0 | 25 | YES | YES | NO | SOURCE_GAP | expand_hbx_and_official_parent_discovery |
| Barbados | BB | Caribbean | 113 | 58 | 46 | 0 | 55 | 66 | UNKNOWN | YES | YES | NORMALIZATION_PROBLEM | normalize_parent_country_encodings_before_discovery |
| Jamaica | JM | Caribbean | 225 | 152 | 135 | 0 | 170 | 147 | UNKNOWN | YES | YES | NORMALIZATION_PROBLEM | normalize_parent_country_encodings_before_discovery |
| Chile | CL | South America | 528 | 379 | 206 | 0 | 311 | 463 | UNKNOWN | YES | YES | NORMALIZATION_PROBLEM | normalize_parent_country_encodings_before_discovery |
| Ecuador | EC | South America | 307 | 270 | 63 | 0 | 89 | 276 | UNKNOWN | YES | YES | NORMALIZATION_PROBLEM | normalize_parent_country_encodings_before_discovery |
| Peru | PE | South America | 591 | 465 | 187 | 0 | 305 | 534 | UNKNOWN | YES | YES | NORMALIZATION_PROBLEM | normalize_parent_country_encodings_before_discovery |
| Uruguay | UY | South America | 167 | 157 | 28 | 0 | 40 | 151 | UNKNOWN | YES | YES | NORMALIZATION_PROBLEM | normalize_parent_country_encodings_before_discovery |
| Trinidad and Tobago | TT | Caribbean | 36 | 27 | 12 | 0 | 14 | 29 | UNKNOWN | YES | YES | NORMALIZATION_PROBLEM | normalize_parent_country_encodings_before_discovery |
| Puerto Rico | PR | Caribbean | 88 | 0 | 118 | 0 | 82 | 39 | NO | YES | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| Belize | BZ | Central America | 76 | 0 | 123 | 0 | 118 | 64 | NO | YES | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| Guatemala | GT | Central America | 64 | 0 | 86 | 0 | 74 | 51 | NO | YES | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| Honduras | HN | Central America | 56 | 0 | 60 | 0 | 48 | 44 | NO | YES | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| Aruba | AW | Caribbean | 77 | 57 | 19 | 0 | 17 | 64 | YES | YES | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| Bahamas | BS | Caribbean | 61 | 39 | 67 | 0 | 55 | 52 | YES | YES | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| Cayman Islands | KY | Caribbean | 31 | 12 | 33 | 0 | 34 | 24 | YES | YES | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| Turks and Caicos Islands | TC | Caribbean | 46 | 49 | 57 | 0 | 57 | 46 | YES | YES | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| U.S. Virgin Islands | VI | Caribbean | 44 | 26 | 25 | 0 | 24 | 44 | YES | YES | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| Bermuda | BM | Caribbean | 14 | 16 | 0 | 0 | 0 | 14 | YES | NO | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| Dominica | DM | Caribbean | 2 | 0 | 23 | 0 | 15 | 0 | NO | YES | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| Anguilla | AI | Caribbean | 9 | 5 | 19 | 0 | 0 | 9 | YES | YES | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| Saint Barthélemy | BL | Caribbean | 14 | 11 | 104 | 0 | 6 | 14 | YES | YES | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| El Salvador | SV | Central America | 47 | 0 | 30 | 0 | 22 | 37 | NO | YES | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| Nicaragua | NI | Central America | 56 | 0 | 46 | 0 | 42 | 49 | NO | YES | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| Grenada | GD | Caribbean | 25 | 0 | 26 | 0 | 23 | 21 | NO | YES | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| Antigua and Barbuda | AG | Caribbean | 48 | 35 | 28 | 0 | 3 | 43 | YES | YES | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| Curaçao | CW | Caribbean | 42 | 0 | 52 | 0 | 3 | 37 | YES | YES | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| Guadeloupe | GP | Caribbean | 41 | 31 | 22 | 0 | 21 | 41 | YES | YES | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| Martinique | MQ | Caribbean | 41 | 28 | 49 | 0 | 28 | 41 | YES | YES | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| Saint Lucia | LC | Caribbean | 35 | 25 | 29 | 0 | 27 | 32 | YES | YES | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| Saint Vincent and the Grenadines | VC | Caribbean | 10 | 6 | 18 | 0 | 19 | 10 | YES | YES | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| Bolivia | BO | South America | 126 | 103 | 29 | 0 | 27 | 126 | YES | YES | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| Venezuela | VE | South America | 76 | 64 | 17 | 0 | 17 | 76 | YES | YES | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| Haiti | HT | Caribbean | 33 | 27 | 9 | 0 | 9 | 33 | YES | YES | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| French Guiana | GF | South America | 3 | 4 | 4 | 0 | 7 | 3 | YES | YES | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| Guyana | GY | South America | 8 | 8 | 14 | 0 | 14 | 8 | YES | YES | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| Suriname | SR | South America | 17 | 12 | 10 | 0 | 10 | 17 | YES | YES | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| Montserrat | MS | Caribbean | 12 | 0 | 50 | 0 | 49 | 12 | YES | YES | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| Paraguay | PY | South America | 61 | 42 | 76 | 0 | 52 | 61 | YES | YES | NO | DISCOVERY_PARTIAL | targeted_gap_fill_then_enrichment_later |
| Costa Rica | CR | Central America | 748 | 607 | 242 | 0 | 202 | 641 | YES | YES | NO | DISCOVERY_STRONG | hold_for_enrichment_phase_after_universe_complete |
| Mexico | MX | Central America | 2724 | 1124 | 2085 | 634 | 1137 | 1808 | YES | YES | NO | DISCOVERY_STRONG | hold_for_enrichment_phase_after_universe_complete |
| Panama | PA | Central America | 325 | 157 | 117 | 0 | 0 | 280 | YES | YES | NO | DISCOVERY_STRONG | hold_for_enrichment_phase_after_universe_complete |
| Argentina | AR | South America | 974 | 665 | 549 | 0 | 542 | 845 | YES | YES | NO | DISCOVERY_STRONG | hold_for_enrichment_phase_after_universe_complete |
| Colombia | CO | South America | 967 | 737 | 363 | 0 | 299 | 793 | YES | YES | NO | DISCOVERY_STRONG | hold_for_enrichment_phase_after_universe_complete |
| Dominican Republic | DO | Caribbean | 654 | 331 | 189 | 0 | 0 | 416 | YES | YES | NO | DISCOVERY_STRONG | hold_for_enrichment_phase_after_universe_complete |
| Brazil | BR | South America | 5742 | 4477 | 3298 | 0 | 3444 | 5248 | YES | YES | NO | DISCOVERY_STRONG | hold_for_enrichment_phase_after_universe_complete |

## Safety

- Read-only — no Census / Brand Explorer / Brand Setup / VIC writes
- No shell inserts, enrichment, brand promotion, or HOLD enrichment
- SAFE+HBX identity gate not weakened
