# Colombia RNT ↔ Hotel Property Census Match + Gated Plan

**Status:** `colombia_rnt_hpc_match_plan_dry_run_complete`
**Generated:** 2026-08-07T15:59:30.215Z
**Adapter:** `colombia-rnt-open-data-adapter-v1` / `colombia-rnt-hpc-match-plan-v1` / `hotel-property-census-match-v1`
**Airtable writes:** none
**Owner Name writes:** none (NIT on ownership_signal only)
**Auto-insert:** disabled (government Source URL ≠ Official Property URL)

## Dedupe SoT

- Hotel Property Census `tbl9aY5ijiuIzzWam` only
- Legacy Hotel Census: forbidden
- Colombia pool size: 114 (of 1224 loaded)

## HPC match summary

| Action | Count |
| --- | ---: |
| likely_existing | 0 |
| possible_duplicate_review | 0 |
| likely_new_candidate | 393 |
| needs_research | 7 |
| identity_key_collisions | 0 |

## Gated plan decisions

| Decision | Count |
| --- | ---: |
| auto_enrich_only | 0 |
| steward_hold | 0 |
| steward_hold_insert_candidate | 400 |
| reject | 0 |

## Future apply confirms (not enabled)

- `--confirm-colombia-rnt-steward-insert`
- `--confirm-no-owner-operator-writes`
- `--confirm-hotel-property-census-only`
- `--confirm-no-legacy-census-writes`

## Insert-candidate sample (steward review)

| Identity | Name | City | Rooms | NIT signal | HPC action |
| --- | --- | --- | ---: | --- | --- |
| gov_co_rnt_123 | HOTEL PUERTA DEL SOL S.A. | Barranquilla | 109 | 800157788 | likely_new_candidate |
| gov_co_rnt_181 | HOTEL SORATAMA | Pereira | 77 | 800156522 | likely_new_candidate |
| gov_co_rnt_185 | ERTUR LTDA. Y/O HOTEL VERDE MAR | San Andres | 44 | 860508374 | likely_new_candidate |
| gov_co_rnt_253 | HOTEL CASINO INTERNACIONAL | Cucuta | 128 | 890502101 | likely_new_candidate |
| gov_co_rnt_254 | HOTEL IROTAMA | Santa Marta | 192 | 891700612 | likely_new_candidate |
| gov_co_rnt_263 | HOTEL CARTAGENA HILTON. | Cartagena | 348 | 890401427 | likely_new_candidate |
| gov_co_rnt_338 | BLU HOTEL BY TAMACA | Santa Marta | 61 | 891701294 | likely_new_candidate |
| gov_co_rnt_387 | GRAN HOTEL | Pereira | 79 | 900651886 | likely_new_candidate |
| gov_co_rnt_396 | HOTEL DANN | Bogota D.c. | 129 | 860014195 | likely_new_candidate |
| gov_co_rnt_397 | HOTEL DECAMERON SAN ANDRES SAN LUIS | San Andres | 239 | 806000179 | likely_new_candidate |
| gov_co_rnt_407 | FOUR POINTS BY SHERATON TEQUENDAMA | Bogota D.c. | 573 | 860006543 | needs_research |
| gov_co_rnt_417 | HOTEL ARIZONA SUITES | Cucuta | 80 | 890505594 | likely_new_candidate |

## Field mapping

- `MAP_COLOMBIA_RNT` → inventory fields only
- Identity: `gov_co_rnt_{codigo_rnt}`
- Forbidden on insert preview: Owner Name, Operator, Opening Date, Brand Status, etc.
