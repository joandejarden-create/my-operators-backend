# Peru MINCETUR ↔ Hotel Property Census Match + Gated Plan

**Status:** `peru_mincetur_hpc_match_plan_dry_run_complete`
**Generated:** 2026-08-07T16:55:00.054Z
**Adapter:** `peru-mincetur-open-data-adapter-v1` / `peru-mincetur-hpc-match-plan-v1` / `hotel-property-census-match-v1`
**Airtable writes:** none
**Owner Name writes:** none (RUC on ownership_signal only)
**Auto-insert:** disabled (steward gate required; PAGINA_WEB → Official Property URL when present)

## Dedupe SoT

- Hotel Property Census `tbl9aY5ijiuIzzWam` only
- Legacy Hotel Census: forbidden
- Peru pool size: 0 (of 1224 loaded)

## HPC match summary

| Action | Count |
| --- | ---: |
| likely_existing | 0 |
| possible_duplicate_review | 0 |
| likely_new_candidate | 250 |
| needs_research | 0 |
| identity_key_collisions | 0 |

## Gated plan decisions

| Decision | Count |
| --- | ---: |
| auto_enrich_only | 0 |
| steward_hold | 0 |
| steward_hold_insert_candidate | 250 |
| reject | 0 |
| insert candidates with Official Property URL | 84 |

## Future apply confirms (not enabled)

- `--confirm-peru-mincetur-steward-insert`
- `--confirm-no-owner-operator-writes`
- `--confirm-hotel-property-census-only`
- `--confirm-no-legacy-census-writes`

## Insert-candidate sample (steward review)

| Identity | Name | City | Rooms | RUC signal | Official URL | HPC action |
| --- | --- | --- | ---: | --- | --- | --- |
| gov_pe_mincetur_20608180070_95481c1073 | AVA SPOTS | Urubamba | 32 | 20608180070 | yes | likely_new_candidate |
| gov_pe_mincetur_20605249036_8fe61eea12 | LA MERCED | Arequipa | 31 | 20605249036 |  | likely_new_candidate |
| gov_pe_mincetur_20513286989_e59f0f9e78 | SAN AGUSTIN RECOLETA | Urubamba | 32 | 20513286989 | yes | likely_new_candidate |
| gov_pe_mincetur_20608239856_6069a785ef | LUXOTEL | Chincha Alta | 20 | 20608239856 |  | likely_new_candidate |
| gov_pe_mincetur_20512423516_82c07c1880 | QALLWA HOTELS | Sunampe | 30 | 20512423516 | yes | likely_new_candidate |
| gov_pe_mincetur_20606492198_c7fdc91adf | LA COLPA CHINCHANA | Grocio Prado | 20 | 20606492198 |  | likely_new_candidate |
| gov_pe_mincetur_20600462220_c1c8b64584 | LUNA | Chincha Alta | 20 | 20600462220 |  | likely_new_candidate |
| gov_pe_mincetur_20534953381_c83d70973f | LOS ANGELES CAMPESTRE | Grocio Prado | 21 | 20534953381 |  | likely_new_candidate |
| gov_pe_mincetur_20534953381_5a328028f0 | LOS ANGELES | Chincha Alta | 21 | 20534953381 |  | likely_new_candidate |
| gov_pe_mincetur_20494430569_3524035176 | LA COLMENA | Ayacucho |  | 20494430569 | yes | likely_new_candidate |
| gov_pe_mincetur_10218121832_9760427317 | CALLAO | Chincha Alta | 28 | 10218121832 |  | likely_new_candidate |
| gov_pe_mincetur_10282642901_191c4c3721 | GRAU | Ayacucho |  | 10282642901 |  | likely_new_candidate |

## Field mapping

- `MAP_PERU_MINCETUR` → inventory fields only
- Identity: `gov_pe_mincetur_{NRO_CERTIFICADO}` (fallback RUC+slug)
- Forbidden on insert preview: Owner Name, Operator, Opening Date, Brand Status, etc.
