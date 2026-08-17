# Peru MINCETUR Open-Data Dry-Run

**Status:** `peru_mincetur_open_data_dry_run_complete`
**Adapter:** `peru-mincetur-open-data-adapter-v1`
**Generated:** 2026-08-07T16:06:00.708Z
**Airtable writes:** none

## Source

- CSV: https://www.mincetur.gob.pe/Datos_abiertos/DGPDT/Establecimientos_hospedajes_calificados.csv
- Catalog: https://www.datosabiertos.gob.pe/dataset/directorio-nacional-de-prestadores-de-servicios-turisticos-calificados
- Classes: HOTEL
- FECHA_CORTE sample: 20260806
- max-rows: 300

## Summary

| Metric | Count |
| --- | ---: |
| Raw CSV rows | 5091 |
| After lodging class filter | 2391 |
| Validation pass | 300 |
| Validation fail | 0 |
| Official Property URL present | 102 |
| RUC ownership signal | 300 |

## Ownership lane

RUC is on `ownership_signal` only. **Owner Name is not written.**

## Sample

| Identity | Name | City | Rooms | URL? | RUC |
| --- | --- | --- | ---: | --- | --- |
| gov_pe_mincetur_20608180070_95481c1073 | AVA SPOTS | Urubamba | 32 | yes | 20608180070 |
| gov_pe_mincetur_20605249036_8fe61eea12 | LA MERCED | Arequipa | 31 |  | 20605249036 |
| gov_pe_mincetur_20513286989_e59f0f9e78 | SAN AGUSTIN RECOLETA | Urubamba | 32 | yes | 20513286989 |
| gov_pe_mincetur_20608239856_6069a785ef | LUXOTEL | Chincha Alta | 20 |  | 20608239856 |
| gov_pe_mincetur_20512423516_82c07c1880 | QALLWA HOTELS | Sunampe | 30 | yes | 20512423516 |
| gov_pe_mincetur_20606492198_c7fdc91adf | LA COLPA CHINCHANA | Grocio Prado | 20 |  | 20606492198 |
| gov_pe_mincetur_20600462220_c1c8b64584 | LUNA | Chincha Alta | 20 |  | 20600462220 |
| gov_pe_mincetur_20534953381_c83d70973f | LOS ANGELES CAMPESTRE | Grocio Prado | 21 |  | 20534953381 |
| gov_pe_mincetur_20534953381_5a328028f0 | LOS ANGELES | Chincha Alta | 21 |  | 20534953381 |
| gov_pe_mincetur_20494430569_3524035176 | LA COLMENA | Ayacucho |  | yes | 20494430569 |
| gov_pe_mincetur_10218121832_9760427317 | CALLAO | Chincha Alta | 28 |  | 10218121832 |
| gov_pe_mincetur_10282642901_191c4c3721 | GRAU | Ayacucho |  |  | 10282642901 |
| gov_pe_mincetur_049-2022 | SOL DE LUNA | Lunahuana | 28 |  | 20538268811 |
| gov_pe_mincetur_416 | AMERICAS | Miraflores | 76 | yes | 10028719596 |
| gov_pe_mincetur_044-13Dzp | SAN JORGE RESIDENCIAL | Pisco |  | yes | 20452674239 |

## Notes vs Colombia RNT

- Uses **NOMBRE_COMERCIAL** (better matchability than legal-only names)
- Often includes **PAGINA_WEB** → Official Property URL candidate
- Still steward-gated for apply; no auto-insert from this dry-run

## Next

1. `npm run census:peru-mincetur-hpc-match-plan` (when wired) or reuse Colombia match pattern
2. Wait for Wave 2 Webhound before prioritizing PR/Central America over Peru
3. Colombia apply remains paused
