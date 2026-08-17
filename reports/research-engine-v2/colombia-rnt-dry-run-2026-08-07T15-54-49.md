# Colombia RNT Open-Data Dry-Run

**Status:** `colombia_rnt_open_data_dry_run_complete`
**Adapter:** `colombia-rnt-open-data-adapter-v1`
**Generated:** 2026-08-07T15:54:49.421Z
**Airtable writes:** none (dry-run only)

## Source

- Dataset: [thwd-ivmp](https://www.datos.gov.co/Comercio-Industria-y-Turismo/Registro-Nacional-de-Turismo-RNT/thwd-ivmp)
- Year filter: 2026
- Subcategories: HOTEL

## Summary

| Metric | Count |
| --- | ---: |
| Raw rows fetched | 200 |
| Unique codigo_rnt | 200 |
| Validation pass | 200 |
| Validation fail | 0 |
| Rooms sanity Hold | 0 |
| NIT present (ownership signal only) | 200 |

## Ownership lane

NIT is captured on `ownership_signal` only. **Owner Name is not written** (Autopilot forbidden / blocked enrichment lane).

## Sample candidates

| Identity Key | Property Name | City | State | Rooms | NIT signal |
| --- | --- | --- | --- | ---: | --- |
| gov_co_rnt_123 | HOTEL PUERTA DEL SOL S.A. | Barranquilla | Atlantico | 109 | 800157788 |
| gov_co_rnt_181 | HOTEL SORATAMA | Pereira | Risaralda | 77 | 800156522 |
| gov_co_rnt_185 | ERTUR LTDA. Y/O HOTEL VERDE MAR | San Andres | San Andres Y Providencia | 44 | 860508374 |
| gov_co_rnt_253 | HOTEL CASINO INTERNACIONAL | Cucuta | Norte De Santander | 128 | 890502101 |
| gov_co_rnt_254 | HOTEL IROTAMA | Santa Marta | Magdalena | 192 | 891700612 |
| gov_co_rnt_263 | HOTEL CARTAGENA HILTON. | Cartagena | Bolivar | 348 | 890401427 |
| gov_co_rnt_338 | BLU HOTEL BY TAMACA | Santa Marta | Magdalena | 61 | 891701294 |
| gov_co_rnt_387 | GRAN HOTEL | Pereira | Risaralda | 79 | 900651886 |
| gov_co_rnt_396 | HOTEL DANN | Bogota D.c. | Bogota | 129 | 860014195 |
| gov_co_rnt_397 | HOTEL DECAMERON SAN ANDRES SAN LUIS | San Andres | San Andres Y Providencia | 239 | 806000179 |
| gov_co_rnt_407 | FOUR POINTS BY SHERATON TEQUENDAMA | Bogota D.c. | Bogota | 573 | 860006543 |
| gov_co_rnt_417 | HOTEL ARIZONA SUITES | Cucuta | Norte De Santander | 80 | 890505594 |
| gov_co_rnt_447 | HOTEL DEL TURISMO | Versalles | Valle Del Cauca | 10 | 891902843 |
| gov_co_rnt_456 | HOTEL TIU MARA | Cartago | Valle Del Cauca | 16 | 31423912 |
| gov_co_rnt_481 | HOSTAL INTERNACIONAL | Bogota D.c. | Bogota | 24 | 19434659 |

## Field mapping

- Object: `MAP_COLOMBIA_RNT`
- Identity: `gov_co_rnt_{codigo_rnt}`
- Inventory fields: Property Name, City, State / Region, Country, Rooms / Keys*, Source URL, Family / Source Family
- Forbidden: Owner Name / Operator / dates (never in patch)

## Next steps

1. Review dry-run sample for name/city quality.
2. Match `gov_co_rnt_*` identity keys against Hotel Property Census (read-only).
3. Only then design a controlled insert gate (separate approval) — still no Owner Name writes.
