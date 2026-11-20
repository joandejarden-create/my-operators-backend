# Dealality Census CLI Migration Catalog v1

Generated: 2026-11-19T23:54:13.171Z

Active CLI table: **Hotel Property Census** (`tbl9aY5ijiuIzzWam`)

## Counts by disposition

| Disposition | Count |
| --- | ---: |
| RETARGET_HPC_AUTOPILOT | 30 |
| RETARGET_HPC_NULL_FILL | 29 |
| DISABLE_MIGRATION_ONLY | 27 |
| RETARGET_HPC_UPSERT | 9 |

## Shared retarget

- `lib/hotel-census/fields.js` — `HOTEL_CENSUS_TABLE` → HPC; `CENSUS_FIELDS` → HPC names
- `lib/hotel-census/platform-base.js` — legacy write Proxy block
- `lib/hotel-census/dealality-census-cli-migration-v1.js` — field map + enforce helpers

Total CLIs catalogued: **95**

Live Airtable apply for brand enrichment batches remains founder-gated; dry-run is the default.