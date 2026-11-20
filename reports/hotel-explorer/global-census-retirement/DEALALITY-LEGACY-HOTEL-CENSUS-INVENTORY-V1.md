# Dealality — Legacy Hotel Census Dependency Inventory v1

Generated: 2026-11-19T23:26:51.119Z

## Canonical vs legacy

| Role | Table | ID |
| --- | --- | --- |
| RETAIN | Hotel Property Census | `tbl9aY5ijiuIzzWam` |
| RETIRE (do not delete yet) | Hotel Census | `tblgj2qEwxjTcg6q0` |

## Counts

- Files referencing legacy markers: **259**
- By status: {"MIGRATED_HPC":6,"INACTIVE":11,"REVIEW":100,"MIGRATION_REQUIRED_CLI":95,"AUDIT_HPC_ALIASES":2,"HE_LEGACY_INDEPENDENT":6,"DEPRECATED_FOR_RUNTIME":3,"TEST":36}
- By product: {"Brand Presence / Radar / HE":1,"tmp":5,"shared":100,"Scout/Radar":5,"census-enrichment-cli":95,"ADP":2,"reports":6,"Hotel Explorer":6,"shared-legacy-field-map":3,"probes":28,"tests":8}

## Runtime migrations in this wave

- lib/scout/census-read-cache.js → HPC
- api/brand-presence.js list/search → HPC
- api/operators-by-brand-region.js → HPC
- api/brand-fit-analyzer.js → HPC
- lib/hotel-census/aggregate-presence-summary.js → HPC

## Remaining blockers

- Enrichment/import CLIs still target Hotel Census (IMPORT) — retarget to HPC Autopilot pipelines via Integration Controller
- Linked-record Airtable migration dry-run for HI graph edges pointing at legacy census ids — founder apply later
- Population denominator shifts (Census Open vs HPC Radar Status Open|Pipeline) require before/after comparison evidence
- ADP/GDI alias map coverage for non-HPC keys — validate, do not rewrite opportunities

## Certification

| Status | Value |
| --- | --- |
| `DEPENDENCY_INVENTORY_COMPLETE` | **true** |
| `HPC_DATA_RECONCILED` | **false** |
| `SHARED_RESOLVER_VALIDATED` | **true** |
| `PRODUCT_MIGRATIONS_COMPLETE` | **false** |
| `LEGACY_RUNTIME_ACCESS_ZERO` | **false** |
| `AIRTABLE_LINKS_MIGRATED` | **false** |
| `GLOBAL_RETIREMENT_READY` | **false** |

## Note

Enrichment CLIs and historical probes remain classified IMPORT/TEST/INACTIVE. They must not block product runtime independence, but they **do** block `GLOBAL_RETIREMENT_READY` until retargeted or retired.
