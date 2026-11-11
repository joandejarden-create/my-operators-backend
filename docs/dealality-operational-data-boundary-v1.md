# Operational data boundary

Tracked hotel stores, ADP artifacts, generated reports, and share registries are operational data. Application source and `fixtures/` stay in Git.

## Current locations

These remain the authoritative locations. This document does not move them.

| Data | Path | Existing override |
| --- | --- | --- |
| GDI hotel stores, feedback, research | `data/group-demand-intelligence` | `GDI_DATA_ROOT` |
| Published GDI inventory | registry root from `resolveGdiPublishedInventoryRoot` | `GDI_PUBLISHED_INVENTORY_ROOT` |
| ADP measurements and published reports | `data/ai-demand-positioning` | filesystem read source; Airtable stays off unless explicitly enabled |
| Share registries | `config/client-share` | none in this pass |
| Generated reports | `reports/` | none in this pass |

Research rows do not become published when the strict registry has no current rows. `mergePublishedInventoryIntoOpportunities(..., { strictPublishedOnly: true })` returns an empty list.

## Migration, pending approval

`npm run dealality:operational-data-boundary` prints the tracked inventory and does not change Git. `--apply` exits with `migration_not_approved`.

When approval is given:

1. Copy the operational trees outside the repo and record hashes.
2. Set `GDI_DATA_ROOT` and `GDI_PUBLISHED_INVENTORY_ROOT` for local development.
3. Compare hotel ids, publication states, and history against the copy.
4. Stop tracking only those operational paths, and ignore them, after that comparison.
5. Roll back by copying the hash-checked backup onto the original paths.

Railway volume paths and Airtable records stay unchanged.
