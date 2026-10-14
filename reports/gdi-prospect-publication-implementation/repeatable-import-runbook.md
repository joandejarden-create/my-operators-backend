# Repeatable reviewed-prospect import runbook

This phase proves **import → eligibility → rendering → feedback preservation**. It does not prove automated discovery/enrichment for new hotels.

## Input schema

Package directory (default: `reports/gdi-bethesda-process-replication/hotel-review/`):

- Prospect register / review cards (versioned package)
- `contact-packages.json` (person, method, business phone, phone type, verification dates)
- Shared eligibility via `prospect-publication-eligibility-v1`

Importer identity: stable account + cohort/occasion within each hotel (NADO 2027 and 2028 stay separate occasions).

## Commands (same path for both hotels)

Set an isolated root for local proof:

```powershell
$dataRoot = Resolve-Path "reports/gdi-prospect-publication-implementation/_local-gdi-data"
```

### Dry-run

```powershell
node scripts/gdi-import-reviewed-prospects.mjs --hotel recLuxvwwxID7U2B8 --dry-run --data-root $dataRoot
node scripts/gdi-import-reviewed-prospects.mjs --hotel rec0eTEnCqlsGJJt9 --dry-run --data-root $dataRoot
# or
node scripts/gdi-import-reviewed-prospects.mjs --both --dry-run --data-root $dataRoot
```

### Local apply

```powershell
node scripts/gdi-import-reviewed-prospects.mjs --both --apply --data-root $dataRoot
```

Outputs: `import-manifest.csv`, `import-result-*.json`, backups under `backups/`.

### Local server preview

```powershell
$env:GDI_DATA_ROOT = $dataRoot
$env:GDI_OPPORTUNITIES_PERSISTENCE = "filesystem"
$env:GDI_REVIEWED_PROSPECT_PUBLICATION_V1 = "1"
$env:GROUP_DEMAND_INTELLIGENCE_PILOT_READ = "1"
$env:PORT = "8091"
node server.js
```

Open:

- `/group-demand-intelligence.html?hotelId=recLuxvwwxID7U2B8&embed=1`
- `/group-demand-intelligence.html?hotelId=rec0eTEnCqlsGJJt9&embed=1`

## Resume interrupted work

1. Inspect latest `import-result-*.json` and `backups/` for the hotel.
2. Re-run `--dry-run` then `--apply` with the same `--data-root` and package dir.
3. Matching uses known IDs / card IDs — safe to resume; does not delete unmatched records.

## Rerun without duplication

Re-apply is idempotent: matched rows update in place; IDs stay stable; no second copy of NADO 2027/2028.

## Refresh research without erasing hotel feedback

- Importer does **not** replace entire hotel lists.
- Does **not** clear `share-validation.json` / feedback stores.
- Does **not** silently overwrite newer user-edited fields when conflict rules fire (see conflict reporting in import result JSON).
- Re-run after packaging contact updates; feedback remains.

## Rollback (this import only)

Restore files from `reports/gdi-prospect-publication-implementation/backups/` for the hotel’s `opportunities.json` / `profile.json` timestamp pair written by that apply.
