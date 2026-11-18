# Airtable 659 reconciliation

| Layer | Count | Notes |
|-------|------:|-------|
| Post-#371 Airtable research | 659 | `tblbkDj0vhQLfrssV` baseline |
| Clean HEAD filesystem | 659 | matches backfill reconciliation |
| Working-tree filesystem | 686 | Bethesda local +27 (not on HEAD) |
| Prepared Airtable creates | 27 | dry-run manifest only |
| Prepared updates | 0 | |
| GDO writes | 0 | |
| Publication writes | 0 | |

## Material differences vs Airtable

Only the **27 Bethesda** composite keys present in local WT `opportunities.json` but absent from the #371 allowlist/Airtable baseline. See `NEW_OR_CHANGED_RESEARCH_MANIFEST.json`.

Idempotency expectation if only the baseline 659 is dry-run: **659 UNCHANGED** (certified by #371; not re-applied here).

## Research-state WIP vs inventory

27 `hist_*`/`disc_*` research-state files do **not** introduce 27 Airtable opportunities by filename.

