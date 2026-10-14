# Production change set — GDI reviewed prospect publication

**Status:** Ready for approval — **not deployed / not applied to production**  
**Date prepared:** 2026-10-14

## 1. Code & configuration to deploy

### Code

- `lib/group-demand-intelligence/prospect-publication-eligibility-v1.js` (contract SoT)
- `lib/group-demand-intelligence/reviewed-prospect-publication-v1.js`
- `lib/group-demand-intelligence/reviewed-prospect-import-v1.js`
- `lib/group-demand-intelligence/customer-visibility.js` (reviewed path)
- `lib/group-demand-intelligence/feature-flag.js` (`GDI_REVIEWED_PROSPECT_PUBLICATION_V1`)
- `lib/group-demand-intelligence/opportunity-list-dto.js` / UI labels in `dealality-gdi-ui.js`
- `api/group-demand-intelligence.js` (filesystem-aware detail load; attach `shareValidation`)
- `config/group-demand-intelligence/hotels/rec0eTEnCqlsGJJt9.json` (Mayflower registration)
- `scripts/gdi-import-reviewed-prospects.mjs`
- Focused tests: `scripts/test-gdi-prospect-publication-eligibility-v1.mjs`, `scripts/test-gdi-reviewed-prospect-publication-v1.mjs`

### Runtime configuration

| Variable | Production intent |
|----------|-------------------|
| `GDI_REVIEWED_PROSPECT_PUBLICATION_V1` | `1` to enable scoped reviewed path |
| `GDI_REVIEWED_PROSPECT_HOTEL_IDS` | Optional override; default allowlist = Bethesda + Mayflower |
| Persistence | Keep existing production Airtable (or declared mode); **do not** point production at the local `_local-gdi-data` root |

## 2. Package identities

| Artifact | Hash |
|----------|------|
| Register | `f5f348d67f26a06e5337f511175c6b1e65d42f64f2b04a4ef7bfcccf90bf1c3e` |
| Contact packages | `b5ff071ab8e97050710553330d89d62a578854ecc3b71456f45819665bb70be3` |

### Expected package counts (eligibility)

| Hotel | Priority facing | Exploratory facing | Enrichment (internal only) |
|-------|-----------------|--------------------|----------------------------|
| Bethesda `recLuxvwwxID7U2B8` | 11 | 2 | 1 (CTN) |
| Mayflower `rec0eTEnCqlsGJJt9` | 9 | 2 | 4 |

Record-level create/update/unchanged/conflict: see `import-manifest.csv` (local apply used as template). **Re-run dry-run against live production before apply** — local manifests must not overwrite newer production facts.

## 3. Preflight (required before production apply)

1. Deploy code + flags; confirm other hotels’ GDI pages unchanged.
2. Fresh production pull/compare for both hotel opportunity docs vs local backup baseline.
3. Dry-run import against production data root / Airtable mode with **no write**:
   ```bash
   node scripts/gdi-import-reviewed-prospects.mjs --both --dry-run
   ```
4. Review dry-run conflicts (user-edited fields, newer verification dates).
5. Confirm Mayflower hotel config present; initialize store only via supported path if still empty.
6. Backup production opportunity + validation docs for both hotels.
7. Confirm `GDI_DATA_ROOT` is **unset** (or production path) so import does not hit the local proof folder.

## 4. Deployment / import sequence

1. Deploy application build with reviewed-prospect code.
2. Enable `GDI_REVIEWED_PROSPECT_PUBLICATION_V1=1` for the deployment environment.
3. Production dry-run; approve manifest.
4. Apply per hotel (or `--both`) with backups enabled.
5. Immediate read-back: opportunities API facing counts + sample NDSS/CMSS contact fields.
6. Browser check both preview/auth hotel pages (labels, helpline, sourcing).
7. Spot-check feedback write on one record; confirm commercial status not auto-confirmed.

## 5. Scoped rollback

1. Disable `GDI_REVIEWED_PROSPECT_PUBLICATION_V1` (hides reviewed path; Ready/legacy unchanged).
2. Restore opportunity docs from pre-import backup for the two hotels only.
3. Restore share-validation / feedback if import touched them (importer should not wipe feedback; restore only if a mistaken apply did).
4. Do not bulk-delete unmatched historical records.

## 6. Post-publication checks

- Bethesda: ≥11 priority + 2 exploratory reviewed labels; CTN absent from action list.
- Mayflower: 9 + 2 facing; enrichment four absent from list.
- No duplicate IDs; NADO 2027 ≠ 2028.
- Print/export retains sourcing uncertainty language.
- Unrelated hotels: no behavior change.

## 7. Explicit non-goals of this change set

- No outreach send.
- No Parallel lab mutation.
- No lowering Ready / inventing housing-control evidence.
- No global surface-active bypass.
