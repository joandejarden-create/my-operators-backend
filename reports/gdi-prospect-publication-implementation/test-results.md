# Test results — GDI prospect publication

**Date:** 2026-10-14  
**Isolated data root:** `reports/gdi-prospect-publication-implementation/_local-gdi-data`

## Focused unit / integration tests

| Command | Result |
|---------|--------|
| `node scripts/test-gdi-prospect-publication-eligibility-v1.mjs` | PASS |
| `node scripts/test-gdi-reviewed-prospect-publication-v1.mjs` | PASS |

## Import sequence

| Step | Result |
|------|--------|
| Dry-run (`--both`) | Manifest written; no store mutation |
| First apply | Bethesda updates matched by known IDs; Mayflower store + profile initialized; package rows created/updated |
| Second apply | Idempotent `update` / unchanged IDs; no duplicates |
| Reimport after feedback | Opportunity IDs stable; `share-validation.json` preserved (`ALREADY_KNOWN` / `WORTH_PURSUING_NOW` / note intact) |

Package hashes (register / packages):

- `registerHash`: `f5f348d67f26a06e5337f511175c6b1e65d42f64f2b04a4ef7bfcccf90bf1c3e`
- `packagesHash`: `b5ff071ab8e97050710553330d89d62a578854ecc3b71456f45819665bb70be3`

## API (filesystem persistence)

Server env: `GDI_OPPORTUNITIES_PERSISTENCE=filesystem`, `GDI_DATA_ROOT=…/_local-gdi-data`, `GDI_REVIEWED_PROSPECT_PUBLICATION_V1=1`.

| Hotel | HTTP | Facing count | Reviewed facing | Priority | Exploratory |
|-------|------|--------------|-----------------|----------|-------------|
| Bethesda | 200 | 32 | 13 | 11 | 2 |
| Mayflower | 200 | 11 | 11 | 9 | 2 |

Detail source after fix: `full_doc` (not `airtable_single`) when persistence is filesystem.

NDSS detail labels: `Hotel sourcing unconfirmed`, `Prospect to qualify`, phone `Organization helpline`, email `adrow@ndss.org`.

## Feedback

| Check | Result |
|-------|--------|
| POST `/customer-validation` (local bypass, no Bearer) | 200 |
| Detail returns `shareValidation` | Yes |
| Visible after page reload | Yes (Already known / Worth pursuing now / note) |
| Commercial status not auto-confirmed | Yes (prospect labels retained; note states not confirmed demand) |
| Reimport preserves feedback | Yes |

## Enrichment queue exclusion

- Bethesda CTN: stored, `prospectHotelFacing=false`, not in hotel action list.
- Mayflower four exploratory enrichment rows: stored, not customer-facing.

## Variance vs expected package counts

None for hotel-facing package eligibility. Bethesda page total (32) > package facing (13) because pre-existing Ready/legacy records remain and are not duplicated.
