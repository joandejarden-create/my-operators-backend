# GDI prospect publication — local implementation summary

**Date:** 2026-10-14  
**Hotels:** Bethesda Marriott (`recLuxvwwxID7U2B8`), The Mayflower Hotel, Autograph Collection (`rec0eTEnCqlsGJJt9`)  
**Production / customer-visible data:** not changed

## What now works (local)

Reviewed, contact-ready prospects from the hotel-review package appear on the existing GDI page for both hotels when the server uses:

- `GDI_OPPORTUNITIES_PERSISTENCE=filesystem`
- `GDI_DATA_ROOT=reports/gdi-prospect-publication-implementation/_local-gdi-data`
- `GDI_REVIEWED_PROSPECT_PUBLICATION_V1=1`

Customer-facing labels render correctly: **Prospect to qualify**, **Exploratory account**, **Hotel sourcing unconfirmed**, **Potential / Organizational contact**, and NDSS **Organization helpline**. Hotel validation feedback saves to the isolated root and survives reimport. Mayflower’s opportunities store was initialized via the canonical GDI repository path (not a parallel store).

## Architecture (smallest compatible change)

| Piece | Role |
|--------|------|
| `prospect-publication-eligibility-v1.js` | Single eligibility SoT (route + rationale + fit + sources + next action) |
| `reviewed-prospect-publication-v1.js` | Scoped hotel allowlist + `REVIEWED_PACKAGE_V1` visibility (does not lower Ready) |
| `reviewed-prospect-import-v1.js` | Idempotent import, Mayflower store init, contact mapping, backups |
| `customer-visibility.js` | Reviewed path before Ready/legacy traps for allowlisted hotels |
| `scripts/gdi-import-reviewed-prospects.mjs` | Dry-run / apply CLI |
| Detail load fix | `loadOpportunityForDetail` skips Airtable when persistence mode is filesystem |
| Detail DTO | Attaches `shareValidation` so feedback shows after reload |
| UI | Phone type label in contact drawer (`Organization helpline`, etc.) |

Ready / QUALIFIED / legacy paths remain unchanged for out-of-scope hotels and records.

## Package eligibility (actual)

| Hotel | Priority facing | Exploratory facing | Enrichment (internal) |
|--------|-----------------|--------------------|------------------------|
| Bethesda | 11 | 2 | 1 (CTN — no usable relevant route) |
| Mayflower | 9 | 2 | 4 exploratory pending contact |

Matches expected package counts. Page totals differ where prior Ready/legacy records remain (Bethesda local page **32** facing; Mayflower **11**, all reviewed).

## Verification completed

- Dry-run → apply → apply×2 (idempotent updates, stable IDs)
- API list + detail from isolated filesystem root
- Feedback POST (local auth bypass) → reload → fields preserved
- Reimport after feedback → validation note/timestamps preserved
- Focused tests: `test-gdi-prospect-publication-eligibility-v1`, `test-gdi-reviewed-prospect-publication-v1` PASS
- Browser screenshots under `screenshots/`

## Remaining local caveats

- Browser Memberstack session JWT can block UI Save (401); authorized save was proven via the same `customer-validation` API with `DEV_AUTH_BYPASS_EMAIL` and no Bearer token, then confirmed visible after reload.
- Default server without `GDI_OPPORTUNITIES_PERSISTENCE=filesystem` still prefers Airtable when configured — production change set must set persistence/config explicitly for import target.

## Production

Concrete change set is ready for approval in `production-change-set.md`. **Not deployed.**
