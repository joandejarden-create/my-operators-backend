# GDI drawer contact-card parity — founder results

## Root cause

**DTO / projection gap on publication-package + locked customer-facing detail paths** — not a CSS redesign gap and not proven as cache-alone.

- Approved comparison applies `applyUnifiedWhoToContactStandard({ force: true })`, so the drawer receives `scalingTwoContactPresentation` and renders the structured Primary Contact card.
- Main / external GDI package + locked paths returned `primaryContact` without building `scalingTwoContactPresentation`.
- Client `whoShouldSalesContactHtml` then fell back to the older plain-text Who to Contact block.
- Cache-bust tags on main HTML were also behind comparison (`gdi-pdi-3-v1` / `gdi-svg-icon-color-restore-v1` vs `gdi-drawer-cmp-v11-contact-reveal`), which could amplify stale asset risk after deploy — secondary, not the primary miss.

## Classification

| Hypothesis | Result |
|---|---|
| Stale cached CSS/JS alone | Not sufficient; production detail API lacked two-contact DTO |
| Incorrect asset version refs | Contributing; bumped to `gdi-drawer-contact-card-parity-v1` |
| Different renderer branch | Same `dealality-gdi-ui.js` renderer; DTO branch differed |
| Older contact presentation DTO | **Primary** — package/locked skipped unified who-to-contact |
| CSS specificity / missing styles | Styles already present for `.gdi-contact-slot` |
| Sanitization stripping fields | Package path skipped sanitize entirely |
| Locked publication bypass | Locked path used hydrate-only prepare without contact card DTO |
| Wrong deploy commit | Will re-verify after IC merge + release |

## Fix

1. `attachPackageCustomerPresentation` — apply `applyUnifiedWhoToContactStandard({ force: true })` (read-time; package bytes unchanged).
2. `prepareCustomerFacingOpportunity` — same hydration for locked / published snapshots.
3. `ensureDrawerContactParityPresentation` on auth + share detail paths — same public-channel recovery used by comparison RIGHT (no Surfe), then rebuild two-contact DTO.
4. Production host refuses Surfe mock fixture values even if mock env is accidentally set.
5. Cache-bust main + share GDI assets to `?v=gdi-drawer-contact-card-parity-v1`.
6. Regression: `npm run test:gdi-drawer-contact-card-parity-v1` (includes EUROGUSS live public recovery).

## EUROGUSS / Gissella data path

| Layer | Result |
|---|---|
| Publication package `primaryContact` | Name/title/org only — **no** email/phone in locked snapshot |
| Pre-fix production detail API | `hasTwo=false`; plain-text drawer fallback |
| Public source recovery (euroguss-mexico.com/contacts) | `gissella.palma@nmna.us` + `+1 (770) 618-5833` |
| Airtable research-inventory readback from this workstation | **Blocked** (`INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND` on configured token/base) — treat as separate persistence verification, not drawer render |

## Mock data

Fixture values `alex.buyer@fixture.example` / `+1-202-555-0199` remain session-only mock provider output and are blocked on production customer hosts.

## Next

PR → CI → Integration Controller merge → deploy → authenticated external visual parity → **STOP for founder visual review**. No all-hotel Airtable batches until external parity is confirmed.
