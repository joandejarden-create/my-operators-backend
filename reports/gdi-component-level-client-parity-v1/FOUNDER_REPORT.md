# GDI Component-Level Client Parity V1

**Verdict: CLIENT READY**

**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Fingerprint (unchanged):** `441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f`  
**Package:** `gdi_pub_recLuxvwwxID7U2B8_20261113_reconciliation_v1`  
**Certified surface:** local customer-share preview ↔ Railway customer-share

## Release

| Item | Value |
|------|-------|
| PR | [#256](https://github.com/joandejarden-create/my-operators-backend/pull/256) merged |
| Merge / artifact SHA | `5f89019ba3d666c054446733cf8588522cc66e94` |
| GHA deploy | [run 43437226582](https://github.com/joandejarden-create/my-operators-backend/actions/runs/43437226582) |
| Railway deployment | `b0ec577b-4251-47e6-bc05-86d3dcaa2b0a` |
| Production UI marker | `dealality-gdi-ui.js?v=gdi-component-parity-v1` |

## Independent post-deploy verification

| Check | Result |
|-------|--------|
| Fingerprint both sides | PASS |
| Counts 67 / 61 / 10 | PASS |
| Contact DTO (67) | 0 mismatches |
| SVG/icon DTO (67) | 0 mismatches |
| Commercial-action DTO (67) | 0 mismatches |
| Rendered UI cards (contact / SVG / action) | 0 mismatches |
| `test:gdi-component-level-client-parity` | CLIENT READY |
| `test:gdi-complete-release-parity` (assets + list + UI; tour skipped this pass) | CLIENT READY |
| `test:gdi-opportunity-card-quality-v1` | PASS |

## Lorna Kipfer / Regulatory Information Conference

Production and local share both render:

- **Name:** Lorna Kipfer  
- **Role:** RIC Conference Contact (`professional_title`)  
- **Not** Group Travel Coordinator  
- **Not** `U.s.` mangling on org strings when the long title is shown elsewhere

## Root causes fixed

1. **CONTACT_MAPPING_DIFFERENCE** — Keep assigned `* Conference Contact` titles; attach `tileContactPreview` on PACKAGE list/detail; preserve dotted acronyms (`U.S.`) in Proper Case.
2. **SVG_STATE_DIFFERENCE (auth vs share screenshots)** — Auth-live gold icons come from live sanitize `revenueStreamClass`. Approved PACKAGE rows lack class/thesis → muted icons on share. Share↔share matched; inventing COMBINED was rejected (would need a new fingerprint to publish class into the package).
3. **COMMERCIAL_ACTION_DIFFERENCE** — None on share↔share.
4. **DEPLOYED_ASSET_DIFFERENCE** — Cleared after Railway served `gdi-component-parity-v1`.

## Intentional non-defects

Authenticated GDI **without** `?publicationPackage=1` may still recompute live-store commercial/icon fields. That is a permission/data-path difference, not a customer-share defect. Customer-facing certification is share ↔ share on the approved fingerprint.

## Report index

- `01-SVG-ICON-PARITY.json` … `10-FINAL-PARITY-CERTIFICATE.json`
- Screenshots under `screenshots/`
