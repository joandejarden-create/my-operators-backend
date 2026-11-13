# GDI Component-Level Client Parity V1

**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Fingerprint (unchanged):** `441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f`  
**Package:** `gdi_pub_recLuxvwwxID7U2B8_20261113_reconciliation_v1`  
**Certified surface:** local customer-share preview ↔ Railway customer-share (not auth-live research browse)

## Verdict (pre-deploy baseline in this pack)

**NOT CERTIFIED** until this release is merged and deployed to Railway, then independently re-verified. Pre-deploy local already applies the fixes below; production still runs the prior tip, so contact DTO/UI still diverge until deploy.

## Root causes (exact)

| Classification | Cause | Fix |
|----------------|-------|-----|
| CONTACT_MAPPING_DIFFERENCE | `extractVerifiedProfessionalTitle` demoted assigned roles like `RIC Conference Contact` to opportunity-role `Group Travel Coordinator` on live sanitize paths; PACKAGE share omitted `tileContactPreview`, so the client fell back to long `primaryContact.title` (and Proper-Cased `U.S.` → `U.s.`). | Accept `* Conference Contact` titles; attach `tileContactPreview` on PACKAGE list/detail via `attachPackageCustomerPresentation`; preserve dotted acronyms in `toProperCaseContactRoleLabel`. |
| SVG_STATE_DIFFERENCE | Auth-live screenshots showed gold icons from live sanitize `revenueStreamClass`; PACKAGE rows lack class/thesis, so share correctly renders muted. Inventing COMBINED at read-time would be unapproved content. | Do **not** invent icons on thin package rows. Share↔share SVG states already match (muted/muted) when both use PACKAGE. Auth-live gold vs share muted is **PERMISSION/DATA path**, not a share defect. Publishing class into the package requires a **new fingerprint**. |
| COMMERCIAL_ACTION_DIFFERENCE | None on share↔share for the 67 (actions already matched). | No change. |
| DEPLOYED_ASSET_DIFFERENCE | Production missing `gdi-component-parity-v1` UI cache + presentation attach until deploy. | Ship + Railway release. |

## Lorna Kipfer / Regulatory Information Conference

| Surface | Contact role shown |
|---------|-------------------|
| Local share (post-fix) | `Lorna Kipfer` / `RIC Conference Contact` |
| Production share (pre-deploy) | `Lorna Kipfer` / `RIC Conference Contact — U.s. Nuclear Regulatory…` (client fallback + Proper Case bug) |
| Auth live without `publicationPackage=1` | May still differ if live store recomputes — out of certified scope |

Evidence-supported title: `RIC Conference Contact` (professional_title). Org remains on `primaryContact.professionalTitle` full string; tile uses short verified title, not invented `Group Travel Coordinator`.

## Code changed

- `lib/group-demand-intelligence/universal-tile-contact-preview-v1.js`
- `lib/group-demand-intelligence/publication-package/gdi-package-customer-presentation-attach-v1.js` (new)
- `api/group-demand-intelligence.js` (PACKAGE list/detail attach)
- `public/js/group-demand-intelligence/dealality-gdi-ui.js` (U.S. Proper Case)
- `public/group-demand-intelligence-share.html` / `public/group-demand-intelligence.html` (cache `gdi-component-parity-v1`)
- `scripts/test-gdi-component-level-client-parity-v1.mjs` + `npm run test:gdi-component-level-client-parity`
- `scripts/test-gdi-complete-release-parity-v1.mjs` (per-card contact/SVG/action)
- `scripts/test-gdi-opportunity-card-quality-v1.mjs` (RIC / U.S. / package attach fixtures)

## Pre-deploy comparison summary

| Check | Result |
|-------|--------|
| Fingerprint both sides | PASS |
| Counts 67/67 | PASS |
| Commercial-action DTO | 0 mismatches |
| SVG/icon DTO | 0 mismatches (both thin / muted) |
| Contact DTO | 67 mismatches (local has `tileContactPreview`; prod does not yet) |
| UI card contacts | ~36 mismatches (same root cause + `U.s.` casing) |

## Post-deploy requirement

1. Merge PR → deploy merge SHA via Dealality production release workflow.  
2. Confirm Railway artifact SHA.  
3. Re-run `npm run test:gdi-component-level-client-parity` and `npm run test:gdi-complete-release-parity`.  
4. Only then set `10-FINAL-PARITY-CERTIFICATE.json` verdict to **CLIENT READY** if zero unexplained material differences.

## Report index

- `01-SVG-ICON-PARITY.json`
- `02-CONTACT-PARITY.json`
- `03-COMMERCIAL-ACTION-PARITY.json`
- `04-CARD-DOM-AND-CSS-PARITY.json`
- `05-COMPLETE-UI-COMPONENT-PARITY.json`
- `06-ROOT-CAUSE-REGISTER.json`
- `07-REGRESSION-TEST-RESULTS.json`
- `08-GITHUB-AND-RAILWAY-RELEASE.json`
- `09-INDEPENDENT-PRODUCTION-VERIFICATION.json`
- `10-FINAL-PARITY-CERTIFICATE.json`
