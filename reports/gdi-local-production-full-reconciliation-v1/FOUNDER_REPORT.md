# Bethesda GDI — Local vs External Full Reconciliation

**Date:** 2026-11-13  
**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Client-ready certificate:** **DENIED**

---

## Executive verdict

Production and the founder-facing “approved” experience diverge because they are not on the same immutable package.

| Layer | Version | Fingerprint | Lifecycle |
|-------|---------|-------------|-----------|
| **Live production CURRENT** | `…_remediated_v1` | `9816d7d9…` | CUSTOMER_PUBLISHED (you marked superseded) |
| Prior founder-review candidate | `…_founder_review_v1` | `60e197ba…` | FOUNDER_REVIEWED — **no FOUNDER_APPROVED receipt** |
| **Proposed successor (STAGED)** | `…_reconciliation_v1` | `441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f` | PRODUCTION_STAGED — **awaits fingerprint approval** |

**Publication blocker:** `FOUNDER_FINGERPRINT_APPROVAL_REQUIRED`  
Do **not** CUSTOMER_PUBLISH `44177321…` until you explicitly approve that exact fingerprint.  
Do **not** treat `60e197ba…` as approved.

---

## 1. Discrepancies found

1. **Commercial recommendations** — 16 opportunities differ (`Monitor` on live `9816d7d9` vs `Qualify Opportunity` on reconciliation), including Bethesda Premier Cup and Potomac Memorial.
2. **Hotel Need-Date Match** — Missing on production package; present on live founder UI / reconciliation (Premier Cup: November 2026 confirmed overlap). Root cause: content contract omitted need-date fields; package build never attached need-date intelligence; PACKAGE serve correctly refuses live recompute.
3. **Drawer section sets** — Screenshots with Need-Date Match vs What-Is-Happening-only reflect package omission vs live attach, not two unrelated UIs.
4. **Opportunity Type “(4)” vs “(5)”** — Consistent with **filter/quick-filter user state**, not inventory. Package has 12 type labels. Reset Market Scope / Sales Opportunities / Type before comparing.
5. **Fingerprint split** — Local staging advanced; production CURRENT remained on superseded remediated package.

## 2. Root causes (architecture)

Research → live list attaches need dates + commercial resolver → founder UI.  
Share CURRENT package → frozen DTO without need dates → customer UI.

Once published, share must not recompute. Therefore need dates and commercial actions must be **frozen into the package**.

## 3. Source files corrected (this PR)

- `lib/.../gdi-customer-publication-package-v1.js` — expand `PUBLICATION_CONTENT_FIELDS` (need-date + drawer); durable JSON fingerprint
- `scripts/gdi-build-customer-publication-package-v1.mjs` — attach need dates before freeze
- `lib/.../gdi-share-approved-package-resolver-v1.js` — founder preview resolver (CURRENT‖STAGED)
- `api/group-demand-intelligence.js` — `?publicationPackage=1` / `surface=customer` serves package
- `scripts/test-gdi-publication-parity-e2e.mjs` + `npm run test:gdi-publication-parity-e2e`
- Staged package: `…_reconciliation_v1` (not published)

## 4. Filter-only differences

Toolbar Market Scope / Opportunity Type / Sales Opportunities category / sort — reset to identical state before DOM compare. Not a package defect.

## 5–7. Proposed publication identity

- **Version:** `gdi_pub_recLuxvwwxID7U2B8_20261113_reconciliation_v1`
- **Fingerprint:** `441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f`
- **Inventory:** 67 / 61 / 10  
- **Quality blocking:** 0  
- **Need-date frozen:** 67/67  
- **Premier Cup:** Qualify Opportunity + November 2026 need-date match (timing ≠ open room block)

## 8–9. GitHub / Railway

- PR #240 merge SHA `014011c44…` is an ancestor of current main (`2eb283eb6…`).
- Architecture PR for this reconciliation will merge next; **CUSTOMER_PUBLISHED pointer will not switch** until fingerprint approval.
- Live Railway still serves `9816d7d9…` until explicitly replaced.

## 10. Production parity tests

- `npm run test:gdi-publication-parity-e2e` **PASS** against reconciliation package (self + negatives).
- Live production vs reconciliation: **not certified** (fingerprint mismatch by design until publish).

## 11. Remaining blockers

1. Explicit founder approval of fingerprint `44177321…`.
2. Then: set FOUNDER_APPROVED → CUSTOMER_PUBLISHED CURRENT → redeploy bundled package → browser certify 67/67.

## 12. External share URL (unchanged token)

https://my-operators-backend-production.up.railway.app/group-demand-intelligence-share.html?share=gdishare.v1.eyJ2IjoxLCJ0aWQiOiJnZGlzaHRfNDdjMjVkNzRjNzkyMTYwMjFmYjM2MTUwIiwiaG90ZWxJZCI6InJlY0x1eHZ3d3hJRDdVMkI4Iiwic3VyZmFjZXMiOlsiYnJpZWYiLCJvcHBvcnR1bml0aWVzIiwib3Bwb3J0dW5pdHlfZGV0YWlsIiwic3VtbWFyeSJdLCJtb2RlIjoicmVhZF9vbmx5IiwiaWF0IjoxNzg5MjE0OTMyLCJleHAiOjE3OTg2NzUyMDB9.DcmVDLtAQD_XkAvgI8mn6ir8YREmAwJ0uG3VOQXY-gc

---

## Required founder decision

Reply with exact approval:

`Approve Bethesda publication fingerprint 441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f`

Only then will CURRENT switch and production be re-certified.
