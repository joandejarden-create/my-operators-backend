# Bethesda GDI — Rendered Parity Root Cause & Re-Certification

## Verdict: RENDERED PARITY CERTIFIED (CLIENT READY)

Prior **CLIENT READY** from fingerprint/LIST-only gates is **INVALIDATED** (append-only; not deleted).  
After technical fixes that **preserve** fingerprint `441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f`, the external customer share was re-proven with full browser DOM section parity (67/67).

---

## 1. Why the previous certification falsely passed

Tests compared the **share LIST** API / package DTO fields to the approved package.

They did **not**:

- Call `getGdiShareOpportunityDetail` for every opportunity
- Open drawers in a browser and extract **DOM section headings in order**
- Catch conditional sections gated by `needDateMatchV1` on the DETAIL payload

Fingerprint match proved package identity on the list path — not rendered drawer parity.

## 2. Exact cause of missing Hotel Need-Date Match

| Layer | Behavior |
|---|---|
| Approved package | Contains `needDateMatchV1` for Premier Cup (November 2026 overlap) |
| Share LIST | Serves PACKAGE → field present |
| Share DETAIL (bug) | Loaded live reconstruct; **omitted** `needDateMatchV1` |
| Drawer UI | `openDetail()` fetches DETAIL and replaces list row → section skipped |
| Renderer | `needDateMatchDrawerSectionHtml` correctly returns `""` when block absent |
| Local founder UI | Auth detail attaches live need-date → section **visible** |

**Secondary crash after DETAIL fix:** package DTOs hit `whoShouldSalesContactHtml` → undefined `prose()` → drawer error. Fixed in PR #247.

## 3. Other section-level differences (all 67)

Post-fix browser DOM audit (`04-ALL-67-SECTION-PARITY.json`):

- **0** material failures
- **39/39** package need-date rows render `Hotel Need-Date Match`
- **0** unexpected need-date sections
- No missing/extra controlled sections vs package contract for conditional need-date

Premier Cup order: Opportunity overview → **Hotel Need-Date Match** → What is happening → …

## 4. Was the approved package incomplete?

**No.** Package already freezes `needDateMatchV1` in publication content fields. Technical serve-path + UI helper fixes only. **Fingerprint preserved.**

## 5. Files changed

See `08-CODE-FIXES-AND-REGRESSIONS.json` (resolver, share DETAIL, `prose()`, DOM regressions).

## 6. Fingerprint preserved?

**Yes** — `441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f`

## 7. All 67 card/drawer results

- Cards/inventory: 67 / Hotel 61 / Competitive 10  
- Drawer DOM: **67/67 OK**, `materialFailures: 0`  
- Artifacts: `04-ALL-67-SECTION-PARITY.json`, `05-COMPLETE-DOM-TEXT-PARITY.json`, `06-NEED-DATE-PARITY.json`

## 8. New regression evidence

- `scripts/test-gdi-premier-cup-need-date-drawer-dom-v1.mjs` — browser `#gdiShareDrawer` Need-Date body fields  
- `scripts/test-gdi-bethesda-complete-drawer-dom-parity-v1.mjs` — all 67 DOM section/need-date gates  
- `scripts/test-gdi-share-detail-publication-package-v1.mjs` — LIST≡DETAIL package fields

## 9. PR and merge SHA

| PR | Merge |
|---|---|
| #246 DETAIL package resolution | `3db8b2b34` |
| #247 `prose()` drawer crash | `54fd044273a3c193ae3a73205efff16a52dd43ce` |

## 10. Railway

- GHA: `43415593132` (control plane `release/gha-deploy-detached-head-guard-v1`)  
- Approved artifact SHA: `54fd044273a3c193ae3a73205efff16a52dd43ce`  
- Deployment: `e7bed4e9-9ccd-4aba-ad73-ba7f62f83283` **SUCCESS**

## 11. External share

`https://my-operators-backend-production.up.railway.app/group-demand-intelligence-share.html?share=<GDI_BETHESDA_PRODUCTION_SHARE_TOKEN>`  
Status: **CUSTOMER_PUBLISHED** — rendered Need-Date Match confirmed on Premier Cup and 39/39 package need-date rows.

## 12. Remaining limitations

- Certified surface = **external customer share** vs approved package DOM contract.  
- Founder auth preview **without** `?publicationPackage=1` may still overlay live need-dates; certified founder preview mode is `publicationPackage=1`.  
- Do not treat fingerprint match alone as CLIENT READY going forward.

---

**Revised certificate:** `11-REVISED-PUBLICATION-CERTIFICATE.json`
