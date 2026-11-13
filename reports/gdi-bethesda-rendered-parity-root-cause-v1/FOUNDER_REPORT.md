# Bethesda GDI — Rendered Parity Root Cause (Certificate Invalidation)

## Status: NOT CERTIFIED (pending post-deploy DOM verification)

Prior **CLIENT READY** certification is **INVALIDATED**. See `01-PREVIOUS-CERTIFICATE-INVALIDATION.json` and `reports/gdi-bethesda-final-parity-release-v1/15-CERTIFICATE-INVALIDATION.json`.

Approved fingerprint **preserved** (no content change):

`441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f`

---

## 1. Why the previous certification falsely passed

Tests compared the **share LIST** API (and/or package DTO fields) to the approved package.

They did **not**:

- Call `getGdiShareOpportunityDetail`
- Open drawers in a browser and extract **DOM section headings**
- Compare founder preview (live need-date attach) to share detail (live reconstruct without package)

Fingerprint match proved package identity on the list path only — not rendered drawer parity.

## 2. Exact cause of missing Hotel Need-Date Match

| Layer | Behavior |
|---|---|
| Approved package | Contains `needDateMatchV1` for Premier Cup (November 2026 overlap) |
| Share LIST | Serves PACKAGE → field present |
| Share DETAIL (bug) | Loaded live `loadOppDoc` / reconstruct; **omitted** `needDateMatchV1` |
| Drawer UI | `openDetail()` fetches DETAIL and replaces list row → section skipped |
| Renderer | `needDateMatchDrawerSectionHtml` correctly returns `""` when block absent |
| Local founder UI | Auth detail runs `attachHotelNeedDateTilePreviews` → section **visible** |

Pre-fix live probe: `listHasNeedDate: true`, `detailHasNeedDate: false` (same fingerprint).

## 3. Other section-level differences

Any conditional drawer section frozen only in the package and not recomputed on the live detail path was at risk (Need-Date Match first). Full 67 DOM audit runs after deploy (`04`/`05` artifacts).

## 4. Was the approved package incomplete?

**No.** Package already freezes `needDateMatchV1` in `PUBLICATION_CONTENT_FIELDS`. Technical serve-path fix only.

## 5. Files changed

See `08-CODE-FIXES-AND-REGRESSIONS.json`.

## 6. Fingerprint preserved?

**Yes** — `44177321…`

## 7–12. Post-merge / deploy

Updated in `09-GITHUB-MERGE-AND-DEPLOYMENT.json` and `10-INDEPENDENT-PRODUCTION-VERIFICATION.json` after Railway cutover + Premier Cup DOM test.

---

**Verdict until DOM gates pass: NOT CERTIFIED**
