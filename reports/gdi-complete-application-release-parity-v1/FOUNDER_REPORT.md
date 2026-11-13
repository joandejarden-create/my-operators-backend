# GDI Complete Application Release Parity — Founder Report

## Status: NOT CERTIFIED (awaiting production deploy + independent verification)

Technical root causes identified and fixed in working branch. **Do not treat CLIENT READY until Railway serves the merge SHA and full client-app parity gates pass against production.**

Approved publication fingerprint **preserved**:

`441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f`

---

## 1. Why past certifications missed application differences

1. Fingerprint / LIST / drawer content gates ≠ frontend chrome parity  
2. Auth GDI compared to external share (different shells)  
3. Windows CRLF vs LF false asset “mismatches”  
4. PACKAGE share path omitted `hotelNeedDateConfigV1`  
5. Share Reset View hardcoded without purple active-filter badge  
6. Screenshots compared unequal filter states (`All types (61)` vs `(4)`)

## 2–4. Frontend / UI mismatches and sources

| Difference | Cause | Fix |
|---|---|---|
| Need Dates: Hotel Confirmed vs Not Established | PACKAGE early-return omitted `hotelNeedDateConfigV1`; share UI defaulted to NOT_ESTABLISHED | Attach live hotel-config status on PACKAGE share list (`api/group-demand-intelligence.js`) |
| Reset View purple badge missing on share | Share used hardcoded button; auth used `resetViewButtonHtml` | `share-app.js` now calls shared helper |
| Asset hash noise | CRLF working tree vs LF production | EOL-normalized identity script |
| All types 61 vs 4 | Facet subset size under other active filters (Hotel≈61; Priority/Sales can shrink to 4) | Documented contract — not a hardcode |
| Auth vs share shell | Admin HCP / Need Times tabs only on auth HTML | Certify **share↔share**, not auth↔share |

## 5. Files corrected

- `api/group-demand-intelligence.js` — PACKAGE `hotelNeedDateConfigV1`  
- `public/js/group-demand-intelligence/share-app.js` — Reset View badge parity  
- `scripts/gdi-frontend-asset-identity-v1.mjs`  
- `scripts/test-gdi-full-client-app-parity-v1.mjs`  
- `scripts/test-gdi-share-package-hotel-need-date-config-v1.mjs`

## 6. Outstanding before CLIENT READY

- Merge to main (merge commit)  
- Deploy artifact to Railway  
- Re-run asset identity (expect only intentional post-merge equality)  
- `npm run test:gdi-full-client-app-parity` against local share + production  
- Confirm production Need Dates = Hotel Confirmed  
- Confirm 67 unique cards / drawers still pass

## 7–13. Identity / evidence

See `01`–`13` JSON artifacts in this folder as gates complete.
