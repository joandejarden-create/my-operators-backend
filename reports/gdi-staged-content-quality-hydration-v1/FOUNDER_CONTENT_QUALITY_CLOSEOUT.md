# GDI STAGED Content Quality Hydration — Founder Closeout

**Status:** PARTIAL — Wyndham mandatory hydration gate PASS; residual WHO/HOW & product-wide gaps remain  
**STAGED version:** `gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261127_60abf8da`  
**Lifecycle:** FULL E2E → STAGED (CURRENT unchanged)  
**Auth:** `FOUNDER_GO AUTHORIZE_WYNDHAM_STAGED_CONTENT_HYDRATION_REMATERIALIZE`

## 1. Exact root cause (Wyndham quality)

Not UI-only. Locked-26 staging used slim dossier titles (research sentences) and omitted `opportunityTypeFilterCategory` / narrative / WHO-HOW hydration for the 15 additions. Missing taxonomy caused Association Conference fallbacks. Market Scope / Demand Type columns were empty when baked labels were absent.

## 2. Corrections applied (existing evidence only)

- Evidence-backed titles (Saronic, Seasats, dual Bora, Stacy Witbeck, Turner, ACG, …)
- Demand classification from segment → filter category (Scaling / Projects / Relocation / Association / Local)
- Narrative + contact hydration from queue dossiers + CLEAN11 + QA write-set
- Permanent gate `gdi-content-hydration-presentation-gate-v1` wired into `Invoke-GdiValidateFullE2E.ps1`
- List Demand Type prefers `opportunityTypeFilterLabel`

## 3. Gate results

| Metric | Before | After |
|---|---:|---:|
| Blocking failures | 15 | **0** |
| Certification | BLOCKED | **CONTENT_HYDRATION_PASS** |
| Research-paragraph titles on :8080 | many | **0** |

## 4. Remaining blockers (PARTIAL)

1. Abbreviated names (`Lily Y.`, `Suzanne K.`) — do not invent; need authorized contact research  
2. Weak CLEAN11 titles (`Events` / gala descriptors) still present as Title Not Verified — honest gap  
3. `Hotel Sourcing Not Confirmed (11)` / unverified hotel-demand statements retained — founder disposition required (do not silent-downgrade)  
4. Product-wide hotels audited only (Bethesda freeze preserved; no unauthorized remediation)  
5. No paid research executed under this instruction  

## 5. Commands

```powershell
.\scripts\gdi\Invoke-GdiContentHydrationValidate.ps1 -HotelId recnPB3Q02NO8Ma1o -RequirePort8080
.\scripts\gdi\Invoke-GdiValidateFullE2E.ps1 -HotelId recnPB3Q02NO8Ma1o
npm run test:gdi-content-hydration-presentation-gate-v1
npm run report:gdi-content-hydration-audit-v1
node scripts/gdi-wyndham-content-hydration-rematerialize-v1.mjs --dry-run
```

## 6. Non-regression

- Same 26 opportunity IDs  
- Bethesda STAGED/CURRENT unchanged  
- Wyndham CURRENT still null  
- No Airtable / no CURRENT / no paid research  

