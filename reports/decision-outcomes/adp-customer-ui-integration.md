# ADP customer UI — Decision & Outcome integration

**Date:** 2026-09-17  
**Status:** Customer feedback layer added on Executive Priority Actions

## What shipped

On each actionable priority action card (`#adpExecActions`):

- **Hotel Feedback**
  - Response (AGREE / DISAGREE / ALREADY_ADDRESSING / …)
  - Action (ADP action enums)
  - Outcome (ADP outcome enums + Pending)
- Save → `ensure-adp` → Validation / Action / Outcome events

## Save path

1. `POST /api/hotels/:hotelId/decisions/ensure-adp`  
   - `hotelId` resolved to **HPC canonical** via `resolveCanonicalHotelId` (ADP `adp_*` → `rec…`)
2. `POST /api/decisions/:decisionId/validations|actions|outcomes`
3. Readback via subject decision route

## Canonical base

- Base: `appa2cE7FTRmIbB32`
- Tables: Decisions `tblulPWvEmd3iiDhJ`, Decision Events `tblLL1CuKpvI43DtB`
- No ADP-specific outcome store

## Files

- `public/js/ai-demand-positioning/ai-demand-positioning.js` — UI + save
- `public/js/ai-demand-positioning/ai-demand-positioning.css` — feedback styles
- `api/decision-outcomes.js` — ensure-adp canonical hotel resolve

## Not redesigned

ADP report layout, measurement methodology, and research engine are unchanged.
