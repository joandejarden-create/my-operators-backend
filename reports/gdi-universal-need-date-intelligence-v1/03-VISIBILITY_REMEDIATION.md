# GDI Universal Need-Date Visibility Remediation V1

**Version:** `GDI_UNIVERSAL_NEED_DATE_VISIBILITY_REMEDIATION_V1`  
**Branch:** `feat/gdi-universal-need-date-intelligence-v1`  
**PR:** #152 (remains open)

## Root cause

1. **Production / published Spice report** still runs pre-PR presentation assets. Hotel status is computed live by API + rendered by `propertyBarHtml`; without deploying this branch’s JS/CSS/API, founders cannot see the control on Railway or published customer links.
2. **Presentation omission risk:** `propertyBarHtml` previously rendered the Need Dates strip only when `hotelNeedDateConfigV1` was truthy. A null/failed DTO hid the entire control. Remediation always renders with `NOT_ESTABLISHED` fallback.
3. **Share load path:** opportunities response was accepted only when `data.opportunities` was truthy in a weak way; now requires `Array.isArray` and always applies the DTO (or client normalize fallback).
4. **Audit labels** used nested `config/profile` fields that `listGdiSelectableHotels()` does not return — hotel names appeared as record IDs. Fixed to `displayName` / `hotelName`.
5. **Local Spice inventory** can show 0 opportunities when published-inventory registry env is off; status still renders. Empty inventory ≠ missing Need Dates status.

## Spice Island acceptance

- Hotel: Spice Island Beach Resort (`recKRJjcPnb4tVDDS`)
- Status: `NOT_ESTABLISHED`
- Icon: gray calendar-minus
- Accessible label: `Need dates not established`
- Legend: clickable info control with full definitions
- No fabricated market estimates or occupancy gaps

## Local vs production visibility

| Surface | Visibility after this commit |
| --- | --- |
| Local authenticated GDI (`/group-demand-intelligence.html`) on this branch | **Visible** (verified in browser) |
| Local share preview | Code path wired; active share tokens may be inactive in this worktree without production registry volume |
| Published customer report on Railway | **Not visible until presentation/API deploy** of this branch. No opportunity republish required for hotel-config status (live compute). Market estimates remain demoted to Not Established on share. |

## Screenshots (actual report UI)

Under `screenshots/visibility-remediation/`:

- `spice-island-local-not-established.png`
- `spice-island-legend-open.png`
- `spice-island-mobile-layout.png`
- `bethesda-local-hotel-confirmed.png`
- `waterstone-local-market-estimated.png`
- `ac-coruna-local-not-established.png`

## Remaining publication / deployment requirements

- Founder-authorized Railway deploy of this PR’s presentation + API code (auto-deploy stays disabled until authorized).
- No customer republish required solely for hotel need-date configuration icons.
- Do not inject unpublished market estimates into published/share surfaces.
- PR #151 and #152 must not be merged by this agent.
