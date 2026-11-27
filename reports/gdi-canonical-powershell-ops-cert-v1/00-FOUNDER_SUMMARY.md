# GDI Canonical PowerShell Operational Certification

Status: **PARTIAL — INTEGRATED_ON_8080 — AWAITING_FOUNDER_REVIEW**

Not claimed: `GDI_CANONICAL_POWERSHELL_SUITE_OPERATIONALLY_CERTIFIED`

## Why PARTIAL

DryRun/fixture path certified for canonical runners; unified Live Full E2E + hotel-agnostic Findymail Live + clean reconstruction remain open.

## Outstanding

- UNIFIED_V3_LIVE_ORCHESTRATOR_GAP — generic stage CLI refuses Live paid (exit 3)
- FINDYMAIL_LIVE_HOTEL_AGNOSTIC_PATH_PARTIAL — Live Findymail still partially tied to Wyndham gold-standard hydrate helpers
- CLEAN_AIRTABLE_ONLY_RECONSTRUCTION_PARTIAL
- WEEKLY_REFRESH_LIVE_NOT_RE_EXECUTED_THIS_CYCLE
- NO_FRESH_LIVE_HOTEL_RUN_THIS_CYCLE — propose scope+budget before Live certification

## Commands (DryRun)

```powershell
pwsh -File scripts/gdi/Invoke-GdiFullE2E.ps1 -HotelId <rec...> -Mode DryRun
pwsh -File scripts/gdi/Invoke-GdiValidateFullE2E.ps1 -HotelId <rec...> -RequirePort8080
pwsh -File scripts/gdi/Invoke-GdiContactResearch.ps1 -HotelId <rec...> -Mode DryRun
```
