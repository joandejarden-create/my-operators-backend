# GDI Canonical PowerShell Operational Certification

Status: **PARTIAL — INTEGRATED_ON_8080 — AWAITING_FOUNDER_REVIEW**

Not claimed: `GDI_CANONICAL_POWERSHELL_SUITE_OPERATIONALLY_CERTIFIED`

## Why PARTIAL

DryRun/fixture path certified for canonical runners; Live orchestrator is wired and fail-closed tested, but an authorized-spend hotel-agnostic Live Full E2E run + Findymail Live + clean reconstruction remain unproven.

## Outstanding

- UNIFIED_V3_LIVE_WIRING_CLOSED — runFullE2eV3Live + CLI Live path fail-closed tested; AUTHORIZED_SPEND_LIVE_PROOF still required for operational certification
- FINDYMAIL_LIVE_HOTEL_AGNOSTIC_PATH_PARTIAL — Live Findymail wired through Stage 4 (key + authorization required); not yet exercised with credits this cycle
- CONTACT_CHANNEL_GAPS_REMAIN — email NOT_YET=1 (Gutierrez Findymail-eligible), telephone=14, LinkedIn=12; see reports/gdi-close-e2e-cert-gaps-v1/
- BUYING_AUTHORITY_ANNOTATED_IN_MEMORY — Airtable persistence + STAGED rematerialize outstanding
- CLEAN_AIRTABLE_FIELD_PARITY_PASS_AUTHORITY_PERSISTENCE_OUTSTANDING
- WEEKLY_REFRESH_LIVE_NOT_RE_EXECUTED_THIS_CYCLE
- NO_FRESH_LIVE_HOTEL_RUN_THIS_CYCLE — propose scope+budget before Live certification

## Commands (DryRun)

```powershell
pwsh -File scripts/gdi/Invoke-GdiFullE2E.ps1 -HotelId <rec...> -Mode DryRun
pwsh -File scripts/gdi/Invoke-GdiValidateFullE2E.ps1 -HotelId <rec...> -RequirePort8080
pwsh -File scripts/gdi/Invoke-GdiContactResearch.ps1 -HotelId <rec...> -Mode DryRun
```
