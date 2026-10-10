# FOUNDER REPORT — GDI Pursuit Workflow V1

Generated: 2026-10-07  
Hotels: AC Hotel A Coruña · Radisson Santo Domingo

## FINAL VERDICT

Pursuit is now a **separate sales workflow** from GDI Ready / Watch intelligence. Five pursuits were created with Spanish outreach drafts. Ready counts unchanged (AC 0, RAD 0, Bethesda 28, YOTEL 24). Watch remains Watch.

## What shipped

| Capability | Status |
|------------|--------|
| Pursuit domain model | YES |
| Pursuit / inclusion / response enums | YES |
| Filesystem store + audit trail | YES |
| API (list/get/start/patch/response/outcome/follow-up) | YES |
| Watch Start / View Pursuit | YES |
| Ready shares same Pursuit entity | YES |
| Workflow filters (Active / Follow-up / Hotel Selection / Closed) | YES |
| Hotel-supplied evidence provenance | YES |
| Auto-promote Ready from pursuit? | **NO** |

## Five pursuits

| Key | Status | Contact |
|-----|--------|---------|
| BioCultura | OUTREACH_READY | expositores@vidasana.org |
| IAPS | PREPARE | ricardo.garcia.mira@udc.es |
| RIF | OUTREACH_READY | adofil333@gmail.com |
| CIELO | OUTREACH_READY | congresocielo6@gmail.com |
| AUTOAMERICAS | OUTREACH_READY | acaballero@autoamericas.show |

## Critical separation

- **GDI readiness** = evidence strength (Ready / Watch gates unchanged)
- **Pursuit status** = what the hotel is doing about it

Sending outreach does **not** make an opportunity Ready.

## Next product steps (optional)

1. Browser QA on AC + Radisson with live auth
2. Optional Decision Events dual-write on CONTACTED / HOTEL_INCLUDED
3. Notifications when FOLLOW_UP_DUE (not in this pass)
