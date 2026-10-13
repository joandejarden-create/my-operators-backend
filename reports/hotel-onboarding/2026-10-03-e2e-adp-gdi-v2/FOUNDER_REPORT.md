# True End-to-End Hotel Onboarding — ADP + GDI V2

**Date:** 2026-10-03  
**Law:** `HI_COMPLETE && ADP_TERMINAL && GDI_TERMINAL`  
**ADP READY does NOT terminate hotel onboarding.**

---

## A. Orchestrator root cause

`onboardHotelIntelligence()` previously set `adpGdiReady: true` whenever the HI completeness gate passed — including the `already_hi_complete` short-circuit. That flag was treated as “ready for ADP/GDI” and, in the multi-hotel sprint, allowed a run to stop after ADP READY while GDI discovery had never executed (YOTEL: `market_first_discovery_not_yet_run`).

### Fix (wired live)

- `lib/hotel-intelligence/onboarding/hotel-e2e-onboarding-state-v1.js` — terminal state model + `buildE2eOnboardingFlags()`
- `lib/hotel-intelligence/onboarding/index.js` — exports e2e state module
- `onboardHotelIntelligence` now uses `buildE2eOnboardingFlags()` on both short-circuit and full paths:
  - `adpGdiReady` always **false** as e2e complete signal (deprecated)
  - `gdiResearchRequired: true` until a current GDI terminal cycle exists
  - `hotelOnboardingComplete` only when HI + ADP terminal + GDI terminal
  - returns `gdiStatus`, `founderVerdict`, `e2eOnboarding`
- Gate test: `node scripts/test-hotel-e2e-onboarding-state-v1.mjs` (7/7 PASS)

---

## B. Cross-hotel status

| Hotel | HI | ADP | GDI terminal | Founder verdict | E2E complete |
|-------|----|-----|--------------|-----------------|--------------|
| YOTEL Geneva Lake | COMPLETE | READY | PROCESSED_PUBLIC_DATA_CEILING | ADP READY — GDI VERIFIED PUBLIC DATA CEILING | YES |
| Spice Island Beach Resort | COMPLETE | READY | PROCESSED_PUBLIC_DATA_CEILING | ADP READY — GDI VERIFIED PUBLIC DATA CEILING | YES |
| AC Hotel A Coruña | COMPLETE | READY | PROCESSED_PUBLIC_DATA_CEILING | ADP READY — GDI VERIFIED PUBLIC DATA CEILING | YES |

---

## C. YOTEL Geneva Lake (`recrPQcZg7SFARRb2`)

| Metric | Value |
|--------|------:|
| GDI discovery run | **YES** (first market-first) |
| Market | La Côte / Nyon / Geneva Airport / Founex corridor |
| Queries | 22 |
| Discovery candidates | 12 |
| Qualified for fit | 12 |
| Customer ready | 0 |
| Future watch | 1 (HOLD_WATCH) |
| Top rejection reasons | INSUFFICIENT_EVIDENCE 11 · NO_LODGING_SIGNAL 6 · OTHER 6 · FUTURE_UNCONFIRMED 1 |
| GDI terminal | **PROCESSED_PUBLIC_DATA_CEILING** |

Geography treated as Founex/Vaud–Nyon–Geneva Airport catchment — not downtown Geneva CBD.

Report: `reports/group-demand-intelligence/yotel-geneva-lake-v1/GDI_FIRST_CYCLE_SUMMARY.json`

---

## D. Spice Island (`recKRJjcPnb4tVDDS`)

| Metric | Value |
|--------|------:|
| Current GDI cycle | **YES** (closure v2; prior evidence reused) |
| Queries | 20 |
| Candidates | 17 |
| Customer ready | 0 |
| Future watch | 78 (includes prior corpus watches) |
| PUBLIC DATA CEILING verified (current cycle) | **YES** |
| GDI terminal | **PROCESSED_PUBLIC_DATA_CEILING** |

Report: `reports/group-demand-intelligence/spice-island-beach-resort-v1/GDI_CURRENT_CLOSURE_V2_SUMMARY.json`

---

## E. AC Hotel A Coruña (`rec2PVBDavppGpenm`)

Prior cycle2 dry-block ≠ completed GDI. Current closure v2 ran apply.

| Metric | Value |
|--------|------:|
| Current GDI cycle | **YES** (closure v2) |
| Queries | 20 |
| Candidates | 11 |
| Customer ready | 0 |
| Future watch | 33 |
| PUBLIC DATA CEILING verified (current cycle) | **YES** |
| GDI terminal | **PROCESSED_PUBLIC_DATA_CEILING** |

Report: `reports/group-demand-intelligence/ac-hotel-a-coruna-v1/GDI_CURRENT_CLOSURE_V2_SUMMARY.json`

---

## F. Controls

| Control | Result |
|---------|--------|
| Bethesda regression | **NO** (bleed assert + no Bethesda hotel writes) |
| GDI thresholds changed | **NO** |
| HI + ADP + GDI required for complete | **YES** |

---

## G. Final cross-hotel verdict

All three hotels now have a **current** GDI processed cycle. None may return “ADP READY — GDI MORE RESEARCH REQUIRED” as a final onboarding verdict without an actual discovery run.
`
