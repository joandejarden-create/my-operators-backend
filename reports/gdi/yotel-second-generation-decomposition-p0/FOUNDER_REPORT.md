# FOUNDER REPORT — YOTEL Second-Generation Decomposition P0

**Date:** 2026-10-05  
**Engine:** `gdi_yotel_second_generation_decomposition_p0`  
**Live path:** `runHotelDemandCampaignDecompositions` + research-orchestrator hook  
**Apify:** NO · **Jev:** KEEP_SHADOW · **Thresholds:** unchanged

## Verdict

PASS_SECOND_GEN_WIRED — named participating accounts admitted on live path; Ready standard unchanged; Apify unused

## What changed

Second-generation decomposition is now on the **live** YOTEL campaign path:

`EVENT / GENERATOR → official participant universe → named organization → traveling entity → buyer function → hotel motion → Ready/Watch`

Organizer/venue shells are blocked or held at SIGNAL_ONLY before RESEARCH_LEAD. Participation alone is not enough — traveling entity proof is required.

## Headline metrics

| Metric | Value |
|---|---|
| Target campaigns | 7 |
| Official-list accounts identified | 24 |
| Named participating admitted | 20 (83.3%) |
| Traveling entities proven | 19 (95.0%) |
| Venue/organizer placeholders blocked | 9 |
| Buyer roles resolved | 20 (100.0%) |
| Relevant contact paths | 16 (80.0%) |
| Complete STRONG / PLAUSIBLE | 12 / 0 |
| Customer Ready before → after | 6 → 6 |
| New actionable Ready | 0 |
| Highest Ready yield campaign | Geneva Health Forum 2026 |
| Highest complete-packet campaign | Watches & Wonders Geneva 2027 |

## Campaign notes

- **Art Genève / Watches & Wonders / GHF:** official-list expansion accounts (galleries, exhibitors, EFA) admitted when traveling entity proven.
- **AI for Good:** 2026 partner list upgraded with traveling-entity stamps; ITU organizer held SIGNAL_ONLY.
- **WHO EB / WHA / ECOSOC HAS:** PUBLIC_DATA_CEILING — secretariat SIGNAL_ONLY; no invented member-state accounts.

## Guards

- APIFY USED? **NO**
- ORPHANS? **NO**
- DUPLICATES? **NO**
- BETHESDA REGRESSION? **YES**
- READY STANDARD LOWERED? **NO**
- PROTECTED READY LOST: NONE

## Top remaining blockers

- why_now_contradicts_ready:10
- surface_eligibility:8
- ORGANIZER_AS_ACCOUNT_WITHOUT_HOUSING_ROLE:6
- force_signal_local_or_secretariat:3
- VENUE_AS_ACCOUNT:3

## Final top blocker

why_now_contradicts_ready:10
