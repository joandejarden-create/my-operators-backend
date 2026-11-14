# Sheraton Guadalajara Expo — Provisional Need Dates, SVG & CMI V1

**Hotel:** Sheraton Guadalajara Expo (`recCAGqkERSXgiku4`)  
**Template:** `GDI_FULL_E2E_HOTEL_TEMPLATE_V1`  
**New staged fingerprint:** `9a75e3e993ce4df03b56b11a9ca4b49473e403f4b913adddc7797790846895f6`  
**Publication version:** `gdi_pub_recCAGqkERSXgiku4_20261114_need_dates_svg_cmi_v1`  
**Customer published:** No  
**Paid spend this run:** $0

## Baseline (why Hotel 0 vs Hotel 62)

| Observation | Cause |
|---|---|
| Hotel (0) / empty Sales chips | Package or UI state missing `inventoryPopulations` / sales quick-filter attach (historical defect path) |
| Hotel (62) / Competitive (0) | Archived Exa historical-recovery candidate `3c5f0948…` — **not** the live STAGED pointer |
| Live STAGED before this run | Expiration-gate cleanup `967d63ff…` — **43** opportunities, Competitive previously over-flagged at 10 |

This run does **not** silently fall back to `3c5f0948…` and does **not** overwrite archived V1/V2/Exa packages.

## Provisional need dates

- **9** provisional windows (2027–2028) researched from Guadalajara seasonality + Expo calendar context.
- Labeled **Provisional Need Dates — Awaiting Hotel Confirmation** (shared `MARKET_ESTIMATED` status).
- Airtable: all rows `Is Test Scenario = true`, `Status = Test Scenario`, `Source = PROVISIONAL_INTERNAL_PLANNING_SCENARIO_SHERATON_GDL_V1`, **no** `Last Confirmed By Hotel`.
- Local market-estimate store used for matching (segregated from hotel-confirmed calendar).
- Need-Date Match: **4** opportunity associations (2 confirmed-period overlaps + 2 ongoing-account potentials). All marked provisional — **never Hotel Confirmed**.

## SVG revenue-stream repair

**Root cause:** packages kept `revenueStreamClass` + Sales Opportunities keys but dropped frozen `revenueStreamIcons` / thesis prose; read-time consistency demoted every icon to muted.

**Fix:** restore icons from evidence-backed class + sales keys (shared attach path). After staging: **43** guestroom-active, **25** catering-active — no cosmetic invention.

## Competitive Market Intelligence

- **Competitive set established** (Hilton Expo/WTC, Westin, Real Inn Expo, Barceló, AC Expo, Crowne Plaza Plaza del Sol) from public Expo-allied / corridor sources.
- Prior `competitiveMarketScopeEligible=true` on 10 paid Discovery Expo-capture rows **cleared** — Expo hotel capture ≠ CMI eligibility.
- **Competitive count = 0** after honest audit (acceptable). Named competitor strings flagged `NEEDS_FURTHER_VERIFICATION` only; no fabricated competitor bookings.

## Founder preview verification

`http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=recCAGqkERSXgiku4&preview=staged`

| Check | Result |
|---|---|
| Fingerprint | `9a75e3e9…` matched |
| Hotel / Competitive | 43 / 0 |
| Need status | Provisional Need Dates — Awaiting Hotel Confirmation (9 estimates) |
| Guest / catering SVGs | 43 / 25 active |
| Need-date matches | 4 |
| Diagnostic banner | None |
| CUSTOMER_PUBLISHED | Not activated |

## Remaining gaps (not FULL E2E COMPLETE)

1. Hotel sales/RM must confirm or reject every provisional window.
2. June–August Expo / medical congress holds may invalidate soft-period assumptions.
3. No verified competitor-hosted CMI rows yet — further bounded research may be needed.
4. Core opportunity Airtable upsert still requires separate founder authorization (need periods/matches only were written here).

## Artifacts

All files under `reports/gdi-sheraton-need-dates-svg-cmi-v1/` (`01`–`16` + screenshots).
