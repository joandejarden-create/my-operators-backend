# GDI Universal Need-Date Intelligence V1 — Implementation Report

**Version:** `GDI_UNIVERSAL_NEED_DATE_INTELLIGENCE_V1`  
**Branch:** `feat/gdi-universal-need-date-intelligence-v1`  
**Generated:** 2026-11-06

## Scope delivered

1. Preserved existing five opportunity/revenue SVG icons and colors (bed, utensils, calendar-check, calendar-clock, calendar-search).
2. Added hotel-level need-date configuration statuses: `HOTEL_CONFIRMED`, `MARKET_ESTIMATED`, `NOT_ESTABLISHED`.
3. Applied status consistently across all selectable GDI hotels (18/18 coverage).
4. Evidence-gated market estimates stored separately from hotel-confirmed calendars; never auto-promoted; never yield unqualified verified date matches.
5. Clickable Need-Date Intelligence info legend (Radar-style definitions).
6. Existing opportunity-level need-date matching preserved (tile icon eligibility + provenance guard).
7. Integrated through existing GDI presentation / calendar / share architecture (`hotelNeedDateConfigV1`).
8. Tests, coverage audit, and before/after UI screenshots produced.
9. No Railway deploy, no customer publish, no production Airtable writes, no paid research, no share-token changes.

## Hotel coverage

| Status | Count |
| --- | ---: |
| Total selectable GDI hotels | 18 |
| HOTEL_CONFIRMED | 1 |
| MARKET_ESTIMATED | 1 |
| NOT_ESTABLISHED | 16 |

Demo seeds (local filesystem only): Bethesda North Marriott (`recLuxvwwxID7U2B8`) confirmed periods; Waterstone (`recgMYovrrZDJMqzX`) market-estimated period. Remaining hotels correctly resolve to `NOT_ESTABLISHED` when evidence is insufficient.

## Tests

| Gate | Result |
| --- | --- |
| `npm run test:gdi-universal-need-date-intelligence-v1` | 33/33 pass |
| `npm run test:gdi-universal-need-date-tile-icons-v1` | 21 checks, 0 failures |
| `node scripts/test-gdi-universal-need-date-icons-cross-hotel-rollout-v1.mjs` | pass |
| `npm run gdi-universal-need-date-coverage-audit` | ok |

## Screenshots

- `screenshots/before-need-dates-heading-only.png` — baseline Need Dates heading without hotel status/legend
- `screenshots/after-three-hotel-statuses.png` — Confirmed / Market Estimated / Not Established
- `screenshots/after-statuses-and-legend-open.png` — info legend open with definitions

Fixtures: `ui-fixture-before.html`, `ui-fixture-statuses.html`

## Remaining issues (non-blocking)

- Most hotels remain `NOT_ESTABLISHED` until evidence-backed market estimates or hotel-confirmed calendars are authorized per property.
- Coverage audit table currently keys by hotel record id; display-name enrichment can follow without changing status logic.
- Local demo seed JSON is for visual/status verification only — not published customer inventory.
