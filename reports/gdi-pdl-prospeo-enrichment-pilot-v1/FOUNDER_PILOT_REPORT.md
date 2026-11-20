# GDI PDL + Prospeo Controlled Enrichment Pilot — Founder Report

Generated: 2026-11-20T10:10:25.177Z

## Authorization respected

- PDL ≤ 10 successful credits (consumed: **2**)
- Prospeo ≤ 10 email credits (consumed est: **3**; account Δ remaining: **3**)
- Mobile / FullEnrich / Surfe enrich / Apollo: **not used**

## Balances

- Prospeo before: plan=FREE remaining=100 used=0
- Prospeo after: remaining=97 used=3
- PDL: no free balance API; successful-match credits counted = 2

## Metrics

| Measure | Value |
|---|---:|
| PARTIAL → READY | 0 |
| PDL matches | 2 |
| PDL identity accepted | 2 |
| PDL LinkedIn verified | 0 |
| Prospeo named emails accepted | 2 |
| Quarantined | 8 |
| PDL∩Prospeo same email | 0 |
| Surfe input fields improved | 2 |

## Recommendations

- **PDL:** YES_OPTIONAL — use when LinkedIn/employer unresolved after public research; gate on identity_match_accepted
- **Prospeo:** YES_SELECTIVE — email-only after public+PDL miss; keep provider-derived / non-customer until Order permits
- **FullEnrich:** HOLD — not tested; revisit only if PDL+Prospeo miss rate stays high on larger PARTIAL set
- **Surfe Search:** HOLD — zero search credits; Search is org/role discovery not exact-person; Enrich remains on-click
- **Customer-facing:** Public-verified only by default. PDL embedded permission needs counsel check for hotel disclosure. Prospeo standard ToS does not authorize customer disclosure without Schedule 5/Order.

## Persistence

INTERNAL_EVALUATION_REPORT_ONLY — no canonical public-source writes; no STAGED→CURRENT.

## STOP

STOP after ten-contact evaluation. No full-database backfill. No mobile. No Surfe/FullEnrich/Apollo.
