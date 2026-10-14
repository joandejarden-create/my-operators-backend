# Browser verification — GDI prospect publication

**Executed:** 2026-10-14  
**Server:** `http://localhost:8091` with isolated `GDI_DATA_ROOT` + `GDI_OPPORTUNITIES_PERSISTENCE=filesystem`  
**Status:** EXECUTED (both hotels)

## Preview routes

- Bethesda: http://localhost:8091/group-demand-intelligence.html?hotelId=recLuxvwwxID7U2B8&embed=1
- Mayflower: http://localhost:8091/group-demand-intelligence.html?hotelId=rec0eTEnCqlsGJJt9&embed=1

## Screenshots

| File | Content |
|------|---------|
| `screenshots/01-bethesda-list.png` | Bethesda list — 32 opportunities; Prospect to qualify + contact phones |
| `screenshots/02-bethesda-ndss-sourcing.png` | NDSS detail — **Hotel sourcing unconfirmed** |
| `screenshots/03-bethesda-ndss-contact.png` | NDSS contact — Amy Drow, email, **Organization helpline** |
| `screenshots/04-bethesda-feedback-after-reload.png` | Hotel validation prefilled after reload (Already known / Worth pursuing now) |
| `screenshots/05-mayflower-list.png` | Mayflower list — 11 opportunities; 9 Prospect / 2 Exploratory |
| `screenshots/06-mayflower-ndss-expanded.png` | Mayflower NDSS expanded — sourcing + helpline |

## Checks

| Check | Bethesda | Mayflower |
|-------|----------|-----------|
| List renders reviewed prospects | Pass (13 reviewed in 32) | Pass (11/11) |
| Prospect / Exploratory labels | Pass | Pass |
| Contact email/phone on cards | Pass | Pass |
| Organization helpline (NDSS) | Pass | Pass |
| Hotel sourcing unconfirmed in detail | Pass | Pass |
| Feedback visible after reload | Pass | N/A (Bethesda NDSS used for write path) |
| Enrichment-only rows absent from list | Pass (CTN) | Pass (4 exploratory) |
| Cross-hotel isolation | Pass (selector switches hotels) | Pass |

## Notes / blockers for UI Save button

In-browser **Save** with an existing Memberstack session JWT returned “Please log in again.” Authorized write was executed against the same `customer-validation` endpoint without a Bearer token under `DEV_AUTH_BYPASS_EMAIL=dealalitydemo@dealality.com`, then confirmed in the UI after reload. Production will use normal authenticated hotel users.

## Production data

Not modified. Browser pointed only at local server + isolated data root.
