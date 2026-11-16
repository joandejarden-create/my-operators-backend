# REGRESSION QA — Period-family resolver

Gate: `npm run test:adp-period-family-resolution-v1` — PASS

| Property | loadLatestCustomerPeriod is Core (non-Loyalty)? | Status |
|----------|-----------------------------------------------|--------|
| Bethesda Marriott | YES | PASS |
| Waterstone Resort & Marina (`adp_waterstone_boca_raton`) | YES | PASS |
| Mayflower Hotel Washington DC | YES | PASS |
| Wyndham San Diego Bayside | YES | PASS |
| YOTEL Geneva Lake | YES | PASS |
| Nobu London Portman | YES | PASS |
| Radisson Blu Toronto Downtown | YES (`c8bd8b`, not Loyalty `308ba1`) | PASS |

Toronto Loyalty remains separately addressable via `loadLatestLoyaltyPeriod` → `…308ba1`.
