# Targeted Research Results

Mode: APPLY (Airtable upserted)

## YOTEL — gdi_opp_aidex_geneva_11
- Source: https://aid-expo.com/when-where + https://aid-expo.com/accommodation
- Cost: $0
- Question: Confirm AidEx 2026 dates, venue, and hotel accommodation path for Geneva/Palexpo.
- Result: CONFIRMED: AidEx Geneva 21–22 Oct 2026 at Palexpo Hall 4 (airport). Palexpo Hotel Reservation platform + onsite Ibis/Hilton. YOTEL airport-corridor overflow thesis is plausible; no exclusive room block named.
- Before → After: INVALID/GEO_CONFLICT false-positive → VALID_FUTURE_WATCH candidate (CONTACT/WHO still required for customer-ready)

## YOTEL — gdi_opp_the_changemakers_retreat_cultivating_resilience__5
- Source: https://www.iofc.ch/changemakers-retreat-november-2026
- Cost: $0
- Question: Confirm lodging relationship for Changemakers Retreat Nov 2026.
- Result: PLACED: 5–8 Nov 2026 at Caux Palace with package overnight stays (shared/single room pricing). No third-party hotel overflow path.
- Before → After: INVALID/GEO_CONFLICT → REJECTED — PLACED / NO OVERFLOW

## YOTEL — gdi_opp_summit_2026_10
- Source: https://summit2026.merconsortium.eu/accommodation/
- Cost: $0
- Question: Confirm Summit 2026 host city and lodging geography.
- Result: OUT_OF_MARKET: Université de Bordeaux / Talence (France). Hostel + Nemea blocks in Bordeaux. Not Lake Geneva.
- Before → After: INVALID/GEO_CONFLICT → REJECTED — OUT_OF_MARKET


## Research log (runtime)

```json
{
  "YOTEL": [
    {
      "opportunityId": "gdi_opp_aidex_geneva_11",
      "source": "https://aid-expo.com/when-where + https://aid-expo.com/accommodation",
      "costUsd": 0,
      "result": "CONFIRMED: AidEx Geneva 21–22 Oct 2026 at Palexpo Hall 4 (airport). Palexpo Hotel Reservation platform + onsite Ibis/Hilton. YOTEL airport-corridor overflow thesis is plausible; no exclusive room block named.",
      "statusBefore": "INVALID/GEO_CONFLICT false-positive",
      "statusAfter": "VALID_FUTURE_WATCH candidate (CONTACT/WHO still required for customer-ready)",
      "readinessBefore": "DQ",
      "readinessAfter": "DQ",
      "customerReadyAfter": false,
      "applied": true
    },
    {
      "opportunityId": "gdi_opp_the_changemakers_retreat_cultivating_resilience__5",
      "source": "https://www.iofc.ch/changemakers-retreat-november-2026",
      "costUsd": 0,
      "result": "PLACED: 5–8 Nov 2026 at Caux Palace with package overnight stays (shared/single room pricing). No third-party hotel overflow path.",
      "statusBefore": "INVALID/GEO_CONFLICT",
      "statusAfter": "REJECTED — PLACED / NO OVERFLOW",
      "readinessBefore": "DQ",
      "readinessAfter": "DQ",
      "customerReadyAfter": false,
      "applied": true
    },
    {
      "opportunityId": "gdi_opp_summit_2026_10",
      "source": "https://summit2026.merconsortium.eu/accommodation/",
      "costUsd": 0,
      "result": "OUT_OF_MARKET: Université de Bordeaux / Talence (France). Hostel + Nemea blocks in Bordeaux. Not Lake Geneva.",
      "statusBefore": "INVALID/GEO_CONFLICT",
      "statusAfter": "REJECTED — OUT_OF_MARKET",
      "readinessBefore": "DQ",
      "readinessAfter": "DQ",
      "customerReadyAfter": false,
      "applied": true
    }
  ],
  "SPICE": [],
  "AC": []
}
```
