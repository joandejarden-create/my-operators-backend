# SerpApi Call Reduction Plan (V2.2)

## Prior forecasts
- Full-universe (old): **14301**
- V2.1 minimized: **12400**

## V2.2 levers
1. Official-first discovery for native/partial families before paid calls
2. Property ID capture to avoid re-research
3. Dealality SerpApi research cache
4. Expected-value gate (skip phone-only / low-gain)
5. One-call stop when search root already has property details
6. Field-gap routing (never Rooms via SerpApi)
7. Candidate dedupe to property_identity_id

## New forecast
**9560** searches (−23% vs V2.1 minimized; −33% vs old full)

{
  "new_confirmation_after_official_and_ev": 9381,
  "existing_non_rooms_gaps": 179,
  "official_absorb_estimate": 674,
  "serpapi_candidates_before_ev": 10978
}
