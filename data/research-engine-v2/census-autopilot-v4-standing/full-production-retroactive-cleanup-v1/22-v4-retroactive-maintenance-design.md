# V4 Retroactive Maintenance Design

V4 must operate **forward** (new hotels) and **retroactive** (existing Census).

## Queues (persistent)

ADDRESS_RESEARCH · CITY_RESEARCH · STATE_RESEARCH · MARKET_REGISTRY · SUBMARKET_RESEARCH · COORDINATE_RESEARCH · PHONE_RESEARCH · ROOMS_VALIDATION · CURRENT_AFFILIATION_REVIEW · RIGHTS_BLOCKED · STEWARD_REVIEW

## Behavior

1. New writes pass semantic gates (no Country→Market, no parent→Brand, no object Address).
2. Incomplete existing hotels remain in queues until verified / exhausted / N/A / steward.
3. When a new adapter/source becomes available, Autopilot re-visits queued hotels.
4. Systemic defect → circuit break; Legitimate Unknown → write safe property / queue gap.
