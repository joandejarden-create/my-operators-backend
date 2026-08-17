# Autopilot Integration Design (recommendation only — NOT implemented)

## Recommendation: DO_NOT_INTEGRATE

### If limited fields
Lane B after official brand/page ladder for:
- Address
- Latitude / Longitude
- Property Type (mapped)
- Amenities (Yes-only from tokens; never No from absence)

Gate: Exact/High match + country/city constraints.

Never: Rooms/Keys, Owner, Operator, Opening, Meetings, F&B counts, images to production.

### Hierarchy challenge
Official structured > official page > approved geocode of official address > StayingAPI Exact/High.

Benchmark should confirm whether StayingAPI address quality rivals directory for IHG gaps.
