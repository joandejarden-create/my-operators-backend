# Cvent → SerpApi Independent Confirmation — Discovery Value

## Sample
- Challenges tested: 20
- Independently confirmed: 16 (80%)
- Already VIC duplicates: 0
- Non-hotels: 0
- Unresolved / probable: 4
- Identity conflict: 0
- Exact/High-ish identity rate: 19/20
- Avg Golden fields per confirmed: 12.9

## Firewall
- `challenge_origin = Cvent`
- `cvent_used_as_production_evidence = false`
- No Cvent rooms/amenities/address/coords copied into independent record

## Verdict
Cvent coverage universe → SerpApi independent confirmation appears **viable** as a discovery pattern (still needs rights + corroboration for production).
