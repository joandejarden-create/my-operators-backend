# Temporal Affiliation V1

Implemented in `lib/research-engine-v2/clean-census/temporal-affiliation.js` (temporal-affiliation-v1).

## Period fields

brand · parent · affiliation_start · affiliation_end · current · evidence · evidence_date · confidence

## Date precision

- `exact`
- `as_of` → "As of [date]"
- `before` → "Before [date]"
- `unknown`

Wave 1C seeds **current** affiliation as `As of [discovery date]` without fabricating earlier start dates.

Records seeded: 68
