# Cvent Data Minimization Design

## Preferred post-resolution retention
Retain only:
- challenge_id
- challenge_outcome (BOTH / CVENT_ONLY_RESOLVED / UNRESOLVED)
- audit_timestamp
- matching_status

Do **not** retain Cvent factual hotel content (rooms, address, amenities, descriptions).

## This run
No deletions. Freeze retains minimum match fields (`_match_name_slug`, country) for audit only.

## Production evidence
`cvent_used_as_production_evidence = false` always.
