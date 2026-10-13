# Public Data Ceiling QA

## YOTEL
- PUBLIC_DATA_CEILING_VERIFIED: **NO**
- Queries/fetches/candidates: {"queries":22,"fetches":32,"candidates":12}
- WHO/contact attempted: false
- Follow-up research: true
- Missing: who_contact_not_attempted
- Note: Ceiling claim incomplete: who_contact_not_attempted

## SPICE
- PUBLIC_DATA_CEILING_VERIFIED: **NO**
- Queries/fetches/candidates: {"queries":20,"fetches":34,"candidates":17}
- WHO/contact attempted: false
- Follow-up research: false
- Missing: who_contact_not_attempted
- Note: Ceiling claim incomplete: who_contact_not_attempted

## AC
- PUBLIC_DATA_CEILING_VERIFIED: **NO**
- Queries/fetches/candidates: {"queries":20,"fetches":28,"candidates":11}
- WHO/contact attempted: false
- Follow-up research: false
- Missing: who_contact_not_attempted
- Note: Ceiling claim incomplete: who_contact_not_attempted


## Interpretation

Closure scripts set `publicDataCeilingVerified: true` while **contact was skipped** for Spice/AC and YOTEL contact was null. Watch counts (78 / 33) were **inflated** by summing HOLD_WATCH + priorWatch + bag filter — not valid Future Watch law.

Ceiling may still be directionally true for **customer-ready yield from public web**, but the verified flag is **NO** until WHO/contact pass or an explicit documented waiver, and until watch counts are quality-gated.
