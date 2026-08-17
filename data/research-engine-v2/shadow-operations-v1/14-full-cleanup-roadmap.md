# Full Cleanup Roadmap (not executed)

Estimates from local census extract (~4k MX rows; CALA larger) + BE factory posture.

## Wave 1 — Highest-value CALA / Mexico majors (IHG first)
- Hotel Indigo, Kimpton, then Crowne/Staybridge/etc. as adapters allow
- Daily shadow already covers Indigo/Kimpton MX
- Expected batches: ~5–8 of 40–100 hotels
- Human review: ~15–30 min/day on P0/P1

## Wave 2 — Remaining supported major families
- Marriott soft brands (Tribute/Autograph/AC) via directory
- Choice / Radisson Individuals Americas sitemap
- Hilton code-backed GraphQL (identity backfill first for ctyhocn gaps)
- Batches: ~10–15 weekly

## Wave 3 — Inactive brand activation cohort
- Under Review / Draft / census-without-BE (Avani-class)
- 3–5 brands/week via activation mode → steward queue
- Never auto-activate

## Wave 4 — Long-tail / Webhound escalation
- Bot-blocked, opaque ownership, gov/project, no-directory brands
- Only with explicit WH authorization + budget

## Rough burden
| Area | Scale (order-of-magnitude) | WH escalation share |
|------|----------------------------|---------------------|
| Active BE maintenance brands | dozens | low (~5–10%) |
| Inactive/Under Review | dozens | medium (~20–30%) |
| Census validation CALA | thousands | low if directory-backed; higher for independents |
| Image remediation | hundreds of candidates | mostly manual (no safe auto-write) |
