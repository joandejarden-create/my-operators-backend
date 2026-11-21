# Track B — Weekly automation readiness (NOT ACTIVATED)

**Status:** Design-ready · scheduling **OFF**  
**Publication:** CUSTOMER_BLOCKED  
**Prerequisite:** Historical foundation reconciled + validated before unattended runs.

## Incremental signals the foundation supports

| Signal | Detection basis |
|--------|-----------------|
| New hotels | New `Discovery Property ID` / HPC identity absent from prior inventory snapshot |
| New contracts | New finding rows with agreement evidence + source URL |
| Contract amendments | `CURRENT_STATUS_CLASS.AGREEMENT_EXTENDED_OR_AMENDED` + chronology events |
| Brand changes | `BRAND_CHANGED` + prior brand preserved in event history |
| Operator changes | `OPERATOR_CHANGED` |
| New commercial events | Chronology append (never erase prior belief) |
| Opportunity timing changes | Confirmed decision dates vs EXPIRATION_ONLY_PROXY |
| Superseded records | `SUPERSEDED` + superseding event link |
| Source freshness | Filing / press vintage on findings |
| Geographic coverage changes | Country×Timing matrix delta (`hpcHotels`, contractual events) |

## Required weekly inputs (future)

1. Idempotent inventory + findings upsert (Track B persist batch pattern proven: CREATE → NOOP).
2. Country×Timing×Gap matrix regeneration from HPC + contractual ledgers.
3. Track A reserved-ID coordination (Lakeville and future Track A discoveries).
4. HOLD/STAGED portfolio items remain out of hotel inventory until hotel-level IDs exist.

## Explicitly deferred

- Unattended cron / scheduler enablement
- Metered research without founder authorization
- Customer publication
- Schema mutations
