# Shadow Mode Design (proposal only — NOT built)

## Flow

Scheduled Cohort → Official Directory Check → Contradiction Research → Temporal Claims → Proposed Corrections → Cross-Table Integrity Queue → Human Review → Existing Dealality Validation Gates → Optional Approved Write

## Cadence

- Daily: IHG + Marriott soft brands Mexico/CALA status + gaps (delta only)
- Weekly: Choice/Radisson Individuals Americas gap scan
- Monthly: Hilton GraphQL status audit for census codes

## Brands first

1. Hotel Indigo + Kimpton  
2. Tribute / Autograph  
3. Radisson Individuals Americas  
4. Hilton (code-backed)

## Fields

status, Affiliation, Parent Company (review), Missing Census Candidate — **never auto-write**

## Alert thresholds

- Exact/High + High corroboration → Proposed queue
- Medium → Review digest
- Suppress repeat alerts for same hotel+field+observed value for 30 days

## Evidence retention

Store claim JSON + URL + retrieval timestamp + match level (90 days hot, 1 year cold)

## Cost

~$0 incremental (public directory fetches). Steward time: ~15–30 min/day digest.

## Webhound boundary

Native: routine status, affiliation freshness, missing directory records, basic cross-table.  
Webhound: periodic blind validation, gov/project discovery, opaque ownership, long-tail sources.
