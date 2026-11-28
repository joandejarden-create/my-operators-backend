# Airtable schema authorization request — Inventory Membership

**Status:** PROPOSED — awaiting founder authorization  
**No live schema or data writes performed in this assignment.**

## Base / table

- **Base:** AI Demand Positioning / GDI ops — `appa2cE7FTRmIbB32`
- **Table:** Group Demand Opportunities — `tblRuReslJMwsfRQj`

## Proposed additive fields

| Field | Type | Purpose |
|---|---|---|
| Hotel Inventory Membership JSON | multilineText | Hotel-scoped membership envelope (status, staged/current versions, first/last verification, change class) |
| Staged Publication Version | singleLineText | Exact STAGED `publicationVersion` membership |
| Current Publication Version | singleLineText | Exact CURRENT `publicationVersion` when founder-approved |
| Inventory Population Primary | singleSelect | `SALES_OPPORTUNITIES` / `COMPETITIVE_MARKET_INTELLIGENCE` / `WATCHLIST` / `RESEARCH_ONLY` / `EXCLUDED` (may already exist in payload) |

## Relationships

- No new tables.
- No new linked-record tables.
- Canonical opportunity identity remains the GDO opportunity id; membership is hotel-scoped annotation on the same row (or payload embed until fields exist).

## Migration / write behavior

1. Derive membership from STAGED/CURRENT + research bag (`deriveMembershipFromPublications`).
2. Persist durable FS ledger: `data/group-demand-intelligence/hotels/{hotelId}/inventory-membership-v1.json` (authorized apply).
3. Dry-run Airtable ExactWriteSet; founder-authorized apply with `GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES=1`.
4. Read-back parity before claiming reconstruction PASS.

## Interim (until schema auth)

Payload-embed plan only (`PAYLOAD_EMBED_UNTIL_SCHEMA_AUTH`) — reconstruction still reports `INVENTORY_MEMBERSHIP_NOT_PERSISTED` for Airtable until fields or embed are durably written.
