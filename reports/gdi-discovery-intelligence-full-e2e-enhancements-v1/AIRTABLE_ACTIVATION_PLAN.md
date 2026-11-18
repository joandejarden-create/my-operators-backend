# Airtable Activation Plan — Global Intelligence (PR1)

**Status:** Prepared for activation — **production writes NOT authorized**.

## Ownership

| Data | Authoritative store | Airtable role |
|---|---|---|
| Global public demand facts | `data/group-demand-intelligence/global-intelligence/` | Operational index / search |
| Hotel opportunity matches | `.../global-intelligence/hotel-matches/` + hotel research state | Linked index |
| CUSTOMER_PUBLISHED inventory | Publication packages / CURRENT registry | Unchanged |
| Hotel sales activity / CRM | Hotel-private feedback only | **Never** synced to global tables |

## Proposed schema

See `proposeGlobalIntelligenceAirtableSchema()` in:

`lib/group-demand-intelligence/global-intelligence/gdi-global-intelligence-airtable-mapping-v1.js`

Two tables:

1. **GDI Global Demand Intelligence** — keyed by Global Demand ID  
2. **GDI Hotel Opportunity Matches** — Hotel ID + link to global demand  

Prefer extending an **existing** Dealality GDI/platform base. Do not create a competing base.

## Upsert + read-back

- Idempotent on Global Demand ID / Match Key  
- Dry-run adapter: `dryRunUpsertGlobalIntelligenceToAirtable`  
- Live `apply: true` is fail-closed until founder approval  

## Privacy

Denylist strips sales feedback, rates, outreach, share tokens before any mapping.

## Safe activation steps

1. Founder approves base + table creation.  
2. Document fields in `docs/gdi-global-intelligence-airtable-fields.md`.  
3. `ensure-*` dry-run → apply options.  
4. Wire read-back fixture tests.  
5. Separate approval for first production upsert batch.  

## Explicitly out of scope now

- Production Airtable mutations  
- Migrating historical CUSTOMER_PUBLISHED rows  
- Syncing hotel-private sales activity  
