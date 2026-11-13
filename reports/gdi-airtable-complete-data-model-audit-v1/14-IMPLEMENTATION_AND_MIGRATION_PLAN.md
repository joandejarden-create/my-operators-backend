# Implementation and migration plan (audit only — no execution)

## P0 — Customer content cannot be reconstructed from durable canonical data

| Item | Problem | Fix | Risk |
|------|---------|-----|------|
| Presentation pills / icons / drawer body | Approved V2 fields missing or mismatched vs Airtable payload | Persist required presentation semantic fields at upsert; stop compacting them; backfill from approved package **into research registry only** under separate auth | Medium — must not mutate publication |
| sharedTenSectionDrawer / CIB | Compacted out | Guarantee CIB or drawer inputs in payload; rebuild drawer from CIB with versioned builder | Medium |
| Weekly change filter | MISSING_SOURCE | Define durable change ledger or drop filter until backed | Low |

**Rollback:** Airtable row versioning via re-upsert from snapshots; publication untouched.

## P1 — Essential fields/relationships inconsistent

- Typed columns for salesActionKey, revenueStreamClass, cardContactSubtitle
- Need-date match rows for Bethesda coverage audit (populate matches if feature shown)
- Contact professional title column
- Research target/run linkage fields populated for published IDs

## P2 — Quality / filtering / efficiency

- Normalize Contacts if multi-hotel reuse needed
- Evidence index table for URL dedupe
- Deprecate legacy seasonality dual-write

## P3 — Optional

- Full Contacts CRM graph
- Materialized weekly change

## Authorization required before any apply

Founder approval for: schema column adds, backfill from publication DTOs into Airtable research rows, any production write. **Never** republish Bethesda without separate approval.
