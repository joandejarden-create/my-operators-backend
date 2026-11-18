# Production pilot authorization request

**Status:** Authorized and executed as Bethesda four-record pilot V1  
(`bethesda-marriott-four-record-pilot-v1`). See  
`reports/gdi-airtable-bethesda-production-pilot-v1/`.  
General / unrestricted Airtable production writes remain **NOT** authorized.

## Proposed first live write set (Bethesda)

Authorize **only** these eligible operations from `BETHESDA_EXACT_WRITE_MANIFEST.json`:

1. CREATE Demand Generator `dg_5320e4f7f6cd2bf0` — Amentum Holdings, Inc. / Amentum  
2. CREATE Demand Generator `dg_b5b62937a741d606` — American Political Science Association  
3. CREATE Demand Generator `dg_82dd10dca7693c2b` — SAME Foundation  
4. UPDATE Private Event Venue `pev_42542bf58aac936b` / record `recf35Q9B6jpX3Ivm` — Rock Creek Mansion (existing match)

**Do not authorize in the first pilot:**

- GDO `disc_ff8f41b34c5b`, `disc_ab0b19495c15`, `disc_2cac04452eec` (deferred — WATCHLIST below commercial eligibility)  
- Any YOTEL Airtable-only rows  
- Any Guadalajara GDO creates from FS-only inventory  
- Need-date / commercial profile / Decisions tables  
- 165 raw Bethesda Exa candidates

## Required apply flags (future)

```text
authorizeProductionWrites=true
exactWriteSetAuthorization=<founder-signed-manifest-id>
apply=true
```

Until those are explicitly provided in a later turn, the adapter refuses writes.
