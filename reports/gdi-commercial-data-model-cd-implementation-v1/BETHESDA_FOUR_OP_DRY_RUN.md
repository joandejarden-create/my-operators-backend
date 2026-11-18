# Bethesda four-operation dry-run (revalidated)

**Source:** `reports/gdi-full-e2e-airtable-persistence-adapter-v1/BETHESDA_EXACT_WRITE_MANIFEST.json`  
**Mutation:** false · **productionWritesAuthorized:** false

| Metric | Value |
|--------|------:|
| Structural proposed ops | 7 |
| Eligible writes | **4** |
| Deferred | 3 (all GDOs — WATCHLIST below eligibility) |

## Eligible (pilot ask — not applied)

| Outcome | Table | Canonical ID | Airtable record |
|---------|-------|--------------|-----------------|
| CREATE | Demand Generators | `dg_5320e4f7f6cd2bf0` (Amentum) | none |
| CREATE | Demand Generators | `dg_b5b62937a741d606` (APSA) | none |
| CREATE | Demand Generators | `dg_82dd10dca7693c2b` (SAME Foundation) | none |
| UPDATE | Private Event Venues | `pev_42542bf58aac936b` (Rock Creek Mansion) | `recf35Q9B6jpX3Ivm` |

## Deferred

- GDO `disc_ff8f41b34c5b`, `disc_ab0b19495c15`, `disc_2cac04452eec` — WATCHLIST / missing CONFIRMED or RELIABLE_INFERRED demand thesis.

Collision checks re-run live: DG creates remain `NONE`; Rock Creek remains existing venue UPDATE.
