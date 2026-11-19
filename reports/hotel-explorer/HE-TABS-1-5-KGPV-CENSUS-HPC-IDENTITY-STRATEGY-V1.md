# KGPV Census ↔ HPC identity strategy (approved Gate 1 integration)

**Status:** STRATEGY LOCKED — no blind fixture/demo migration  
**Hotel:** Krystal Grand Puerto Vallarta `recUNycnMwOVFX0hc`  
**Date:** 2026-11-19

## Facts

| Check | Result |
|---|---|
| Record id | `recUNycnMwOVFX0hc` |
| Owning table (RECORD_ID filter) | **Hotel Census** `tblgj2qEwxjTcg6q0` |
| On Hotel Property Census `tbl9aY5ijiuIzzWam` | **No** |
| HI `HPC Hotel` link target | Hotel Property Census only |
| Consequence | HI Hotel–Org Relationship writes with this id fail (`ROW_TABLE_DOES_NOT_MATCH_LINKED_TABLE`) |

## Approved strategy (do / don’t)

### Do

1. Keep KGPV as **golden UX reference** (fixtures + Census-backed Overview).
2. Serve KGPV Overview from **Hotel Census** with explicit `propertyIdentity.owningTableKind=HOTEL_CENSUS`.
3. Allow Mexico golden-demo **presentation overlay** only on Hotel Census–owned Mexico demo hotels, with `presentationOverlay` provenance (not HPC).
4. Use Bethesda (`recLuxvwwxID7U2B8` on HPC) as the Airtable-first persistence pilot.
5. Before any KGPV HI relationship persistence: create/link a **canonical HPC twin** (or founder-approved retarget of the HI link field) under a dedicated identity task.

### Don’t

1. Do **not** migrate golden-demo fixtures into HI Organizations / Contacts / Channels without independent fact verification.
2. Do **not** invent a duplicate hotel row casually to “make the link work.”
3. Do **not** retarget HI `HPC Hotel` away from Hotel Property Census without founder + Integration Controller schema approval.
4. Do **not** treat Contact Intelligence local/slice data as production Airtable.

## Next identity task (separate, post-integration)

`chore/he-kgpv-hpc-identity-twin` (proposed):

1. Dry-run search HPC for equivalent property (name/geo/brand).
2. If missing → steward-approved HPC shell create with provenance.
3. Map `Hotel Census id → HPC id` in a durable registry (not env-only).
4. Only then migrate **verified** GSF ownership/contact facts into HI graph linked to the HPC id.

Until then: KGPV remains demo-rich; Bethesda remains the live Airtable integration proof.
