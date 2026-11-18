# Full E2E Airtable persistence adapter V1 — founder summary

**Phase:** Capability + dry-run certification  
**Production writes:** **NOT AUTHORIZED** — stop before first live write  
**Base:** `appa2cE7FTRmIbB32`

## What shipped

1. Approved D1–D8 target-state mapping encoded in contracts + eligibility gates.  
2. Persistence adapter reusing existing DG / PE / GDO sync adapters (fail-closed apply).  
3. Exact Bethesda 7-op write manifest with live collision + commercial eligibility.  
4. YOTEL / Radisson / Guadalajara ID-level FS↔Airtable reconciliation.  
5. Synthetic/provisional isolation classifier + recommended filtered views.  
6. Match Key documented as Match table upsert identity (D7).

## Bethesda D2 result (exact)

| Metric | Value |
|--------|------:|
| Structural proposed ops | **7** |
| Eligible writes now | **4** |
| Deferred (eligibility) | **3** (all GDOs) |

| Op | Canonical ID | Airtable record | Outcome |
|----|--------------|-----------------|---------|
| DG Amentum | `dg_5320e4f7f6cd2bf0` | none | **CREATE** |
| GDO Amentum | `disc_ff8f41b34c5b` | none | **DEFER** (WATCHLIST below CONFIRMED/RELIABLE + entry-point gate; has salesReadyContacts=1 but no demand thesis on processed row) |
| DG APSA | `dg_b5b62937a741d606` | none | **CREATE** |
| GDO APSA | `disc_ab0b19495c15` | none | **DEFER** |
| DG SAME | `dg_82dd10dca7693c2b` | none | **CREATE** |
| GDO SAME | `disc_2cac04452eec` | none | **DEFER** |
| PE Rock Creek Mansion | `pev_42542bf58aac936b` | `recf35Q9B6jpX3Ivm` | **UPDATE** (existing venue match; not a lodging GDO) |

Full field evidence: `BETHESDA_EXACT_WRITE_MANIFEST.json`.

## YOTEL D5-C

| Store | Count | Finding |
|-------|------:|---------|
| FS research | 42 | `gdi_opp_yotel_fe2e_*` / `rx_d_*` namespace |
| Airtable GDO | 108 | Disjoint `gdi_opp_aifg_*` / other IDs |
| Shared by opportunityId | **0** | No silent merge possible |

**Treatment:** Do not delete or import Airtable-only; do not overwrite FS; customer remains publication packages; reconcile by org/event evidence later if desired.

## Radisson / Guadalajara D6

| Hotel | FS | AT | Shared | FS-only | AT-only | Conflicts |
|-------|---:|---:|-------:|--------:|--------:|----------:|
| Radisson | 66 | 36 | 29 | 36 | 6 | 1 (org name wording) |
| Guadalajara | 43 | 0 | 0 | 43 | 0 | 0 |

FS-ahead acceptable; no bulk GDO create — eligibility gate first.

## Production pilot authorization request (when ready)

Authorize **only** after reviewing:

1. `BETHESDA_EXACT_WRITE_MANIFEST.json` eligible subset (currently 3 DG CREATE + 1 PE UPDATE).  
2. Confirm GDO DEFERs remain deferred until demand thesis + entry point certified.  
3. Confirm YOTEL Airtable-only rows stay untouched.

Until then: adapter `apply` returns `AIRTABLE_PRODUCTION_WRITES_NOT_AUTHORIZED`.
