# Production pilot authorization plan

**Not authorized.** This document is the exact plan for a future founder-gated live run.

| Field | Value |
|-------|-------|
| Authorization ID | `gdi-bethesda-automated-research-persistence-pilot-v1` |
| Hotel | Bethesda Marriott (`recLuxvwwxID7U2B8`) |
| Base / table | `appa2cE7FTRmIbB32` / `tblbkDj0vhQLfrssV` |
| Expected CREATE max | 2 |
| Expected UPDATE max | 2 |
| Write budget | 10 |
| Env gate | `GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES=1` |
| GDO / publication / Rock Creek / paid | **excluded** |

## Exact allowed composite identities

- `recLuxvwwxID7U2B8::gdi_pilot_new_bethesda_research_001`
- `recLuxvwwxID7U2B8::gdi_pilot_exploratory_bethesda_001`
- `recLuxvwwxID7U2B8::gdi_opp_ahima_advocacy_2027`
- `recLuxvwwxID7U2B8::gdi_opp_nado_ddaa_washcon_2028`

## Retry / read-back

Required: **true**

## Rollback / recovery

1. Halt env gate GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES
1. Do not re-apply without new founder authorization
1. Use compositeKey dedupe — retry must be UNCHANGED/ALREADY_PRESENT
1. Preserve firstDiscovered* on all updates
1. Do not delete research rows as rollback; quarantine via lifecycle status if needed

Real production pilot requires a separate explicit founder authorization.
