# Founder executive summary — Bethesda two-write production pilot

| Question | Answer |
|----------|--------|
| CREATE applied? | **YES** `recLuxvwwxID7U2B8::gdi_live_pinnacle_lacrosse_championships_2027` → `rec4aFaYuC0gfR1wh` |
| UPDATE applied? | **YES** `recLuxvwwxID7U2B8::disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_` → `recTSOXMxDT7JpxGq` |
| Read-back ok? | **true** (both matched; CREATE WATCHLIST; UPDATE ACTIVE_RESEARCH) |
| Idempotent? | UNCHANGED=2, WOULD_CREATE=0, WOULD_UPDATE=0 |
| 63 updates applied? | **No** |
| 63 root cause? | Bridge injected `firstDiscoveredRunId` into payloads → false material diffs (~61); +2 sourceSummary serialization. Fix shipped. |
| Post-fix residual WOULD_UPDATE | **≤5** sourceSummary-only residuals (**unapplied**) |
| Provenance note | PDA lacked typed firstDiscovered*; brief invented UPDATE stamp cleared; upsert now refuses inventing on UPDATE |
| GDO / publication / Rock Creek | **0 / 0 / 0** |
| Recurring persistence | **OFF** (`GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES` restored off) |
| Next paid Exa | Proposal ready — **not spent** |

Auth: `gdi-bethesda-live-14-segment-persistence-pilot-write-v1`
