# Implementation priority and sequence

1. **Keep persistence fail-closed** until exact pilot write-set auth (already).  
2. **CD-1** — Weekly Change provenance population (no new table).  
3. **CD-2-A** — Payload non-compaction for salesAction/CMI/revenueStream.  
4. **CD-3-A** — Optional GDO text ids for DG/Program/Signal (only schema add if approved).  
5. **CD-4** — Programs write only with recurrence evidence.  
6. **CD-5/CD-7** — CMI + eligibility gap codes in payload.  
7. **CD-6** — nights/revenue policy (no research $).  
8. **Pilot write** — Bethesda eligible 4 ops only after separate auth.

P2 defer: Contacts table, typed competitor columns, seasonality migration, modeled revenue.
