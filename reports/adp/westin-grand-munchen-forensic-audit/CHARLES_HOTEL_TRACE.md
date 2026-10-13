# The Charles Hotel — Forensic Trace

| Stage | Result |
|-------|--------|
| Raw mentions (competitorsMentioned + raw scan) | **101** |
| Scenarios mentioned | **32** |
| Resolved entity ID (after Munich registry) | **the_charles_hotel_munich** ok=true reason=ok |
| In recomputed competitive set | **YES** |
| In recomputed displacement | **YES** |
| In recomputed Overall ranking | **YES** |
| Original published competitive set | **NO** (observed=0) |
| Original published displacement | **NO** (empty) |
| Original published Overall non-subject | **NO** |
| E2E pack topDisplacementCompetitor | The Charles Hotel (from `opportunities.topCompetitors` analytical path — **not** customer competitiveSet) |

## Lineage break
**Stage:** customer entity resolution / competitive-set aggregation  
**Reason:** `ADP_NO_UNBOUND_ANALYTICAL_COMPETITOR_ENTITIES` fail-closed with **no Munich entry in PROPERTY_ENTITY_REGISTRY** at certify/publish time.  
Raw extraction succeeded; customer bind dropped all Munich hotels including The Charles Hotel.
