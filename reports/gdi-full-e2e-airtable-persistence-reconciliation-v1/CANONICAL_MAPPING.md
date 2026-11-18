# Canonical research → Airtable mapping

**Base:** AI Demand Positioning `appa2cE7FTRmIbB32`

| Object | Airtable | Identity | Non-Airtable |
|---|---|---|---|
| Organization / company | Demand Generators | Demand Generator ID | `global-intelligence/` |
| Recurring program | Demand Programs | Program ID | `global-intelligence/` |
| Dated signal / occurrence | Demand Generator Signals | Demand Generator Signal ID | bounded audit |
| Hotel-qualified opportunity | Demand Generators **+** Group Demand Opportunities | Demand Generator ID + opportunityId | hotel `opportunities.json` |
| Out-of-market credible org | Demand Generators (no GDO for current hotel) | Demand Generator ID | global FS + exclusion reason |
| Venue | Private Event Venues (+ Hotel Venue Fit if qualified) | Venue ID | PE / global FS |
| Private event signal | Private Event Signals | Event Signal ID | PE signal store |
| Planner / intermediary | Demand Generators + GDI Research Targets (+ Hotel Fit if qualified) | Demand Generator ID / Target ID | research coverage |
| Research run summary | GDI Research Runs | Run ID | execution ledger FS (full) |
| Research target / target-run | GDI Research Targets / Target Runs | Target ID / Target Run ID | ledger FS |
| Need-date profile / period / campaign / match | Existing need-date tables | Profile / Need Period / Campaign IDs | need-date FS |
| Unsupported / speculative raw hit | *(none)* | — | bounded audit / reports |
| CUSTOMER_PUBLISHED inventory | *(not via this path)* | — | publication packages |
| Hotel sales / confidential feedback | *(never global / never cross-hotel)* | — | hotel-private feedback |

## Checkpoints

1. After global identity matching  
2. After screening (defer raw noise)  
3. After hotel qualification (GDO only if qualified)  
4. After deep research / contact updates  
5. Run summary → Research Runs (+ ledger FS)

## Write outcomes

`CREATED` · `UPDATED` · `UNCHANGED` · `DUPLICATE_LINKED` · `DEFERRED` · `FAILED`

Read-back of identity field is required before CREATED/UPDATED certification.
