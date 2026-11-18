# Airtable Activation Plan — Full E2E Persistence (existing tables)

**Status:** Dry-run contract ready — **production writes NOT authorized**.

**Base:** AI Demand Positioning · `appa2cE7FTRmIbB32`

## Ownership

| Data | Authoritative store | Airtable role |
|---|---|---|
| Global public demand facts | `data/group-demand-intelligence/global-intelligence/` | Operational index via **Demand Generators / Programs / Signals** |
| Hotel-qualified opportunities | hotel `opportunities.json` + publication packages | **Group Demand Opportunities** |
| Venue / PE relationships | PE FS + research stage | **Private Event Venues / Signals / Hotel Venue Fit** |
| Research run history | execution ledger FS | **GDI Research Runs / Targets / Target Runs** (summary only — no duplicate full ledger) |
| CUSTOMER_PUBLISHED inventory | Publication packages / CURRENT registry | Unchanged — never auto-overwritten by persistence |
| Hotel sales activity / CRM | Hotel-private feedback only | **Never** synced across hotels or into global tables |

## Canonical mapping (reuse existing tables)

Do **not** create competing “GDI Global Demand Intelligence” tables.

| Research object | Existing Airtable table | Non-Airtable SoT |
|---|---|---|
| Organization / company | Demand Generators (`tblykUVOjGVkawvdD`) | global-intelligence FS |
| Recurring program | Demand Programs (`tblxm7wqfFNG37Z2m`) | global-intelligence FS |
| Dated signal / occurrence | Demand Generator Signals (`tblZ0cOM78uYMCTXx`) | bounded audit |
| Hotel-qualified opportunity | Group Demand Opportunities (`tblRuReslJMwsfRQj`) | hotel opportunities.json |
| Venue | Private Event Venues (`tblE5p4HjVHfcMDna`) | PE / global FS |
| Venue hotel fit | Hotel Venue Fit (`tbluXqX7ecocCrdE1`) | PE fit store |
| Planner / intermediary | Demand Generators + GDI Research Targets | research coverage |
| Hotel generator fit | Hotel Demand Generator Fit (`tblqamXbIk9n6gXfV`) | hotel research state |
| Research run summary | GDI Research Runs (`tblzNMUIo2T6onaKH`) | execution ledger FS |
| Unsupported / raw search noise | *(none)* | bounded audit / reports |

Contract: `lib/group-demand-intelligence/workflow/gdi-full-e2e-airtable-persistence-contract-v1.js`  
Dry-run planner: `…/gdi-full-e2e-airtable-persistence-dry-run-v1.js`

## Upsert + read-back

- Idempotent on existing identity fields (Demand Generator ID, Venue ID, opportunityId, Run ID, …)
- Outcomes: `CREATED` · `UPDATED` · `UNCHANGED` · `DUPLICATE_LINKED` · `DEFERRED` · `FAILED`
- API 200 alone is **not** verified persistence — identity read-back required
- Live `apply: true` is fail-closed until founder approval (`AIRTABLE_PRODUCTION_WRITES_NOT_AUTHORIZED`)

## Out-of-market intelligence

Credible reusable orgs/venues/signals may be planned for Demand Generators / PE / Signals **without** creating Group Demand Opportunities for the current hotel. Exclusion reason stays on the FS match / ledger.

## Safe activation steps

1. Founder reviews Bethesda reconciliation + dry-run write set.  
2. Confirm existing adapters/permissions on `appa2cE7FTRmIbB32`.  
3. Authorize a narrow production pilot write set (not 165 raw Bethesda candidates).  
4. Enable apply path with read-back certification.  
5. Separate approval for any historical backfill.

## Explicitly out of scope until authorized

- Production Airtable mutations  
- Backfilling all 165 Bethesda raw Exa candidates  
- Migrating / overwriting CURRENT publication packages  
- Syncing hotel-private sales activity  
- Creating new competing Airtable bases or global-demand tables  
