# GDI Airtable Purpose, Data Integrity & Full E2E Persistence Audit V1

**Status:** AUDIT COMPLETE — **STOP FOR FOUNDER REVIEW**  
**Base:** AI Demand Positioning `appa2cE7FTRmIbB32`  
**Live read:** 2026-11-18 (read-only; no mutations, schema changes, paid research, or publication changes)  
**Checkout:** `C:\Dev\dealality\my-operators-backend` on branch from synchronized `main`  
**Prior inputs:** PR #356 (`reports/gdi-full-e2e-airtable-persistence-reconciliation-v1/`) · prior schema audit (`reports/gdi-airtable-complete-data-model-audit-v1/`)

## Verdict

We can explain the **actual** purpose of every GDI-related table in this base, distinguish **documented vs inferred** purpose, and recommend which tables should (and should not) receive Canonical Full E2E writes.

**Do not activate production Airtable persistence until the founder approves the table-purpose / target-state matrix in `FOUNDER_DECISIONS_REQUIRED.md`.**

## Answers to acceptance questions

| Question | Answer |
|----------|--------|
| Do we understand actual purpose of every GDI-related table? | **Yes**, with a small set of founder clarifications (see Decisions). Confidence high for opportunity/DG/PE/need-date/decision spines; medium for some facility HI tables’ long-term GDI role. |
| Which are canonical vs supporting/monitoring? | See matrix. Spine: **Demand Generators → Programs/Signals → GDO**; PE venue/signal/fit; Research Targets/Runs; **GDI** commercial profiles + need periods; Decisions/Events. Facility HI tables are supporting/ADP-adjacent. |
| Incorrect or overlapping responsibilities? | **Yes:** facility `Hotel Commercial Profiles` / `Hotel Seasonality & Need Periods` vs **GDI** commercial/need-period tables; Research Targets heavily seeded with `generic_seed` vs org intelligence in Demand Generators; Match table identity field mis-documented as Need Period ID. |
| Synthetic / provisional / historical? | **Yes:** Waterstone SYNTHETIC_TEST commercial profile; Waterstone + Sheraton Guadalajara provisional/test need periods; widespread `generic_seed` Fit + Research Targets; GDO payloads sometimes contain provisional language (not proof of invalidity). |
| Duplicate / inconsistent identities? | **opportunityId** and Demand Generator IDs: no duplicate identity keys observed. **Match table “dups”** are mostly a wrong identity-field assumption (Need Period ID vs Match Key). **Cross-store drift:** YOTEL GDO 108 vs FS 42; Guadalajara FS 43 / GDO 0; Radisson FS 66 / GDO 36. |
| Relationships correct? | Graph design is sound (DG↔Program↔Signal↔Fit↔GDO; Venue↔Fit↔Signal; Need Period↔Matches↔GDO). Link fields often empty on older rows — incomplete population, not broken schema. |
| Which tables receive Full E2E data? | **Proposed:** Demand Generators (+ Programs/Signals when present), Group Demand Opportunities (hotel-qualified only), Private Event Venues (+ Fit/Signals when qualified), Research Runs (summary only), Research Targets (monitoring subjects — **not** every discovery hit). **Not** need-date / GDI commercial / Decisions / facility HI / customer publication via this path. |
| Cleanup? | No automatic cleanup. Later: isolate synthetic/provisional need periods; stop treating generic onboard seeds as verified demand; fix Match Key identity in contracts; reconcile YOTEL/Radisson/Guadalajara inventory drift. |
| Can we activate persistence without compromising intelligence? | **Yes, if** writes stay selective, idempotent, fail-closed on missing packages elsewhere, never overwrite hotel-confirmed need periods or publication CURRENT, and never promote synthetic/provisional records to confirmed. |

## Live snapshot (selected counts)

| Table | Records (live) |
|-------|---------------:|
| Group Demand Opportunities | 998 |
| Demand Generators / Programs / Signals | 95 / 94 / 23 |
| Hotel Demand Generator Fit | 276 |
| GDI Research Targets / Runs / Target Runs | 480 / 40 / 383 |
| Private Event Venues / Signals / Hotel Venue Fit | 38 / 26 / 44 |
| GDI Hotel Need Periods | 22 |
| GDI Need Period Opportunity Matches | 139 |
| GDI Hotel Commercial Profiles | 2 |
| Decisions / Decision Events | 93 / 160 |
| Hotel Intelligence Evidence | 722 |

## Bethesda revalidation (`gdi_e2e_14seg_mwladic1_ff9898`)

| Fact | Status |
|------|--------|
| 165 raw / 20 processed / 0 campaign Airtable writes | Reconfirmed (campaign ledger + PR #356) |
| Prior / current Bethesda GDO | **86** (FS 86; Airtable 86; identity overlap complete) |
| Original 7 dry-run ops | **Still plausible** as DG(3)+GDO(3)+PE venue(1) — **but only after founder approves** that Full E2E may write those tables; do **not** force Programs/Signals/Targets/Need-Date from that batch |

## Hard stops (this PR)

- No production Airtable writes  
- No schema migrations / deletions  
- No paid research  
- No customer publication changes  
- **No automatic start of persistence activation after merge**

## What founder must approve next

See `FOUNDER_DECISIONS_REQUIRED.md` — especially: Full E2E write destinations, treatment of `generic_seed` Fit/Targets, synthetic Waterstone / provisional Guadalajara need periods isolation, and whether YOTEL Airtable-only opportunities are authoritative or drift.
