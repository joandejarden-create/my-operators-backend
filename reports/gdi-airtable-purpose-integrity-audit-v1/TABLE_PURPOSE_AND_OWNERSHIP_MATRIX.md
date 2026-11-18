# Table purpose and ownership matrix

**Legend — purpose source:** `DOCUMENTED` = schema docs / prior founder reports / module headers; `INFERRED` = code + record content when docs are thin.  
**Classifications are audit findings, not production enums.**

| Table | Documented purpose | Actual purpose (today) | Owner | SoR / derivative | Classification | Future Full E2E role |
|-------|--------------------|------------------------|-------|------------------|---------------|----------------------|
| Group Demand Opportunities | Hotel-scoped qualified opportunity registry | Canonical hotel opportunity rows (`opportunityId`+`hotelId`); research/`live=1` read path; not customer publication SoT | GDI | SoR for research registry; presentation may still need payload/package | ACTIVE_CANONICAL | Write **only** hotel-qualified / watchlist-retained opportunities |
| Demand Generators | Organization / demand-source identity | Global org intelligence reused across hotels | GDI | SoR for org identity (with FS global-intelligence) | ACTIVE_CANONICAL | Upsert credible orgs (incl. retainGlobally without GDO) |
| Demand Programs | Recurring series under a generator | Program/series objects linked to generators | GDI | Derivative of generator graph | ACTIVE_SUPPORTING | Write when recurring program evidence exists — not every one-off |
| Demand Generator Signals | Dated occurrences / monitoring signals | Sparse (23); mostly Bethesda-era | GDI | Supporting signal layer | ACTIVE_SUPPORTING | Optional; do not invent signals from raw Exa hits |
| Hotel Demand Generator Fit | Hotel × generator qualification | Mix of real fits + large `generic_seed` onboard graph | GDI | Hotel-specific derivative | ACTIVE_SUPPORTING | Write when hotel-qualified; **do not** treat generic_seed as verified |
| Private Event Venues | Venue identity (PE / wedding / social) | Venue master for PE workflow | GDI | SoR for venue identity | ACTIVE_CANONICAL | Write venues for PE/preferred-lodging segments |
| Private Event Signals | Event/activity signals at venues | Dated PE activity | GDI | Supporting | ACTIVE_SUPPORTING | When signal evidence exists |
| Hotel Venue Fit | Hotel × venue partnership assessment | Hotel-specific PE fit | GDI | Hotel-specific | ACTIVE_SUPPORTING | When hotel-qualified for PE |
| GDI Research Targets | Monitoring / research subject queue | Persistent subjects; **many** `generic_seed` reasons | GDI | Monitoring SoR (not opportunity SoR) | ACTIVE_MONITORING | Targets for ongoing monitoring — **not** every discovery candidate |
| GDI Research Runs | Campaign / run metadata | Hotel-scoped run summaries (40) | GDI | Summary; full ledger stays FS | ACTIVE_MONITORING | Run **summary** linked to Full E2E ledger ID — no ledger dump |
| GDI Research Target Runs | Target × Run execution grain | Dense execution history (383) | GDI | Monitoring derivative | ACTIVE_MONITORING | Optional; prefer FS ledger for Full E2E execution detail |
| GDI Hotel Commercial Profiles | Hotel-declared commercial priorities (portal) | 2 rows: Bethesda (Not Provided) + Waterstone **Synthetic Test** | GDI | SoR for portal priorities | ACTIVE_CANONICAL | **Do not** populate from Full E2E discovery |
| GDI Commercial Profile Change History | Profile audit trail | Includes synthetic provenance rows | GDI | History | ACTIVE_SUPPORTING | Portal-only |
| GDI Hotel Need Periods | Hotel need windows for matching | Mix: Bethesda HOTEL_REPORTED Active + Waterstone test + Sheraton **PROVISIONAL** Test Scenario | GDI | SoR for need-date product | ACTIVE_CANONICAL | **Do not** create from Full E2E discovery |
| GDI Need Period Change History | Period version history | Includes provisional/test change reasons | GDI | History | ACTIVE_SUPPORTING | Portal/need-date only |
| GDI Need-Date Campaign Runs | Need-date research campaign drafts | 1 row | GDI | Campaign | ACTIVE_MONITORING | Need-date campaigns only |
| GDI Need Period Opportunity Matches | Period ↔ opportunity overlap | 139 rows; join for need-date tiles | GDI | Derivative join | ACTIVE_SUPPORTING | Need-date matcher — not Full E2E org discovery |
| Decisions | Recommended / validated decisions | Outcome ledger with GDI/ADP bridges | SHARED_ADP_GDI | SoR for decisions | SHARED_ADP_GDI | Hotel feedback / validation — not discovery writes |
| Decision Events | Append-only decision timeline | Events for validation/actions | SHARED_ADP_GDI | History | SHARED_ADP_GDI | Feedback path only |
| Hotel Commercial Profiles (facility) | Physical/product commercial attributes | ADP/HI facility profile | SHARED / HI | Facility SoR | SHARED_ADP_GDI | **Not** Full E2E; do not overwrite from GDI portal |
| Hotel Event Spaces | Meeting space inventory | Facility spaces | HI / ADP | Facility | ACTIVE_SUPPORTING | Not Full E2E |
| Hotel Demand Nodes | Demand node map | Facility/demand geography nodes | HI / ADP | Facility | ACTIVE_SUPPORTING | Not Full E2E |
| Hotel Seasonality & Need Periods | Legacy seasonality | Superseded for GDI need-date authority | LEGACY | Legacy | LEGACY / DUPLICATIVE_CANDIDATE | Do not dual-write |
| Hotel Intelligence Evidence | Research evidence trail | AVA + HI evidence (722) | SHARED / AVA | Evidence | ACTIVE_SUPPORTING | Not Full E2E opportunity path |

## Documented vs inferred highlights

1. **Need-date authority is documented:** GDI Hotel Need Periods + Matches override facility seasonality (`09-NEED_DATE_DATA_MODEL_REVIEW.md`).  
2. **Demand Generators as global org layer is documented** in PR #356 mapping and global-intelligence mapping module.  
3. **Research Targets as onboard seed queue is inferred from live `Reason Monitored=generic_seed`** — docs call them “research coverage,” but population is heavily seed-driven.  
4. **Full E2E execution ledger remains filesystem** — documented in contract `NON_AIRTABLE_DESTINATIONS`; Airtable Runs are summaries only.
