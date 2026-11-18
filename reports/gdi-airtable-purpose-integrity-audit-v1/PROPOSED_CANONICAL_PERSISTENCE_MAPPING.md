# Proposed Canonical Full E2E → storage mapping

**Status:** PROPOSAL ONLY — requires founder approval before implementation.  
**Principle:** Not every research object needs an Airtable record.

| Research object | Authoritative source today | Airtable table? | Necessary? | Stable identity | Create when | Material update when | Dedup | Privacy | Checkpoint | Founder approval |
|-----------------|----------------------------|-----------------|------------|-----------------|-------------|----------------------|-------|---------|------------|------------------|
| Global organization | FS `global-intelligence/` + DG | Demand Generators | Yes for reusable orgs | Demand Generator ID | Credible org / retainGlobally | Name, geo, evidence, contacts upgrade | By Demand Generator ID | No hotel sales notes | AFTER_GLOBAL_IDENTITY | Yes (pilot set) |
| Recurring program | FS + Programs | Demand Programs | Only if recurring evidence | Program ID | Series confirmed | Schedule/title changes | Program ID | | AFTER_DEEP_RESEARCH | Yes |
| Dated demand signal | FS / bounded audit | Demand Generator Signals | Optional | Signal ID | Dated occurrence evidence | New date/evidence | Signal ID | | AFTER_DEEP_RESEARCH | Yes |
| Hotel-qualified opportunity | FS `opportunities.json` | Group Demand Opportunities | Yes for registry sync | opportunityId | Hotel-qualified / watchlist retain | Thesis, contacts, priority, evidence | opportunityId | Hotel-scoped | AFTER_HOTEL_QUALIFICATION | Yes |
| Out-of-market credible org | FS + DG | Demand Generators **only** | Org yes / GDO **no** | Demand Generator ID | retainGlobally | | | Exclusion reason on FS | AFTER_SCREENING | Yes |
| Venue | PE FS + Venues | Private Event Venues | Yes for PE spine | Venue ID | Venue identity | | Venue ID | | AFTER_GLOBAL_IDENTITY | Yes |
| PE signal | PE signal store | Private Event Signals | When evidence | Event Signal ID | | | | | AFTER_DEEP_RESEARCH | Yes |
| Hotel × venue fit | PE fit store | Hotel Venue Fit | When hotel-qualified | Hotel Venue Fit ID | | | | Hotel-scoped | AFTER_HOTEL_QUALIFICATION | Yes |
| Hotel × generator fit | Fit table / reuse | Hotel Demand Generator Fit | When hotel-qualified (**not** generic_seed-only invent) | Fit ID | Qualification evidence | | Fit ID | Hotel-scoped | AFTER_HOTEL_QUALIFICATION | Yes |
| Planner / intermediary | DG + optional Target | DG ± Research Targets | Target only if monitoring committed | DG ID / Target ID | | | | | AFTER_HOTEL_QUALIFICATION | Yes |
| Research target | Targets table | GDI Research Targets | Monitoring subjects only | Target ID | Commit to monitor | Status/cadence | Target ID | | RUN_SUMMARY / coverage | Clarify with founder |
| Full E2E run detail | **FS execution ledger** | — | Airtable not required for full detail | runId | Always on FS | Append-only ledger | runId | May include provider meta | RUN_SUMMARY | N/A (FS) |
| Run summary | Optional | GDI Research Runs | Optional summary | Run ID | End of authorized run | Status/counts | Run ID | No raw provider dumps | RUN_SUMMARY | Yes |
| Target × run grain | Optional | GDI Research Target Runs | Prefer FS for Full E2E | Target Run ID | Monitoring campaigns | | | | | Clarify |
| Raw Exa / unsupported hits | Bounded audit / reports | **None** | No | — | Never auto | | | | AFTER_SCREENING | N/A |
| Hotel commercial priorities | Portal / GDI profiles | GDI Hotel Commercial Profiles | Portal-owned | Profile ID | Hotel/portal only | | | Confidential | — | **Block Full E2E** |
| Hotel need periods | Portal / hotel-reported | GDI Hotel Need Periods | Portal-owned | Need Period ID | Hotel/portal / explicit provisional scenarios only | | | Confidential | — | **Block Full E2E** |
| Need-date matches | Matcher | Matches table | Need-date workflow | Match Key | Matcher | | Match Key | | Need-date only | Separate |
| Decisions / feedback | Decisions + FS feedback | Decisions / Events | Feedback workflow | decisionId / eventId | Validation/actions | Append events | | Confidential; never global | Feedback | Separate |
| Facility attributes | HI packet | Facility HI tables | HI/ADP | Profile/Space/Node keys | HI research | | | | HI | Out of Full E2E |
| CUSTOMER_PUBLISHED inventory | Publication packages | **None via this path** | No | package version | Founder publish flow | | | Customer | — | Separate publish auth |
| Weekly Change history | Derived / FS | Not a dedicated Full E2E table | Prefer existing change fields / FS | | | | | | | |

## Explicit non-Airtable destinations (keep)

- `data/group-demand-intelligence/global-intelligence/`
- `data/group-demand-intelligence/hotels/{hotelId}/opportunities.json`
- `data/group-demand-intelligence/execution-observability/...`
- `data/group-demand-intelligence/research-stage/`
- `lib/group-demand-intelligence/bundled-publication-packages/{hotelId}/`
- `data/group-demand-intelligence/hotels/{hotelId}/feedback.json`
- Bounded reports / ledger provider rows for raw noise

## Differences from PR #356 mapping (intentional)

1. Research Targets are **not** automatic for every planner/org — only monitoring commitments.  
2. Need-date tables remain **restricted** — even stronger “do not write from Full E2E discovery.”  
3. Bethesda 7 may shrink after collision/idempotency against existing rows.  
4. Match identity documented as **Match Key**.
