# Table consolidation and cleanup recommendations

**No automatic cleanup or migration in this audit.**

| Table | Recommendation | Evidence | Compatibility risk | Impact | Migration approach (later) | Production records affected? |
|-------|----------------|----------|--------------------|--------|----------------------------|------------------------------|
| Group Demand Opportunities | KEEP WITH FIELD OR CONTRACT CORRECTIONS | Core SoR; presentation gaps known from prior audit | Low if additive fields only | High (registry) | Continue presentation persistence plan separately | Yes — additive only |
| Demand Generators | KEEP AS IS | Correct org SoR | Low | High | Provenance labeling for seeds | Existing seed orgs remain |
| Demand Programs | KEEP AS IS | Valid recurring layer | Low | Medium | Write only with evidence | Sparse updates |
| Demand Generator Signals | KEEP AS IS / RESTRICT TO SPECIFIC WORKFLOW | Sparse; signal semantics | Low | Low | Monitoring/signal campaigns | Historical 23 remain |
| Hotel Demand Generator Fit | KEEP WITH FIELD OR CONTRACT CORRECTIONS | Useful but `generic_seed` heavy | Medium if misread as verified | Medium | Add/standardize seed provenance field; filter in reuse | Seed fits remain until reviewed |
| Private Event Venues / Signals / Hotel Venue Fit | KEEP AS IS | Clear PE spine | Low | Medium | Continue PE canonical persist | Existing PE rows |
| GDI Research Targets | KEEP WITH FIELD OR CONTRACT CORRECTIONS / RESTRICT | Monitoring table polluted with onboard seeds | Medium | Medium | Separate seed vs committed monitor views; stop silent seed→verified | 480 rows preserved |
| GDI Research Runs | RESTRICT TO A SPECIFIC WORKFLOW | Summary only; not Full E2E ledger | Low | Low | Link to FS runId | 40 rows |
| GDI Research Target Runs | RESTRICT TO A SPECIFIC WORKFLOW | Dense monitoring grain | Low | Low | Prefer FS for Full E2E execution | 383 rows |
| GDI Hotel Commercial Profiles | KEEP AS IS | Correct portal SoR; 1 synthetic Waterstone | Medium isolation | High for need-date | Label/quarantine synthetic; never Full E2E write | 2 rows |
| GDI Hotel Need Periods | KEEP WITH FIELD OR CONTRACT CORRECTIONS | Mix of HOTEL_REPORTED + provisional/test | High if Status confused | High | Enforce Status/Source gates; no Full E2E create | 22 rows preserved |
| GDI Need Period Change History | KEEP AS IS | Audit trail | Low | Low | Portal only | |
| GDI Need-Date Campaign Runs | RESTRICT TO A SPECIFIC WORKFLOW | Need-date campaigns | Low | Low | | 1 row |
| GDI Need Period Opportunity Matches | KEEP WITH FIELD OR CONTRACT CORRECTIONS | Fix identity = Match Key in contracts | Medium if wrong upsert key | Medium | Contract + upsert key fix; dedupe later if true Match Key collisions | 139 rows |
| Decisions / Decision Events | KEEP AS IS | Shared outcome ledger | Low | High for feedback | | |
| Hotel Commercial Profiles (facility) | KEEP AS IS (facility) | Distinct from GDI portal profiles | Medium if confused | Medium | Document dual-table rule | |
| Hotel Event Spaces / Demand Nodes | KEEP AS IS | Facility inventory | Low | Low | HI workflow | |
| Hotel Seasonality & Need Periods | DEPRECATION CANDIDATE | Superseded by GDI Need Periods | Medium if dual-write resumes | Low for GDI | Separate deprecation review; do not delete now | 27 rows preserved |
| Hotel Intelligence Evidence | KEEP AS IS / SHARED | AVA/HI evidence | Low | Medium | Out of Full E2E path | |

## New structure required?

**No new competing tables recommended.** Prior “GDI Global Demand Intelligence” / “GDI Hotel Opportunity Matches” proposals remain rejected in favor of existing DG/Fit/Venue/GDO tables.

## Cleanup candidates (founder-gated, later)

1. Quarantine Waterstone synthetic commercial profile from “live hotel” views.  
2. Ensure provisional Guadalajara need periods stay Status Test Scenario until hotel confirms.  
3. Inventory YOTEL Airtable-only GDOs vs FS research (drift reconciliation project).  
4. Optional: mark `generic_seed` Fit/Targets with a dedicated select for filtering.
