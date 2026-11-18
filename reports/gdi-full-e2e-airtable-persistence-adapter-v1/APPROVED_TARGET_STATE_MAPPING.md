# Approved target-state mapping (founder D1–D8)

**Status:** Approved architecture for non-mutating implementation.  
**Production Airtable writes:** NOT AUTHORIZED until exact pilot write-set approval.

## D1 — Full E2E destinations (conditional)

| Object | Table | Create when |
|--------|-------|-------------|
| Credible reusable org | Demand Generators | Credible org / retainGlobally |
| Verified recurring series | Demand Programs | Recurring program evidence |
| Evidence-backed dated trigger | Demand Generator Signals | Dated signal evidence |
| Hotel-qualified opportunity | Group Demand Opportunities | CONFIRMED or RELIABLE_INFERRED + credible commercial entry point |
| Verified venue | Private Event Venues | Venue identity |
| PE activity | Private Event Signals | Evidence-backed PE signal |
| Hotel×venue | Hotel Venue Fit | Evidence-supported hotel relationship |
| Hotel×generator | Hotel Demand Generator Fit | Evidence-supported hotel relationship |
| Run summary | GDI Research Runs | Optional; link FS ledger `runId` |
| Monitoring subject | GDI Research Targets | **Only** explicit monitoring commitment (D3-A) |

**Never auto:** org≠opportunity; program≠dated event; monitoring≠signal; venue≠lodging GDO; WATCHLIST≠customer without eligibility; hotel sales feedback≠global.

**Deny-list for generic Full E2E discovery:** GDI commercial profiles; need periods/history/matches/campaigns; Decisions/Events; facility HI; seasonality; ADP measurement; publication packages; raw unsupported Exa.

## D2 — Bethesda Option B
Exact seven structural ops + eligibility outcomes in `BETHESDA_EXACT_WRITE_MANIFEST.json`.

## D3 — Research Targets Option A
Explicit monitoring commitment only; no Target Run spam for every discovery.

## D4 — Synthetic/provisional Option B
Operator-visible classes via existing Source/Status/Provenance + filtered views (`gdi-synthetic-provisional-isolation-v1.js`). No deletes.

## D5 — YOTEL Option C
FS = research execution SoT; Airtable = historical registry; publication packages = customer SoT. See `YOTEL_ID_RECONCILIATION.json`.

## D6 — Radisson / Guadalajara
FS-ahead OK temporarily; ID reconciliation required before bulk sync. See `RADISSON_ID_RECONCILIATION.json`, `GUADALAJARA_ID_RECONCILIATION.json`.

## D7 — Match Key
Canonical upsert identity for GDI Need Period Opportunity Matches = **Match Key** (contract updated).

## D8 — Seasonality
`Hotel Seasonality & Need Periods` = deprecation candidate; no delete/migrate.
