# Root-cause analysis — segment classification + preview UI

## 1. Scaling Companies mislabels

**Symptom:** OVMA True North Veterinary Conference 2027 appeared under Scaling Company (`corporate_accounts_scaling` / `SCALING_COMPANY`).

**Cause:** Package rows carried a baked `opportunityTypeFilterCategory`. The browser classifier trusted that baked value first and never re-checked demand mechanism. Conferences / trade shows discovered under scaling research lanes retained Scaling labels.

**Fix:** Shared Full E2E module `primary-demand-segment-classification-v1.js` requires an evidence-backed expansion trigger + hotel-demand thesis for Scaling, and remaps organized conferences/shows to Associations. Server + client classifiers both apply the guard. Radisson restaged to a new immutable package (prior fingerprint preserved on disk).

## 2. Technical founder-preview banner

**Symptom:** Large banner exposed package version + fingerprint.

**Cause:** Local founder-preview UI intentionally rendered STAGED metadata for debugging.

**Fix:** Replaced with a subtle authenticated **Preview** chip. Internal `preferStaged` / `?preview=staged` authorization unchanged. Package IDs and fingerprints remain in logs/API meta only.

## 3. Showing 1 of 66

**Symptom:** Inventory loaded 66 but UI showed one card; Priority/Sales facets showed All (1).

**Cause:** Opportunity Type filter was set to **Scaling Company (1)** (the sole baked SCALING row — OVMA). Facet counts recompute against the filtered set, so Priority/Sales appeared as All (1). Weekly Change still showed All (66) because that dropdown uses the unfiltered weekly base in some paths.

**Fix:** Removing the false Scaling classification eliminates the one-row Scaling filter. Reset View already clears `opportunityType` via `PC.createDefaultFilters()`; hotel switches also reset filters. Default view remains unfiltered All.
