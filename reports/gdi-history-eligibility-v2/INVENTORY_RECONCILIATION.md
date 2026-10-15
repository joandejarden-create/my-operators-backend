# Inventory reconciliation — Mayflower 35→30 / San Diego 46→38

Generated: 2026-10-15  
Eligibility unchanged by this history fix. Counts explained only.

**Record-level inventory (stable IDs, timestamps, categories):**  
[`../gdi-quality-review/INVENTORY_RECONCILIATION_RECORD_LEVEL.md`](../gdi-quality-review/INVENTORY_RECONCILIATION_RECORD_LEVEL.md)

## Snapshot sources

| Snapshot | Source | Mayflower | San Diego |
|----------|--------|----------:|----------:|
| Earlier broad facing | `gdi-hotel-fit-assessment-parity` backfill manifests (`facingBroadCount`) + `excludedFromReviewedPath` | **35** | **46** |
| Current customer freeze | `gdi-research-history-opportunity-type/facing-inventory-freeze.json` | **30** | **38** |
| Reviewed-path only (same parity docs) | `facingReviewedCount` | 30 | 37 |

## Mayflower — 5 IDs in earlier 35, not in current 30

All five remain in the local canonical store (`opportunities.json`, 57 total). They are **filter/scope**, not deletions.

| Opportunity ID | Title | Disposition |
|----------------|-------|-------------|
| `gdi_opp_prospect_mv8fnmk1_c41c0a4c` | U.S. Travel Association Destination Capitol Hill | Present; `customerVisible` true, `prospectHotelFacing` false — excluded from reviewed-path facing |
| `gdi_opp_prospect_mv8fnmk1_e4668916` | U.S. Conference of Mayors — next DC cycle | Same filter |
| `gdi_opp_prospect_mv8fnmk2_a845a23a` | AAD Annual Meeting 2028 | Same filter |
| `gdi_opp_prospect_mv8fnmk1_ad300879` | Arts Advocacy Summit — next DC cycle | Same filter |
| `gdi_sc_0fe98055e8df9f75` | Wisedocs — Scaling Corporate Account | Present; `prospectHotelFacing` true but **missing** `prospectPublicationBasis` |

**Explanation:** Earlier 35 used broad facing (`prospectHotelFacing OR customerVisible`). Current 30 is the reviewed-path / salesperson customer list. No unsupported restore; no silent extra removals.

## San Diego — 8 IDs in earlier 46, not in current 38

Reconstruct: current 38 + 8 excluded-not-in-current = 46. One prior exclusion (`gdi_sc_2340eb7f036aecac`) is **inside** current 38 (eligibility later satisfied).

| Opportunity ID | Title | Disposition |
|----------------|-------|-------------|
| `gdi_pe_295cfef5822e63c3` | Sparks Gallery | Present locally; PE preferred-lodging; out of current reviewed-path facing |
| `gdi_pe_879047c3211d2e82` | City Cruises San Diego | Same |
| `gdi_pe_5ce20a54b903e243` | University Club San Diego | Same |
| `gdi_pe_ca47fef6aa622566` | deck655 | Same |
| `gdi_pe_5f72b9d7665314b5` | Flagship Cruises & Events | Same |
| `gdi_pe_4b9b49bae4136428` | Nolita House | Same |
| `gdi_pe_a7a2fda2a474aa0b` | The House Next Door | Same |
| `gdi_sc_188e25b6b3b56637` | The Shipyard | Present; missing publication basis |

**Explanation:** Scope mismatch between broad facing (46) and current customer-facing freeze (38). Records retained in store; not restored into the customer list.

## Consistency

Type counts, list totals, and exports must continue to use the **same** customer-facing filter as the list API. This history work does not alter that filter.

## Live production after `fed35cc7` deploy (2026-10-15)

| Hotel | Live facing | Notes |
|-------|------------:|-------|
| Mayflower | **30** | Matches freeze / reviewed-path total |
| San Diego | **45** | Live list is above freeze 38 and below earlier broad 46; PE preferred-lodging rows that were out of the freeze reviewed-path set are visible again (e.g. Sparks Gallery, City Cruises). No eligibility code change in this history PR — count drift is inventory/filter state on Airtable, not forced restore/removal. |
