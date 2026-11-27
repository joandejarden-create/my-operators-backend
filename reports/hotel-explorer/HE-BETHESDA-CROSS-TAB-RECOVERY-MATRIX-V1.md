# Bethesda Marriott — cross-tab recovery matrix (V1)

**Hotel:** Bethesda Marriott  
**Canonical HPC:** `recLuxvwwxID7U2B8`  
**Legacy Hotel Census:** PROHIBITED  
**Date:** 2026-11-27

## Method

Inventory existing research / HPC / HI graph / staged artifacts **before** any new paid research.
Classify each gap as mapping/rendering, relationship, API omission, query exclusion, staged-not-persisted, or genuinely unverified.

## Source → destination matrix

| Attribute | Canonical source | Overview | Org & Portfolio (before) | Submarket (before) | Classification | Recovery action |
| --- | --- | --- | --- | --- | --- | --- |
| 407 guestrooms | HPC `Rooms / Keys` + physical-product adapter | Shown | Unresolved / missing on focus row | Selected hotel rooms when HPC loads | **MAPPING_RENDERING_DEFECT** | Shared hotel view model + portfolio hydrate |
| Market Bethesda / Montgomery County | HPC `Market` | Shown | Often blank on portfolio row | Grouping label when present | **MAPPING_RENDERING_DEFECT** | Hydrate focus row from shared VM |
| Brand Marriott Hotels | HPC `Current Brand` | Shown | Often blank | Selected brand | **MAPPING_RENDERING_DEFECT** | Shared VM |
| Operator Marriott International | HPC Operator + HI Operator edge | Shown | Operator groups may show Not Verified for focus | N/A as PropCo | **MAPPING_RENDERING_DEFECT** / relationship OK | Shared VM + HI Operator |
| PropCo Marbeth Acquisition | HI Legal Owner (Validated) | Sometimes gap | Not rendered as property attribute | N/A | **PERSISTED_NOT_CONNECTED** | Shared VM `propertyCompany` from HI Legal Owner |
| Economic sponsor Mullen | HI Economic Owner | Over-promoted Validated historically | Portfolio org name | N/A | **CLASSIFICATION** | Preserve **Proposed** founder classification |
| Submarket Pooks Hill / Rockville Pike peers | HPC exact Submarket match | N/A | N/A | 1 hotel (selected force-include) | **QUERY_COVERAGE** (not fabricated market) | Distinct cohorts: matched / geographic market / competitive set + coverage disclosure |
| Geographic market peers | HPC Market cohort | N/A | N/A | Not shown separately | **ABSENT_FROM_PAYLOAD** | `geographicSubmarketInventory` cohort |
| Room-type mix 171/236/8 | Staged brand materials | Rooms tab overlay | N/A | N/A | **STAGED_REFERENCE** — do not invent | Leave as staged; not Org Portfolio |

## Prior artifacts reused (no restart)

- `reports/hotel-explorer/he-intelligence-factory-v1/PHASE1_BETHESDA_RECOVERY_INVENTORY.json`
- `reports/hotel-explorer/HE-BETH-HPC-SUBMARKET-COVERAGE-V1.*` (note: older report used legacy Hotel Census — superseded by HPC SoT)
- `reports/hotel-explorer/phase4-quality/HE-BETH-MULLEN-OWNER-PORTFOLIO-*`
- HI graph apply: `BETH_PHASE3_HI_GRAPH_*.json`

## Permanent gates

- `HE_SUBMARKET_COHORT_VALIDATED`
- `HE_SUBMARKET_COVERAGE_DISCLOSED`
- `HE_ORG_PORTFOLIO_HYDRATION_VALIDATED`
- `HE_SHARED_HOTEL_ATTRIBUTES_CONSISTENT`
- `HE_CROSS_TAB_PARITY_VALIDATED`

## Eight-hotel rule

Mappings and cohort logic are generic. Bethesda rooms/brand/market values must never be hardcoded onto unrelated hotels.
