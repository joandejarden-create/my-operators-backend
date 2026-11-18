# Synthetic and provisional data audit

**Goal:** Ensure test/seed/provisional rows cannot silently become hotel-confirmed need periods, customer-approved priorities, verified opportunities, or production account intelligence.

## Confirmed synthetic / test records

### GDI Hotel Commercial Profiles (`tbl8KSNUCRbIzEv15`)

| Record | Hotel | Provenance | Risk |
|--------|-------|------------|------|
| `rec86UINJDXLGaMcM` (`gcp_e2c2d2ae3be9`) | Waterstone `recgMYovrrZDJMqzX` | `Synthetic Test`; SYNTHETIC_TEST accounts; notes say not hotel-confirmed | **High** if treated as customer commercial priorities |
| `rec71k2C0BMAvLS3T` (`gcp_cf1e984a3bfa`) | Bethesda `recLuxvwwxID7U2B8` | Provenance `Not Provided` | Low — empty/placeholder, not synthetic accounts |

### GDI Hotel Need Periods (`tbl50kj5XkGs22C6j`)

| Pattern | Hotels | Markers | Risk |
|---------|--------|---------|------|
| Waterstone founder test / lifecycle | Waterstone | `founder_test_scenario_v3`, `founder_lifecycle_isolate`, Status Test Scenario / Withdrawn | High if Status promoted to Active without hotel confirm |
| Sheraton provisional planning | Guadalajara `recCAGqkERSXgiku4` | Source `PROVISIONAL_INTERNAL_PLANNING_SCENARIO_SHERATON_GDL_V1`, Status **Test Scenario**, notes “NOT hotel-confirmed” | Medium for research matching; **must not** equal hotel-confirmed occupancy |
| Bethesda hotel-reported | Bethesda | Source `HOTEL_REPORTED`, Status Active | Legitimate production need windows |

Presentation code already refuses TEST_SCENARIO / SYNTHETIC for customer need-date icons (`opportunity-need-date-tile-preview-v1.js`). Keep that fail-closed.

### Hotel Demand Generator Fit + Research Targets

Large populations with `generic_seed` / `gdi_hotel_onboard_seed_v1` in Fit Rationale / Reason Monitored / Generator Research Reason.

| Risk | Detail |
|------|--------|
| Misread as verified hotel-demand fit | Seed graph is portable template onboarding, not hotel sales confirmation |
| Full E2E overwrite | Upserts must not “upgrade” seed provenance to verified without evidence |
| Customer confusion | Seeds must never enter CURRENT publication as confirmed accounts |

### Demand Generators

Some generators carry `generic_onboard_seed` provenance in payload/Research Reason. Org may still be a real association — provenance marks **how it entered**, not that the org is fake. Treat as **watchlist/seed** until research upgrades evidence.

## Isolation weaknesses

1. **Same tables hold production + synthetic/provisional** (need periods, commercial profiles) — isolation is field/status based, not separate bases.  
2. **Match rows can attach to provisional periods** (evidence shows `provisional:true` in Matching Evidence) — matches must inherit non-customer eligibility from period Status/Source.  
3. **Generic Fit/Targets lack a single hard “SEED_ONLY” select** on all rows — reliance on free-text rationale.  
4. **No automatic promotion path found that rewrites Status Test Scenario → Active** in Full E2E contract (good); portal writers remain the sensitive path.

## Required controls before persistence activation

1. Full E2E must **not write** GDI Hotel Commercial Profiles or Need Periods.  
2. Persist provenance on DG/Fit/Target upserts; never clear `generic_seed` markers when only identity-matching.  
3. Customer / publication paths continue to exclude TEST_SCENARIO / SYNTHETIC / PROVISIONAL-only planning periods.  
4. Founder decision: quarantine or label Waterstone synthetic profile for operator clarity (no deletion in this audit).
