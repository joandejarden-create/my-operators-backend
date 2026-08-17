# Brand Registry Resolution v1

**Status:** `production_census_brand_registry_resolution_v1_no_safe_writes_remaining`
**Objective:** `brand-registry-resolution-v1`
**Write target:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)
**Airtable writes:** no
**Brand Setup writes:** false
**Brand Explorer writes:** false

## Before / After

| Metric | Before | After |
| --- | ---: | ---: |
| Unknown brands (not in official census registry) | 16 | 16 |
| Human Review (brand-related) | 16 | 16 |
| brand_code_unresolved | 0 | 0 |
| Clean Core pass (approx) | 784 | 784 |
| Excluded from Clean Core | 307 | 307 |
| High brand remaps applied | — | 0 |
| Dirty partner labels | — | 16 |
| Evidence-backed non-active brands | — | 369 |
| Brand Setup promotion candidates | — | 9 |

## Classification counters

| Class | Count |
| --- | ---: |
| Scanned | 1091 |
| Targets | 385 |
| high_confidence_brand_remap | 0 |
| evidence_backed_non_active_brand | 369 |
| source_code_decoded | 0 |
| brand_code_unresolved | 0 |
| brand_source_conflict | 0 |
| dirty_partner_label | 16 |

## Examples before / after

_None_

## Unresolved steward

- `SAM` — accor_managed_by_sam_code / dirty_partner_label — Club Regina Los Cabos managed by Accor
- `IHG Partner / Spnd` — ihg_partner_spnd_artifact / dirty_partner_label — 2026 Best Vacation Destinations
- `Choice Hotels` — generic_choice_partner_label / dirty_partner_label — Choice property MX197
- `Marriott Bonvoy — Brand Unconfirmed` — marriott_brand_unconfirmed / dirty_partner_label — Gran Hotel de Puebla by HNF
- `SAM` — accor_managed_by_sam_code / dirty_partner_label — The Paragon Hotel Mexico City Santa Fe By Accor
- `SAM` — accor_managed_by_sam_code / dirty_partner_label — Club Regina Puerto Vallarta managed by Accor
- `Choice Hotels` — generic_choice_partner_label / dirty_partner_label — Choice property MX210
- `SAM` — accor_managed_by_sam_code / dirty_partner_label — Park Royal Huatulco managed by Accor
- `SAM` — accor_managed_by_sam_code / dirty_partner_label — Park Royal Home Stay Los Tules managed by Accor
- `Marriott Bonvoy — Brand Unconfirmed` — marriott_brand_unconfirmed / dirty_partner_label — CASA MAYOR Saltillo, Hotel Hacienda
- `Marriott Bonvoy — Brand Unconfirmed` — marriott_brand_unconfirmed / dirty_partner_label — SJ Grand Hotel Monterrey
- `SAM` — accor_managed_by_sam_code / dirty_partner_label — Club Regina Cancun managed by Accor
- `IHG Partner / Spnd` — ihg_partner_spnd_artifact / dirty_partner_label — 2026 Best Vacation Destinations
- `SAM` — accor_managed_by_sam_code / dirty_partner_label — Park Royal Mazatlan managed by AccorHotels
- `SAM` — accor_managed_by_sam_code / dirty_partner_label — Hotel Boutique Bovedas de Santa Clara By Accor
- `Marriott Bonvoy — Brand Unconfirmed` — marriott_brand_unconfirmed / dirty_partner_label — Hotel Guadalajara Country Club by HNF

## Chained source-confirmed-census-v2

- Ran: yes
- Status: `production_census_source_confirmed_census_v2_partial_steward_remaining`
- Updates: 0
- Clean Core after chain: 784

## Safety

- Hotel Property Census only
- Brand Setup / Brand Explorer untouched
- No address / coords / phone / rooms
- No owner/operator/date writes
- No opaque code guessing / no hotel-name-only brand inference
- Evidence-backed non-active brands reported in promotion pack, not forced into Active dictionary
- Unresolved codes excluded from Clean Core
