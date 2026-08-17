# Brand Registry Resolution v1

**Status:** `production_census_brand_registry_resolution_v1_partial_remaining`
**Objective:** `brand-registry-resolution-v1`
**Write target:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)
**Airtable writes:** yes
**Brand Setup writes:** false
**Brand Explorer writes:** false

## Before / After

| Metric | Before | After |
| --- | ---: | ---: |
| Unknown brands (not in official census registry) | 27 | 16 |
| Human Review (brand-related) | 24 | 16 |
| brand_code_unresolved | 0 | 0 |
| Clean Core pass (approx) | 779 | 784 |
| Excluded from Clean Core | 312 | 307 |
| High brand remaps applied | — | 119 |
| Dirty partner labels | — | 16 |
| Evidence-backed non-active brands | — | 367 |
| Brand Setup promotion candidates | — | 52 |

## Classification counters

| Class | Count |
| --- | ---: |
| Scanned | 1091 |
| Targets | 385 |
| high_confidence_brand_remap | 0 |
| evidence_backed_non_active_brand | 367 |
| source_code_decoded | 2 |
| brand_code_unresolved | 0 |
| brand_source_conflict | 0 |
| dirty_partner_label | 16 |

## Examples before / after

- `SAM` → `SAM` (dirty_partner_label / accor_managed_by_sam_code) — Club Regina Los Cabos managed by Accor
- `IHG Partner / Spnd` → `IHG Partner / Spnd` (dirty_partner_label / ihg_partner_spnd_artifact) — 2026 Best Vacation Destinations
- `Choice Hotels` → `Choice Hotels` (dirty_partner_label / generic_choice_partner_label) — Choice property MX197
- `Marriott Bonvoy — Brand Unconfirmed` → `Marriott Bonvoy — Brand Unconfirmed` (dirty_partner_label / marriott_brand_unconfirmed) — Gran Hotel de Puebla by HNF
- `SAM` → `SAM` (dirty_partner_label / accor_managed_by_sam_code) — The Paragon Hotel Mexico City Santa Fe By Accor
- `SLS` → `SLS` (source_code_decoded / accor_catalog_brand_code) — SLS PLAYA MUJERES
- `SAM` → `SAM` (dirty_partner_label / accor_managed_by_sam_code) — Club Regina Puerto Vallarta managed by Accor
- `Choice Hotels` → `Choice Hotels` (dirty_partner_label / generic_choice_partner_label) — Choice property MX210
- `SAM` → `SAM` (dirty_partner_label / accor_managed_by_sam_code) — Park Royal Huatulco managed by Accor
- `SAM` → `SAM` (dirty_partner_label / accor_managed_by_sam_code) — Park Royal Home Stay Los Tules managed by Accor
- `Marriott Bonvoy — Brand Unconfirmed` → `Marriott Bonvoy — Brand Unconfirmed` (dirty_partner_label / marriott_brand_unconfirmed) — CASA MAYOR Saltillo, Hotel Hacienda
- `Marriott Bonvoy — Brand Unconfirmed` → `Marriott Bonvoy — Brand Unconfirmed` (dirty_partner_label / marriott_brand_unconfirmed) — SJ Grand Hotel Monterrey
- `SLS` → `SLS` (source_code_decoded / accor_catalog_brand_code) — SLS Cancun
- `SAM` → `SAM` (dirty_partner_label / accor_managed_by_sam_code) — Club Regina Cancun managed by Accor
- `IHG Partner / Spnd` → `IHG Partner / Spnd` (dirty_partner_label / ihg_partner_spnd_artifact) — 2026 Best Vacation Destinations
- `SAM` → `SAM` (dirty_partner_label / accor_managed_by_sam_code) — Park Royal Mazatlan managed by AccorHotels
- `SAM` → `SAM` (dirty_partner_label / accor_managed_by_sam_code) — Hotel Boutique Bovedas de Santa Clara By Accor
- `Marriott Bonvoy — Brand Unconfirmed` → `Marriott Bonvoy — Brand Unconfirmed` (dirty_partner_label / marriott_brand_unconfirmed) — Hotel Guadalajara Country Club by HNF

## Unresolved steward

- `SAM` — accor_managed_by_sam_code / dirty_partner_label — Club Regina Los Cabos managed by Accor
- `JW Marriott` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — JW Marriott Hotel Bogota
- `Esplendor by Wyndham` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Wyndham Alltra Playa del Carmen Adults Only All Inclusive
- `St. Regis` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — The St. Regis Costa Mujeres Resort, Cancun
- `Holiday Inn` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Holiday Inn Tuxtla Gutierrez
- `City Express Junior by Marriott` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — City Express Junior by Marriott León Centro De Convenciones
- `Esplendor by Wyndham` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Microtel Inn & Suites by Wyndham Guadalajara Sur
- `Staybridge Suites` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Staybridge Suites Puebla
- `Candlewood Suites` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Candlewood Suites Queretaro Juriquilla
- `JOIA Iberostar` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — JOIA Paraíso by Iberostar
- `Holiday Inn Resort` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Holiday Inn Resort Monterrey - Santiago
- `Le Méridien` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Le Méridien Mexico City Reforma
- `Delta Hotels` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Delta Hotels by Marriott San Jose Aurola
- `W Hotels` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — W Punta Cana, Adult All-Inclusive
- `Wyndham Alltra` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Wyndham Alltra Playa del Carmen Adults Only All Inclusive
- `InterContinental` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — InterContinental Presidente Cozumel Resort Spa
- `InterContinental` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — InterContinental Medellin - Movich
- `Holiday Inn` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Holiday Inn Cartagena Morros
- `City Express Plus by Marriott` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — City Express Plus by Marriott Ciudad de México Reforma El Ángel
- `Holiday Inn` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Holiday Inn Villahermosa Aeropuerto
- `Holiday Inn` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Holiday Inn Mexico Dali Airport
- `Esplendor by Wyndham` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Los Cabos Golf Resort, Trademark Collection by Wyndham
- `Fairfield by Marriott` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Fairfield by Marriott Los Cabos
- `Fairfield by Marriott` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Fairfield by Marriott San Jose Airport Alajuela
- `Staybridge Suites` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Staybridge Suites Guadalajara Expo
- `Four Points by Sheraton` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Four Points by Sheraton San Jose Costa Rica
- `Holiday Inn` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Holiday Inn Queretaro-Centro Historico
- `IHG Partner / Spnd` — ihg_partner_spnd_artifact / dirty_partner_label — 2026 Best Vacation Destinations
- `Iberostar Waves` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Iberostar Waves Dominicana
- `Holiday Inn` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Holiday Inn Huatulco
- `Wyndham Garden` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Wyndham Garden Monterrey Norte
- `Fairfield by Marriott` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Fairfield by Marriott Inn & Suites Queretaro Juriquilla
- `Crowne Plaza` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Crowne Plaza Merida
- `Choice Hotels` — generic_choice_partner_label / dirty_partner_label — Choice property MX197
- `InterContinental` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — InterContinental Costa Rica at Multiplaza Mall
- `Ramada` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Ramada Encore by Wyndham Guadalajara Sur
- `Holiday Inn` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Holiday Inn Santo Domingo
- `Registry Collection` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Grand Palladium Palace, All Inclusive Resort, Spa & Casino
- `Sofitel` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Sofitel Legend Santa Clara Cartagena
- `Four Points by Sheraton` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Four Points by Sheraton Mexico City, Colonia Roma
- `JW Marriott` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — JW Marriott Cancun Resort & Spa
- `Iberostar` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Iberostar Waves Quetzal
- `Holiday Inn` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Holiday Inn Monterrey Norte
- `City Express Plus by Marriott` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — City Express Plus by Marriott Guadalajara Palomar
- `Holiday Inn` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Holiday Inn Orizaba
- `Renaissance Hotels` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Renaissance Panama City Hotel
- `Hilton Grand Vacations` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Hilton Grand Vacations Club Zihuatanejo Mexico
- `St. Regis` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — The St. Regis Punta Mita Resort
- `Holiday Inn` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Holiday Inn & Suites Monterrey Apodaca Zona Airport
- `Holiday Inn` — evidence_backed_non_active_brand / evidence_backed_non_active_brand — Holiday Inn Bogota Airport

## Chained source-confirmed-census-v2

- Ran: yes
- Status: `production_census_source_confirmed_census_v2_partial_steward_remaining`
- Updates: 2
- Clean Core after chain: 784

## Safety

- Hotel Property Census only
- Brand Setup / Brand Explorer untouched
- No address / coords / phone / rooms
- No owner/operator/date writes
- No opaque code guessing / no hotel-name-only brand inference
- Evidence-backed non-active brands reported in promotion pack, not forced into Active dictionary
- Unresolved codes excluded from Clean Core
