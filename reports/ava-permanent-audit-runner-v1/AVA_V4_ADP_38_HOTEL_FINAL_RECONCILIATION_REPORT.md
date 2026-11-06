# AVA V4 ADP 38-Hotel Final Reconciliation Report

- **Generated:** 2026-11-06T11:10:25.502Z
- **Contract:** AVA_V4_ADP_38_HOTEL_FINAL_RECONCILIATION_V1
- **Mode:** APPLY_REGENERATE
- **Branch context:** release/ava-v4-canonical-integration-v1 (PR #147)
- **Stop rule:** no merge / deploy / customer publication in this wave

## Before / after certification counts

| Status | Before | After |
|---|---:|---:|
| CERTIFIED_MATCH | 1 | 21 |
| CERTIFIED_HISTORICAL_REFERENCE | 17 | 17 |
| RECONCILIATION_REQUIRED | 5 | 0 |
| CERTIFICATION_BLOCKED | 15 | 0 |
| **Total** | **38** | **38** |

## Phase 1 — Root-cause matrix (20 blocked / reconciliation-required)

All 20 hotels shared the same primary failure class: **C. Incorrect metric projection** — AVA KPI tiles still mirrored the lean staged pin while the same-period customer-facing approved/published ADP package carried the authoritative certified measurement (typically AI Consideration).

| Hotel | Status (before) | Class | Conflicting metric | AVA (actual) | ADP authoritative (expected) | Source | Root cause | Remediation |
|---|---|---|---|---|---|---|---|---|
| Casas del XVI (`adp_casas_del_xvi`) | RECONCILIATION_REQUIRED | C | AI_CONSIDERATION | 10.6 | 10.1 | PUBLISHED_CURRENT | AVA KPI mirrored lean staged pin; same-period approved/published certifiedMeasurement differs | Project AVA consideration (and any mismatched reality/scenario tiles) from authoritative customer ADP package; preserve findings/actions |
| Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca (`adp_castillo_hotel_son_vida`) | CERTIFICATION_BLOCKED | C | AI_CONSIDERATION | 50 | 57.7 | PUBLISHED_CURRENT | AVA KPI mirrored lean staged pin; same-period approved/published certifiedMeasurement differs | Project AVA consideration (and any mismatched reality/scenario tiles) from authoritative customer ADP package; preserve findings/actions |
| Faranda Collection Bogotá (`adp_faranda_collection_bogota`) | CERTIFICATION_BLOCKED | C | AI_CONSIDERATION | 0.5 | 5 | PUBLISHED_CURRENT | AVA KPI mirrored lean staged pin; same-period approved/published certifiedMeasurement differs | Project AVA consideration (and any mismatched reality/scenario tiles) from authoritative customer ADP package; preserve findings/actions |
| Fiesta Americana Condesa Cancún All Inclusive (`adp_fiesta_americana_condesa_cancun_ai`) | CERTIFICATION_BLOCKED | C | AI_CONSIDERATION | 2.8 | 9.7 | PUBLISHED_CURRENT | AVA KPI mirrored lean staged pin; same-period approved/published certifiedMeasurement differs | Project AVA consideration (and any mismatched reality/scenario tiles) from authoritative customer ADP package; preserve findings/actions |
| Grand Fiesta Americana Chapultepec (`adp_grand_fiesta_americana_chapultepec`) | CERTIFICATION_BLOCKED | C | AI_CONSIDERATION | 4.7 | 8.9 | PUBLISHED_CURRENT | AVA KPI mirrored lean staged pin; same-period approved/published certifiedMeasurement differs | Project AVA consideration (and any mismatched reality/scenario tiles) from authoritative customer ADP package; preserve findings/actions |
| Hilton New York Times Square (`adp_hilton_times_square`) | CERTIFICATION_BLOCKED | C | AI_CONSIDERATION | 10.4 | 9.6 | PUBLISHED_CURRENT | AVA KPI mirrored lean staged pin; same-period approved/published certifiedMeasurement differs | Project AVA consideration (and any mismatched reality/scenario tiles) from authoritative customer ADP package; preserve findings/actions |
| Hotel Caribe by Faranda Grand, a member of Radisson Individuals (`adp_hotel_caribe_faranda_grand`) | CERTIFICATION_BLOCKED | C | AI_CONSIDERATION | 30.7 | 47.3 | PUBLISHED_CURRENT | AVA KPI mirrored lean staged pin; same-period approved/published certifiedMeasurement differs | Project AVA consideration (and any mismatched reality/scenario tiles) from authoritative customer ADP package; preserve findings/actions |
| JW Marriott Hotel Monterrey Valle (`adp_jw_marriott_monterrey_valle`) | CERTIFICATION_BLOCKED | C | AI_CONSIDERATION | 36.7 | 34.3 | PUBLISHED_CURRENT | AVA KPI mirrored lean staged pin; same-period approved/published certifiedMeasurement differs | Project AVA consideration (and any mismatched reality/scenario tiles) from authoritative customer ADP package; preserve findings/actions |
| JW Marriott Hotel Santo Domingo (`adp_jw_marriott_santo_domingo`) | CERTIFICATION_BLOCKED | C | AI_CONSIDERATION | 65.1 | 64 | PUBLISHED_CURRENT | AVA KPI mirrored lean staged pin; same-period approved/published certifiedMeasurement differs | Project AVA consideration (and any mismatched reality/scenario tiles) from authoritative customer ADP package; preserve findings/actions |
| Radisson Blu Toronto Downtown (`adp_radisson_blu_toronto_downtown`) | CERTIFICATION_BLOCKED | C | AI_CONSIDERATION | 29 | 33.2 | PUBLISHED_CURRENT | AVA KPI mirrored lean staged pin; same-period approved/published certifiedMeasurement differs | Project AVA consideration (and any mismatched reality/scenario tiles) from authoritative customer ADP package; preserve findings/actions |
| Radisson Hotel Santo Domingo (`adp_radisson_santo_domingo`) | RECONCILIATION_REQUIRED | C | AI_CONSIDERATION | 14.6 | 14.5 | PUBLISHED_CURRENT | AVA KPI mirrored lean staged pin; same-period approved/published certifiedMeasurement differs | Project AVA consideration (and any mismatched reality/scenario tiles) from authoritative customer ADP package; preserve findings/actions |
| Sheraton Guadalajara Expo (`adp_sheraton_guadalajara_expo`) | CERTIFICATION_BLOCKED | C | AI_CONSIDERATION | 11.9 | 12.7 | PUBLISHED_CURRENT | AVA KPI mirrored lean staged pin; same-period approved/published certifiedMeasurement differs | Project AVA consideration (and any mismatched reality/scenario tiles) from authoritative customer ADP package; preserve findings/actions |
| Sheraton Mallorca Arabella Golf Hotel (`adp_sheraton_mallorca_arabella_golf`) | CERTIFICATION_BLOCKED | C | AI_CONSIDERATION | 20.6 | 22.4 | PUBLISHED_CURRENT | AVA KPI mirrored lean staged pin; same-period approved/published certifiedMeasurement differs | Project AVA consideration (and any mismatched reality/scenario tiles) from authoritative customer ADP package; preserve findings/actions |
| The St. Regis Cap Cana Resort (`adp_st_regis_cap_cana`) | RECONCILIATION_REQUIRED | C | AI_CONSIDERATION | 17.6 | 17.3 | PUBLISHED_CURRENT | AVA KPI mirrored lean staged pin; same-period approved/published certifiedMeasurement differs | Project AVA consideration (and any mismatched reality/scenario tiles) from authoritative customer ADP package; preserve findings/actions |
| The St. Regis Mexico City (`adp_st_regis_mexico_city`) | CERTIFICATION_BLOCKED | C | AI_CONSIDERATION | 71.9 | 69.2 | PUBLISHED_CURRENT | AVA KPI mirrored lean staged pin; same-period approved/published certifiedMeasurement differs | Project AVA consideration (and any mismatched reality/scenario tiles) from authoritative customer ADP package; preserve findings/actions |
| UMusic Hotel Madrid (`adp_umusic_hotel_madrid`) | RECONCILIATION_REQUIRED | C | AI_CONSIDERATION | 1.1 | 1.3 | PUBLISHED_CURRENT | AVA KPI mirrored lean staged pin; same-period approved/published certifiedMeasurement differs | Project AVA consideration (and any mismatched reality/scenario tiles) from authoritative customer ADP package; preserve findings/actions |
| voco Cancún Zona Hotelera (`adp_voco_cancun_zona_hotelera`) | RECONCILIATION_REQUIRED | C | AI_CONSIDERATION | 0.4 | 0.5 | PUBLISHED_CURRENT | AVA KPI mirrored lean staged pin; same-period approved/published certifiedMeasurement differs | Project AVA consideration (and any mismatched reality/scenario tiles) from authoritative customer ADP package; preserve findings/actions |
| The Westin Monterrey Valle (`adp_westin_monterrey_valle`) | CERTIFICATION_BLOCKED | C | AI_CONSIDERATION | 27.1 | 26.2 | PUBLISHED_CURRENT | AVA KPI mirrored lean staged pin; same-period approved/published certifiedMeasurement differs | Project AVA consideration (and any mismatched reality/scenario tiles) from authoritative customer ADP package; preserve findings/actions |
| Wyndham Alltra Punta Cana (`adp_wyndham_alltra_punta_cana`) | CERTIFICATION_BLOCKED | C | AI_CONSIDERATION | 3.4 | 2.7 | PUBLISHED_CURRENT | AVA KPI mirrored lean staged pin; same-period approved/published certifiedMeasurement differs | Project AVA consideration (and any mismatched reality/scenario tiles) from authoritative customer ADP package; preserve findings/actions |
| YOTEL Geneva Lake (`adp_yotel_geneva_lake`) | CERTIFICATION_BLOCKED | C | AI_CONSIDERATION | 21.7 | 24.4 | PUBLISHED_CURRENT | AVA KPI mirrored lean staged pin; same-period approved/published certifiedMeasurement differs | Project AVA consideration (and any mismatched reality/scenario tiles) from authoritative customer ADP package; preserve findings/actions |

## Phase 2 — Discrepancy resolutions

### Casas del XVI (`adp_casas_del_xvi`)

- **Before status:** RECONCILIATION_REQUIRED
- **After status:** CERTIFIED_MATCH
- **Class:** C — Incorrect metric projection
- **AVA audit version (pre):** AVA-RUN-CASAS_DEL_XVI-20261106T105051Z-f75e09d4
- **AVA ADP period:** adp_period_adp_casas_del_xvi_20261020091515_9971ab
- **Authoritative customer ADP period:** adp_period_adp_casas_del_xvi_20261020091515_9971ab
- **Fingerprints:** methodology `c097b23588c3` · content `625b8a47996f` · package `945115d42f4a`
- **Repairs applied:** 6
  - kpis.aiConsideration.value: "10.6%" → "10.1%"
  - kpis.aiConsideration.meta: "Mentioned in 20 of 188 comparable AI answers" → "Mentioned in 17 of 168 comparable monitored AI answers"
  - narrative.aiConsideration: 10.6 → 10.1
  - reporting.adpPackageDisclosure: null → "Customer-facing ADP package metrics (approved/published same period) are authoritative for AVA KPI tiles."
  - reporting.adpMethodologyFingerprintSha256: null → "c097b23588c365dc468957846005c5e104bc705cd813d5e62e49bbaf742960e1"
  - reporting.adpContentFingerprintSha256: null → "625b8a47996fd2e73e98a4b16308e9533b1974834d8a67da7f064f2e10ce54e3"
- **Regeneration:** ok=true auditRunId=AVA-RUN-CASAS_DEL_XVI-20261106T110732Z-a635f5b7 pdfSha=`fef69540f255`

### Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca (`adp_castillo_hotel_son_vida`)

- **Before status:** CERTIFICATION_BLOCKED
- **After status:** CERTIFIED_MATCH
- **Class:** C — Incorrect metric projection
- **AVA audit version (pre):** AVA-RUN-CASTILLO_HOTEL_SON_VIDA-20261106T105058Z-abf052ee
- **AVA ADP period:** adp_period_adp_castillo_hotel_son_vida_20261020101643_17a397
- **Authoritative customer ADP period:** adp_period_adp_castillo_hotel_son_vida_20261020101643_17a397
- **Fingerprints:** methodology `c097b23588c3` · content `97bc2f0ae4eb` · package `c245e14cbc6c`
- **Repairs applied:** 6
  - kpis.aiConsideration.value: "50.0%" → "57.7%"
  - kpis.aiConsideration.meta: "Share of comparable monitored AI answers that mentioned the hotel" → "Mentioned in 90 of 156 comparable monitored AI answers"
  - narrative.aiConsideration: 50 → 57.7
  - reporting.adpPackageDisclosure: null → "Customer-facing ADP package metrics (approved/published same period) are authoritative for AVA KPI tiles."
  - reporting.adpMethodologyFingerprintSha256: null → "c097b23588c365dc468957846005c5e104bc705cd813d5e62e49bbaf742960e1"
  - reporting.adpContentFingerprintSha256: null → "97bc2f0ae4eb508c8389d04f80281c9fb38bf4b5f0f3b77c07d01ef6e6751cda"
- **Regeneration:** ok=true auditRunId=AVA-RUN-CASTILLO_HOTEL_SON_VIDA-20261106T110744Z-e479e0e4 pdfSha=`85da77a23daa`

### Faranda Collection Bogotá (`adp_faranda_collection_bogota`)

- **Before status:** CERTIFICATION_BLOCKED
- **After status:** CERTIFIED_MATCH
- **Class:** C — Incorrect metric projection
- **AVA audit version (pre):** AVA-RUN-FARANDA_COLLECTION_BOGOT-20261106T105123Z-ef901134
- **AVA ADP period:** adp_period_adp_faranda_collection_bogota_20261020112903_714c94
- **Authoritative customer ADP period:** adp_period_adp_faranda_collection_bogota_20261020112903_714c94
- **Fingerprints:** methodology `c097b23588c3` · content `27ce5f3bbe8c` · package `803fc51a44a7`
- **Repairs applied:** 6
  - kpis.aiConsideration.value: "0.5%" → "5%"
  - kpis.aiConsideration.meta: "Mentioned in 1 of 192 comparable AI answers" → "Mentioned in 9 of 180 comparable monitored AI answers"
  - narrative.aiConsideration: 0.5 → 5
  - reporting.adpPackageDisclosure: null → "Customer-facing ADP package metrics (approved/published same period) are authoritative for AVA KPI tiles."
  - reporting.adpMethodologyFingerprintSha256: null → "c097b23588c365dc468957846005c5e104bc705cd813d5e62e49bbaf742960e1"
  - reporting.adpContentFingerprintSha256: null → "27ce5f3bbe8c547e5413801038600605e174a7f5874f16e3193b98284cd4a61f"
- **Regeneration:** ok=true auditRunId=AVA-RUN-FARANDA_COLLECTION_BOGOT-20261106T110753Z-a1c13e90 pdfSha=`e587c174b53e`

### Fiesta Americana Condesa Cancún All Inclusive (`adp_fiesta_americana_condesa_cancun_ai`)

- **Before status:** CERTIFICATION_BLOCKED
- **After status:** CERTIFIED_MATCH
- **Class:** C — Incorrect metric projection
- **AVA audit version (pre):** AVA-RUN-FIESTA_AMERICANA_CONDESA-20261106T105130Z-a14621d7
- **AVA ADP period:** adp_period_adp_fiesta_americana_condesa_cancun_ai_20261022082249_9aac31
- **Authoritative customer ADP period:** adp_period_adp_fiesta_americana_condesa_cancun_ai_20261022082249_9aac31
- **Fingerprints:** methodology `c097b23588c3` · content `f6495e02de9d` · package `139b4df59c89`
- **Repairs applied:** 6
  - kpis.aiConsideration.value: "2.8%" → "9.7%"
  - kpis.aiConsideration.meta: "Mentioned in 5 of 180 comparable monitored AI answers" → "Mentioned in 17 of 176 comparable monitored AI answers"
  - narrative.aiConsideration: 2.8 → 9.7
  - reporting.adpPackageDisclosure: null → "Customer-facing ADP package metrics (approved/published same period) are authoritative for AVA KPI tiles."
  - reporting.adpMethodologyFingerprintSha256: null → "c097b23588c365dc468957846005c5e104bc705cd813d5e62e49bbaf742960e1"
  - reporting.adpContentFingerprintSha256: null → "f6495e02de9da5fdbbda819f6ae4a986d32e85876279dc286ae84466e0b5e6ad"
- **Regeneration:** ok=true auditRunId=AVA-RUN-FIESTA_AMERICANA_CONDESA-20261106T110800Z-d737780d pdfSha=`7e2fa64d2091`

### Grand Fiesta Americana Chapultepec (`adp_grand_fiesta_americana_chapultepec`)

- **Before status:** CERTIFICATION_BLOCKED
- **After status:** CERTIFIED_MATCH
- **Class:** C — Incorrect metric projection
- **AVA audit version (pre):** AVA-RUN-GRAND_FIESTA_AMERICANA_C-20261106T105140Z-5ab57339
- **AVA ADP period:** adp_period_adp_grand_fiesta_americana_chapultepec_20261021225401_0726b1
- **Authoritative customer ADP period:** adp_period_adp_grand_fiesta_americana_chapultepec_20261021225401_0726b1
- **Fingerprints:** methodology `c097b23588c3` · content `267188d5cc86` · package `413798383300`
- **Repairs applied:** 6
  - kpis.aiConsideration.value: "4.7%" → "8.9%"
  - kpis.aiConsideration.meta: "Mentioned in 9 of 192 comparable monitored AI answers" → "Mentioned in 16 of 180 comparable monitored AI answers"
  - narrative.aiConsideration: 4.7 → 8.9
  - reporting.adpPackageDisclosure: null → "Customer-facing ADP package metrics (approved/published same period) are authoritative for AVA KPI tiles."
  - reporting.adpMethodologyFingerprintSha256: null → "c097b23588c365dc468957846005c5e104bc705cd813d5e62e49bbaf742960e1"
  - reporting.adpContentFingerprintSha256: null → "267188d5cc86fbd14f870a550702f660a4def84ceeefa183c9749ef6fcd954b5"
- **Regeneration:** ok=true auditRunId=AVA-RUN-GRAND_FIESTA_AMERICANA_C-20261106T110809Z-1beebd5e pdfSha=`4cec590c287a`

### Hilton New York Times Square (`adp_hilton_times_square`)

- **Before status:** CERTIFICATION_BLOCKED
- **After status:** CERTIFIED_MATCH
- **Class:** C — Incorrect metric projection
- **AVA audit version (pre):** AVA-RUN-HILTON_TIMES_SQUARE-20261106T105149Z-46c81758
- **AVA ADP period:** adp_period_adp_hilton_times_square_20261020121725_48a06f
- **Authoritative customer ADP period:** adp_period_adp_hilton_times_square_20261020121725_48a06f
- **Fingerprints:** methodology `c097b23588c3` · content `fdcc0dc77fa4` · package `d85c3b604c61`
- **Repairs applied:** 6
  - kpis.aiConsideration.value: "10.4%" → "9.6%"
  - kpis.aiConsideration.meta: "Mentioned in 22 of 212 comparable monitored AI answers" → "Mentioned in 18 of 188 comparable monitored AI answers"
  - narrative.aiConsideration: 10.4 → 9.6
  - reporting.adpPackageDisclosure: null → "Customer-facing ADP package metrics (approved/published same period) are authoritative for AVA KPI tiles."
  - reporting.adpMethodologyFingerprintSha256: null → "c097b23588c365dc468957846005c5e104bc705cd813d5e62e49bbaf742960e1"
  - reporting.adpContentFingerprintSha256: null → "fdcc0dc77fa4f0e03e5656bcfa119697f8c30f15ff079b62512ee6987b8574ba"
- **Regeneration:** ok=true auditRunId=AVA-RUN-HILTON_TIMES_SQUARE-20261106T110819Z-30f21c35 pdfSha=`6dcbb6177b89`

### Hotel Caribe by Faranda Grand, a member of Radisson Individuals (`adp_hotel_caribe_faranda_grand`)

- **Before status:** CERTIFICATION_BLOCKED
- **After status:** CERTIFIED_MATCH
- **Class:** C — Incorrect metric projection
- **AVA audit version (pre):** AVA-RUN-HOTEL_CARIBE_FARANDA_GRA-20261106T105158Z-1e604be3
- **AVA ADP period:** adp_period_adp_hotel_caribe_faranda_grand_20261020130953_f15cde
- **Authoritative customer ADP period:** adp_period_adp_hotel_caribe_faranda_grand_20261020130953_f15cde
- **Fingerprints:** methodology `c097b23588c3` · content `dd35e8bcda10` · package `a24b77696c74`
- **Repairs applied:** 6
  - kpis.aiConsideration.value: "30.7%" → "47.3%"
  - kpis.aiConsideration.meta: "Mentioned in 59 of 192 comparable AI answers" → "Mentioned in 106 of 224 comparable monitored AI answers"
  - narrative.aiConsideration: 30.7 → 47.3
  - reporting.adpPackageDisclosure: null → "Customer-facing ADP package metrics (approved/published same period) are authoritative for AVA KPI tiles."
  - reporting.adpMethodologyFingerprintSha256: null → "c097b23588c365dc468957846005c5e104bc705cd813d5e62e49bbaf742960e1"
  - reporting.adpContentFingerprintSha256: null → "dd35e8bcda10de939af79479e7088fa49855a357348c097b4bb7582a15dbd3f3"
- **Regeneration:** ok=true auditRunId=AVA-RUN-HOTEL_CARIBE_FARANDA_GRA-20261106T110828Z-98a5cc1c pdfSha=`1bbb64c6edbd`

### JW Marriott Hotel Monterrey Valle (`adp_jw_marriott_monterrey_valle`)

- **Before status:** CERTIFICATION_BLOCKED
- **After status:** CERTIFIED_MATCH
- **Class:** C — Incorrect metric projection
- **AVA audit version (pre):** AVA-RUN-JW_MARRIOTT_MONTERREY_VA-20261106T105214Z-d741312f
- **AVA ADP period:** adp_period_adp_jw_marriott_monterrey_valle_20261020134516_c6f5aa
- **Authoritative customer ADP period:** adp_period_adp_jw_marriott_monterrey_valle_20261020134516_c6f5aa
- **Fingerprints:** methodology `c097b23588c3` · content `3ddab68bec30` · package `cfe3cbddcc23`
- **Repairs applied:** 6
  - kpis.aiConsideration.value: "36.7%" → "34.3%"
  - kpis.aiConsideration.meta: "Mentioned in 72 of 196 comparable AI answers" → "Mentioned in 59 of 172 comparable monitored AI answers"
  - narrative.aiConsideration: 36.7 → 34.3
  - reporting.adpPackageDisclosure: null → "Customer-facing ADP package metrics (approved/published same period) are authoritative for AVA KPI tiles."
  - reporting.adpMethodologyFingerprintSha256: null → "c097b23588c365dc468957846005c5e104bc705cd813d5e62e49bbaf742960e1"
  - reporting.adpContentFingerprintSha256: null → "3ddab68bec305dc4bdc10af3c1454fa863f82b7e844154481f00839cbfebf981"
- **Regeneration:** ok=true auditRunId=AVA-RUN-JW_MARRIOTT_MONTERREY_VA-20261106T110835Z-6affd050 pdfSha=`cf476bb05c11`

### JW Marriott Hotel Santo Domingo (`adp_jw_marriott_santo_domingo`)

- **Before status:** CERTIFICATION_BLOCKED
- **After status:** CERTIFIED_MATCH
- **Class:** C — Incorrect metric projection
- **AVA audit version (pre):** AVA-RUN-JW_MARRIOTT_SANTO_DOMING-20261106T105222Z-d155c740
- **AVA ADP period:** adp_period_adp_jw_marriott_santo_domingo_20261020143300_c70cb0
- **Authoritative customer ADP period:** adp_period_adp_jw_marriott_santo_domingo_20261020143300_c70cb0
- **Fingerprints:** methodology `c097b23588c3` · content `6baae13eedde` · package `9a48098f284d`
- **Repairs applied:** 5
  - kpis.aiConsideration.value: "65.1%" → "64%"
  - kpis.aiConsideration.meta: "Mentioned in 125 of 192 comparable monitored AI answers" → "Mentioned in 110 of 172 comparable monitored AI answers"
  - reporting.adpPackageDisclosure: null → "Customer-facing ADP package metrics (approved/published same period) are authoritative for AVA KPI tiles."
  - reporting.adpMethodologyFingerprintSha256: null → "c097b23588c365dc468957846005c5e104bc705cd813d5e62e49bbaf742960e1"
  - reporting.adpContentFingerprintSha256: null → "6baae13eeddec658f6725e9c669811fba9f55602433c06edd844a15332f51e25"
- **Regeneration:** ok=true auditRunId=AVA-RUN-JW_MARRIOTT_SANTO_DOMING-20261106T110843Z-a5a3e5d7 pdfSha=`2c1912406c88`

### Radisson Blu Toronto Downtown (`adp_radisson_blu_toronto_downtown`)

- **Before status:** CERTIFICATION_BLOCKED
- **After status:** CERTIFIED_MATCH
- **Class:** C — Incorrect metric projection
- **AVA audit version (pre):** AVA-RUN-RADISSON_BLU_TORONTO_DOW-20261106T105254Z-9ebe26a3
- **AVA ADP period:** adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b
- **Authoritative customer ADP period:** adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b
- **Fingerprints:** methodology `c097b23588c3` · content `24c69d668429` · package `9fef571ea36b`
- **Repairs applied:** 6
  - kpis.aiConsideration.value: "29.0%" → "33.2%"
  - kpis.aiConsideration.meta: "Mentioned in 73 of 252 comparable monitored AI answers" → "Mentioned in 73 of 220 comparable monitored AI answers"
  - narrative.aiConsideration: 29 → 33.2
  - reporting.adpPackageDisclosure: null → "Customer-facing ADP package metrics (approved/published same period) are authoritative for AVA KPI tiles."
  - reporting.adpMethodologyFingerprintSha256: null → "c097b23588c365dc468957846005c5e104bc705cd813d5e62e49bbaf742960e1"
  - reporting.adpContentFingerprintSha256: null → "24c69d66842923c2c1acd8bb04ccebcf9860c5643153dc574e59540ce805a702"
- **Regeneration:** ok=true auditRunId=AVA-RUN-RADISSON_BLU_TORONTO_DOW-20261106T110851Z-f68a3011 pdfSha=`b5f75445d07d`

### Radisson Hotel Santo Domingo (`adp_radisson_santo_domingo`)

- **Before status:** RECONCILIATION_REQUIRED
- **After status:** CERTIFIED_MATCH
- **Class:** C — Incorrect metric projection
- **AVA audit version (pre):** AVA-RUN-RADISSON_SANTO_DOMINGO-20261106T105303Z-6d9312f7
- **AVA ADP period:** adp_period_adp_radisson_santo_domingo_20261020154516_c9b220
- **Authoritative customer ADP period:** adp_period_adp_radisson_santo_domingo_20261020154516_c9b220
- **Fingerprints:** methodology `c097b23588c3` · content `490a27f45681` · package `c8a9a1f1a964`
- **Repairs applied:** 6
  - kpis.aiConsideration.value: "14.6%" → "14.5%"
  - kpis.aiConsideration.meta: "Mentioned in 28 of 192 comparable AI answers" → "Mentioned in 25 of 172 comparable monitored AI answers"
  - narrative.aiConsideration: 14.6 → 14.5
  - reporting.adpPackageDisclosure: null → "Customer-facing ADP package metrics (approved/published same period) are authoritative for AVA KPI tiles."
  - reporting.adpMethodologyFingerprintSha256: null → "c097b23588c365dc468957846005c5e104bc705cd813d5e62e49bbaf742960e1"
  - reporting.adpContentFingerprintSha256: null → "490a27f456813ec0e7206e03b956a583b1c363cee1ff5f756e9133944442003a"
- **Regeneration:** ok=true auditRunId=AVA-RUN-RADISSON_SANTO_DOMINGO-20261106T110900Z-8ff17385 pdfSha=`46fd1b3ed220`

### Sheraton Guadalajara Expo (`adp_sheraton_guadalajara_expo`)

- **Before status:** CERTIFICATION_BLOCKED
- **After status:** CERTIFIED_MATCH
- **Class:** C — Incorrect metric projection
- **AVA audit version (pre):** AVA-RUN-SHERATON_GUADALAJARA_EXP-20261106T105326Z-dc5ddd4e
- **AVA ADP period:** adp_period_adp_sheraton_guadalajara_expo_20261103133203_79f540
- **Authoritative customer ADP period:** adp_period_adp_sheraton_guadalajara_expo_20261103133203_79f540
- **Fingerprints:** methodology `c097b23588c3` · content `2d21639b23f8` · package `a8bc5c72c832`
- **Repairs applied:** 8
  - kpis.aiConsideration.value: "11.9%" → "12.7%"
  - kpis.aiConsideration.meta: "Mentioned in 30 of 252 comparable monitored AI answers" → "Mentioned in 28 of 220 comparable monitored AI answers"
  - kpis.realityCoverage.value: "58.0%" → "32%"
  - narrative.aiConsideration: 11.9 → 12.7
  - narrative.realityCoverage: 58 → 32
  - reporting.adpPackageDisclosure: null → "Customer-facing ADP package metrics (approved/published same period) are authoritative for AVA KPI tiles."
  - reporting.adpMethodologyFingerprintSha256: null → "c097b23588c365dc468957846005c5e104bc705cd813d5e62e49bbaf742960e1"
  - reporting.adpContentFingerprintSha256: null → "2d21639b23f8ec41427f400381de843babf8a29261c01daac90efc016141d1b8"
- **Regeneration:** ok=true auditRunId=AVA-RUN-SHERATON_GUADALAJARA_EXP-20261106T110907Z-4163d14f pdfSha=`eebbd65c3ba5`

### Sheraton Mallorca Arabella Golf Hotel (`adp_sheraton_mallorca_arabella_golf`)

- **Before status:** CERTIFICATION_BLOCKED
- **After status:** CERTIFIED_MATCH
- **Class:** C — Incorrect metric projection
- **AVA audit version (pre):** AVA-RUN-SHERATON_MALLORCA_ARABEL-20261106T105334Z-c0607dd5
- **AVA ADP period:** adp_period_adp_sheraton_mallorca_arabella_golf_20261020170125_534b15
- **Authoritative customer ADP period:** adp_period_adp_sheraton_mallorca_arabella_golf_20261020170125_534b15
- **Fingerprints:** methodology `c097b23588c3` · content `3e9dddd3962c` · package `636d19b47d5d`
- **Repairs applied:** 6
  - kpis.aiConsideration.value: "20.6%" → "22.4%"
  - kpis.aiConsideration.meta: "Share of comparable monitored AI answers that mentioned the hotel" → "Mentioned in 35 of 156 comparable monitored AI answers"
  - narrative.aiConsideration: 20.6 → 22.4
  - reporting.adpPackageDisclosure: null → "Customer-facing ADP package metrics (approved/published same period) are authoritative for AVA KPI tiles."
  - reporting.adpMethodologyFingerprintSha256: null → "c097b23588c365dc468957846005c5e104bc705cd813d5e62e49bbaf742960e1"
  - reporting.adpContentFingerprintSha256: null → "3e9dddd3962c773d6a2e7275bfe2e5d429818f429f3acbdf00232ba9e6161921"
- **Regeneration:** ok=true auditRunId=AVA-RUN-SHERATON_MALLORCA_ARABEL-20261106T110915Z-f12ae113 pdfSha=`db1a6a6fa46a`

### The St. Regis Cap Cana Resort (`adp_st_regis_cap_cana`)

- **Before status:** RECONCILIATION_REQUIRED
- **After status:** CERTIFIED_MATCH
- **Class:** C — Incorrect metric projection
- **AVA audit version (pre):** AVA-RUN-ST_REGIS_CAP_CANA-20261106T105353Z-8ced53c2
- **AVA ADP period:** adp_period_adp_st_regis_cap_cana_20261020174932_a2bd90
- **Authoritative customer ADP period:** adp_period_adp_st_regis_cap_cana_20261020174932_a2bd90
- **Fingerprints:** methodology `c097b23588c3` · content `90156cdce2fc` · package `a95d3afccf99`
- **Repairs applied:** 5
  - kpis.aiConsideration.value: "17.6%" → "17.3%"
  - kpis.aiConsideration.meta: "Mentioned in 33 of 188 comparable monitored AI answers" → "Mentioned in 29 of 168 comparable monitored AI answers"
  - reporting.adpPackageDisclosure: null → "Customer-facing ADP package metrics (approved/published same period) are authoritative for AVA KPI tiles."
  - reporting.adpMethodologyFingerprintSha256: null → "c097b23588c365dc468957846005c5e104bc705cd813d5e62e49bbaf742960e1"
  - reporting.adpContentFingerprintSha256: null → "90156cdce2fc01cfad13e38d0a836ad92527a009e782f8075762d08c5c286051"
- **Regeneration:** ok=true auditRunId=AVA-RUN-ST_REGIS_CAP_CANA-20261106T110925Z-0b6d4ec5 pdfSha=`af58d02620fc`

### The St. Regis Mexico City (`adp_st_regis_mexico_city`)

- **Before status:** CERTIFICATION_BLOCKED
- **After status:** CERTIFIED_MATCH
- **Class:** C — Incorrect metric projection
- **AVA audit version (pre):** AVA-RUN-ST_REGIS_MEXICO_CITY-20261106T105402Z-695416f3
- **AVA ADP period:** adp_period_adp_st_regis_mexico_city_20261020184110_b23fe3
- **Authoritative customer ADP period:** adp_period_adp_st_regis_mexico_city_20261020184110_b23fe3
- **Fingerprints:** methodology `c097b23588c3` · content `08601481bd94` · package `83521a5b9035`
- **Repairs applied:** 6
  - kpis.aiConsideration.value: "71.9%" → "69.2%"
  - kpis.aiConsideration.meta: "Mentioned in 138 of 192 comparable monitored AI answers" → "Mentioned in 119 of 172 comparable monitored AI answers"
  - narrative.aiConsideration: 71.9 → 69.2
  - reporting.adpPackageDisclosure: null → "Customer-facing ADP package metrics (approved/published same period) are authoritative for AVA KPI tiles."
  - reporting.adpMethodologyFingerprintSha256: null → "c097b23588c365dc468957846005c5e104bc705cd813d5e62e49bbaf742960e1"
  - reporting.adpContentFingerprintSha256: null → "08601481bd945156b4fdfa69e016295a02248415b673322187ce08c23e862c5f"
- **Regeneration:** ok=true auditRunId=AVA-RUN-ST_REGIS_MEXICO_CITY-20261106T110934Z-1b91eda6 pdfSha=`16f520603c8c`

### UMusic Hotel Madrid (`adp_umusic_hotel_madrid`)

- **Before status:** RECONCILIATION_REQUIRED
- **After status:** CERTIFIED_MATCH
- **Class:** C — Incorrect metric projection
- **AVA audit version (pre):** AVA-RUN-UMUSIC_HOTEL_MADRID-20261106T105418Z-d5081cc2
- **AVA ADP period:** adp_period_adp_umusic_hotel_madrid_20261103073304_4bec18
- **Authoritative customer ADP period:** adp_period_adp_umusic_hotel_madrid_20261103073304_4bec18
- **Fingerprints:** methodology `c097b23588c3` · content `1e198d026942` · package `92db73a004b8`
- **Repairs applied:** 6
  - kpis.aiConsideration.value: "1.1%" → "1.3%"
  - kpis.aiConsideration.meta: "Mentioned in 2 of 184 comparable monitored AI answers" → "Mentioned in 2 of 160 comparable monitored AI answers"
  - kpis.realityCoverage.value: "50.0%" → "37.5%"
  - reporting.adpPackageDisclosure: null → "Customer-facing ADP package metrics (approved/published same period) are authoritative for AVA KPI tiles."
  - reporting.adpMethodologyFingerprintSha256: null → "c097b23588c365dc468957846005c5e104bc705cd813d5e62e49bbaf742960e1"
  - reporting.adpContentFingerprintSha256: null → "1e198d0269429e1d9ea8db620a4a1ad4e1ab16bc4249b7bb568ce299f86ecd68"
- **Regeneration:** ok=true auditRunId=AVA-RUN-UMUSIC_HOTEL_MADRID-20261106T110941Z-3c99f5cc pdfSha=`6c01b652ee1d`

### voco Cancún Zona Hotelera (`adp_voco_cancun_zona_hotelera`)

- **Before status:** RECONCILIATION_REQUIRED
- **After status:** CERTIFIED_MATCH
- **Class:** C — Incorrect metric projection
- **AVA audit version (pre):** AVA-RUN-VOCO_CANCUN_ZONA_HOTELER-20261106T105426Z-1b765334
- **AVA ADP period:** adp_period_adp_voco_cancun_zona_hotelera_20261103141133_bb45de
- **Authoritative customer ADP period:** adp_period_adp_voco_cancun_zona_hotelera_20261103141133_bb45de
- **Fingerprints:** methodology `c097b23588c3` · content `358a7c86792a` · package `98f98a975b05`
- **Repairs applied:** 8
  - kpis.aiConsideration.value: "0.4%" → "0.5%"
  - kpis.aiConsideration.meta: "Mentioned in 1 of 252 comparable monitored AI answers" → "Mentioned in 1 of 220 comparable monitored AI answers"
  - kpis.realityCoverage.value: "85.2%" → "14.8%"
  - narrative.aiConsideration: 0.4 → 0.5
  - narrative.realityCoverage: 85.2 → 14.8
  - reporting.adpPackageDisclosure: null → "Customer-facing ADP package metrics (approved/published same period) are authoritative for AVA KPI tiles."
  - reporting.adpMethodologyFingerprintSha256: null → "c097b23588c365dc468957846005c5e104bc705cd813d5e62e49bbaf742960e1"
  - reporting.adpContentFingerprintSha256: null → "358a7c86792a9fef2fbd8944e2a12b03e75d28278796645299230c8950050df7"
- **Regeneration:** ok=true auditRunId=AVA-RUN-VOCO_CANCUN_ZONA_HOTELER-20261106T110950Z-f41f991e pdfSha=`a3aafeb9a2ac`

### The Westin Monterrey Valle (`adp_westin_monterrey_valle`)

- **Before status:** CERTIFICATION_BLOCKED
- **After status:** CERTIFIED_MATCH
- **Class:** C — Incorrect metric projection
- **AVA audit version (pre):** AVA-RUN-WESTIN_MONTERREY_VALLE-20261106T105507Z-4ded9cc0
- **AVA ADP period:** adp_period_adp_westin_monterrey_valle_20261020203329_7e426f
- **Authoritative customer ADP period:** adp_period_adp_westin_monterrey_valle_20261020203329_7e426f
- **Fingerprints:** methodology `c097b23588c3` · content `6ced042d12c3` · package `c03b17d25dde`
- **Repairs applied:** 6
  - kpis.aiConsideration.value: "27.1%" → "26.2%"
  - kpis.aiConsideration.meta: "Mentioned in 52 of 192 comparable AI answers" → "Mentioned in 44 of 168 comparable monitored AI answers"
  - narrative.aiConsideration: 27.1 → 26.2
  - reporting.adpPackageDisclosure: null → "Customer-facing ADP package metrics (approved/published same period) are authoritative for AVA KPI tiles."
  - reporting.adpMethodologyFingerprintSha256: null → "c097b23588c365dc468957846005c5e104bc705cd813d5e62e49bbaf742960e1"
  - reporting.adpContentFingerprintSha256: null → "6ced042d12c3856e1bb867107aeb7bea5275216a7947875c7094ead7899c3b58"
- **Regeneration:** ok=true auditRunId=AVA-RUN-WESTIN_MONTERREY_VALLE-20261106T111000Z-17b8de3b pdfSha=`828def309a63`

### Wyndham Alltra Punta Cana (`adp_wyndham_alltra_punta_cana`)

- **Before status:** CERTIFICATION_BLOCKED
- **After status:** CERTIFIED_MATCH
- **Class:** C — Incorrect metric projection
- **AVA audit version (pre):** AVA-RUN-WYNDHAM_ALLTRA_PUNTA_CAN-20261106T105522Z-a9c9d314
- **AVA ADP period:** adp_period_adp_wyndham_alltra_punta_cana_20261103145403_091760
- **Authoritative customer ADP period:** adp_period_adp_wyndham_alltra_punta_cana_20261103145403_091760
- **Fingerprints:** methodology `c097b23588c3` · content `d230d8356a09` · package `ecd8d6c766e5`
- **Repairs applied:** 8
  - kpis.aiConsideration.value: "3.4%" → "2.7%"
  - kpis.aiConsideration.meta: "Mentioned in 8 of 236 comparable monitored AI answers" → "Mentioned in 6 of 224 comparable monitored AI answers"
  - kpis.realityCoverage.value: "68.2%" → "27.3%"
  - narrative.aiConsideration: 3.4 → 2.7
  - narrative.realityCoverage: 68.2 → 27.3
  - reporting.adpPackageDisclosure: null → "Customer-facing ADP package metrics (approved/published same period) are authoritative for AVA KPI tiles."
  - reporting.adpMethodologyFingerprintSha256: null → "c097b23588c365dc468957846005c5e104bc705cd813d5e62e49bbaf742960e1"
  - reporting.adpContentFingerprintSha256: null → "d230d8356a092c3fb2e7e2f70a8cfdd009eaeb4d482226f321a2b9f6696b2c9f"
- **Regeneration:** ok=true auditRunId=AVA-RUN-WYNDHAM_ALLTRA_PUNTA_CAN-20261106T111007Z-902bcd77 pdfSha=`30aab26e417b`

### YOTEL Geneva Lake (`adp_yotel_geneva_lake`)

- **Before status:** CERTIFICATION_BLOCKED
- **After status:** CERTIFIED_MATCH
- **Class:** C — Incorrect metric projection
- **AVA audit version (pre):** AVA-RUN-YOTEL_GENEVA_LAKE-20261106T105540Z-65b67570
- **AVA ADP period:** adp_period_adp_yotel_geneva_lake_20261020214553_0c6f77
- **Authoritative customer ADP period:** adp_period_adp_yotel_geneva_lake_20261020214553_0c6f77
- **Fingerprints:** methodology `c097b23588c3` · content `32c476b4ceba` · package `ae8b31777a52`
- **Repairs applied:** 6
  - kpis.aiConsideration.value: "21.7%" → "24.4%"
  - kpis.aiConsideration.meta: "Mentioned in 40 of 184 comparable AI answers" → "Mentioned in 39 of 160 comparable monitored AI answers"
  - narrative.aiConsideration: 21.7 → 24.4
  - reporting.adpPackageDisclosure: null → "Customer-facing ADP package metrics (approved/published same period) are authoritative for AVA KPI tiles."
  - reporting.adpMethodologyFingerprintSha256: null → "c097b23588c365dc468957846005c5e104bc705cd813d5e62e49bbaf742960e1"
  - reporting.adpContentFingerprintSha256: null → "32c476b4ceba8e5c1a19651986b4b0eafa5f8eb997efa1c2fb6688a6a2f45448"
- **Regeneration:** ok=true auditRunId=AVA-RUN-YOTEL_GENEVA_LAKE-20261106T111017Z-f1c3cd62 pdfSha=`26e53b7c0270`

## Phase 3 — Historical current-period eligibility matrix (17)

Do **not** force these into CERTIFIED_MATCH. Newer certified customer ADP packages exist for all 17; AVA lean pins remain accurate historical references until an authorized lean re-pin + payload rebind is explicitly approved.

| Hotel | AVA lean period | Selected customer period | AVA cons. | Selected cons. | Lean file for selected? | Eligible without paid research? | Recommendation |
|---|---|---|---:|---:|---|---|---|
| AC Hotel A Coruña | `9220742_472e0c` | `5210617_3513a7` | 34.4 | 38 | no | yes* | PRESERVE_CERTIFIED_HISTORICAL_REFERENCE_UNTIL_LEAN_BASELINE_FOR_SELECTED_PERIOD_EXISTS |
| Bethesda Marriott | `2142154_62428a` | `5213117_f695d6` | 30.1 | 24 | no | yes* | PRESERVE_CERTIFIED_HISTORICAL_REFERENCE_UNTIL_LEAN_BASELINE_FOR_SELECTED_PERIOD_EXISTS |
| Cambridge Beaches Resort & Spa | `3010829_044437` | `5215424_e93d82` | 78.6 | 89.1 | no | yes* | PRESERVE_CERTIFIED_HISTORICAL_REFERENCE_UNTIL_LEAN_BASELINE_FOR_SELECTED_PERIOD_EXISTS |
| Charlotte Marriott City Center | `9071607_f9f0ee` | `5221934_1831f7` | 12.5 | 14.6 | no | yes* | PRESERVE_CERTIFIED_HISTORICAL_REFERENCE_UNTIL_LEAN_BASELINE_FOR_SELECTED_PERIOD_EXISTS |
| Falls Church Marriott Fairview Park | `9084033_55523d` | `5224238_bec296` | 39.1 | 41.1 | no | yes* | PRESERVE_CERTIFIED_HISTORICAL_REFERENCE_UNTIL_LEAN_BASELINE_FOR_SELECTED_PERIOD_EXISTS |
| Hotel Phillips Kansas City, Curio Collection by Hilton | `3003324_88084d` | `5230600_34436a` | 32.1 | 28.1 | no | yes* | PRESERVE_CERTIFIED_HISTORICAL_REFERENCE_UNTIL_LEAN_BASELINE_FOR_SELECTED_PERIOD_EXISTS |
| The Mayflower Hotel, Autograph Collection | `3213022_f9bdd0` | `5232853_43adfd` | 23.4 | 28.6 | no | yes* | PRESERVE_CERTIFIED_HISTORICAL_REFERENCE_UNTIL_LEAN_BASELINE_FOR_SELECTED_PERIOD_EXISTS |
| Nobu Hotel London Portman Square | `7005955_8dde1d` | `5235337_a53216` | 19.6 | 22.9 | no | yes* | PRESERVE_CERTIFIED_HISTORICAL_REFERENCE_UNTIL_LEAN_BASELINE_FOR_SELECTED_PERIOD_EXISTS |
| NOW NOW NOHO | `3014340_520794` | `6001840_2ab7ce` | 3.2 | 6.8 | no | yes* | PRESERVE_CERTIFIED_HISTORICAL_REFERENCE_UNTIL_LEAN_BASELINE_FOR_SELECTED_PERIOD_EXISTS |
| Renaissance Arlington Capital View Hotel | `9084033_2acbb1` | `6004624_f01f1e` | 19 | 16.7 | no | yes* | PRESERVE_CERTIFIED_HISTORICAL_REFERENCE_UNTIL_LEAN_BASELINE_FOR_SELECTED_PERIOD_EXISTS |
| Renaissance New York Times Square Hotel | `2235427_7d74ca` | `6011025_c02f05` | 10.5 | 10.4 | no | yes* | PRESERVE_CERTIFIED_HISTORICAL_REFERENCE_UNTIL_LEAN_BASELINE_FOR_SELECTED_PERIOD_EXISTS |
| Tampa Airport Marriott | `9082302_7b632f` | `6013505_73d688` | 2.2 | 4.2 | no | yes* | PRESERVE_CERTIFIED_HISTORICAL_REFERENCE_UNTIL_LEAN_BASELINE_FOR_SELECTED_PERIOD_EXISTS |
| W Rome | `9224104_a23f6f` | `6015838_91e771` | 11.5 | 14.6 | no | yes* | PRESERVE_CERTIFIED_HISTORICAL_REFERENCE_UNTIL_LEAN_BASELINE_FOR_SELECTED_PERIOD_EXISTS |
| Waterstone Resort & Marina | `2231159_77334c` | `6022505_87beeb` | 53.5 | 42.2 | no | yes* | PRESERVE_CERTIFIED_HISTORICAL_REFERENCE_UNTIL_LEAN_BASELINE_FOR_SELECTED_PERIOD_EXISTS |
| The Westin Grand München | `7145836_73c25a` | `6024739_1c32c0` | 30.7 | 32.8 | no | yes* | PRESERVE_CERTIFIED_HISTORICAL_REFERENCE_UNTIL_LEAN_BASELINE_FOR_SELECTED_PERIOD_EXISTS |
| The Westin Tampa Waterside | `3205651_c27cab` | `6031239_d31715` | 38 | 45.3 | no | yes* | PRESERVE_CERTIFIED_HISTORICAL_REFERENCE_UNTIL_LEAN_BASELINE_FOR_SELECTED_PERIOD_EXISTS |
| Wyndham San Diego Bayside | `3221340_775a48` | `6033639_7b4ef4` | 13.6 | 14.6 | no | yes* | PRESERVE_CERTIFIED_HISTORICAL_REFERENCE_UNTIL_LEAN_BASELINE_FOR_SELECTED_PERIOD_EXISTS |

\*Eligible means certified customer metrics exist and could be projected without new paid research; it does **not** authorize automatic MATCH conversion in this wave.

## Phase 4 — Full portfolio certification

- Payload certification after reconciliation: {"CERTIFIED_MATCH":21,"CERTIFIED_HISTORICAL_REFERENCE":17,"RECONCILIATION_REQUIRED":0,"CERTIFICATION_BLOCKED":0}
- Regenerated this wave: 20 hotel(s); historical archive append-only (no overwrite)
- Admin Preview/Download SHA parity: 38/38
- Airtable sync: 20/20 regen ok (Airtable enabled unless runner disabled)

### Remaining blockers

- None among the former 20 same-period projection failures.

## Phase 5 — PR #147 release separation

### A. Permanent AVA infrastructure / methodology (code-only merge candidate)

Safe to evaluate as a separately authorized merge candidate **if** regression tests pass and the merge cannot silently activate uncertified customer reports (feature flags / certify-only paths remain gated).

- Permanent audit runner + Full E2E orchestration
- ADP↔AVA reference + cross-report certification contracts
- Cover title/subtitle contract (AI VISIBILITY & DIGITAL PRESENCE ASSESSMENT / AUDIT & ACTION PLAN)
- Airtable OA against existing ADP base (no new six-table mirror)
- Admin permanent-runs + SHA parity plumbing
- Archive immutability / audit-store versioning

### B. Certified AVA report / data activation (separate authorization)

Must stay out of a code-only merge until intended customer-delivery hotels reach CERTIFIED_MATCH (or an explicit HISTORICAL delivery policy is accepted).

- Payload KPI projections / package bindings
- Regenerated PDFs + archive manifests
- Airtable indexed customer-facing artifacts
- AI Admin Preview/Download activation

**Recommendation:** SPLIT PR #147: (A) code-only infrastructure/methodology may proceed to a separately authorized merge candidate after regression tests; (B) hold report/data activation until founder accepts CERTIFIED_MATCH subset and/or authorizes historical re-pins. STOP before merge/deploy/publish.

## Success / readiness verdict

- CERTIFIED_MATCH: **21**
- CERTIFIED_HISTORICAL_REFERENCE: **17**
- RECONCILIATION_REQUIRED: **0**
- CERTIFICATION_BLOCKED: **0**

- **Current-period customer readiness:** PARTIAL — 21 CERTIFIED_MATCH; 17 remain CERTIFIED_HISTORICAL_REFERENCE (not current-period MATCH)
- **Joint portfolio readiness:** SAME_PERIOD_PROJECTION_CLEAR — historical pins preserved; joint current-period delivery still requires historical re-pin authorization


## Post-verify (full 38)

- Certification recount: MATCH=38 · HISTORICAL=0 · RECON=0 · BLOCKED=0
- Admin store↔disk SHA parity: 38/38
- Cover title detected in PDF sample: 38/38
- Cover subtitle detected in PDF sample: 38/38
- Action Register / findings retained: 38/38

STOP — no merge, deploy, or customer publication performed.

