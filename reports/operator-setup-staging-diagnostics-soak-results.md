# Operator Setup Staging Diagnostics Soak Results

Generated: 2026-06-02T11:31:07.133Z

## 1. Run metadata

- Base URL: http://127.0.0.1:8097
- Profiles tested: 10
- Deal ID used (alignment/score): recIeGRZP21udmTnt
- Server log: C:\Users\joand\OneDrive\Documents\deal-capture-proxy\reports\.soak-server.log

## 2. Commit refs deployed

- `d5ff0d824acd417537c2f2bbb32b2ef0558faf9b`
- `bf8865b7f4e00a780090e8cf3915982699056fc0`
- `275136933beda9ed4de55ba44ce13d1eee762091`
- `d5ff0d824acd417537c2f2bbb32b2ef0558faf9b`

## 3. Environment variable snapshot

```json
{
  "NODE_ENV": "production",
  "OPERATOR_SETUP_WRITE_MODE": "canonical",
  "OPERATOR_SETUP_ALLOW_NON_CANONICAL_PROD": "0",
  "OPERATOR_EXPLORER_DIAGNOSTIC_ALLOW_MOCKS": "0",
  "OPERATOR_SETUP_USE_NEW_BASE_WRITER": "1",
  "OPERATOR_SETUP_NEW_BASE_SHADOW_WRITE": "0",
  "OPERATOR_SETUP_NEW_BASE_FAIL_OPEN": "0",
  "OPERATOR_SETUP_CONTRACT_DIAGNOSTICS": "1"
}
```

## 4. Profiles tested

- **recZKRDG8eH2e9Tiy** — Soak Newly Created; cohorts: newly_created, existing_new_base, sparse_missing_fields, without_logo; prefill keys: 3; logo: false; readPath: new_base
- **recTUjuDxL96yWcQA** — Antillano Norte Hospitality Group; cohorts: existing_new_base, fuller_profile, with_logo; prefill keys: 532; logo: true; readPath: new_base
- **recq3NiRxOerg4kZU** — Barrio Hotelero CDMX; cohorts: existing_new_base, fuller_profile, with_logo; prefill keys: 529; logo: true; readPath: new_base
- **rec0l0XFLG3pKTtu4** — Batch12 Fail Closed Runtime 1780388676333; cohorts: existing_new_base, sparse_missing_fields, without_logo; prefill keys: 2; logo: false; readPath: new_base
- **recfQ1w5OR0a5Digr** — Batch3A Validation 1780395424427 Canonical Logo; cohorts: existing_new_base, with_logo; prefill keys: 12; logo: true; readPath: new_base
- **recyHakOliyjuJGKT** — Batch12 Runtime Validation 1780388615022; cohorts: existing_new_base, sparse_missing_fields, without_logo; prefill keys: 5; logo: false; readPath: new_base
- **recWynAfAXb6aznyb** — Batch3A Validation 1780395125334; cohorts: existing_new_base, sparse_missing_fields, without_logo; prefill keys: 11; logo: false; readPath: new_base
- **rectBocAs5gVyKD7c** — Batch3A Validation 1780395297048; cohorts: existing_new_base, sparse_missing_fields, without_logo; prefill keys: 1; logo: false; readPath: new_base
- **recPIkCoZi9FRs8h5** — Batch3A Validation 1780395424427; cohorts: existing_new_base, sparse_missing_fields, without_logo; prefill keys: 11; logo: false; readPath: new_base
- **recP78Z4fiObIhmCM** — Batch3A Validation 1780395479581; cohorts: existing_new_base, sparse_missing_fields, without_logo; prefill keys: 11; logo: false; readPath: new_base

## 5. Flows executed

### recZKRDG8eH2e9Tiy
- my_operator_create: **PASS**
- my_operator_reload_prefill_1: **PASS**
- my_operator_update: **PASS**
- my_operator_reload_prefill_2: **PASS**
- operator_explorer_list: **PASS**
- operator_explorer_detail: **PASS**
- non_rec_mock_blocking: **PASS**
- operator_alignment_snapshot: **FAIL** (module-layer (HTTP auth not used))
- operator_alignment_score_breakdown: **PASS** (module-layer score stability)
- operator_capability_snapshot_regression: **PASS** (export present; full HTTP requires my-deals auth)

### recTUjuDxL96yWcQA
- my_operator_reload_prefill_1: **PASS**
- my_operator_update: **PASS**
- my_operator_reload_prefill_2: **PASS**
- operator_explorer_list: **PASS**
- operator_explorer_detail: **PASS**
- non_rec_mock_blocking: **PASS**
- operator_alignment_snapshot: **FAIL** (module-layer (HTTP auth not used))
- operator_alignment_score_breakdown: **PASS** (module-layer score stability)
- operator_capability_snapshot_regression: **PASS** (export present; full HTTP requires my-deals auth)

### recq3NiRxOerg4kZU
- my_operator_reload_prefill_1: **PASS**
- my_operator_update: **PASS**
- my_operator_reload_prefill_2: **PASS**
- operator_explorer_list: **PASS**
- operator_explorer_detail: **PASS**
- non_rec_mock_blocking: **PASS**
- operator_alignment_snapshot: **FAIL** (module-layer (HTTP auth not used))
- operator_alignment_score_breakdown: **PASS** (module-layer score stability)
- operator_capability_snapshot_regression: **PASS** (export present; full HTTP requires my-deals auth)

### rec0l0XFLG3pKTtu4
- my_operator_reload_prefill_1: **PASS**
- my_operator_update: **PASS**
- my_operator_reload_prefill_2: **PASS**
- operator_explorer_list: **PASS**
- operator_explorer_detail: **PASS**
- non_rec_mock_blocking: **PASS**
- operator_alignment_snapshot: **FAIL** (module-layer (HTTP auth not used))
- operator_alignment_score_breakdown: **PASS** (module-layer score stability)
- operator_capability_snapshot_regression: **PASS** (export present; full HTTP requires my-deals auth)

### recfQ1w5OR0a5Digr
- my_operator_reload_prefill_1: **PASS**
- my_operator_update: **PASS**
- my_operator_reload_prefill_2: **PASS**
- operator_explorer_list: **PASS**
- operator_explorer_detail: **PASS**
- non_rec_mock_blocking: **PASS**
- operator_alignment_snapshot: **FAIL** (module-layer (HTTP auth not used))
- operator_alignment_score_breakdown: **PASS** (module-layer score stability)
- operator_capability_snapshot_regression: **PASS** (export present; full HTTP requires my-deals auth)

### recyHakOliyjuJGKT
- my_operator_reload_prefill_1: **PASS**
- my_operator_update: **PASS**
- my_operator_reload_prefill_2: **PASS**
- operator_explorer_list: **PASS**
- operator_explorer_detail: **PASS**
- non_rec_mock_blocking: **PASS**
- operator_alignment_snapshot: **FAIL** (module-layer (HTTP auth not used))
- operator_alignment_score_breakdown: **PASS** (module-layer score stability)
- operator_capability_snapshot_regression: **PASS** (export present; full HTTP requires my-deals auth)

### recWynAfAXb6aznyb
- my_operator_reload_prefill_1: **PASS**
- my_operator_update: **PASS**
- my_operator_reload_prefill_2: **PASS**
- operator_explorer_list: **PASS**
- operator_explorer_detail: **PASS**
- non_rec_mock_blocking: **PASS**
- operator_alignment_snapshot: **FAIL** (module-layer (HTTP auth not used))
- operator_alignment_score_breakdown: **PASS** (module-layer score stability)
- operator_capability_snapshot_regression: **PASS** (export present; full HTTP requires my-deals auth)

### rectBocAs5gVyKD7c
- my_operator_reload_prefill_1: **PASS**
- my_operator_update: **PASS**
- my_operator_reload_prefill_2: **PASS**
- operator_explorer_list: **PASS**
- operator_explorer_detail: **PASS**
- non_rec_mock_blocking: **PASS**
- operator_alignment_snapshot: **FAIL** (module-layer (HTTP auth not used))
- operator_alignment_score_breakdown: **PASS** (module-layer score stability)
- operator_capability_snapshot_regression: **PASS** (export present; full HTTP requires my-deals auth)

### recPIkCoZi9FRs8h5
- my_operator_reload_prefill_1: **PASS**
- my_operator_update: **PASS**
- my_operator_reload_prefill_2: **PASS**
- operator_explorer_list: **PASS**
- operator_explorer_detail: **PASS**
- non_rec_mock_blocking: **PASS**
- operator_alignment_snapshot: **FAIL** (module-layer (HTTP auth not used))
- operator_alignment_score_breakdown: **PASS** (module-layer score stability)
- operator_capability_snapshot_regression: **PASS** (export present; full HTTP requires my-deals auth)

### recP78Z4fiObIhmCM
- my_operator_reload_prefill_1: **PASS**
- my_operator_update: **PASS**
- my_operator_reload_prefill_2: **PASS**
- operator_explorer_list: **PASS**
- operator_explorer_detail: **PASS**
- non_rec_mock_blocking: **PASS**
- operator_alignment_snapshot: **FAIL** (module-layer (HTTP auth not used))
- operator_alignment_score_breakdown: **PASS** (module-layer score stability)
- operator_capability_snapshot_regression: **PASS** (export present; full HTTP requires my-deals auth)

## 6. Pass/fail counts

- Pass: 81
- Fail: 10
- Total checks: 91

## 7. Determinism checks

- Canonical write mode only: PASS
- No shadow/fail-open observed: PASS

## 8. Mock-blocking verification

- Non-rec blocked: PASS
- Mock leaks detected: NO

## 9. Canonical write verification

- Updates canonical: PASS

## 10. Read-path verification

- new_base profiles: 10
- legacy profiles: 0

## 11. Diagnostics findings

- Server/module diagnostics captured: 56
- Log rows written: 108

## 12. Fallback-used counts by field/concept

```json
{
  "companyName": 0,
  "serviceModelsSupported": 13,
  "chainScalesSupported": 0,
  "activeCountries": 0,
  "activeMarkets": 0,
  "specificMarkets": 0,
  "marketPresenceType": 0,
  "managementStructuresSupported": 1,
  "offeredServices": 0,
  "newBuildOpeningExperience": 0,
  "preOpeningSupportCapability": 0,
  "ownerReportingLevel": 0,
  "dataConfidenceLevel": 0,
  "sourceType": 0,
  "lastUpdatedDate": 0,
  "brandFamiliesOperated": 0,
  "conversionReflagExperience": 0,
  "softBrandLifestyleExperience": 0,
  "fbCapabilityLevel": 0,
  "revenueManagementCapability": 0,
  "salesPlatform": 0,
  "governanceCadence": 0,
  "minimumKeyCount": 0,
  "leadership_child_mapping": 0
}
```

## 13. Unresolved-field counts by field/concept

```json
{
  "companyName": 0,
  "serviceModelsSupported": 1,
  "chainScalesSupported": 0,
  "activeCountries": 1,
  "activeMarkets": 1,
  "specificMarkets": 0,
  "marketPresenceType": 1,
  "managementStructuresSupported": 1,
  "offeredServices": 1,
  "newBuildOpeningExperience": 1,
  "preOpeningSupportCapability": 1,
  "ownerReportingLevel": 1,
  "dataConfidenceLevel": 1,
  "sourceType": 1,
  "lastUpdatedDate": 1,
  "brandFamiliesOperated": 1,
  "conversionReflagExperience": 1,
  "softBrandLifestyleExperience": 1,
  "fbCapabilityLevel": 1,
  "revenueManagementCapability": 1,
  "salesPlatform": 1,
  "governanceCadence": 1,
  "minimumKeyCount": 1,
  "leadership_child_mapping": 0
}
```

## 14. Mirror masking findings

_None captured in API/module soak (browser-only path)._

## 15. Legacy read-path findings

_None in selected profile set._

## 16. Finding classification table

```json
{
  "fallback_acceptable_legacy_only": 57,
  "confirmed_canonical_mapping_miss": 14,
  "missing_data_issue": 20,
  "deferred_cleanup": 17
}
```

## 17. P0/P1/P2 recommendations

- **P0:** None
- **P1 (sample):** serviceModelsSupported:primaryServiceModel; serviceModelsSupported:primaryServiceModel; serviceModelsSupported:primaryServiceModel; serviceModelsSupported:primaryServiceModel; serviceModelsSupported:primaryServiceModel; serviceModelsSupported:primaryServiceModel; serviceModelsSupported:primaryServiceModel; serviceModelsSupported:primaryServiceModel; serviceModelsSupported:primaryServiceModel; serviceModelsSupported:primaryServiceModel; serviceModelsSupported:primaryServiceModel; serviceModelsSupported:primaryServiceModel; serviceModelsSupported:primaryServiceModel; managementStructuresSupported:bf_selected_deal_structures
- **P2:** legacy alias documentation; browser-only mirror diagnostics manual pass

## 18. Batch 3C candidate list

- serviceModelsSupported: expected `serviceModelsSupported`, used `primaryServiceModel` (explorer_read_key_resolution_inferred)
- managementStructuresSupported: expected `managementStructuresSupported`, used `bf_selected_deal_structures` (alignment_prefill_alias_resolution)

## 19. Deferred/non-actionable items

- companyName: legacy_acceptable
- serviceModelsSupported: legacy_acceptable
- chainScalesSupported: legacy_acceptable
- activeCountries: legacy_acceptable
- activeMarkets: legacy_acceptable
- specificMarkets: legacy_acceptable
- marketPresenceType: legacy_acceptable
- managementStructuresSupported: legacy_acceptable
- offeredServices: legacy_acceptable
- newBuildOpeningExperience: legacy_acceptable

## 20. Risk assessment

Low-Medium — core Operator Setup paths are stable. Remaining work is targeted Batch 3C contract closure and deal-linked OAS manual QA.

## Soak interpretation (analyst notes)

- **Stop conditions:** none triggered.
- **Core flows:** My Operator create/update/reload, Explorer list/detail, non-rec mock blocking, canonical `writeMode`, and `readPath=new_base` all passed across 10 profiles.
- **10 flow failures:** all are `operator_alignment_snapshot` module checks where the operator was **not** in `companiesForConsideration` for deal `recIeGRZP21udmTnt`. Treat as soak fixture/deal-cohort gap, not a product regression.
- **Score stability:** duplicate module-layer snapshot builds produced stable scores (pass).
- **Mirror diagnostics:** not captured in API/module soak; run a short browser pass with diagnostics enabled in `localStorage`.
- **Deal-linked retest:** repeat OAS/OCS with `SOAK_DEAL_ID` + operator on that deal (e.g. `recBVEgtm8cS96mu7`).

## 21. Decision recommendation

| Gate | Status |
|------|--------|
| Internal QA | **Ready** |
| External demo | **Ready** (with Batch 3C P1 items + deal-linked OAS pass) |
| Go-live | **Not ready** |
| Overall | **Proceed to internal QA; hold go-live pending Batch 3C** |

## Stop conditions triggered

_None_

---

Full machine-readable output: `reports/operator-setup-staging-diagnostics-soak-results.json`  
Log capture CSV: `reports/operator-setup-staging-diagnostics-log-capture.csv`
