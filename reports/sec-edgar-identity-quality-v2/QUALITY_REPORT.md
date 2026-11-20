# SEC EDGAR Identity Quality Report V2

**Generated:** 2026-11-20T06:54:49.391Z

## Before (baseline HOTEL_INVENTORY)
- Inventory rows: **290**
- Distinct nonempty name strings: **228**
- **Do not promote these as verified unique hotels.**

## After classification
| Class | Count |
|---|---|
| VERIFIED_PROPERTY | 18 |
| PROBABLE_PROPERTY | 71 |
| AMBIGUOUS_PROPERTY | 1124 |
| MULTIPLE_PROPERTIES | 21 |
| GENERIC_BRAND_OR_COMPANY | 178 |
| EXTRACTION_ARTIFACT | 200 |
| DUPLICATE_ALIAS | 0 |

- **VERIFIED_PROPERTY:** 18
- **PROBABLE_PROPERTY:** 71
- Precision gate (deterministic sample): {"sampleSize":18,"correct":18,"precision":1,"meetsGate":true,"gateThreshold":0.95}

## Before/after reconciliation
228 distinct name strings → 18 VERIFIED_PROPERTY (promotion gate); 71 PROBABLE pending

## Artifacts
- `RAW_RESEARCH_EVIDENCE_INVENTORY.json` — all mentions + rejected
- `VERIFIED_HOTEL_INTELLIGENCE_INVENTORY.json/.csv` — promotion-gated only
- `CLASSIFIED_ALL_CANDIDATES.csv`
