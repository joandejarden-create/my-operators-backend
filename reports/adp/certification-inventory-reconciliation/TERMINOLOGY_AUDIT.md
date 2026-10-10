# Terminology Audit

| Prior label | Actually meant | Normalized |
|-------------|----------------|------------|
| CERTIFIED/PASS COUNT | Certification-era CERTIFIED periods | CERTIFICATION-ERA CERTIFIED PERIOD COUNT |
| LEGACY_ONLY | Legacy official current period | CURRENT_PERIOD_CLASS=LEGACY_OFFICIAL |
| FINAL STATUS: CERTIFIED (Cambridge/Caribe/NOW NOW) | engineWouldCertify / Property QA PASS | Property QA: PASS; Period: LEGACY_QA_PASS |
| CERTIFIED CONTROL STILL PASS (Bethesda) | Property QA PASS on legacy period | Property QA: PASS; Period: LEGACY_QA_PASS; Class: LEGACY_OFFICIAL |
| HILTON CERTIFIED | Period CERTIFIED + era | Period CERTIFICATION: CERTIFIED; Class: CERTIFICATION_ERA_OFFICIAL |

## Fix applied
- Status dimensions module + inventory builder
- This pack never emits `CERTIFIED/PASS`
- Prior pack script `run-adp-final-review-cleanup-yotel-pack-v1.mjs` founder table updated to separate fields
