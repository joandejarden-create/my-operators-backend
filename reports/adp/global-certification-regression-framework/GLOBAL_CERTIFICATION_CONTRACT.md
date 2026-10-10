# Global ADP Certification Contract

Version: adp_global_certification_engine_v1

## Law
A surprising result is allowed. An unexplained result is not certifiable.

## Flow
```
runAdpMonitoring()
→ persistDraftPeriod()
→ runAdpIdentityPreflight()
→ (provider execution)
→ certifyAdpPeriod()
→ CERTIFIED / QA_REVIEW_REQUIRED / QA_FAILED
→ publishExistingHotelAdpSnapshot()  // blocks unless CERTIFIED
```

## States
DRAFT · RUNNING · QA_FAILED · QA_REVIEW_REQUIRED · QA_PASSED · CERTIFIED · SUPERSEDED · LEGACY_UNCERTIFIED

Only CERTIFIED is customer-official by default. Legacy periods without engine stamps are LEGACY_UNCERTIFIED (not auto-promoted).

## Engine
`lib/ai-demand-positioning/certification/certify-adp-period-v1.js` → `certifyAdpPeriod`

## Publish gate
`savePublishedSnapshotBundle` + `publishExistingHotelAdpSnapshot` require certification when `officialCustomerPublish` or `ADP_REQUIRE_CERTIFICATION_BEFORE_PUBLISH=1`.

## No metric forcing
QA may correct identity, parsing, attribution, denominators, or rerun — then recompute. QA must not force metrics toward historical expectations.
