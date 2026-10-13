# Changelog — Global ADP Certification + Regression Framework

## 2026-10-05 — Framework v1

- Added global `certifyAdpPeriod` engine + certification manifests
- Period pipeline states: DRAFT → … → CERTIFIED / SUPERSEDED / LEGACY_UNCERTIFIED
- Identity contract + canary + pre-flight on shared monitoring path
- Scenario universe contract + versioning stamps on monitoring periods
- Formal `evaluateAdpComparability` + customer comparison gate
- Provider completeness + zero-presence forensic
- Raw metric recompute + denominator grain audit
- Source attribution taxonomy + domain ownership registry
- Anomaly rules + explainAdpAnomaly
- Period immutability + correction linkage helpers
- Wired `publishExistingHotelAdpSnapshot` / `savePublishedSnapshotBundle` certification gate
- Customer read blocks explicit uncertified statuses
- Permanent regression control set + inventory audit pack

### Explicit non-changes
- ADP methodology: unchanged
- ADP thresholds: unchanged
- Old certified periods: not overwritten
- Metrics: not forced to expected values
