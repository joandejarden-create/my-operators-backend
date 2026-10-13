# Changelog — Global Certification Remediation Audit

## Deterministic fixes applied
1. Softened `LIVE_SCENARIO_UNIVERSE_DRIFT_VS_PERIOD` to warning for legacy / matched-control periods
2. Added `identityAliases` to W Rome, Cambridge Beaches, NOW NOW NOHO, Waterstone
3. Normalized YOTEL Geneva Lake profile for identity contract (`name` + aliases + domains)
4. Stamped Hilton + Renaissance matched-control periods + published manifests with CERTIFIED + `globalCertificationEngineVersion` (metadata only; observations untouched)

## Explicit non-changes
- ADP methodology: unchanged
- ADP thresholds: unchanged
- Observation corpora: not overwritten
- Metrics: not forced
- Legacy periods: not silently certified (except matched-control pair already certified + stamped)
